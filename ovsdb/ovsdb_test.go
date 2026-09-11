// SPDX-License-Identifier: Apache-2.0

package ovsdb

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/openstack-k8s-operators/openstack-network-exporter/config"
	"github.com/openstack-k8s-operators/openstack-network-exporter/ovsdb/ovs"
	"github.com/ovn-kubernetes/libovsdb/database/inmemory"
	"github.com/ovn-kubernetes/libovsdb/model"
	"github.com/ovn-kubernetes/libovsdb/server"
	dto "github.com/prometheus/client_model/go"
)

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// startServer runs an in-memory Open_vSwitch database on a unix socket.
func startServer(t *testing.T, path string) *server.OvsdbServer {
	t.Helper()
	clientModel, err := ovs.FullDatabaseModel()
	if err != nil {
		t.Fatal(err)
	}
	logger := logr.Discard()
	db := inmemory.NewDatabase(map[string]model.ClientDBModel{"Open_vSwitch": clientModel}, &logger)
	dbModel, errs := model.NewDatabaseModel(ovs.Schema(), clientModel)
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	srv, err := server.NewOvsdbServer(db, &logger, dbModel)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve("unix", path) }()
	waitFor(t, 2*time.Second, "the ovsdb server", srv.Ready)
	return srv
}

// socketProxy stands for ovsdb-server's db.sock. Stopping it closes every
// client connection and removes the socket file, as a restart of
// ovsdb-server does, while the database behind it keeps running.
type socketProxy struct {
	listener net.Listener
	mu       sync.Mutex
	conns    []net.Conn
}

func startProxy(t *testing.T, path, backend string) *socketProxy {
	t.Helper()
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	p := &socketProxy{listener: l}
	go func() {
		for {
			front, err := l.Accept()
			if err != nil {
				return
			}
			back, err := net.Dial("unix", backend)
			if err != nil {
				front.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, front, back)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(back, front); back.Close() }()
			go func() { _, _ = io.Copy(front, back); front.Close() }()
		}
	}()
	return p
}

func (p *socketProxy) stop() {
	p.listener.Close() // also unlinks the socket file
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
}

func listBridges() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var bridges []ovs.Bridge
	return List(ctx, &bridges)
}

func reconnects(t *testing.T) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := ReconnectsTotal.Write(m); err != nil {
		t.Fatal(err)
	}
	return m.GetCounter().GetValue()
}

// ovsdb-server restarts (openvswitch pod or container restart) must not leave
// the exporter with a dead client that fails every scrape.
func TestReconnectAfterServerRestart(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("OPENSTACK_NETWORK_EXPORTER_OVS_RUNDIR", dir)
	if err := config.Parse(); err != nil {
		t.Fatal(err)
	}
	backend := filepath.Join(dir, "backend.sock")
	srv := startServer(t, backend)
	defer srv.Close()

	sock := filepath.Join(dir, "db.sock")
	proxy := startProxy(t, sock, backend)
	if err := listBridges(); err != nil {
		t.Fatalf("first call: %v", err)
	}
	before := reconnects(t)

	proxy.stop()
	waitFor(t, 5*time.Second, "calls to fail while ovsdb is down", func() bool { return listBridges() != nil })

	proxy = startProxy(t, sock, backend)
	defer proxy.stop()
	waitFor(t, 5*time.Second, "calls to succeed after ovsdb is back", func() bool { return listBridges() == nil })

	if got := reconnects(t) - before; got < 1 {
		t.Fatalf("ovsdb reconnects counter increased by %v, want at least 1", got)
	}
}
