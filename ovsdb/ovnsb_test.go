// SPDX-License-Identifier: Apache-2.0

package ovsdb

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/openstack-k8s-operators/openstack-network-exporter/config"
	"github.com/openstack-k8s-operators/openstack-network-exporter/ovsdb/ovnsb"
	"github.com/ovn-kubernetes/libovsdb/database/inmemory"
	"github.com/ovn-kubernetes/libovsdb/model"
	"github.com/ovn-kubernetes/libovsdb/server"
)

// startSBServer runs an in-memory OVN_Southbound database on a unix socket.
func startSBServer(t *testing.T, path string) *server.OvsdbServer {
	t.Helper()
	clientModel, err := ovnsb.FullDatabaseModel()
	if err != nil {
		t.Fatal(err)
	}
	logger := logr.Discard()
	db := inmemory.NewDatabase(map[string]model.ClientDBModel{"OVN_Southbound": clientModel}, &logger)
	dbModel, errs := model.NewDatabaseModel(ovnsb.Schema(), clientModel)
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	srv, err := server.NewOvsdbServer(db, &logger, dbModel)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve("unix", path) }()
	waitFor(t, 2*time.Second, "the SB server", srv.Ready)
	return srv
}

func listDatapaths() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var datapaths []ovnsb.DatapathBinding
	return SBList(ctx, &datapaths)
}

// The SB client is cached like the OVSDB one: a restart or leader change of
// the SB database must not leave it dead.
func TestSBReconnectAfterServerRestart(t *testing.T) {
	dir := t.TempDir()
	backend := filepath.Join(dir, "backend.sock")
	srv := startSBServer(t, backend)
	defer srv.Close()

	sock := filepath.Join(dir, "sb.sock")
	t.Setenv("OPENSTACK_NETWORK_EXPORTER_OVN_SB_CONNECTION", "unix:"+sock)
	if err := config.Parse(); err != nil {
		t.Fatal(err)
	}
	proxy := startProxy(t, sock, backend)
	if err := listDatapaths(); err != nil {
		t.Fatalf("first call: %v", err)
	}
	before := reconnects(t)

	proxy.stop()
	waitFor(t, 5*time.Second, "calls to fail while the SB server is down", func() bool { return listDatapaths() != nil })

	proxy = startProxy(t, sock, backend)
	defer proxy.stop()
	waitFor(t, 5*time.Second, "calls to succeed after the SB server is back", func() bool { return listDatapaths() == nil })

	if got := reconnects(t) - before; got < 1 {
		t.Fatalf("ovsdb reconnects counter increased by %v, want at least 1", got)
	}
}
