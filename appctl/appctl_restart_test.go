// SPDX-License-Identifier: Apache-2.0

package appctl

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/openstack-k8s-operators/openstack-network-exporter/config"
)

// startDaemon answers unixctl JSON-RPC calls on path with a fixed reply.
// Closing the returned listener keeps the socket file, as a daemon killed
// with SIGKILL does; a clean exit is simulated by removing the file too.
func startDaemon(t *testing.T, path, reply string) *net.UnixListener {
	t.Helper()
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	ln.SetUnlinkOnClose(false)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				dec, enc := json.NewDecoder(c), json.NewEncoder(c)
				for {
					var req struct {
						Id uint64 `json:"id"`
					}
					if dec.Decode(&req) != nil {
						return
					}
					_ = enc.Encode(map[string]any{"id": req.Id, "result": reply, "error": nil})
				}
			}(conn)
		}
	}()
	t.Cleanup(func() { ln.Close() })
	return ln
}

func setRundir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for _, v := range []string{"OVS_RUNDIR", "OVN_RUNDIR", "OVSDB_RUNDIR"} {
		t.Setenv("OPENSTACK_NETWORK_EXPORTER_"+v, dir)
	}
	cfg := filepath.Join(dir, "exporter.yaml")
	if err := os.WriteFile(cfg, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OPENSTACK_NETWORK_EXPORTER_YAML", cfg)
	if err := config.Parse(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func writePid(t *testing.T, dir, daemon, pid string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, daemon+".pid"), []byte(pid+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

var pidDaemons = []struct {
	name string
	call func(string, ...string) string
}{
	{"ovn-northd", OvnNorthd},
	{"ovn-controller", OvnController},
	{"ovs-vswitchd", OvsVSwitchd},
}

// A daemon exits cleanly (pidfile and socket removed) and starts again with
// another pid: the next call reaches the new socket.
func TestCallFollowsCleanRestart(t *testing.T) {
	for _, d := range pidDaemons {
		t.Run(d.name, func(t *testing.T) {
			dir := setRundir(t)
			old := filepath.Join(dir, d.name+".100.ctl")
			ln := startDaemon(t, old, "first")
			writePid(t, dir, d.name, "100")
			if got := d.call("coverage/show"); got != "first" {
				t.Fatalf("before restart: got %q", got)
			}

			ln.Close()
			os.Remove(old)
			os.Remove(filepath.Join(dir, d.name+".pid"))
			if got := d.call("coverage/show"); got != "" {
				t.Fatalf("while stopped: got %q", got)
			}

			startDaemon(t, filepath.Join(dir, d.name+".200.ctl"), "second")
			writePid(t, dir, d.name, "200")
			if got := d.call("coverage/show"); got != "second" {
				t.Fatalf("after restart: got %q", got)
			}
		})
	}
}

// A daemon is killed: its pidfile and socket stay behind until the new
// process overwrites the pidfile. Calls fail in between, then reach the new
// socket although the stale one is still there.
func TestCallIgnoresStaleSocketOnceThePidfileIsRewritten(t *testing.T) {
	for _, d := range pidDaemons {
		t.Run(d.name, func(t *testing.T) {
			dir := setRundir(t)
			ln := startDaemon(t, filepath.Join(dir, d.name+".100.ctl"), "first")
			writePid(t, dir, d.name, "100")
			if got := d.call("coverage/show"); got != "first" {
				t.Fatalf("before kill: got %q", got)
			}

			ln.Close()
			if got := d.call("coverage/show"); got != "" {
				t.Fatalf("after kill, stale pidfile: got %q", got)
			}

			startDaemon(t, filepath.Join(dir, d.name+".200.ctl"), "second")
			writePid(t, dir, d.name, "200")
			if got := d.call("coverage/show"); got != "second" {
				t.Fatalf("after restart with stale socket present: got %q", got)
			}
		})
	}
}

// Without a pidfile the first *.ctl in lexical order is used, dead or not.
// This records current behaviour: ovnkube.sh deletes the pidfile and the old
// sockets before it starts the daemon, so both are never missing/stale at once.
func TestCallWithoutPidfileTakesFirstSocketInLexicalOrder(t *testing.T) {
	dir := setRundir(t)
	dead := startDaemon(t, filepath.Join(dir, "ovn-northd.100.ctl"), "dead")
	dead.Close()
	startDaemon(t, filepath.Join(dir, "ovn-northd.200.ctl"), "alive")
	if got := OvnNorthd("status"); got != "" {
		t.Fatalf("stale ovn-northd.100.ctl sorts first, want the call to fail, got %q", got)
	}

	os.Remove(filepath.Join(dir, "ovn-northd.100.ctl"))
	if got := OvnNorthd("status"); got != "alive" {
		t.Fatalf("only the live socket left: got %q", got)
	}
}

// The raft ovsdb-server sockets have fixed names: a restart recreates the
// file at the same path and the next call reaches it.
func TestCallDbServerFollowsRecreatedSocket(t *testing.T) {
	for _, name := range []string{"ovnnb_db.ctl", "ovnsb_db.ctl"} {
		t.Run(name, func(t *testing.T) {
			dir := setRundir(t)
			path := filepath.Join(dir, name)
			ln := startDaemon(t, path, "first")
			if got := OvsDbServer("cluster/status"); got != "first" {
				t.Fatalf("before restart: got %q", got)
			}

			ln.Close()
			if got := OvsDbServer("cluster/status"); got != "" {
				t.Fatalf("killed, stale socket: got %q", got)
			}
			os.Remove(path)
			if got := OvsDbServer("cluster/status"); got != "" {
				t.Fatalf("stopped, no socket: got %q", got)
			}

			startDaemon(t, path, "second")
			if got := OvsDbServer("cluster/status"); got != "second" {
				t.Fatalf("after restart: got %q", got)
			}
		})
	}
}
