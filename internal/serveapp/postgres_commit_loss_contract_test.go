package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

// PostgreSQL wire fault injection belongs only to this disposable proof. The
// application still selects unmodified pq and its ordinary TCP connections.
func contractLostCommitProxy(t *testing.T, target, marker string) (string, <-chan struct{}, *atomic.Int32) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lost := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	var armedCommits atomic.Int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer client.Close()
				up, err := net.DialTimeout("tcp", target, time.Second)
				if err != nil {
					return
				}
				defer up.Close()
				mu.Lock()
				conns = append(conns, client, up)
				mu.Unlock()
				var size [4]byte
				if _, err = io.ReadFull(client, size[:]); err != nil {
					return
				}
				n := int(binary.BigEndian.Uint32(size[:]))
				if n < 8 || n > 1<<20 {
					return
				}
				startup := make([]byte, n)
				copy(startup, size[:])
				if _, err = io.ReadFull(client, startup[4:]); err != nil {
					return
				}
				if _, err = up.Write(startup); err != nil {
					return
				}
				var armed atomic.Bool
				frontDone := make(chan struct{})
				go func() {
					defer close(frontDone)
					defer up.Close()
					for {
						frame, err := contractPGFrame(client)
						if err != nil {
							return
						}
						if bytes.Contains(frame, []byte(marker)) {
							armed.Store(true)
						}
						if _, err = up.Write(frame); err != nil {
							return
						}
					}
				}()
				defer func() { _ = client.Close(); _ = up.Close(); <-frontDone }()
				for {
					frame, err := contractPGFrame(up)
					if err != nil {
						return
					}
					if armed.Load() && frame[0] == 'C' && bytes.Equal(frame[5:], []byte("COMMIT\x00")) {
						armedCommits.Add(1)
						drop := false
						once.Do(func() { drop = true; close(lost) })
						if drop {
							return
						}
						armed.Store(false)
					}
					if _, err = client.Write(frame); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return l.Addr().String(), lost, &armedCommits
}

func contractPGFrame(r io.Reader) ([]byte, error) {
	var h [5]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(h[1:]))
	if n < 4 || n > 32<<20 {
		return nil, fmt.Errorf("invalid postgres frame %d", n)
	}
	f := make([]byte, n+1)
	copy(f, h[:])
	_, err := io.ReadFull(r, f[5:])
	return f, err
}

func TestServePostgresLostCommitResponseRecoversDurablePublication(t *testing.T) {
	server := newContractPostgresServer(t)
	observer, err := sql.Open("postgres", server.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	var port string
	if _, err := fmt.Sscanf(server.options, "-h 127.0.0.1 -p %s", &port); err != nil {
		t.Fatal(err)
	}
	address, lost, commits := contractLostCommitProxy(t, net.JoinHostPort("127.0.0.1", port), "2444-commit-cut")
	host, proxyPort, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	dsn := fmt.Sprintf("host=%s port=%s user=swarm_probe dbname=postgres sslmode=disable connect_timeout=2", host, proxyPort)
	stubServeRuntimeWorkspaceLifecycle(t)
	oldBuild := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: storebackend.Selection{Backend: storebackend.BackendPostgres}, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})
	}
	t.Cleanup(func() { buildStoresForServe = oldBuild })
	root := writeServedEventPublishFollowUpFixture(t)
	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	if err := os.WriteFile(configPath, []byte("runtime:\n  recovery_on_startup: true\nworkspace: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	opts := cliapp.ServeOptions{ConfigPath: configPath, SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, StoreMode: "postgres", StoreModeSet: true, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, ShutdownGrace: time.Second}
	first := startServeRuntimeTestProcess(t, opts)
	first.waitForReadyLine()
	endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, first.outputString()) + "/v1/rpc"
	params := map[string]any{"event_name": "item.received", "bundle_hash": servedEventPublishFixtureBundleHash(t, root), "payload": map[string]any{"item_id": "2444-commit-cut"}, "idempotency_key": "lost-response-publication"}
	response := requestServedJSONRPC(t, endpoint, "event.publish", params)
	select {
	case <-lost:
	case <-time.After(5 * time.Second):
		t.Fatal("did not drop real COMMIT response")
	}
	if response.Error == nil {
		t.Fatalf("unacknowledged COMMIT claimed successful publication: %s", response.Result)
	}
	if commits.Load() != 1 {
		t.Fatalf("uncertain commit retried callback: commits=%d", commits.Load())
	}
	var runID, eventID string
	if err := observer.QueryRow(`SELECT run_id::text,event_id::text FROM events WHERE event_name='item.received'`).Scan(&runID, &eventID); err != nil {
		t.Fatalf("COMMIT acknowledgement lost but durable event missing: %v", err)
	}
	t.Logf("actual server COMMIT completion dropped: caller=%#v durable event=%s run=%s", response.Error, eventID, runID)
	if code := first.stop(); code != 0 {
		t.Fatalf("stop after isolated publication failure: code=%d\n%s", code, first.outputString())
	}
	var deliveredBeforeRestart int
	if err := observer.QueryRow(`SELECT count(*) FROM event_deliveries WHERE event_id=$1 AND status='delivered'`, eventID).Scan(&deliveredBeforeRestart); err != nil {
		t.Fatal(err)
	}
	if deliveredBeforeRestart != 0 {
		t.Fatalf("cut reached delivery before restart: %d; cannot credit startup recovery", deliveredBeforeRestart)
	}
	restarted := startServeRuntimeTestProcess(t, opts)
	restarted.waitForReadyLine()
	nextEndpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, restarted.outputString()) + "/v1/rpc"
	entity := requireServedEventPublishEntityState(t, observer, "postgres", runID, "", "waiting")
	requireServedEntityReadback(t, nextEndpoint, runID, entity, "waiting")
	replay := requireServedEventPublishRPCResult(t, nextEndpoint, params)
	if replay.RunID != runID || replay.EventID != eventID {
		t.Fatalf("public idempotency replay lost exact durable identity: %#v", replay)
	}
	if n := servedEventPublishScalarCount(t, observer, "postgres", "application_events_by_run", runID, ""); n != 1 {
		t.Fatalf("durable publication count=%d want 1", n)
	}
	var deliveries, outcomes int
	if err := observer.QueryRow(`SELECT count(*) FROM event_deliveries WHERE event_id=$1 AND status='delivered'`, eventID).Scan(&deliveries); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRow(`SELECT count(*) FROM event_delivery_outcomes o JOIN event_deliveries d ON d.delivery_id=o.delivery_id WHERE d.event_id=$1 AND o.outcome='delivered'`, eventID).Scan(&outcomes); err != nil {
		t.Fatal(err)
	}
	if deliveries != 1 || outcomes != 1 {
		t.Fatalf("recovery delivery=%d outcomes=%d want exactly 1 each", deliveries, outcomes)
	}
	if code := restarted.stop(); code != 0 {
		t.Fatalf("restarted serve exit=%d\n%s", code, restarted.outputString())
	}
	t.Log("lost COMMIT response retained one event; actual serve recovery reached entity and delivered outcome; exact public replay did not redispatch")
}
