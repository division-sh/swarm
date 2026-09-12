package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
)

// Reuse the private-cluster and wire-frame fixtures. Only this test proxy knows
// protocol framing: stock pq and production construction are unchanged. Hold a
// reply from the exact process session without dropping bytes or closing TCP.
func contractSilentMonitorProxy(t *testing.T, target string) (string, *atomic.Int32, <-chan struct{}, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var targetPID atomic.Int32
	var pauseOnce, resumeOnce sync.Once
	paused, resume := make(chan struct{}), make(chan struct{})
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	var mu sync.Mutex
	var conns []net.Conn
	closing := false
	track := func(conn net.Conn) bool {
		mu.Lock()
		defer mu.Unlock()
		if closing {
			_ = conn.Close()
			return false
		}
		conns = append(conns, conn)
		return true
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			if !track(client) {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer client.Close()
				upstream, err := net.DialTimeout("tcp", target, time.Second)
				if err != nil {
					return
				}
				defer upstream.Close()
				if !track(upstream) {
					return
				}
				var size [4]byte
				if _, err := io.ReadFull(client, size[:]); err != nil {
					return
				}
				n := int(binary.BigEndian.Uint32(size[:]))
				if n < 8 || n > 1<<20 {
					return
				}
				startup := make([]byte, n)
				copy(startup, size[:])
				if _, err := io.ReadFull(client, startup[4:]); err != nil {
					return
				}
				if _, err := upstream.Write(startup); err != nil {
					return
				}
				var pid atomic.Int32
				var proof atomic.Bool
				frontDone := make(chan struct{})
				go func() {
					defer close(frontDone)
					defer upstream.Close()
					for {
						frame, err := contractPGFrame(client)
						if err != nil {
							return
						}
						if pid.Load() != 0 && pid.Load() == targetPID.Load() && bytes.Contains(frame, []byte("FROM pg_locks")) && bytes.Contains(frame, []byte("pid = pg_backend_pid()")) {
							proof.Store(true)
						}
						if _, err := upstream.Write(frame); err != nil {
							return
						}
					}
				}()
				defer func() { _ = client.Close(); _ = upstream.Close(); <-frontDone }()
				for {
					frame, err := contractPGFrame(upstream)
					if err != nil {
						return
					}
					if frame[0] == 'K' && len(frame) >= 13 {
						pid.Store(int32(binary.BigEndian.Uint32(frame[5:9])))
					}
					if proof.Load() {
						pauseOnce.Do(func() { close(paused); <-resume })
					}
					if _, err := client.Write(frame); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		release()
		_ = listener.Close()
		mu.Lock()
		closing = true
		for _, conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return listener.Addr().String(), &targetPID, paused, release
}

type silentMonitorWorkspace struct {
	serveRuntimeWorkspaceStub
	released *atomic.Int32
}

func (w silentMonitorWorkspace) ReleaseSourceProjection(ctx context.Context) error {
	w.released.Add(1)
	return w.serveRuntimeWorkspaceStub.ReleaseSourceProjection(ctx)
}

func TestServePostgresSilentMonitorWithdrawsReadinessAndJoins(t *testing.T) {
	server := newContractPostgresServer(t)
	var port string
	if _, err := fmt.Sscanf(server.options, "-h 127.0.0.1 -p %s", &port); err != nil {
		t.Fatal(err)
	}
	address, targetPID, paused, release := contractSilentMonitorProxy(t, net.JoinHostPort("127.0.0.1", port))
	host, proxyPort, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	routed := fmt.Sprintf("host=%s port=%s user=swarm_probe dbname=postgres sslmode=disable connect_timeout=2", host, proxyPort)
	observer, err := sql.Open("postgres", server.dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	var workspaceReleased atomic.Int32
	oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
	cliapp.ConfiguredWorkspaceLifecycleForServe = func(*config.Config, *sourceartifact.RuntimeProjection, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
		return silentMonitorWorkspace{released: &workspaceReleased}, nil
	}
	t.Cleanup(func() { cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace })
	oldBuild := buildStoresForServe
	buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
		return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: storebackend.Selection{Backend: storebackend.BackendPostgres}, PostgresDSN: routed, SessionLockTTL: runtimeSessionLockTTL(cfg)})
	}
	t.Cleanup(func() { buildStoresForServe = oldBuild })
	database := make(chan *sql.DB, 1)
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
		db, _, _ := selectedRuntimeStoreForTest(t, p)
		database <- db
	})
	serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
		ConfigPath: writeServeRuntimeTestConfig(t), SourceRoot: filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		PlatformSpecPath: defaultPlatformSpecPath, StoreMode: "postgres", StoreModeSet: true,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", ShutdownGrace: time.Second, SelfCheck: true, Verbose: true,
	})
	// On assertion failure, release the held reply before process cleanup joins.
	t.Cleanup(release)
	serve.waitForReadyLine()
	db := <-database
	serve.mu.Lock()
	rt := serve.runtime
	serve.mu.Unlock()
	readyURL := "http://" + serveRuntimeAPIListenerFromOutput(t, serve.outputString()) + "/readyz"
	client := &http.Client{Timeout: 300 * time.Millisecond}
	response, err := client.Get(readyURL)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("healthy readiness control=%s", response.Status)
	}
	const ownershipSQL = `SELECT pid FROM pg_locks WHERE locktype='advisory' AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND classid::bigint=CASE WHEN hashtext($1)<0 THEN 4294967295::bigint ELSE 0::bigint END AND objid::bigint=(hashtext($1)::bigint & 4294967295::bigint) AND objsubid=1 LIMIT 1`
	var ownerPID int32
	if err := observer.QueryRow(ownershipSQL, "swarm:runtime:shared-store-owner").Scan(&ownerPID); err != nil {
		t.Fatal(err)
	}
	targetPID.Store(ownerPID)
	select {
	case <-paused:
	case <-time.After(5 * time.Second):
		t.Fatal("exact process possession monitor did not reach held-reply barrier")
	}
	started := time.Now()
	deadline := started.Add(4 * time.Second)
	for {
		_, grantErr := rt.CurrentStartupGrantEvidence()
		response, requestErr := client.Get(readyURL)
		withdrawn := errors.Is(requestErr, syscall.ECONNREFUSED)
		if requestErr == nil {
			_ = response.Body.Close()
			withdrawn = response.StatusCode == http.StatusServiceUnavailable
		}
		if grantErr != nil && withdrawn {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("silent proof did not withdraw readiness and grant authority: grant=%v request=%v\n%s", grantErr, requestErr, serve.outputString())
		}
		time.Sleep(10 * time.Millisecond)
	}
	fencedAfter := time.Since(started)
	if code, exited := serve.waitForExit(150 * time.Millisecond); exited {
		t.Fatalf("serve exited %d before held proof reply/cleanup joined", code)
	}
	if workspaceReleased.Load() != 0 {
		t.Fatal("workspace dependency released before proof cleanup joined")
	}
	pingCtx, cancelPing := context.WithTimeout(context.Background(), time.Second)
	err = db.PingContext(pingCtx)
	cancelPing()
	if err != nil {
		t.Fatalf("selected database dependency unavailable before proof joined: %v", err)
	}
	var retainedPID int32
	if err := observer.QueryRow(ownershipSQL, "swarm:runtime:shared-store-owner").Scan(&retainedPID); err != nil || retainedPID != ownerPID {
		t.Fatalf("local fence did not preserve remote-possession counterexample: pid=%d want=%d err=%v", retainedPID, ownerPID, err)
	}
	release()
	code, exited := serve.waitForExit(10 * time.Second)
	if !exited || code == 0 {
		t.Fatalf("silent monitor failure did not join with nonzero exit: code=%d exited=%t\n%s", code, exited, serve.outputString())
	}
	if !strings.Contains(serve.outputString(), "can no longer verify project ownership") {
		t.Fatalf("silence was not reported as unprovable ownership:\n%s", serve.outputString())
	}
	if _, err := rt.CurrentStartupGrantEvidence(); err == nil {
		t.Fatal("late healthy proof response restored generation authority")
	}
	if workspaceReleased.Load() == 0 {
		t.Fatal("joined shutdown did not release workspace dependency")
	}
	if err := db.PingContext(context.Background()); err == nil {
		t.Fatal("joined shutdown left the selected database open")
	}
	t.Logf("silent exact backend %d: readiness and grant withdrawn after %s; TCP/remote lock and dependencies retained while proof blocked; held reply released, cleanup joined, exit=%d, no grant revival", ownerPID, fencedAfter, code)
}
