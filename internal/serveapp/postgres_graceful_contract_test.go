package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/testutil"
)

type gracefulContractWorkspace struct {
	serveRuntimeWorkspaceStub
	active   *atomic.Bool
	released *atomic.Int32
	failure  error
}

func (w gracefulContractWorkspace) ReleaseSourceProjection(ctx context.Context) error {
	w.released.Add(1)
	if w.active.Load() {
		return errors.New("workspace released before admitted SQL settled")
	}
	return errors.Join(w.serveRuntimeWorkspaceStub.ReleaseSourceProjection(ctx), w.failure)
}

// The real serve lifecycle owns admission and dependencies. The admitted leaf
// uses the selected idempotency owner, or an already-open SQL result. Its explicit
// operation cancellation is distinct from requesting process shutdown. This is
// not a crash/restart proof or a claim that every HTTP caller has that separation.
func TestServePostgresGracefulContract(t *testing.T) {
	for _, phase := range []string{"mutation_commit", "operation_cancel", "result_drain", "cleanup_failure"} {
		t.Run(phase, func(t *testing.T) {
			dsn, observer, cleanup := testutil.StartPostgres(t)
			t.Cleanup(cleanup)
			var active atomic.Bool
			var released atomic.Int32
			var cleanupErr error
			if phase == "cleanup_failure" {
				cleanupErr = errors.New("graceful proof workspace cleanup failure")
			}
			oldWorkspace := cliapp.ConfiguredWorkspaceLifecycleForServe
			cliapp.ConfiguredWorkspaceLifecycleForServe = func(*config.Config, *sourceartifact.RuntimeProjection, semanticview.Source, cliapp.WorkspaceMountSources, cliapp.WorkspaceBackendSelection) (cliapp.ServeWorkspaceLifecycle, error) {
				return gracefulContractWorkspace{active: &active, released: &released, failure: cleanupErr}, nil
			}
			t.Cleanup(func() { cliapp.ConfiguredWorkspaceLifecycleForServe = oldWorkspace })
			oldBuild := buildStoresForServe
			buildStoresForServe = func(ctx context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
				return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: storebackend.Selection{Backend: storebackend.BackendPostgres}, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})
			}
			t.Cleanup(func() { buildStoresForServe = oldBuild })
			type selected struct {
				db    *sql.DB
				store *store.PostgresStore
			}
			selectedReady := make(chan selected, 1)
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
				db, pg, _ := selectedRuntimeStoreForTest(t, p)
				selectedReady <- selected{db, pg}
			})
			serve := startServeRuntimeTestProcess(t, cliapp.ServeOptions{
				ConfigPath: writeServeRuntimeTestConfig(t), SourceRoot: filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
				PlatformSpecPath: defaultPlatformSpecPath, StoreMode: "postgres", StoreModeSet: true,
				APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", ShutdownGrace: 5 * time.Second, SelfCheck: true, Verbose: true,
			})
			serve.waitForReadyLine()
			selectedStore := <-selectedReady
			serve.mu.Lock()
			rt := serve.runtime
			serve.mu.Unlock()
			opCtx, cancelOperation := context.WithCancel(context.Background())
			defer cancelOperation()
			lease, err := rt.WorkOccurrence().Begin(opCtx)
			if err != nil {
				t.Fatal(err)
			}
			active.Store(true)
			done := make(chan error, 1)
			releaseResult := make(chan struct{})
			var holder *sql.Conn
			const lock = 2444301
			var started atomic.Bool
			t.Cleanup(func() {
				if holder != nil {
					_, _ = holder.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", lock)
					_ = holder.Close()
				}
				close(releaseResult)
				if !started.Load() {
					active.Store(false)
					_ = lease.Done()
				}
			})
			var callbacks atomic.Int32
			request := apiidempotency.Request{Method: "proof.graceful", ActorTokenID: "actor", IdempotencyKey: phase, RequestHash: "hash", ResourceID: "resource", Now: time.Now().UTC(), TTL: time.Hour}
			if phase == "result_drain" {
				rows, err := selectedStore.db.QueryContext(opCtx, "SELECT i, repeat('x', 8192), pg_sleep(0.01) FROM generate_series(1, 30) i")
				if err != nil {
					t.Fatal(err)
				}
				started.Store(true)
				go func() {
					<-releaseResult
					count := 0
					var scanErr error
					for rows.Next() {
						count++
						var number int
						var payload string
						var slept any
						if scanErr = rows.Scan(&number, &payload, &slept); scanErr != nil {
							break
						}
						if number != count || payload != strings.Repeat("x", 8192) {
							scanErr = errors.New("admitted result values changed during drain")
							break
						}
					}
					err := errors.Join(scanErr, rows.Err(), rows.Close())
					if count != 30 {
						err = errors.Join(err, errors.New("incomplete admitted result drain"))
					}
					active.Store(false)
					done <- errors.Join(err, lease.Done())
				}()
			} else {
				for _, statement := range []string{
					`CREATE FUNCTION serve_graceful_completion_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(2444301); RETURN NEW; END $$`,
					`CREATE TRIGGER serve_graceful_completion_gate BEFORE INSERT ON api_idempotency FOR EACH ROW EXECUTE FUNCTION serve_graceful_completion_gate()`,
				} {
					if _, err := observer.Exec(statement); err != nil {
						t.Fatal(err)
					}
				}
				holder, err = observer.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := holder.ExecContext(context.Background(), "SELECT pg_advisory_lock($1)", lock); err != nil {
					t.Fatal(err)
				}
				started.Store(true)
				go func() {
					_, _, err := selectedStore.store.WithAPIIdempotency(opCtx, request, func(ctx context.Context) (apiidempotency.Completion, error) {
						callbacks.Add(1)
						if ctx != opCtx {
							return apiidempotency.Completion{}, errors.New("external callback context detached")
						}
						return apiidempotency.Completion{ResourceID: "resource", Response: json.RawMessage(`{"ok":true}`)}, nil
					})
					active.Store(false)
					done <- errors.Join(err, lease.Done())
				}()
				deadline := time.Now().Add(5 * time.Second)
				for {
					var blocked bool
					if err := observer.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=2444301 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`).Scan(&blocked); err != nil {
						t.Fatal(err)
					}
					if blocked {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("completion never reached real SQL barrier")
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			serve.cancel()
			deadline := time.Now().Add(3 * time.Second)
			for {
				next, err := rt.WorkOccurrence().Begin(context.Background())
				if err != nil {
					if !errors.Is(err, worklifetime.ErrAdmissionFenced) && !errors.Is(err, worklifetime.ErrRetired) {
						t.Fatalf("new admission failed for unrelated reason: %v", err)
					}
					break
				}
				_ = next.Done()
				if time.Now().After(deadline) {
					t.Fatal("shutdown did not fence new admission")
				}
				time.Sleep(10 * time.Millisecond)
			}
			client := &http.Client{Timeout: time.Second}
			resp, err := client.Get("http://" + serveRuntimeAPIListenerFromOutput(t, serve.outputString()) + "/healthz")
			if err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("new HTTP request not refused after shutdown: %s", resp.Status)
				}
			}
			if err := opCtx.Err(); err != nil {
				t.Fatalf("process stop canceled independent operation: %v", err)
			}
			if phase == "operation_cancel" {
				cancelOperation()
			}
			if code, exited := serve.waitForExit(100 * time.Millisecond); exited {
				t.Fatalf("serve exited %d before admitted work settled: %s", code, serve.outputString())
			}
			select {
			case err := <-done:
				t.Fatalf("admitted work returned before barrier release: %v", err)
			default:
			}
			if released.Load() != 0 {
				t.Fatal("workspace dependency released before SQL settlement")
			}
			if err := selectedStore.db.PingContext(context.Background()); err != nil {
				t.Fatalf("selected database released before SQL settlement: %v", err)
			}
			if holder != nil {
				if _, err := holder.ExecContext(context.Background(), "SELECT pg_advisory_unlock($1)", lock); err != nil {
					t.Fatal(err)
				}
			} else {
				releaseResult <- struct{}{}
			}
			select {
			case err := <-done:
				if phase == "operation_cancel" {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("explicit operation cancel = %v", err)
					}
				} else if err != nil {
					t.Fatalf("healthy admitted work failed: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("admitted work did not settle after barrier release")
			}
			code, exited := serve.waitForExit(10 * time.Second)
			wantCode := 0
			if cleanupErr != nil {
				wantCode = 1
			}
			if !exited || code != wantCode {
				t.Fatalf("serve exit=%d, joined=%t, want=%d: %s", code, exited, wantCode, serve.outputString())
			}
			if released.Load() == 0 {
				t.Fatal("workspace dependency never released")
			}
			if cleanupErr != nil && !strings.Contains(serve.outputString(), cleanupErr.Error()) {
				t.Fatal("cleanup failure missing from final diagnostics")
			}
			if err := selectedStore.db.PingContext(context.Background()); err == nil {
				t.Fatal("selected database remains open after process exit")
			}
			var count int
			if err := observer.QueryRow("SELECT count(*) FROM api_idempotency WHERE method=$1 AND idempotency_key=$2", request.Method, request.IdempotencyKey).Scan(&count); err != nil {
				t.Fatal(err)
			}
			wantCount := 1
			if phase == "operation_cancel" || phase == "result_drain" {
				wantCount = 0
			}
			if count != wantCount || (phase != "result_drain" && callbacks.Load() != 1) {
				t.Fatalf("durable completions=%d want=%d, callbacks=%d", count, wantCount, callbacks.Load())
			}
			t.Logf("phase=%s: exit=%d, durable completions=%d; new admission refused, SQL joined before dependency release", phase, code, count)
		})
	}
}
