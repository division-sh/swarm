package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Only the diagnostic persistence boundary fails. Lifecycle production, provider
// execution, shutdown, source admission and process restart remain real.
type interruptedLifecycleDiagnosticStore struct {
	runtime.RuntimeLogPersistence
	fail *atomic.Bool
}

func (s interruptedLifecycleDiagnosticStore) PersistLifecycleDiagnostic(ctx context.Context, item diaglog.LifecycleDiagnostic, record runtime.RuntimeLogPersistenceRecord) (bool, error) {
	if s.fail.Load() {
		return false, errors.New("injected lifecycle diagnostic persistence outage")
	}
	return s.RuntimeLogPersistence.PersistLifecycleDiagnostic(ctx, item, record)
}

func TestLifecycleDiagnosticServedCrashRecoveryAndResetBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			unsetStoreSelectorEnv(t)
			source := writeServedLiveAgentFixture(t)
			hash := servedEventPublishFixtureBundleHash(t, source)
			var db *sql.DB
			var cfg, storeMode, backendName string
			if backend == servedparity.BackendDefaultSQLite {
				path := filepath.Join(t.TempDir(), "diagnostics.db")
				cfg = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", path, channelOnboardingHostWorkspaceFields())
				var err error
				db, err = sql.Open("sqlite", path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				backendName = "sqlite"
			} else {
				dsn, postgres, _ := testutil.StartEmptyPostgres(t)
				db, storeMode, backendName = postgres, "postgres", "postgres"
				cfg = writeChannelOnboardingPostgresRuntimeConfig(t, dsn)
			}
			temporary := t.TempDir()
			// SIGKILL deliberately prevents the first child from releasing its
			// read-only projection. Only this test-owned temp tree is writable
			// again, after all child cleanup, so testing.TempDir can remove it.
			t.Cleanup(func() {
				if err := filepath.WalkDir(temporary, func(path string, entry os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if entry.IsDir() {
						return os.Chmod(path, 0o700)
					}
					return nil
				}); err != nil {
					t.Errorf("release killed child's test-owned projection: %v", err)
				}
			})
			env := []string{"SWARM_DIAGNOSTIC_PROCESS_HELPER=1", "SWARM_DIAGNOSTIC_CONFIG=" + cfg,
				"SWARM_DIAGNOSTIC_SOURCE=" + source, "SWARM_DIAGNOSTIC_STORE=" + storeMode, "TMPDIR=" + temporary}
			first := startServedCrashProcess(t, "TestLifecycleDiagnosticServeProcessHelper", append(env, "SWARM_DIAGNOSTIC_FAIL=1"))
			proof := servedControlProofRuntime{Endpoint: first.endpoint(t) + "/v1/rpc", DB: db, Backend: backendName, BundleHash: hash}
			initial := requireServedEventPublishRPCResult(t, proof.Endpoint, map[string]any{
				"event_name": "item.received", "bundle_hash": hash,
				"payload": map[string]any{"item_id": "diagnostic-recovery"}, "idempotency_key": uuid.NewString(),
			})
			requireServedEventPublishEntityState(t, db, backendName, initial.RunID, "", "waiting")
			publishServedLiveAgentHoldEvent(t, proof, initial.RunID, initial.EventID, "before-crash")
			var restarted servedAgentRestartProofResult
			requireServedJSONRPCResult(t, proof.Endpoint, "agent.restart", map[string]any{
				"run_id": initial.RunID, "agent_id": "load-agent", "idempotency_key": uuid.NewString(),
			}, &restarted)
			if !restarted.OK {
				t.Fatal("diagnostic outage changed committed restart success")
			}
			// Join provider work before crashing: the only intentionally unfinished
			// operation is diagnostic projection, not an ambiguous provider attempt.
			publishServedLiveAgentHoldEvent(t, proof, initial.RunID, initial.EventID, "after-restart")
			pending := readServedLifecycleDiagnosticReceipts(t, db, initial.RunID)
			if len(pending) == 0 {
				t.Fatal("real lifecycle operations produced no pending diagnostics")
			}
			for id, receipt := range pending {
				if receipt != nil {
					t.Fatalf("faulted diagnostic %s was acknowledged", id)
				}
			}
			if err := first.kill(); err != nil {
				t.Fatal(err)
			}
			second := startServedCrashProcess(t, "TestLifecycleDiagnosticServeProcessHelper", append(env, "SWARM_DIAGNOSTIC_FAIL=0"))
			proof.Endpoint = second.endpoint(t) + "/v1/rpc"
			projected := readServedLifecycleDiagnosticReceipts(t, db, initial.RunID)
			for id := range pending {
				if projected[id] == nil {
					t.Fatalf("startup readiness preceded pending diagnostic %s settlement", id)
				}
				requireServedDiagnosticEventCount(t, db, id, initial.RunID, 1)
			}
			params := map[string]any{"include_source_artifacts": false, "idempotency_key": uuid.NewString()}
			if response := requestServedJSONRPC(t, proof.Endpoint, "runtime.nuke", params); response.Error != nil {
				t.Fatalf("served reset: %+v", response.Error)
			}
			later := requireServedEventPublishRPCResult(t, proof.Endpoint, map[string]any{
				"event_name": "item.received", "bundle_hash": hash,
				"payload": map[string]any{"item_id": "after-reset"}, "idempotency_key": uuid.NewString(),
			})
			requireServedEventPublishEntityState(t, db, backendName, later.RunID, "", "waiting")
			if response := requestServedJSONRPC(t, proof.Endpoint, "runtime.nuke", params); response.Error != nil {
				t.Fatalf("historical reset replay: %+v", response.Error)
			}
			afterReset := readServedLifecycleDiagnosticReceipts(t, db, initial.RunID)
			for id := range pending {
				if !reflect.DeepEqual(projected[id], afterReset[id]) {
					t.Fatalf("reset changed exact diagnostic receipt %s", id)
				}
				requireServedDiagnosticEventCount(t, db, id, initial.RunID, 0)
			}
			var oldRuns, laterRuns int
			if err := db.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", initial.RunID).Scan(&oldRuns); err != nil || oldRuns != 0 {
				t.Fatalf("diagnostic replay resurrected predecessor: count=%d err=%v", oldRuns, err)
			}
			if err := db.QueryRow("SELECT COUNT(*) FROM runs WHERE run_id = $1", later.RunID).Scan(&laterRuns); err != nil || laterRuns != 1 {
				t.Fatalf("historical replay changed later work: count=%d err=%v", laterRuns, err)
			}
			if err := second.stop(); err != nil {
				t.Fatalf("recovered process shutdown: %v\n%s", err, second.output.String())
			}
		})
	}
}

func readServedLifecycleDiagnosticReceipts(t *testing.T, db *sql.DB, runID string) map[string]map[string]any {
	t.Helper()
	rows, err := db.Query("SELECT outbox_id, projection FROM agent_lifecycle_diagnostic_outbox WHERE run_id = $1", runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := make(map[string]map[string]any)
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			t.Fatal(err)
		}
		var receipt map[string]any
		if len(raw) != 0 {
			if err := json.Unmarshal(raw, &receipt); err != nil {
				t.Fatal(err)
			}
		}
		result[id] = receipt
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func requireServedDiagnosticEventCount(t *testing.T, db *sql.DB, id, runID string, want int) {
	t.Helper()
	rows, err := db.Query("SELECT run_id, payload FROM events WHERE event_name = 'platform.runtime_log'")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var actualRun sql.NullString
		var raw []byte
		var event struct {
			Details map[string]any `json:"details"`
		}
		if err := rows.Scan(&actualRun, &raw); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			t.Fatal(err)
		}
		if event.Details["outbox_id"] == id {
			count++
			if actualRun.String != runID || event.Details["run_id"] != runID {
				t.Fatalf("diagnostic %s changed original run identity: %s %+v", id, actualRun.String, event.Details)
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("diagnostic %s log count=%d want=%d", id, count, want)
	}
}

func TestLifecycleDiagnosticServeProcessHelper(t *testing.T) {
	if os.Getenv("SWARM_DIAGNOSTIC_PROCESS_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	var fail atomic.Bool
	prior := projectRuntimePersistenceForServe
	t.Cleanup(func() { projectRuntimePersistenceForServe = prior })
	projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
		persistence := prior(owner)
		persistence.deps.RuntimeLogStore = interruptedLifecycleDiagnosticStore{persistence.deps.RuntimeLogStore, &fail}
		return persistence
	}
	opts := cliapp.DefaultServeOptions()
	opts.ConfigPath, opts.SourceRoot = os.Getenv("SWARM_DIAGNOSTIC_CONFIG"), os.Getenv("SWARM_DIAGNOSTIC_SOURCE")
	opts.PlatformSpecPath = defaultPlatformSpecPath
	opts.StoreMode = os.Getenv("SWARM_DIAGNOSTIC_STORE")
	opts.StoreModeSet = opts.StoreMode != ""
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.Verbose, opts.SelfCheck = true, true
	opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
	opts.TestLLMRuntime = servedLiveAgentProofLLMRuntime{}
	opts.TestRuntimeReadyHook = func(*runtime.Runtime) { fail.Store(os.Getenv("SWARM_DIAGNOSTIC_FAIL") == "1") }
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if code := runFrom(ctx, repoRootForTest(), opts); code != 0 {
		t.Fatalf("diagnostic process exit=%d", code)
	}
}
