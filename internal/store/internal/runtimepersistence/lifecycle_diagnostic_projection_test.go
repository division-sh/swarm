package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	agentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mockperformance"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type diagnosticProjectionTestStore interface {
	agentfixture.Store
	runtimemanager.AgentLifecycleDiagnosticPersistence
	runtimepkg.RuntimeLogPersistence
	ListPendingAgentLifecycleDiagnostics(context.Context, int) ([]diaglog.LifecycleDiagnostic, error)
}

type diagnosticLostResult struct {
	diagnosticProjectionTestStore
}

func (p diagnosticLostResult) ProjectAgentLifecycleDiagnostics(ctx context.Context, limit int) (int, error) {
	n, err := projectDiagnosticTestBatch(ctx, p.diagnosticProjectionTestStore, limit)
	if err != nil {
		return n, err
	}
	return n, errors.New("injected lost successful projection response")
}

func TestLifecycleDiagnosticIndependentHandlesAndReopen(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		backend := "postgres"
		if sqlite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			var first diagnosticProjectionTestStore
			var open func() (diagnosticProjectionTestStore, *sql.DB, func())
			if sqlite {
				path := filepath.Join(t.TempDir(), "diagnostics.db")
				first = newBootstrappedSQLiteRuntimeStoreForPath(t, path)
				open = func() (diagnosticProjectionTestStore, *sql.DB, func()) {
					s, err := NewSQLiteRuntimeStore(path)
					if err != nil {
						t.Fatal(err)
					}
					if err := s.BootstrapSchema(context.Background(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
						t.Fatal(err)
					}
					s.SetEventPayloadAdmitter(storeTestPayloadAdmitter)
					return s, s.backend.ConstructionHandle(), func() { _ = s.Close() }
				}
			} else {
				dsn, db, _ := testutil.StartPostgres(t)
				first = admitTestPostgresStore(t, db)
				open = func() (diagnosticProjectionTestStore, *sql.DB, func()) {
					db, err := sql.Open("postgres", dsn)
					if err != nil {
						t.Fatal(err)
					}
					return admitTestPostgresStore(t, db), db, func() { _ = db.Close() }
				}
			}
			origin := runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation}
			left := enqueueDiagnosticTestIdentity(t, first, mustTestAgentIdentityForRun(uuid.NewString(), "same-worker", "left/one"), origin, executionmode.Live)
			right := enqueueDiagnosticTestIdentity(t, first, mustTestAgentIdentityForRun(uuid.NewString(), "same-worker", "right/two"), origin, executionmode.Live)
			second, db, closeSecond := open()
			var wg sync.WaitGroup
			errors := make(chan error, 2)
			for _, owner := range []diagnosticProjectionTestStore{first, second} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := projectDiagnosticTestBatch(context.Background(), owner, 100)
					errors <- err
				}()
			}
			wg.Wait()
			close(errors)
			for err := range errors {
				if err != nil {
					t.Fatal(err)
				}
			}
			for _, item := range []diaglog.LifecycleDiagnostic{left, right} {
				assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
				var runID string
				if err := db.QueryRow(`SELECT run_id FROM events WHERE event_id=$1`, diagnosticEventID(item.OutboxID)).Scan(&runID); err != nil || runID != item.Identity.RunID {
					t.Fatalf("cross-run attribution %s: %v", runID, err)
				}
			}
			// Force each independent handle to win; concurrent selection alone can
			// hide missing admission on the losing handle.
			for index, owner := range []diagnosticProjectionTestStore{first, second} {
				item := enqueueDiagnosticTestAgent(t, first, fmt.Sprintf("forced-handle-%d", index))
				if err := runtimepkg.NewRuntimeLogger(owner, executionposture.Live, nil).ProjectLifecycleDiagnostic(context.Background(), item); err != nil {
					t.Fatalf("handle %d projection: %v", index, err)
				}
				assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
			}
			closeSecond()
			later := enqueueDiagnosticTestAgent(t, first, "lost-response-worker")
			if _, err := (diagnosticLostResult{first}).ProjectAgentLifecycleDiagnostics(context.Background(), 100); err == nil {
				t.Fatal("missing committed-but-error control")
			}
			// Discard the successful result as a caller with a lost acknowledgement
			// would; a fresh connection must observe the atomic pair, not append again.
			reopened, db, closeReopened := open()
			defer closeReopened()
			if n, err := projectDiagnosticTestBatch(context.Background(), reopened, 100); err != nil || n != 0 {
				t.Fatalf("reopened replay n=%d err=%v", n, err)
			}
			for _, item := range []diaglog.LifecycleDiagnostic{left, right, later} {
				assertDiagnosticCounts(t, db, item.OutboxID, 1, 0)
			}
			pending := enqueueDiagnosticTestAgent(t, first, "reopened-handle-winner")
			if err := runtimepkg.NewRuntimeLogger(reopened, executionposture.Live, nil).ProjectLifecycleDiagnostic(context.Background(), pending); err != nil {
				t.Fatalf("reopened pending projection: %v", err)
			}
			assertDiagnosticCounts(t, db, pending.OutboxID, 1, 0)
		})
	}
}

func TestLifecycleDiagnosticAtomicProjection(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		name := "postgres"
		if sqlite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			for _, scenario := range []string{"concurrent", "ack_rollback", "before_commit", "before_insert", "foreign_context", "mock_mode", "deleted_actor", "changed_actor", "payload_conflict", "missing_history", "missing_ack", "timestamp_conflict", "cancelled", "batch_prefix"} {
				t.Run(scenario, func(t *testing.T) {
					var selected diagnosticProjectionTestStore
					var db *sql.DB
					if sqlite {
						s := newBootstrappedSQLiteRuntimeStoreForTest(t)
						selected = s
						db = s.backend.ConstructionHandle()
					} else {
						_, db, _ = testutil.StartPostgres(t)
						selected = admitTestPostgresStore(t, db)
					}
					proveLifecycleDiagnosticProjection(t, selected, db, sqlite, scenario)
				})
			}
		})
	}
}

func TestLifecycleDiagnosticBatchCardinality(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		backend := "postgres"
		if sqlite {
			backend = "sqlite"
		}
		for _, size := range []int{0, 1, 100, 101} {
			t.Run(fmt.Sprintf("%s/%d", backend, size), func(t *testing.T) {
				var selected diagnosticProjectionTestStore
				var db *sql.DB
				if sqlite {
					s := newBootstrappedSQLiteRuntimeStoreForTest(t)
					selected = s
					db = s.backend.ConstructionHandle()
				} else {
					_, db, _ = testutil.StartPostgres(t)
					selected = admitTestPostgresStore(t, db)
				}
				if size > 0 {
					first := enqueueDiagnosticTestAgent(t, selected, "batch-worker")
					for i := 1; i < size; i++ {
						op := uuid.NewString()
						_, err := agentfixture.CommitStatic(t, testAuthorActivityContext(), selected, runtimemanager.AgentLifecycleTransition{
							OperationID: op, OperationKind: "restart", RequestHash: op, Identity: first.Identity, AgentID: first.AgentID, Trigger: "restart",
							ExpectedEpoch: 11, ExpectedGeneration: uint64(i), ExpectedPhase: runtimemanager.AgentLifecycleRegistered,
							TargetEpoch: 11, TargetGeneration: uint64(i + 1), TargetPhase: runtimemanager.AgentLifecycleRegistered,
							ConfigRevision: "r1", RunMode: runtimemanager.AgentRunModeStopped, Now: first.CreatedAt.Add(time.Duration(i) * time.Microsecond),
						})
						if err != nil {
							t.Fatalf("enqueue %d: %v", i, err)
						}
					}
				}
				want := min(size, 100)
				if n, err := projectDiagnosticTestBatch(context.Background(), selected, 100); err != nil || n != want {
					t.Fatalf("first batch n=%d want=%d err=%v", n, want, err)
				}
				if n, err := projectDiagnosticTestBatch(context.Background(), selected, 100); err != nil || n != size-want {
					t.Fatalf("tail n=%d want=%d err=%v", n, size-want, err)
				}
				var events, acks int
				if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='platform.runtime_log'`).Scan(&events); err != nil {
					t.Fatal(err)
				}
				if err := db.QueryRow(`SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE projected_at IS NOT NULL`).Scan(&acks); err != nil {
					t.Fatal(err)
				}
				if events != size || acks != size {
					t.Fatalf("cardinality event/ack=%d/%d want=%d", events, acks, size)
				}
			})
		}
	}
}

func TestLifecycleDiagnosticCausalOrigin(t *testing.T) {
	for _, sqlite := range []bool{true, false} {
		backend := "postgres"
		if sqlite {
			backend = "sqlite"
		}
		for _, scenario := range []string{"valid_mode_difference", "missing_parent", "foreign_parent"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				var selected diagnosticProjectionTestStore
				var db *sql.DB
				if sqlite {
					s := newBootstrappedSQLiteRuntimeStoreForTest(t)
					selected = s
					db = s.backend.ConstructionHandle()
				} else {
					_, db, _ = testutil.StartPostgres(t)
					selected = admitTestPostgresStore(t, db)
				}
				first := enqueueDiagnosticTestAgent(t, selected, "causal-worker")
				ctx := testAuthorActivityContext()
				parent, runID := uuid.NewString(), first.Identity.RunID
				if scenario == "foreign_parent" {
					runID = uuid.NewString()
					requireRunningRunForTest(t, ctx, selected, runID, time.Now().UTC())
				}
				if scenario != "missing_parent" {
					event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(parent, events.EventType("diagnostic.input"), "test", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, events.RoutingSource{}, time.Now().UTC(), executionmode.Mock)
					if err := commitSemanticEventFixture(ctx, selected.(semanticEventFixtureStore), event); err != nil {
						t.Fatal(err)
					}
				}
				op := uuid.NewString()
				result, err := agentfixture.CommitStatic(t, ctx, selected, runtimemanager.AgentLifecycleTransition{
					DiagnosticOrigin: runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticAcceptedEvent, ParentEventID: parent},
					OperationID:      op, OperationKind: "start", RequestHash: op, Identity: first.Identity, AgentID: first.AgentID, Trigger: "start",
					ExpectedEpoch: 11, ExpectedGeneration: 1, ExpectedPhase: runtimemanager.AgentLifecycleRegistered, TargetEpoch: 11, TargetGeneration: 2, TargetPhase: runtimemanager.AgentLifecycleRunning,
					ConfigRevision: "r1", RunMode: runtimemanager.AgentRunModeStandard, Now: time.Now().UTC(),
				})
				if scenario != "valid_mode_difference" {
					if err == nil || !strings.Contains(err.Error(), "causal parent") {
						t.Fatalf("wrong causal admission refusal: %v", err)
					}
					var operations int
					if err := db.QueryRow(`SELECT COUNT(*) FROM agent_lifecycle_operations WHERE operation_id=$1`, op).Scan(&operations); err != nil || operations != 0 {
						t.Fatalf("rejected causal enqueue committed: %d %v", operations, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if result.DiagnosticProvenance.ActorMode != executionmode.Live || result.DiagnosticProvenance.EventMode != executionmode.Mock {
					t.Fatalf("collapsed actor and causal modes: %+v", result.DiagnosticProvenance)
				}
				if n, err := projectDiagnosticTestBatch(context.Background(), selected, 100); err != nil || n != 2 {
					t.Fatalf("causal projection n=%d err=%v", n, err)
				}
				var mode, source string
				if err := db.QueryRow(`SELECT e.execution_mode,e.source_event_id FROM events e JOIN agent_lifecycle_diagnostic_outbox o ON e.run_id=o.run_id WHERE o.operation_id=$1 AND e.source_event_id IS NOT NULL`, op).Scan(&mode, &source); err != nil {
					t.Fatal(err)
				}
				if mode != "live" || source != parent {
					t.Fatalf("lost causal facts mode=%s parent=%s", mode, source)
				}
			})
		}
	}
}

func enqueueDiagnosticTestAgent(t *testing.T, selected diagnosticProjectionTestStore, slug string) diaglog.LifecycleDiagnostic {
	t.Helper()
	identity := testAgentIdentity(t, slug, "global")
	return enqueueDiagnosticTestIdentity(t, selected, identity, runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation}, executionmode.Live)
}

func enqueueDiagnosticTestIdentity(t *testing.T, selected diagnosticProjectionTestStore, identity agentidentity.Identity, origin runtimemanager.LifecycleDiagnosticOrigin, mode executionmode.Mode) diaglog.LifecycleDiagnostic {
	t.Helper()
	req := diagnosticTestTransition(t, identity, origin, mode)
	if _, err := agentfixture.CommitStatic(t, testAuthorActivityContext(), selected, req); err != nil {
		t.Fatal(err)
	}
	return diagnosticTestOperation(t, selected, req.OperationID)
}

func diagnosticTestTransition(t *testing.T, identity agentidentity.Identity, origin runtimemanager.LifecycleDiagnosticOrigin, mode executionmode.Mode) runtimemanager.AgentLifecycleTransition {
	t.Helper()
	slug := identity.AgentID()
	now := time.Now().UTC().Truncate(time.Microsecond)
	backend := "anthropic"
	if mode == executionmode.Mock {
		backend = "mock"
	}
	rec := runtimemanager.PersistedAgent{Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{
		ID: slug, Identity: identity, Type: "sonnet", Role: "worker", FlowID: "global", FlowPath: identity.FlowInstance(), Model: "regular", ExecutionMode: mode, ResolvedLLMBackend: backend, Config: []byte(`{}`),
	}), Status: "active", HiredBy: "test", StartedAt: now}
	if mode == executionmode.Mock {
		rec.Config.Mock = mockperformance.Performance{Kind: mockperformance.KindPython, Module: "mocks/worker.py", Source: []byte("def handle(input): return {'text': 'fixture'}\n"), Digest: "sha256:test"}
	}
	op := uuid.NewString()
	return runtimemanager.AgentLifecycleTransition{
		DiagnosticOrigin: origin,
		OperationID:      op, OperationKind: "spawn", RequestHash: op, Identity: identity, AgentID: slug, Trigger: "spawn", TargetEpoch: 11, TargetGeneration: 1,
		TargetPhase: runtimemanager.AgentLifecycleRegistered, ConfigRevision: "r1", RunMode: runtimemanager.AgentRunModeStopped, Agent: &rec, Now: now,
	}
}

func diagnosticTestOperation(t *testing.T, selected diagnosticProjectionTestStore, op string) diaglog.LifecycleDiagnostic {
	t.Helper()
	items, err := selected.ListPendingAgentLifecycleDiagnostics(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if item.OperationID == op {
			return item
		}
	}
	t.Fatal("committed lifecycle has no diagnostic occurrence")
	return diaglog.LifecycleDiagnostic{}
}

func proveLifecycleDiagnosticProjection(t *testing.T, selected diagnosticProjectionTestStore, db *sql.DB, sqlite bool, scenario string) {
	t.Helper()
	mode := executionmode.Live
	if scenario == "mock_mode" {
		mode = executionmode.Mock
	}
	first := enqueueDiagnosticTestIdentity(t, selected, testAgentIdentity(t, "diagnostic-worker", "global"), runtimemanager.LifecycleDiagnosticOrigin{Owner: runtimemanager.LifecycleDiagnosticNormal, Causality: runtimemanager.LifecycleDiagnosticObservation}, mode)
	ctx := context.Background()
	var cleanup func()
	switch scenario {
	case "concurrent":
		var wg sync.WaitGroup
		results := make(chan error, 8)
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); _, err := projectDiagnosticTestBatch(ctx, selected, 100); results <- err }()
		}
		wg.Wait()
		close(results)
		for err := range results {
			if err != nil {
				t.Fatalf("concurrent projection: %v", err)
			}
		}
	case "ack_rollback", "before_commit", "before_insert":
		table, operation := "agent_lifecycle_diagnostic_outbox", "UPDATE"
		if scenario == "before_insert" {
			table, operation = "events", "INSERT"
		}
		if scenario == "before_commit" {
			cleanup = installDiagnosticCommitFailure(t, db, sqlite)
		} else {
			cleanup = installDiagnosticFailureTrigger(t, db, sqlite, table, operation, "")
		}
		if n, err := projectDiagnosticTestBatch(ctx, selected, 100); err == nil || n != 0 {
			t.Fatalf("injected failure n=%d err=%v", n, err)
		}
		assertDiagnosticCounts(t, db, first.OutboxID, 0, 1)
		cleanup()
		cleanup = nil
	case "foreign_context":
		ctx = runtimecorrelation.WithRunID(ctx, uuid.NewString())
	case "deleted_actor":
		if _, err := db.Exec(`DELETE FROM agents WHERE run_id=$1`, first.Identity.RunID); err != nil {
			t.Fatal(err)
		}
	case "changed_actor":
		if _, err := db.Exec(`UPDATE agents SET runtime_descriptor='{}' WHERE run_id=$1`, first.Identity.RunID); err != nil {
			t.Fatal(err)
		}
	case "payload_conflict":
		if _, err := db.Exec(`UPDATE agent_lifecycle_diagnostic_outbox SET payload='{}' WHERE outbox_id=$1`, first.OutboxID); err != nil {
			t.Fatal(err)
		}
		if _, err := projectDiagnosticTestBatch(ctx, selected, 100); err == nil {
			t.Fatal("accepted conflicting payload")
		}
		assertDiagnosticCounts(t, db, first.OutboxID, 0, 1)
		return
	case "timestamp_conflict":
		if _, err := db.Exec(`UPDATE agent_lifecycle_diagnostic_outbox SET created_at=$2 WHERE outbox_id=$1`, first.OutboxID, first.CreatedAt.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := projectDiagnosticTestBatch(ctx, selected, 100); err == nil {
			t.Fatal("accepted conflicting occurrence timestamp")
		}
		assertDiagnosticCounts(t, db, first.OutboxID, 0, 1)
		return
	case "missing_ack":
		if _, err := projectDiagnosticTestBatch(ctx, selected, 100); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`UPDATE agent_lifecycle_diagnostic_outbox SET projected_at=NULL,projection=NULL WHERE outbox_id=$1`, first.OutboxID); err != nil {
			t.Fatal(err)
		}
		if _, err := projectDiagnosticTestBatch(ctx, selected, 100); err == nil {
			t.Fatal("repaired corrupt event-without-ack pair")
		}
		assertDiagnosticCounts(t, db, first.OutboxID, 1, 1)
		return
	case "missing_history":
		if _, err := db.Exec(`DELETE FROM agent_lifecycle_transition_facts WHERE operation_id=$1`, first.OperationID); err != nil {
			t.Fatal(err)
		}
		if _, err := projectDiagnosticTestBatch(ctx, selected, 100); err == nil {
			t.Fatal("accepted missing transition history")
		}
		assertDiagnosticCounts(t, db, first.OutboxID, 0, 1)
		return
	case "cancelled":
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := projectDiagnosticTestBatch(cancelled, selected, 100); err == nil {
			t.Fatal("cancelled projection succeeded")
		}
		assertDiagnosticCounts(t, db, first.OutboxID, 0, 1)
	case "batch_prefix":
		second := enqueueDiagnosticTestAgent(t, selected, "diagnostic-worker-two")
		cleanup = installDiagnosticFailureTrigger(t, db, sqlite, "agent_lifecycle_diagnostic_outbox", "UPDATE", second.OutboxID)
		if n, err := projectDiagnosticTestBatch(ctx, selected, 100); err == nil || n != 1 {
			t.Fatalf("batch failure n=%d err=%v", n, err)
		}
		assertDiagnosticCounts(t, db, first.OutboxID, 1, 0)
		assertDiagnosticCounts(t, db, second.OutboxID, 0, 1)
		cleanup()
		cleanup = nil
	}
	if cleanup != nil {
		defer cleanup()
	}
	if _, err := projectDiagnosticTestBatch(ctx, selected, 100); err != nil {
		t.Fatal(err)
	}
	assertDiagnosticCounts(t, db, first.OutboxID, 1, 0)
	if n, err := projectDiagnosticTestBatch(ctx, selected, 100); err != nil || n != 0 {
		t.Fatalf("settled replay n=%d err=%v", n, err)
	}
	var runID, persistedMode, parent string
	if err := db.QueryRow(`SELECT run_id,execution_mode,COALESCE(CAST(source_event_id AS TEXT),'') FROM events WHERE event_id=$1`, diagnosticEventID(first.OutboxID)).Scan(&runID, &persistedMode, &parent); err != nil {
		t.Fatal(err)
	}
	if runID != first.Identity.RunID || persistedMode != "live" || parent != "" {
		t.Fatalf("wrong immutable event facts run=%s mode=%s parent=%s", runID, persistedMode, parent)
	}
}

func assertDiagnosticCounts(t *testing.T, db *sql.DB, id string, events, pending int) {
	t.Helper()
	var actualEvents, actualPending int
	if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=$1`, diagnosticEventID(id)).Scan(&actualEvents); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM agent_lifecycle_diagnostic_outbox WHERE outbox_id=$1 AND projected_at IS NULL`, id).Scan(&actualPending); err != nil {
		t.Fatal(err)
	}
	if actualEvents != events || actualPending != pending {
		t.Fatalf("event/pending=%d/%d want %d/%d", actualEvents, actualPending, events, pending)
	}
}

func installDiagnosticCommitFailure(t *testing.T, db *sql.DB, sqlite bool) func() {
	t.Helper()
	name := "diagnostic_commit_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if sqlite {
		// A deferred foreign key fails at COMMIT, after both the event and ack
		// statements have succeeded. These tables exist only in this test store.
		for _, q := range []string{
			"CREATE TABLE " + name + "_parent (id TEXT PRIMARY KEY)",
			"CREATE TABLE " + name + "_child (id TEXT REFERENCES " + name + "_parent(id) DEFERRABLE INITIALLY DEFERRED)",
			"CREATE TRIGGER " + name + " AFTER UPDATE ON agent_lifecycle_diagnostic_outbox BEGIN INSERT INTO " + name + "_child(id) VALUES ('absent'); END",
		} {
			if _, err := db.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
		return func() {
			for _, q := range []string{"DROP TRIGGER " + name, "DROP TABLE " + name + "_child", "DROP TABLE " + name + "_parent"} {
				if _, err := db.Exec(q); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if _, err := db.Exec("CREATE FUNCTION " + name + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected diagnostic commit failure'; END $$"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE CONSTRAINT TRIGGER " + name + " AFTER UPDATE ON agent_lifecycle_diagnostic_outbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION " + name + "()"); err != nil {
		t.Fatal(err)
	}
	return func() {
		for _, q := range []string{"DROP TRIGGER " + name + " ON agent_lifecycle_diagnostic_outbox", "DROP FUNCTION " + name + "()"} {
			if _, err := db.Exec(q); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func installDiagnosticFailureTrigger(t *testing.T, db *sql.DB, sqlite bool, table, operation, id string) func() {
	t.Helper()
	name := "diagnostic_failure_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	condition := ""
	if id != "" {
		condition = " WHEN NEW.outbox_id='" + id + "'"
	}
	query := "CREATE TRIGGER " + name + " BEFORE " + operation + " ON " + table + " FOR EACH ROW" + condition + " BEGIN SELECT RAISE(ABORT,'injected diagnostic failure'); END"
	if !sqlite {
		if _, err := db.Exec("CREATE FUNCTION " + name + "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected diagnostic failure'; END $$"); err != nil {
			t.Fatal(err)
		}
		if condition != "" {
			condition = " WHEN (NEW.outbox_id='" + id + "')"
		}
		query = "CREATE TRIGGER " + name + " BEFORE " + operation + " ON " + table + " FOR EACH ROW" + condition + " EXECUTE FUNCTION " + name + "()"
	}
	if _, err := db.Exec(query); err != nil {
		t.Fatal(err)
	}
	return func() {
		query := "DROP TRIGGER " + name
		if !sqlite {
			query += " ON " + table
		}
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
		if !sqlite {
			if _, err := db.Exec("DROP FUNCTION " + name + "()"); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func projectDiagnosticTestBatch(ctx context.Context, selected diagnosticProjectionTestStore, limit int) (int, error) {
	items, err := selected.ListPendingAgentLifecycleDiagnostics(ctx, limit)
	if err != nil {
		return 0, err
	}
	logger := runtimepkg.NewRuntimeLogger(selected, executionposture.Live, nil)
	for i, item := range items {
		if err := logger.ProjectLifecycleDiagnostic(ctx, item); err != nil {
			return i, err
		}
	}
	return len(items), nil
}

func diagnosticEventID(outboxID string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("swarm:lifecycle-diagnostic:"+outboxID)).String()
}
