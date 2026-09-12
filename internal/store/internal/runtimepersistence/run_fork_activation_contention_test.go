package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
	modernsqlite "modernc.org/sqlite"
)

// SQL triggers pause real operations after they own the durable write frontier.
// Neither a pool waiter nor a goroutine-start signal counts as contention.
func TestRunForkActivationFrontierContentionBothStores(t *testing.T) {
	exerciseForkActivationFrontierContention(t, false)
}

func TestSelectedRunForkActivationFrontierContentionBothStores(t *testing.T) {
	exerciseForkActivationFrontierContention(t, true)
}

func exerciseForkActivationFrontierContention(t *testing.T, selected bool) {
	for _, backend := range eventRecordContractBackends() {
		for _, first := range []string{"writer", "activation"} {
			for _, outcome := range []string{"commit", "rollback", "cancel_winner", "cancel_loser"} {
				t.Run(backend.name+"/"+first+"/"+outcome, func(t *testing.T) {
					barrier := newForkContentionBarrier(t, backend.name, outcome == "rollback" || outcome == "cancel_loser")
					f := newForkContentionFixture(t, backend)
					staged, selectedRequest := stageForkContentionFixture(t, f, selected)
					var err error
					writer := f.store
					observer := f.db
					if backend.name == "sqlite" {
						// A separate backend and pool must contend in SQLite, not on
						// the per-backend mutation token or a one-connection pool.
						var seq int
						var name, path string
						if err := f.db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
							t.Fatal(err)
						}
						other := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
						writer = other
						if first == "writer" {
							f.store = barrier.observeSQLiteStore(t, f.store.(*SQLiteRuntimeStore), path)
						} else {
							writer = barrier.observeSQLiteStore(t, other, path)
						}
						observer, err = sql.Open("sqlite", path)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() { _ = observer.Close() })
					} else {
						f.db.SetMaxOpenConns(12)
					}
					barrier.install(t, f.db, f.runID, staged.ForkRunID, first)
					before := snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres")
					ctx, cancel := context.WithTimeout(f.ctx, 25*time.Second)
					defer cancel()
					winnerCtx, cancelWinner := context.WithCancel(ctx)
					defer cancelWinner()
					loserCtx, cancelLoser := context.WithCancel(ctx)
					defer cancelLoser()
					loserCtx = context.WithValue(loserCtx, forkContentionContextKey{}, barrier)
					state := f.state
					state.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
					state.ExpectedRevision, state.ExpectedState = 2, "pending"
					state.CurrentState, state.Name = "done", "Committed Concurrent Writer"
					state.UpdatedAt, state.EnteredStageAt = time.Now().UTC(), time.Now().UTC()
					invoke := func(ctx context.Context, operation string) forkContentionResult {
						if operation == "writer" {
							_, err := writer.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: state})
							return forkContentionResult{err: err}
						}
						if selected {
							result, err := f.store.(runforkexecution.SelectedContractForkLifecycle).ActivateRunForkForSelectedContractExecution(ctx, selectedRequest)
							return forkContentionResult{activation: result, err: err}
						}
						result, err := f.store.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: staged.ForkRunID, AllowSourceFreeze: true})
						return forkContentionResult{activation: result, err: err}
					}
					second := "writer"
					if first == "writer" {
						second = "activation"
					}
					winner := make(chan forkContentionResult, 1)
					loser := make(chan forkContentionResult, 1)
					winnerFinished, loserFinished := make(chan struct{}), make(chan struct{})
					loserStarted := false
					go func() { defer close(winnerFinished); winner <- invoke(winnerCtx, first) }()
					t.Cleanup(func() {
						cancelWinner()
						cancelLoser()
						barrier.release()
						barrier.resume()
						for _, finished := range []<-chan struct{}{winnerFinished, loserFinished} {
							if finished == loserFinished && !loserStarted {
								continue
							}
							select {
							case <-finished:
							case <-time.After(5 * time.Second):
								t.Error("contention worker did not join cleanup")
							}
						}
					})
					winnerPID := barrier.awaitWinner(t, ctx, observer, winner)
					// PostgreSQL queues an observer lock ahead of the losing operation.
					// SQLite holds the loser after a real BUSY rollback, before retry.
					gate := barrier.queueGate(t, ctx, observer, f.runID, first, winnerPID)
					loserStarted = true
					go func() { defer close(loserFinished); loser <- invoke(loserCtx, second) }()
					barrier.awaitContender(t, ctx, observer, winnerPID, gate, loser)
					if outcome == "cancel_loser" {
						cancelLoser()
						barrier.resume()
					}
					if outcome == "cancel_winner" {
						cancelWinner()
					}
					barrier.release()
					won := awaitForkContentionResult(t, ctx, winner)
					switch outcome {
					case "commit":
						if won.err != nil || (first == "activation" && !won.activation.Activated) {
							t.Fatalf("winning %s did not commit: %#v", first, won)
						}
					case "cancel_winner":
						requireForkContentionCancellation(t, won.err)
					default:
						if won.err == nil || !strings.Contains(won.err.Error(), "h18_requested_rollback") {
							t.Fatalf("winning transaction did not roll back at SQL barrier: %v", won.err)
						}
					}
					barrier.awaitGate(t, ctx, gate)
					winnerOnly := snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres")
					if outcome != "commit" && !reflect.DeepEqual(before, winnerOnly) {
						t.Fatal("rolled-back/cancelled winner changed complete source/child/application tables")
					}
					barrier.openGate(t, gate)
					barrier.resume()
					if outcome == "cancel_loser" {
						got := awaitForkContentionResult(t, ctx, loser)
						requireForkContentionCancellation(t, got.err)
						if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres")) {
							t.Fatal("cancelled loser and rolled-back winner left durable effects")
						}
						return
					}
					lost := awaitForkContentionResult(t, ctx, loser)
					if outcome == "commit" {
						if selected && first == "writer" {
							if lost.err != nil || !lost.activation.Activated || !lost.activation.SourceAdvancedAfterFork || lost.activation.SourceFrozen || lost.activation.BranchDivergence == nil {
								t.Fatalf("selected activation must allow lawful source advancement: %v; %+v", lost.err, lost.activation)
							}
							var sourceState, childState, continued string
							if err := observer.QueryRow(`SELECT status,COALESCE(CAST(continued_as_run_id AS TEXT),'') FROM runs WHERE run_id=$1`, f.runID).Scan(&sourceState, &continued); err != nil {
								t.Fatal(err)
							}
							if err := observer.QueryRow(`SELECT status FROM runs WHERE run_id=$1`, staged.ForkRunID).Scan(&childState); err != nil {
								t.Fatal(err)
							}
							if sourceState != "running" || continued != "" || childState != "running" {
								t.Fatalf("selected divergence froze source or failed child activation: %s/%s/%s", sourceState, continued, childState)
							}
							if !reflect.DeepEqual(forkContentionRowsForRun(t, winnerOnly, f.runID), forkContentionRowsForRun(t, snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres"), f.runID)) {
								t.Fatal("selected branch changed complete committed source-run rows")
							}
							requireForkContentionState(t, observer, f, staged.ForkRunID, true, true)
							return
						}
						if first == "writer" {
							if _, fact, ok := runForkReplayResumeBlockerFromError(lost.err); !ok || fact != runfork.RunForkReplayResumeFactSourceAdvanced || lost.activation.Activated {
								t.Fatalf("generic losing activation must retain source-advanced refusal: %#v", lost)
							}
						} else if !errors.Is(lost.err, runlifecycle.ErrRunNotActive) {
							t.Fatalf("losing writer must observe committed source freeze: %v", lost.err)
						}
						if !reflect.DeepEqual(winnerOnly, snapshotForkHistoricalExecutionTables(t, observer, backend.name == "postgres")) {
							t.Fatal("rejected loser changed complete winner-only application contents")
						}
					} else if lost.err != nil || (second == "activation" && !lost.activation.Activated) {
						t.Fatalf("loser did not proceed lawfully after winner rollback/cancellation: %v; activation=%+v", lost.err, lost.activation)
					}
					committedOperation := first
					if outcome != "commit" {
						committedOperation = second
					}
					requireForkContentionState(t, observer, f, staged.ForkRunID, committedOperation == "writer", committedOperation == "activation")
				})
			}
		}
	}
}

func requireForkContentionState(t *testing.T, db *sql.DB, f snapshotOwnershipFixture, child string, writerCommitted, activated bool) {
	t.Helper()
	for _, id := range []string{f.runID, child} {
		var state, name string
		var revision int64
		if err := db.QueryRow(`SELECT current_state,name,revision FROM entity_state WHERE run_id=$1 AND entity_id=$2`, id, f.entityID).Scan(&state, &name, &revision); err != nil {
			t.Fatal(err)
		}
		wantState, wantName, wantRevision := "pending", "At R", int64(1)
		if id == f.runID {
			wantRevision = 2
			if writerCommitted {
				wantState, wantName, wantRevision = "done", "Committed Concurrent Writer", 3
			}
		}
		if state != wantState || name != wantName || revision != wantRevision {
			t.Fatalf("%s entity state=%s/%s/r%d, want %s/%s/r%d", id, state, name, revision, wantState, wantName, wantRevision)
		}
	}
	var status string
	if err := db.QueryRow(`SELECT status FROM runs WHERE run_id=$1`, child).Scan(&status); err != nil {
		t.Fatal(err)
	}
	want := runfork.RunForkMaterializedStatus
	if activated {
		want = runfork.RunForkActivatedStatus
	}
	if status != want {
		t.Fatalf("child status=%s, want %s", status, want)
	}
}

func forkContentionRowsForRun(t *testing.T, snapshot map[string][]string, run string) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for table, columns := range snapshot {
		if !strings.HasSuffix(table, "/columns") {
			continue
		}
		column := -1
		for i, name := range columns {
			if name == "run_id" {
				column = i
			}
		}
		if column < 0 {
			continue
		}
		name := strings.TrimSuffix(table, "/columns")
		out[name] = []string{}
		for _, raw := range snapshot[name] {
			var row []any
			if err := json.Unmarshal([]byte(raw), &row); err != nil {
				t.Fatal(err)
			}
			if row[column] == run {
				out[name] = append(out[name], raw)
			}
		}
	}
	return out
}

func newForkContentionFixture(t *testing.T, backend eventRecordContractBackend) snapshotOwnershipFixture {
	t.Helper()
	opened := backend.open(t)
	// The receipt fixture freezes its SQLite clock in July. This activation
	// fixture uses current real writer timestamps, including run start time.
	if store, ok := opened.store.(*SQLiteRuntimeStore); ok {
		store.nowFn = func() time.Time { return time.Now().UTC() }
	}
	bundle := loadCanonicalSelectedContractStoreSource(t)
	f := snapshotOwnershipFixture{store: opened.store.(snapshotOwnershipStore), db: opened.db, runID: uuid.NewString(), entityID: uuid.NewString(), eventID: uuid.NewString()}
	f.ctx = runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(bundle.SourceArtifact.BundleHash()), f.runID)
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	requireRunFixtureForTest(t, f.ctx, opened.store, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: f.runID, StartedAt: at, BundleHash: bundle.SourceArtifact.BundleHash(), Artifact: bundle.SourceArtifact})
	if _, err := f.store.SetupScenarioEntities(f.ctx, runtimepipeline.ScenarioSetupRequest{RunID: f.runID, CreatedAt: at,
		Entities: []runtimepipeline.ScenarioSetupEntityRequest{{Alias: "subject", EntityID: f.entityID, FlowInstance: "flow-a/1", EntityType: "default", CurrentState: "pending", Fields: map[string]any{"name": "At R"}}},
	}); err != nil {
		t.Fatal(err)
	}
	f.state = stateOnlyWorkflowEngineMutationRecord(t, f.runID, "flow-a/1", "flow-a/1", f.entityID, "pending", 1, at)
	f.state.CurrentState, f.state.EntityType, f.state.Name = "pending", "default", "At R"
	f.state.Fields = json.RawMessage(`{"name":"At R"}`)
	if _, err := f.store.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: f.state}); err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngress(f.eventID, "item.received", "h18-fixture", "", json.RawMessage(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, event); err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRunForkActivationContentionFixtureControlBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, selected := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/selected=%t", backend.name, selected), func(t *testing.T) {
				f := newForkContentionFixture(t, backend)
				staged, req := stageForkContentionFixture(t, f, selected)
				var activation runfork.RunForkActivation
				var err error
				if selected {
					activation, err = f.store.(runforkexecution.SelectedContractForkLifecycle).ActivateRunForkForSelectedContractExecution(f.ctx, req)
				} else {
					activation, err = f.store.ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{ForkRunID: staged.ForkRunID, AllowSourceFreeze: true})
				}
				if err != nil || !activation.Activated || !activation.SourceFrozen {
					t.Fatalf("uncontended canonical activation: %v; result=%+v", err, activation)
				}
			})
		}
	}
}

func stageForkContentionFixture(t *testing.T, f snapshotOwnershipFixture, selected bool) (runfork.RunForkMaterialization, runfork.RunForkSelectedContractExecutionActivateRequest) {
	t.Helper()
	if !selected {
		staged, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
		if err != nil {
			t.Fatal(err)
		}
		return staged, runfork.RunForkSelectedContractExecutionActivateRequest{}
	}
	store := f.store.(interface {
		runforkexecution.SelectedContractForkLifecycle
		runforkexecution.SelectedContractReplayPersistence
		runforkexecution.SourceArtifactSelectedContractSourceStore
	})
	repo := canonicalrouting.RepoRoot(t)
	selection := runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
	loader := runforkexecution.SourceArtifactSelectedContractSourceLoader{RepoRoot: repo, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo), Store: store}
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(f.ctx, runforkexecution.SelectedContractSourceLoadRequest{SourceRunID: f.runID, Selection: selection})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Cleanup != nil {
		t.Cleanup(func() {
			if err := loaded.Cleanup(); err != nil {
				t.Error(err)
			}
		})
	}
	request := prepareSelectedStoreMaterializationForTest(t, f.ctx, f.store, f.runID, f.eventID, selection)
	_, ids, _, err := runfork.RunForkContractFrontierEvidenceBinding(request.FrontierAdmission)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := store.MaterializeRunForkForSelectedContractExecution(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return staged, runfork.RunForkSelectedContractExecutionActivateRequest{ForkRunID: staged.ForkRunID, AllowSourceFreeze: true, ExecutionSource: loaded.Source, AllowedSourceEventIDs: ids,
		FrontierAdmission: request.FrontierAdmission, RouteTopology: request.RouteTopology, RecipientPlanning: request.RecipientPlanning}
}

type forkContentionResult struct {
	activation runfork.RunForkActivation
	err        error
}

func awaitForkContentionResult(t *testing.T, ctx context.Context, done <-chan forkContentionResult) forkContentionResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		t.Fatal("contention operation did not finish: ", ctx.Err())
		return forkContentionResult{}
	}
}

func requireForkContentionCancellation(t *testing.T, err error) {
	t.Helper()
	if err == nil || (!errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "canceling statement due to user request")) {
		t.Fatalf("expected caller/driver cancellation, got %v", err)
	}
}

type forkContentionBarrier struct {
	backend, name                                string
	rollback                                     bool
	entered                                      chan struct{}
	busy                                         chan struct{}
	released                                     chan struct{}
	resumed                                      chan struct{}
	enterOnce, busyOnce, releaseOnce, resumeOnce sync.Once
	control                                      *sql.Conn
	key                                          int64
}

func newForkContentionBarrier(t *testing.T, backend string, rollback bool) *forkContentionBarrier {
	t.Helper()
	b := &forkContentionBarrier{backend: backend, name: "h18_" + strings.ReplaceAll(uuid.NewString(), "-", ""), rollback: rollback,
		entered: make(chan struct{}), busy: make(chan struct{}), released: make(chan struct{}), resumed: make(chan struct{})}
	t.Cleanup(func() { b.release(); b.resume() })
	if backend == "sqlite" {
		if err := modernsqlite.RegisterScalarFunction(b.name, 0, func(*modernsqlite.FunctionContext, []driver.Value) (driver.Value, error) {
			first := false
			b.enterOnce.Do(func() { first = true; close(b.entered) })
			if !first {
				return int64(1), nil
			}
			<-b.released
			if b.rollback {
				return nil, errors.New("h18_requested_rollback")
			}
			return int64(1), nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func (b *forkContentionBarrier) install(t *testing.T, db *sql.DB, source, child, first string) {
	t.Helper()
	table, predicate := "run_fork_revision_heads", fmt.Sprintf("OLD.run_id='%s'", source)
	if first == "activation" {
		table, predicate = "runs", fmt.Sprintf("OLD.run_id='%s' AND NEW.status='running' AND OLD.status<>'running'", child)
	}
	if b.backend == "sqlite" {
		_, err := db.Exec(fmt.Sprintf(`CREATE TRIGGER %s BEFORE UPDATE ON %s WHEN %s BEGIN SELECT %s(); END`, b.name, table, predicate, b.name))
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	var err error
	b.control, err = db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.control.Close() })
	b.key = time.Now().UnixNano() & 0x7fffffff
	if _, err := b.control.ExecContext(context.Background(), `SELECT pg_advisory_lock($1)`, b.key); err != nil {
		t.Fatal(err)
	}
	failure := ""
	if b.rollback {
		failure = `RAISE EXCEPTION 'h18_requested_rollback';`
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE SEQUENCE %s_once;
CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('%s_once')=1 THEN PERFORM pg_advisory_xact_lock(%d); %s END IF; RETURN NEW; END $$;
CREATE TRIGGER %s BEFORE UPDATE ON %s FOR EACH ROW WHEN (%s) EXECUTE FUNCTION %s()`, b.name, b.name, b.name, b.key, failure, b.name, table, predicate, b.name))
	if err != nil {
		t.Fatal(err)
	}
}

func (b *forkContentionBarrier) release() {
	b.releaseOnce.Do(func() {
		close(b.released)
		if b.control != nil {
			_, _ = b.control.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, b.key)
		}
	})
}
func (b *forkContentionBarrier) resume() { b.resumeOnce.Do(func() { close(b.resumed) }) }

func (b *forkContentionBarrier) awaitWinner(t *testing.T, ctx context.Context, db *sql.DB, done <-chan forkContentionResult) int {
	t.Helper()
	if b.backend == "sqlite" {
		select {
		case <-b.entered:
			return 0
		case got := <-done:
			t.Fatalf("winner returned before real write barrier: %v", got.err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	var pid int
	forkContentionPoll(t, ctx, func() bool {
		err := db.QueryRowContext(ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND objid=$1 AND NOT granted`, b.key).Scan(&pid)
		if err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		select {
		case got := <-done:
			t.Fatalf("winner returned before PostgreSQL barrier: %v", got.err)
		default:
		}
		return err == nil
	})
	return pid
}

type forkContentionGate struct {
	tx       *sql.Tx
	pid      int
	acquired chan error
}

func (b *forkContentionBarrier) queueGate(t *testing.T, ctx context.Context, db *sql.DB, run, first string, winnerPID int) *forkContentionGate {
	t.Helper()
	if b.backend == "sqlite" {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	g := &forkContentionGate{tx: tx, acquired: make(chan error, 1)}
	if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&g.pid); err != nil {
		t.Fatal(err)
	}
	table := "runs"
	if first == "writer" {
		table = "run_fork_revision_heads"
	}
	go func() {
		_, err := tx.ExecContext(ctx, `SELECT run_id FROM `+table+` WHERE run_id=$1 FOR UPDATE`, run)
		g.acquired <- err
	}()
	forkContentionPoll(t, ctx, func() bool {
		var waiting bool
		if err := db.QueryRowContext(ctx, `SELECT $1=ANY(pg_blocking_pids($2))`, winnerPID, g.pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		return waiting
	})
	return g
}

func (b *forkContentionBarrier) awaitContender(t *testing.T, ctx context.Context, db *sql.DB, winnerPID int, gate *forkContentionGate, done <-chan forkContentionResult) {
	t.Helper()
	if b.backend == "sqlite" {
		select {
		case <-b.busy:
			return
		case got := <-done:
			t.Fatalf("contender returned without actual SQLite BUSY: %#v", got)
		case <-ctx.Done():
			t.Fatal("contender never reached SQLite write lock: ", ctx.Err())
		}
	}
	forkContentionPoll(t, ctx, func() bool {
		var waiting bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND pid<>$1 AND $2=ANY(pg_blocking_pids(pid)))`, gate.pid, winnerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-done:
			t.Fatalf("contender returned before PostgreSQL lock wait: %#v", got)
		default:
		}
		return waiting
	})
}

func (b *forkContentionBarrier) awaitGate(t *testing.T, ctx context.Context, gate *forkContentionGate) {
	t.Helper()
	if gate == nil {
		return
	}
	select {
	case err := <-gate.acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("observer frontier gate: ", ctx.Err())
	}
}
func (b *forkContentionBarrier) openGate(t *testing.T, gate *forkContentionGate) {
	t.Helper()
	if gate != nil {
		if err := gate.tx.Rollback(); err != nil {
			t.Fatal(err)
		}
	}
}

func forkContentionPoll(t *testing.T, ctx context.Context, ready func() bool) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !ready() {
		select {
		case <-ctx.Done():
			t.Fatal("observable database barrier: ", ctx.Err())
		case <-tick.C:
		}
	}
}

// The observer delegates every statement to the real SQLite driver. It pauses
// only after a real BUSY error and successful rollback on the marked loser,
// retaining no read lock and fabricating neither errors nor transaction results.
type forkContentionContextKey struct{}
type forkContentionDriver struct {
	driver.Driver
	barrier *forkContentionBarrier
}
type forkContentionConn struct {
	driver.Conn
	barrier   *forkContentionBarrier
	contended bool
}
type forkContentionTx struct {
	driver.Tx
	conn *forkContentionConn
}

func (b *forkContentionBarrier) observeSQLiteStore(t *testing.T, original *SQLiteRuntimeStore, path string) *SQLiteRuntimeStore {
	t.Helper()
	name := b.name + "_driver"
	sql.Register(name, &forkContentionDriver{Driver: original.backend.ConstructionHandle().Driver(), barrier: b})
	db, err := sql.Open(name, "file:"+path+"?_pragma=busy_timeout(1)&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	backend, err := sqlitebackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	store, err := newSQLiteStoreComposition(original.schema, backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	registerTestAuthorActivityCatalog(t, store)
	return store
}

func (d *forkContentionDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.Driver.Open(name)
	if err != nil {
		return nil, err
	}
	return &forkContentionConn{Conn: conn, barrier: d.barrier}, nil
}
func (c *forkContentionConn) observe(ctx context.Context, err error) {
	if ctx.Value(forkContentionContextKey{}) == c.barrier && sqliteRuntimeMutationBusyError(err) {
		c.contended = true
	}
}
func (c *forkContentionConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.contended = false
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	c.observe(ctx, err)
	if err != nil {
		c.pauseAfterRollback()
		return nil, err
	}
	return &forkContentionTx{Tx: tx, conn: c}, nil
}
func (c *forkContentionConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.Conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	c.observe(ctx, err)
	return result, err
}
func (c *forkContentionConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	c.observe(ctx, err)
	return rows, err
}
func (tx *forkContentionTx) Rollback() error {
	err := tx.Tx.Rollback()
	if err == nil {
		tx.conn.pauseAfterRollback()
	}
	return err
}
func (c *forkContentionConn) pauseAfterRollback() {
	if c.contended {
		c.barrier.busyOnce.Do(func() { close(c.barrier.busy) })
		<-c.barrier.resumed
	}
}
