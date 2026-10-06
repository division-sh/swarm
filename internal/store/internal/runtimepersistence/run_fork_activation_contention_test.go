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
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	decisionstore "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard"
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
					f := newForkActivationFrontierFixture(t, backend, selected)
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
					invoke := func(ctx context.Context, operation string) forkContentionResult {
						if operation == "writer" {
							err := f.write(ctx, writer)
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
						} else if !errors.Is(lost.err, runlifecycle.ErrRunNotActive) && (lost.err == nil || !strings.Contains(lost.err.Error(), "gate route run "+f.runID+" is not routable in status forked")) {
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

func requireForkContentionState(t *testing.T, db *sql.DB, f forkContentionFixture, child string, writerCommitted, activated bool) {
	t.Helper()
	for _, id := range []string{f.runID, child} {
		var state, name string
		var revision int64
		if err := db.QueryRow(`SELECT f.current_state,e.name,f.revision FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.entity_id=f.entity_id WHERE f.run_id=$1 AND f.entity_id=$2`, id, id).Scan(&state, &name, &revision); err != nil {
			t.Fatal(err)
		}
		wantState, wantName, wantRevision := "pending", "At R", int64(1)
		if id == f.runID {
			wantRevision = 2
			if f.gateOwned {
				wantRevision = 3
			}
			if writerCommitted {
				wantRevision++
				if f.gateOwned {
					wantState = "done"
				} else {
					wantName = "After writer"
				}
			} else if activated && f.gateOwned {
				// Freezing the source durably supersedes its exact open gate.
				// That header CAS is a real activation write, not a field shadow.
				wantRevision = 4
			}
		}
		if state != wantState || name != wantName || revision != wantRevision {
			t.Fatalf("%s entity state=%s/%s/r%d, want %s/%s/r%d", id, state, name, revision, wantState, wantName, wantRevision)
		}
	}
	if !f.gateOwned {
		var raw string
		if err := db.QueryRow(`SELECT bookkeeping FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var before, after map[string]any
		if err := json.Unmarshal(f.state.Bookkeeping, &before); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(raw), &after); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, after) {
			t.Fatal("root-only field mutation or freeze repeated construction/stage entry")
		}
		requireForkContentionChildStatus(t, db, child, activated)
		return
	}
	var accumulatorRaw string
	if err := db.QueryRow(`SELECT accumulator FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID).Scan(&accumulatorRaw); err != nil {
		t.Fatal(err)
	}
	var buckets map[string]map[string]any
	if err := json.Unmarshal([]byte(accumulatorRaw), &buckets); err != nil {
		t.Fatal(err)
	}
	gate, found, err := gateruntime.Load(buckets, ".", "review")
	wantGate, wantOpen := gateruntime.StatusDecisionCommitted, 1
	if writerCommitted {
		wantGate, wantOpen = gateruntime.StatusRouted, 0
	} else if activated {
		wantGate, wantOpen = gateruntime.StatusSuperseded, 0
	}
	if err != nil || !found || gate.CardID != f.cardID || gate.DecisionEventID != f.eventID || gate.Status != wantGate {
		t.Fatalf("canonical gate disposition: %+v found=%t err=%v; want %s", gate, found, err, wantGate)
	}
	dialect := decisionstore.SummaryDialectPostgres
	if _, sqlite := f.store.(*SQLiteRuntimeStore); sqlite {
		dialect = decisionstore.SummaryDialectSQLite
	}
	summary, err := decisionstore.ReadRunSummary(f.ctx, db, dialect, f.runID)
	if err != nil || summary.OpenGateObligations != wantOpen || summary.MalformedObligations != 0 {
		t.Fatalf("completion authority disagrees with canonical gate: %+v err=%v; want open=%d", summary, err, wantOpen)
	}
	if activated && !writerCommitted && !decisionGateStatusMutationExists(t, f.ctx, db, dialect == decisionstore.SummaryDialectPostgres, f.runID, f.entityID, string(gateruntime.StatusSuperseded)) {
		t.Fatal("terminal gate supersession omitted its canonical mutation journal")
	}
	if writerCommitted {
		var raw string
		if err := db.QueryRow(`SELECT bookkeeping FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, f.runID, f.entityID).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var bookkeeping map[string]any
		if err := json.Unmarshal([]byte(raw), &bookkeeping); err != nil {
			t.Fatal(err)
		}
		entry, found, err := workflowlifecycle.LoadStageEntry(bookkeeping)
		if err != nil || !found || entry.Cause != "gate" || entry.EventID != f.eventID || entry.OccurrenceID != f.cardID || entry.TransitionID == "" || entry.Stage != "done" {
			t.Fatalf("writer did not commit its exact admitted new StageEntry: %+v %t %v", entry, found, err)
		}
	}
	requireForkContentionChildStatus(t, db, child, activated)
}

func requireForkContentionChildStatus(t *testing.T, db *sql.DB, child string, activated bool) {
	t.Helper()
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

type forkContentionFixture struct {
	snapshotOwnershipFixture
	write     func(context.Context, snapshotOwnershipStore) error
	cardID    string
	gateOwned bool
	source    semanticview.Source
}

func newForkActivationFrontierFixture(t *testing.T, backend eventRecordContractBackend, selected bool) forkContentionFixture {
	t.Helper()
	if selected {
		return newForkContentionFixture(t, backend)
	}
	f := newReceiverConfigActivationFixtureWithDocuments(t, backend.name, false, map[string]string{
		"schema.yaml":   "name: generic-frontier\nstages:\n  pending: {initial: true}\n  done: {}\n",
		"entities.yaml": "subject:\n  name: {type: text, initial: 'At R'}\n",
		"events.yaml":   "item.received:\n",
	}, nil)
	runID := runtimecorrelation.RunIDFromContext(f.ctx)
	fixture := forkContentionFixture{snapshotOwnershipFixture: snapshotOwnershipFixture{
		store: f.store.(snapshotOwnershipStore), db: f.db, ctx: f.ctx,
		runID: runID, entityID: runID, eventID: uuid.NewString(),
	}, source: semanticview.Wrap(f.bundle)}
	req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
	req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
	req.OccurredAt = time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	plan := constructHistoricalSourceFixture(t, fixture.ctx, f.store.(agentFixtureFlowStore), req)
	persisted, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	fixture.state = persisted.State
	fixture.state.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	fixture.state.ExpectedState, fixture.state.ExpectedRevision = "pending", 1
	fixture.state.Name = "At R"
	if _, err := fixture.store.CommitWorkflowEngineMutation(fixture.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: fixture.state}); err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngress(fixture.eventID, "item.received", "frontier-fixture", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticPipelineProcessedEventFixture(fixture.ctx, fixture.store, event); err != nil {
		t.Fatal(err)
	}
	fixture.write = func(ctx context.Context, selected snapshotOwnershipStore) error {
		state := fixture.state
		state.ExpectedRevision = 2
		state.Name, state.Fields = "After writer", json.RawMessage(`{"name":"After writer"}`)
		state.UpdatedAt = time.Now().UTC()
		_, err := selected.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: state})
		return err
	}
	return fixture
}

func newForkContentionFixture(t *testing.T, backend eventRecordContractBackend) forkContentionFixture {
	return newForkContentionFixtureForFields(t, backend, true)
}

func newForkContentionFixtureForFields(t *testing.T, backend eventRecordContractBackend, fields bool) forkContentionFixture {
	t.Helper()
	return newConstructedGateFixtureForFields(t, backend, fields, true)
}

func newConstructedGateFixtureForFields(t *testing.T, backend eventRecordContractBackend, fields, decided bool) forkContentionFixture {
	return newConstructedGateFixtureWithOrigin(t, backend, fields, decided, runlifecycle.ScenarioSetupRunOrigin())
}

func newConstructedGateFixtureWithOrigin(t *testing.T, backend eventRecordContractBackend, fields, decided bool, origin runlifecycle.RunOrigin) forkContentionFixture {
	t.Helper()
	opened := backend.open(t)
	// The receipt fixture freezes its SQLite clock in July. This activation
	// fixture uses current real writer timestamps, including run start time.
	if store, ok := opened.store.(*SQLiteRuntimeStore); ok {
		store.nowFn = func() time.Time { return time.Now().UTC() }
	}
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo,
		canonicalrouting.CopyConstructedGateForkControl(t, fields), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	f := forkContentionFixture{snapshotOwnershipFixture: snapshotOwnershipFixture{store: opened.store.(snapshotOwnershipStore), db: opened.db, runID: runID, entityID: runID, eventID: uuid.NewString()}, gateOwned: true, source: semanticview.Wrap(bundle)}
	f.ctx = runtimecorrelation.WithRunID(testAuthorActivityContextForBundle(bundle.SourceArtifact.BundleHash()), f.runID)
	f.ctx, err = eventreceiver.NormalExecution().Bind(f.ctx, executionmode.Live)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	requireRunFixtureForTest(t, f.ctx, opened.store, semanticRunFixture{Origin: origin, RunID: f.runID, StartedAt: at, BundleHash: bundle.SourceArtifact.BundleHash(), Artifact: bundle.SourceArtifact})
	req := sqliteFlowActivationRequest(bundle, ".", f.runID, "", f.runID)
	req.Instance = flowidentity.Stored(req.ContractBundle, ".", f.runID, flowidentity.LogicalInstanceID(f.runID), f.entityID, "")
	req.OccurredAt = at
	plan := constructHistoricalSourceFixture(t, f.ctx, opened.store.(agentFixtureFlowStore), req)
	persisted, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	f.state = persisted.State
	f.state.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	f.state.ExpectedState, f.state.ExpectedRevision = "pending", 1
	// Set historical fields through the ordinary writer; retain the constructor's
	// real initial gate and stage-entry evidence.
	f.state.CurrentState, f.state.EntityType, f.state.Name = "pending", "default", "At R"
	f.state.Fields = json.RawMessage(`{"name":"At R"}`)
	if !fields {
		f.state.EntityType, f.state.Name, f.state.Fields = "", "", json.RawMessage(`{}`)
	}
	if _, err := f.store.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: f.state}); err != nil {
		t.Fatal(err)
	}
	coordinatorFor := func(selected snapshotOwnershipStore) *runtimepipeline.PipelineCoordinator {
		store := selected.(workflowTestSelectedStore)
		opts := completeWorkflowTestCoordinatorOptions(runtimepipeline.NewWorkflowPersistence(store), store)
		opts.Module = runForkGateWorkflowModule{source: semanticview.Wrap(bundle)}
		opts.SourceArtifactFact = mustStoreTestSourceArtifactFact(bundle.SourceArtifact.BundleHash())
		return runtimepipeline.NewPipelineCoordinatorWithOptions(&sqliteFlowActivationBus{}, opts)
	}
	coordinator := coordinatorFor(f.store)
	owner := flowidentity.RunScopedFlowInstance{RunID: f.runID, Route: req.Instance.Route()}
	instance, found, err := coordinator.Load(f.ctx, owner)
	if err != nil || !found {
		t.Fatalf("load canonical gate: found=%v error=%v", found, err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	gates, err := gateruntime.List(carrier.StateBuckets)
	if err != nil || len(gates) != 1 {
		t.Fatalf("constructed gate: %+v %v", gates, err)
	}
	card, err := opened.store.(workflowTestSelectedStore).GetDecisionCard(f.ctx, gates[0].CardID)
	if err != nil {
		t.Fatal(err)
	}
	f.cardID = card.CardID
	if !decided {
		return f
	}
	decidedAt := at.Add(time.Second)
	if err := coordinator.CommitDecision(f.ctx, card, f.eventID, decidedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := DecisionCardDomainForTest(opened.store).ApplyDecisionForTest(f.ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", Fields: admitDecisionCardTestObject(t, map[string]any{}), PrincipalID: "operator", ObservedContentHash: card.CardContentHash, DecisionEventID: f.eventID, Now: decidedAt}); err != nil {
		t.Fatal(err)
	}
	control, err := card.Anchor.ControlRoutingSource()
	if err != nil || control.Kind() != events.RoutingSourceFlowOwnedControl || control.Route() != (events.RouteIdentity{FlowID: ".", FlowInstance: f.runID, EntityID: f.entityID}) {
		t.Fatalf("constructed root gate must retain its exact flow-owned control: %+v %v", control, err)
	}
	// The constructed gate keeps its immutable card route; root ingress/control
	// remains separate. No non-agent delivery history is invented.
	event := eventtest.RuntimeControlWithRoutingSource(f.eventID, "mailbox.card_decided", "platform", "", []byte(`{"card_id":"`+card.CardID+`"}`), 0, f.runID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, f.entityID), control, decidedAt)
	if err := commitSemanticEventFixture(f.ctx, opened.store.(workflowTestSelectedStore), event); err != nil {
		t.Fatal(err)
	}
	// The contender uses the real gate consumer. Its accepted decision creates a
	// new StageEntry in the same native transaction as the lifecycle transition.
	// There is no pending node delivery or fabricated delivery history at R.
	f.write = func(ctx context.Context, selected snapshotOwnershipStore) error {
		pass, _, outcome, err := coordinatorFor(selected).Intercept(ctx, event)
		if err == nil && (pass || !outcome.Committed) {
			return fmt.Errorf("gate contender did not commit: pass=%v outcome=%+v", pass, outcome)
		}
		return err
	}
	return f
}

func TestRunForkActivationContentionFixtureControlBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, selected := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/selected=%t", backend.name, selected), func(t *testing.T) {
				f := newForkActivationFrontierFixture(t, backend, selected)
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

func TestGenericConstructedGateForkPreservesRouteHistoryRefusalBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			f := newForkContentionFixture(t, backend)
			before := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
			for attempt := 0; attempt < 2; attempt++ {
				result, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
				if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerFlowRouteHistoryUnproven) || result.ForkRunID != "" {
					t.Fatalf("generic flow-owned gate history escaped its replay boundary: %+v %v", result, err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
					t.Fatal("generic gate history refusal changed source or child state")
				}
			}
		})
	}
}

func stageForkContentionFixture(t *testing.T, f forkContentionFixture, selected bool) (runfork.RunForkMaterialization, runfork.RunForkSelectedContractExecutionActivateRequest) {
	return stageForkContentionFixtureAt(t, f, selected, f.eventID, false)
}

func stageForkContentionFixtureAt(t *testing.T, f forkContentionFixture, selected bool, selector string, withOperation bool) (runfork.RunForkMaterialization, runfork.RunForkSelectedContractExecutionActivateRequest) {
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
	request := prepareSelectedStoreMaterializationForTest(t, f.ctx, f.store, f.runID, selector, selection)
	if withOperation {
		request.ForkOperation = &runfork.ForkOperationRequest{
			OperationID: uuid.NewString(), Actor: "bearer:branch-point-proof", IdempotencyKey: uuid.NewString(),
			TransportHash: "sha256:branch-point-proof", SourceRunID: f.runID, ForkEventID: selector,
			TargetBundleHash: loaded.SourceArtifactFact.BundleHash(), ContractSelection: selection, AllowSourceFreeze: true,
		}
	}
	_, ids, _, err := runfork.RunForkContractFrontierEvidenceBinding(request.FrontierAdmission)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := store.MaterializeRunForkForSelectedContractExecution(f.ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	return staged, runfork.RunForkSelectedContractExecutionActivateRequest{ForkOperation: request.ForkOperation, ForkRunID: staged.ForkRunID, AllowSourceFreeze: true, ExecutionSource: loaded.Source, AllowedSourceEventIDs: ids,
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

type forkContentionWorkflowStore interface {
	snapshotOwnershipStore
	agentFixtureFlowStore
	testAuthorActivityCatalogRegistrar
	runtimebus.CommitPublicationOwner
}

func (b *forkContentionBarrier) observeSQLiteStore(t *testing.T, original *SQLiteRuntimeStore, path string) forkContentionWorkflowStore {
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
