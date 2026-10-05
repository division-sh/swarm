package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

func TestSelectedBranchPointActivationAtomicityBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, kind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
			for _, status := range []string{"running", "cancelled"} {
				t.Run(backend.name+"/"+string(kind)+"/"+status, func(t *testing.T) {
					f := newBranchPointFixture(t, backend, kind)
					selector := f.eventID
					if kind == runfork.RunForkPointDeploymentRevision {
						selector = ""
					}
					staged, req := stageForkContentionFixtureAt(t, f, true, selector, true)
					if err := f.write(f.ctx, f.store); err != nil {
						t.Fatal(err)
					}
					if status == "cancelled" {
						if _, err := markRunTerminalStatusForTest(f.ctx, f.store, f.runID, status, nil, time.Now().UTC()); err != nil {
							t.Fatal(err)
						}
					}
					before := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
					remove := installReceiverComposedFault(t, f.db, backend.name, "run_fork_selected_contract_branch_divergences", "INSERT", "")
					store := f.store.(runforkexecution.SelectedContractForkLifecycle)
					activation, err := store.ActivateRunForkForSelectedContractExecution(f.ctx, req)
					remove()
					if err == nil || !strings.Contains(err.Error(), "typed_receiver_owner_fault") || activation.Activated {
						t.Fatalf("divergence failure did not refuse activation: %+v err=%v", activation, err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
						t.Fatal("failed divergence write partially committed source/child/author activity/operation state")
					}
					activation, err = store.ActivateRunForkForSelectedContractExecution(f.ctx, req)
					if err != nil || !activation.Activated || activation.SourceFrozen || activation.BranchDivergence == nil {
						t.Fatalf("branch retry did not activate: %+v err=%v", activation, err)
					}
					point := activation.BranchDivergence.ForkPoint
					if point.Kind != kind || point.Revision <= 0 || point.EventID != selector || activation.BranchDivergence.ForkEventID != selector ||
						activation.SourceRunStatus != status || activation.BranchDivergence.SourceRunStatusAfterActivation != status {
						t.Fatalf("activation lost fixed typed point/source state: %+v", activation)
					}
					var persistedKind string
					var revision int64
					var eventID sql.NullString
					if err := f.db.QueryRow(`SELECT fork_point_kind,fork_revision,CAST(fork_event_id AS TEXT)
						FROM run_fork_selected_contract_branch_divergences WHERE fork_run_id=$1`, staged.ForkRunID).
						Scan(&persistedKind, &revision, &eventID); err != nil {
						t.Fatal(err)
					}
					if persistedKind != string(kind) || revision != point.Revision || eventID.String != selector || eventID.Valid != (kind == runfork.RunForkPointEvent) {
						t.Fatalf("durable point differs from activation: %s/r%d/%+v != %+v", persistedKind, revision, eventID, point)
					}
					after := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
					if !reflect.DeepEqual(forkContentionRowsForRun(t, before, f.runID), forkContentionRowsForRun(t, after, f.runID)) {
						t.Fatal("advanced source state/history was frozen, rewound or otherwise mutated")
					}
					operations := f.store.(interface {
						LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
					})
					for repetition := 0; repetition < 2; repetition++ {
						operation, found, err := operations.LoadForkOperation(f.ctx, req.ForkOperation.Actor, req.ForkOperation.IdempotencyKey, req.ForkOperation.TransportHash)
						if err != nil || !found || operation.Status != runfork.ForkOperationActivated || operation.ForkRunID != staged.ForkRunID || operation.Result == nil ||
							operation.Result.ForkPoint.Kind != point.Kind || operation.Result.ForkPoint.Revision != point.Revision || operation.Result.ForkPoint.EventID != point.EventID ||
							operation.Result.SourceFrozen || operation.Result.SourceRunStatus != status {
							t.Fatalf("permanent completion lost exact branch point: %+v found=%t err=%v", operation, found, err)
						}
						encoded, err := json.Marshal(operation.Result)
						if err != nil {
							t.Fatal(err)
						}
						var decoded runfork.ForkOperationResult
						if err := json.Unmarshal(encoded, &decoded); err != nil || decoded.ForkPoint != operation.Result.ForkPoint {
							t.Fatalf("serialized public completion point changed: %+v err=%v", decoded, err)
						}
					}
					if _, err := store.ActivateRunForkForSelectedContractExecution(f.ctx, req); err == nil {
						t.Fatal("repeated activation rewrote first completion")
					}
					if !reflect.DeepEqual(after, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
						t.Fatal("exact readback or rejected second activation mutated first committed evidence")
					}
				})
			}
		}
	}
}

func TestSelectedBranchPointCleanupIsolationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		for _, victimKind := range []runfork.RunForkPointKind{runfork.RunForkPointEvent, runfork.RunForkPointDeploymentRevision} {
			for _, operation := range []string{"discard", "reset"} {
				t.Run(backend.name+"/"+string(victimKind)+"/"+operation, func(t *testing.T) {
					opened := backend.open(t)
					shared := eventRecordContractBackend{name: backend.name, open: func(*testing.T) authorActivityReceiptFixture { return opened }}
					f := newBranchPointCleanupEventFixture(t, shared)
					other := newBranchPointFixture(t, shared, runfork.RunForkPointDeploymentRevision)
					// Permanent operations retain their failure/result authority; only
					// operation-free orchestration uses materialized-fork discard.
					first, firstReq := stageForkContentionFixtureAt(t, f, true, f.eventID, operation == "reset")
					second, secondReq := stageForkContentionFixtureAt(t, other, true, "", true)
					if err := f.write(f.ctx, f.store); err != nil {
						t.Fatal(err)
					}
					if err := other.write(other.ctx, other.store); err != nil {
						t.Fatal(err)
					}
					selected := f.store.(runforkexecution.SelectedContractForkLifecycle)
					for _, req := range []runfork.RunForkSelectedContractExecutionActivateRequest{firstReq, secondReq} {
						result, err := selected.ActivateRunForkForSelectedContractExecution(f.ctx, req)
						if err != nil || !result.Activated || result.BranchDivergence == nil ||
							result.BranchDivergence.ForkPoint.Kind != result.ForkPoint.Kind || result.BranchDivergence.ForkPoint.Revision != result.ForkPoint.Revision || result.BranchDivergence.ForkPoint.EventID != result.ForkPoint.EventID {
							t.Fatalf("sibling branch fixture did not activate: %+v err=%v", result, err)
						}
					}
					before := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
					victim, survivor := first.ForkRunID, second.ForkRunID
					if victimKind == runfork.RunForkPointDeploymentRevision {
						victim, survivor = survivor, victim
					}
					wantVictimEvidence := 0
					if operation == "discard" {
						if _, err := transitionRunForTest(f.ctx, f.store, runlifecycle.ActiveTransitionRequest{RunID: victim, State: runlifecycle.StatePaused}); err != nil {
							t.Fatal(err)
						}
						if victimKind == runfork.RunForkPointDeploymentRevision {
							// Deployment forks require a permanent invocation. This
							// operation-free cleanup port must not erase its result.
							beforeRefusal := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
							err := selected.DiscardMaterializedSelectedContractExecutionFork(f.ctx, victim)
							if err == nil || !strings.Contains(err.Error(), "delete selected-contract fork binding") || !strings.Contains(strings.ToUpper(err.Error()), "FOREIGN KEY") {
								t.Fatalf("permanent deployment invocation was not protected: %v", err)
							}
							if !reflect.DeepEqual(beforeRefusal, snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")) {
								t.Fatal("refused operation-free discard partially erased permanent branch evidence")
							}
							wantVictimEvidence = 1
						} else if err := selected.DiscardMaterializedSelectedContractExecutionFork(f.ctx, victim); err != nil {
							t.Fatal(err)
						}
					} else {
						value, _ := storeTestWorkFixtures.Load(t)
						capability := selectedMaterializationProcessForTest(t, value.(*storeTestWorkFixture), f.store)
						resetCtx := testAuthorActivityRuntimeContext()
						request := admitRetainedResetCleanupProofWithContext(t, resetCtx, capability, f.store.(destructivereset.QuiescenceStore), victim, false)
						if _, err := capability.ApplyDestructiveResetCleanup(resetCtx, request, nil); err != nil {
							t.Fatal(err)
						}
					}
					assertSelectedBranchCleanupCount(t, f.db, victim, wantVictimEvidence)
					assertSelectedBranchCleanupCount(t, f.db, survivor, 1)
					after := snapshotForkHistoricalExecutionTables(t, f.db, backend.name == "postgres")
					for _, preserved := range []string{f.runID, other.runID, survivor} {
						if !reflect.DeepEqual(forkContentionRowsForRun(t, before, preserved), forkContentionRowsForRun(t, after, preserved)) {
							t.Fatalf("%s mutated preserved source/sibling %s", operation, preserved)
						}
					}
				})
			}
		}
	}
}

func newBranchPointCleanupEventFixture(t *testing.T, backend eventRecordContractBackend) forkContentionFixture {
	t.Helper()
	f := newConstructedGateFixtureForFields(t, backend, true, false)
	// Commit the declared input without inventing historical node deliveries.
	// The still-pending gate can be superseded by the ordinary reset owner;
	// an accepted verdict awaiting its frozen route cannot be erased by reset.
	event := eventtest.ExistingRunRootIngress(f.eventID, "item.received", "operator", "", []byte(`{}`), 0,
		f.runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixture(f.ctx, f.store.(storeTestDurableEventBusStore), event); err != nil {
		t.Fatal(err)
	}
	f.state.ExpectedRevision = 2
	f.state.Accumulator = json.RawMessage(`{"source_advanced":{"count":1}}`)
	f.write = func(ctx context.Context, store snapshotOwnershipStore) error {
		_, err := store.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: f.state})
		return err
	}
	return f
}

func newBranchPointFixture(t *testing.T, backend eventRecordContractBackend, kind runfork.RunForkPointKind) forkContentionFixture {
	t.Helper()
	if kind == runfork.RunForkPointEvent {
		return newConstructedGateFixtureForFields(t, backend, true, true)
	}
	opened := backend.open(t)
	construction := newReceiverConfigActivationFixtureForStore(t, opened.store.(agentFixtureFlowStore), false, map[string]string{
		"schema.yaml": "name: branch-revision\nstages:\n  pending: {initial: true}\n  later: {terminal: true}\npins:\n  outputs:\n    - records.ready\n",
		"events.yaml": "records.ready:\n  body: text\n",
	}, nil, ownStoreTestAgentManager, nil)
	// Preparation must consume the construction process, not acquire a rival.
	capability, err := agentfixture.ProcessCapability(t, construction.ctx, opened.store.(agentfixture.Store))
	if err != nil {
		t.Fatal(err)
	}
	value, _ := storeTestWorkFixtures.Load(t)
	work := value.(*storeTestWorkFixture)
	work.capabilitiesMu.Lock()
	if work.capabilities == nil {
		work.capabilities = make(map[any]startupownership.ProcessCapability)
	}
	work.capabilities[opened.store] = capability
	work.capabilitiesMu.Unlock()
	data := opened.store.(interface {
		EnsureSourceArtifactWithData(context.Context, *sourceartifact.AdmittedSourceArtifact, durabledata.Catalog) (sourceartifact.EnsureResult, error)
		ExecuteDataSourceOperation(context.Context, durabledata.SourceCommand) (durabledata.SourceOperationResult, error)
	})
	catalog, err := contracts.BuildDurableDataCatalog(construction.bundle)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := data.EnsureSourceArtifactWithData(construction.ctx, construction.bundle.SourceArtifact, catalog); err != nil {
		t.Fatal(err)
	}
	ref, err := durabledata.ParseDeclarationRef(".", "records.ready")
	if err != nil {
		t.Fatal(err)
	}
	imported, err := data.ExecuteDataSourceOperation(construction.ctx, durabledata.SourceCommand{
		Operation: "import", SourceInvocationID: uuid.NewString(), Actor: "operator", BundleHash: catalog.BundleHash,
		Declaration: ref, ExpectedHead: durabledata.AbsentHead(), InputFormat: "jsonl", Input: []byte{},
	})
	if err != nil {
		t.Fatal(err)
	}
	runID, at := uuid.NewString(), time.Now().UTC().Truncate(time.Microsecond)
	ctx := correlation.WithRunID(construction.ctx, runID)
	source := semanticview.Wrap(construction.bundle)
	plan, err := construction.manager.PrepareFlowInstanceActivation(ctx, pipeline.FlowInstanceActivationRequest{
		ContractBundle: source, OccurredAt: at, Instance: flowidentity.Stored(source, ".", runID, runID, "", ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	command := runtimebus.DeploymentRunCreationCommand{
		RunCreation: durabledata.RunCreationCommand{RunID: runID, Actor: "operator", BundleHash: catalog.BundleHash,
			Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: ref, VersionID: imported.Candidate.VersionID}}}},
		Idempotency: apiidempotency.Request{Method: "run.start", Actor: apiidempotency.BearerActor("operator"), Now: at, TTL: time.Hour},
		Root:        runtimebus.FlowInstanceActivationCommand{Plan: plan},
	}
	for _, child := range plan.ConstructionPlans() {
		command.Root.RouteTopology = append(command.Root.RouteTopology, runtimebus.FlowInstanceRouteRecordSet{Identity: flowidentity.RunScopedFlowInstance{RunID: runID, Route: child.Identity.Route()}})
	}
	result, err := opened.store.(runtimebus.DeploymentRunCreationCommitOwner).CommitDeploymentRunCreation(ctx, command)
	if err != nil || !result.Acknowledged {
		t.Fatalf("create real deployment fixture: %+v err=%v", result, err)
	}
	record, err := plan.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	f := forkContentionFixture{snapshotOwnershipFixture: snapshotOwnershipFixture{store: opened.store.(snapshotOwnershipStore), db: opened.db, ctx: ctx, runID: runID, entityID: record.State.EntityID, state: record.State}, source: source}
	f.state.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	f.state.ExpectedState, f.state.ExpectedRevision = "pending", 1
	f.state.Accumulator = json.RawMessage(`{"source_advanced":{"count":1}}`)
	f.write = func(ctx context.Context, store snapshotOwnershipStore) error {
		_, err := store.CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{State: f.state})
		return err
	}
	return f
}

func assertSelectedBranchCleanupCount(t *testing.T, db *sql.DB, forkRunID string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE fork_run_id=$1`, forkRunID).Scan(&count); err != nil || count != want {
		t.Fatalf("branch rows for %s=%d want=%d err=%v", forkRunID, count, want, err)
	}
}
