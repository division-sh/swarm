package runtimepersistence

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// The ordinal evaluator must not load mutable state or dispatch effects. These
// deliberately absent dependencies panic if it acquires either responsibility.
type forkOrdinalUnusedDependencies struct {
	engine.StateRepository
	engine.EngineMutationOwner
	engine.EntityLocker
	engine.PostCommitDispatcher
}

func TestSelectedForkFanOutDeferredWorkRefusalBothStores(t *testing.T) {
	testForkFanOutGenerationWriterEvaluatorBothStores(t, true, false)
}

// Ordinary fork execution is a separate supported path. It does not change the
// archived selected-execution proof or grant that path deferred-work authority.
func TestOrdinaryForkFanOutOriginWriterEvaluatorBothStores(t *testing.T) {
	testForkFanOutGenerationWriterEvaluatorBothStores(t, false, false)
}

func TestOrdinaryForkOfForkFanOutOriginWriterEvaluatorBothStores(t *testing.T) {
	testForkFanOutGenerationWriterEvaluatorBothStores(t, false, true)
}

func testForkFanOutGenerationWriterEvaluatorBothStores(t *testing.T, selectedExecution, forkAgain bool) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			if store, ok := fixture.store.(*SQLiteRuntimeStore); ok && !selectedExecution {
				// The receipt fixture freezes July settlement time; actual schedule
				// preparation and acceptance must share the live store clock.
				store.nowFn = func() time.Time { return time.Now().UTC() }
			}
			for _, cell := range []struct {
				name             string
				loop, historical bool
			}{{"current", true, false}, {"historical", true, true}, {"no_loop", false, false}} {
				t.Run(cell.name, func(t *testing.T) {
					repo := canonicalrouting.RepoRoot(t)
					completeBarrier := !selectedExecution && cell.loop && !cell.historical
					root := canonicalrouting.CopyForkFanOutConsumer(t, cell.loop, true)
					if completeBarrier {
						root = canonicalrouting.CopyForkFanOutCompletionConsumer(t)
					}
					bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
					if err != nil {
						t.Fatal(err)
					}
					source := semanticview.Wrap(bundle)
					original, err := semanticview.CompileOriginalLoopCarriage(source)
					if err != nil {
						t.Fatal(err)
					}
					runID := uuid.NewString()
					ctx := seedSelectedActivitySourceRun(t, fixture, runID, source)
					ctx = correlation.WithRunID(ctx, runID)
					at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
					node := mustPersistenceRootNode("fan-out-source")
					plans := source.FanOutPlansForHandler(node, "items.ready")
					if len(plans) != 1 {
						t.Fatalf("expected one compiled plan, got %d", len(plans))
					}
					activation, err := loopruntime.New(runID, runID, ".", "revision", "revision_id", uuid.NewString(), "review", 3, at)
					if err != nil {
						t.Fatal(err)
					}
					generation := attemptgeneration.Generation{}
					var loopContext map[string]any
					if cell.loop {
						generation, loopContext = activation.Generation(), activation.Context()
					}
					if cell.historical {
						if _, err := activation.Repeat("review", uuid.NewString(), at.Add(time.Second)); err != nil {
							t.Fatal(err)
						}
					}
					buckets := map[string]map[string]any{}
					if cell.loop {
						if err := loopruntime.Store(buckets, activation); err != nil {
							t.Fatal(err)
						}
					}
					seedWorkflowTargetStateForTransition(t, backend.name, fixture.db, runID, runID, runID, "pending", 1, at)
					if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET entity_type='root' WHERE run_id=$1`, runID); err != nil {
						t.Fatal(err)
					}
					routeID := events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID}
					route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(routeID)}
					triggerPayload := map[string]any{"items": []string{"one", "two"}}
					if cell.loop {
						triggerPayload["revision_id"] = generation.RevisionID
					}
					triggerEnvelope := events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, runID), runID)
					if !selectedExecution {
						// A root ingress does not declare a flow instance. Its exact
						// receiver remains carried by the persisted delivery route.
						triggerEnvelope = events.EventEnvelope{}
					}
					trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "items.ready", "operator", "", []byte(forkTestJSON(t, triggerPayload)), 0, runID,
						triggerEnvelope, eventtest.RootRoutingSource(runID), at)
					selected := fixture.store.(storeTestDurableEventBusStore)
					if err := commitSemanticEventFixtureWithRoutes(ctx, selected, trigger, []events.DeliveryRoute{route}); err != nil {
						t.Fatal(err)
					}
					claimed, err := claimDeliveryFixture(ctx, selected, trigger, route)
					if err != nil {
						t.Fatal(err)
					}
					intent := fanoutobligation.IntentRequest{
						Key:     fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: claimed.Claim.DeliveryID(), ElementRef: plans[0].Ref.ElementRef},
						PlanRef: plans[0].Ref, Source: fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: trigger.ID(), Field: "items"}, Cardinality: 2,
						Capsule: fanoutobligation.Capsule{NodeKey: node.Key(), ExecutionFlowID: ".", Route: flowidentity.StoredRoute(".", runID, runID), EntityID: runID,
							HandlerEventKey: "items.ready", CurrentState: "review", ProducerSource: trigger.RoutingSource(), Receiver: &fanoutobligation.ExecutionReceiver{Node: node, Target: route.Target}, Lineage: events.LineageFromEvent(trigger), Loop: loopContext,
							Entity: map[string]any{"business_revision": generation.RevisionID, "exact_integer": json.Number("9007199254740993")}, StateFields: map[string]any{"sentinel": "unchanged"}},
					}
					declaration, err := plans[0].Ref.ElementRef.DeclarationIdentity()
					if err != nil {
						t.Fatal(err)
					}
					join, err := timeridentity.NewFanOutDeliveryJoinRef(node, "items.ready", "all-items-delivered", declaration, plans[0].Ref.BundleHash, plans[0].Ref.SemanticDigest)
					if err != nil {
						t.Fatal(err)
					}
					join, err = join.BindFanOutIntent(claimed.Claim.DeliveryID(), generation)
					if err != nil {
						t.Fatal(err)
					}
					handle, err := timeridentity.JoinCompleteHandle(join)
					if err != nil {
						t.Fatal(err)
					}
					controlSource, err := events.NewRootRoutingSource(runID)
					if err != nil {
						t.Fatal(err)
					}
					barrier := fanoutbarrier.Registration{IntentKey: intent.Key, PlanRef: intent.PlanRef, Handle: handle, Route: intent.Capsule.Route, EntityID: runID, RoutingSource: controlSource, ExecutionMode: trigger.ExecutionMode(), CreatedAt: at}
					record := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", runID, runID, "pending", 1, at)
					record.CurrentState, record.EntityType, record.Mode = "review", "root", "static"
					record.Accumulator = json.RawMessage(forkTestJSON(t, buckets))
					committed, err := fixture.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{
						State: record, FanOutIntent: &intent, FanOutBarrier: &barrier, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
					})
					if err != nil || committed.DeliverySuccess == nil {
						t.Fatalf("ordinary intent/barrier writer failed: %v", err)
					}
					captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, backend.name == "postgres")
					point := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.checkpoint", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at.Add(2*time.Minute))
					if err := insertCanonicalEventRecordFixture(ctx, fixture.store, point); err != nil {
						t.Fatal(err)
					}
					captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, backend.name == "postgres")
					forkOwner := fixture.store.(selectedActivityProjectionStore)
					var materialize func() (runfork.RunForkMaterialization, error)
					if selectedExecution {
						if _, err := forkOwner.(selectedSourceArtifactStore).EnsureSourceArtifact(ctx, bundle.SourceArtifact); err != nil {
							t.Fatal(err)
						}
						// Establish the normal process capability before measuring the
						// first request. Startup is not a fork preparation mutation.
						storeTestWorkOwner(t)
						work, _ := storeTestWorkFixtures.Load(t)
						selectedMaterializationProcessForTest(t, work.(*storeTestWorkFixture), forkOwner)
						before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						for attempt := 0; attempt < 2; attempt++ {
							prepared, err := prepareSelectedStoreForkForTest(t, ctx, forkOwner, runID, point.ID(),
								runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: bundle.SourceArtifact.BundleHash()}, bundle.SourceArtifact.BundleHash())
							failure, classified := runtimefailures.EnvelopeFromError(err)
							if prepared != nil || !classified || failure.Class != runtimefailures.ClassDependencyUnavailable || failure.Detail.Code != "selected_contract_deferred_work_owner_unavailable" {
								t.Fatalf("attempt %d: want exact selected deferred-work refusal and no prepared child; got prepared=%v failure=%+v err=%v", attempt, prepared, failure, err)
							}
							capabilities, ok := failure.Detail.Attributes["capabilities"].([]string)
							if !ok || !reflect.DeepEqual(capabilities, []string{"fan_out_delivery_barrier"}) {
								t.Fatalf("attempt %d: unexpected deferred capabilities: %#v", attempt, failure.Detail.Attributes["capabilities"])
							}
							if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
								t.Fatalf("attempt %d: selected deferred refusal mutated application tables", attempt)
							}
						}
						return
					} else {
						request := runfork.RunForkMaterializeRequest{SourceRunID: runID, At: point.ID(), OriginalLoopCarriage: original}
						materialize = func() (runfork.RunForkMaterialization, error) {
							return fixture.store.(runForkSelectedLifecycleStore).MaterializeRunFork(ctx, request)
						}
					}
					child, err := materialize()
					if err != nil {
						t.Fatalf("fork actual intent/barrier: %v", err)
					}
					projectedGeneration := generation
					protectedRuns := []string{runID}
					if forkAgain {
						firstChild := child.ForkRunID
						protectedRuns = append(protectedRuns, firstChild)
						checkpoint := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.checkpoint", "operator", "", []byte(`{}`), 0, firstChild, events.EventEnvelope{}, eventtest.RootRoutingSource(firstChild), at.Add(3*time.Minute))
						if err := insertCanonicalEventRecordFixture(ctx, fixture.store, checkpoint); err != nil {
							t.Fatal(err)
						}
						captureFanOutBarrierForkRevision(t, ctx, fixture.db, firstChild, backend.name == "postgres")
						request := runfork.RunForkMaterializeRequest{SourceRunID: firstChild, At: checkpoint.ID(), OriginalLoopCarriage: original}
						materialize = func() (runfork.RunForkMaterialization, error) {
							return fixture.store.(runForkSelectedLifecycleStore).MaterializeRunFork(ctx, request)
						}
						child, err = materialize()
						if err != nil {
							t.Fatalf("fork inherited intent a second time: %v", err)
						}
						if cell.loop {
							projectedGeneration, err = loopruntime.ForkGeneration(generation, firstChild, firstChild)
							if err != nil {
								t.Fatal(err)
							}
						}
					}
					want := attemptgeneration.Generation{}
					if cell.loop {
						want, err = loopruntime.ForkGeneration(projectedGeneration, child.ForkRunID, child.ForkRunID)
						if err != nil {
							t.Fatal(err)
						}
					}
					var childHandleRaw []byte
					if err := fixture.db.QueryRowContext(ctx, `SELECT timer_handle FROM fan_out_obligation_barriers WHERE run_id=$1`, child.ForkRunID).Scan(&childHandleRaw); err != nil {
						t.Fatal(err)
					}
					var childHandle timeridentity.TimerHandle
					if err := json.Unmarshal(childHandleRaw, &childHandle); err != nil {
						t.Fatal(err)
					}
					childJoin, ok := childHandle.JoinRef()
					if !ok || childJoin.Generation() != want || (cell.loop && childHandle.TaskID() == handle.TaskID()) {
						t.Fatalf("barrier kept source generation: %#v", childJoin.Generation())
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					if _, err := materialize(); err != nil {
						t.Fatalf("repeat exact fan-out materialization: %v", err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatal("repeated fork mutated durable evidence")
					}
					owner := fixture.store.(selectedFanOutOwner)
					copied, claim, found, err := claimFanOutForRun(t, ctx, owner, child.ForkRunID, intent.PlanRef.BundleHash, at.Add(3*time.Minute))
					if err != nil || !found {
						t.Fatalf("claim child intent: found=%v %v", found, err)
					}
					defer func() {
						if err := owner.ReleaseFanOutClaim(ctx, claim); err != nil {
							if errors.Is(err, fanoutobligation.ErrStaleClaim) {
								var status string
								var cursor, cardinality int
								if readErr := fixture.db.QueryRowContext(ctx, `SELECT status,cursor,cardinality FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`,
									claim.Key.RunID, claim.Key.TriggeringDeliveryID, claim.Key.ElementRef.FlowPath, claim.Key.ElementRef.Family, claim.Key.ElementRef.SemanticPath).Scan(&status, &cursor, &cardinality); readErr == nil && status == "closed" && cursor == cardinality {
									return
								}
							}
							t.Error(err)
						}
					}()
					input, err := owner.LoadFanOutEvaluation(ctx, claim)
					if err != nil {
						t.Fatal(err)
					}
					if (cell.loop && copied.Request.Capsule.Loop["revision_id"] != want.RevisionID) || copied.Request.Capsule.Entity["business_revision"] != generation.RevisionID {
						t.Fatal("loop projection damaged frozen business context")
					}
					if got := forkTestJSON(t, copied.Request.Capsule.Entity); got != forkTestJSON(t, intent.Capsule.Entity) {
						t.Fatalf("frozen entity changed: %s", got)
					}
					unused := forkOrdinalUnusedDependencies{}
					executor, err := engine.NewExecutor(engine.RuntimeDependencies{Source: source, StateRepo: unused, MutationOwner: unused, Locker: unused, Dispatcher: unused}, nil)
					if err != nil {
						t.Fatal(err)
					}
					var emissions []engine.EmitIntent
					for i, value := range input.Items {
						emit, err := executor.EvaluateFanOutOrdinal(ctx, copied, input.Trigger, value, input.StartOrdinal+i)
						if err != nil {
							t.Fatalf("actual deferred ordinal evaluator: %v", err)
						}
						var payload map[string]any
						if err := json.Unmarshal(emit.Event.Payload(), &payload); err != nil {
							t.Fatal(err)
						}
						if (cell.loop && payload["revision_id"] != want.RevisionID) || payload["value"] != value {
							t.Fatalf("emission lost exact generation/value: %#v", payload)
						}
						if emit.Event.RunID() != child.ForkRunID || emit.Event.RoutingSource().Route().EntityID != child.ForkRunID {
							t.Fatalf("emission generation is child-bound but producer ownership is not: run=%s source=%#v", emit.Event.RunID(), emit.Event.RoutingSource())
						}
						emissions = append(emissions, emit)
					}
					expectedFailure := ""
					if !selectedExecution && cell.historical {
						// The original post-revision positive journey remains above
						// under selected execution. Ordinary delivery must preserve
						// the current loop's exact stale-generation refusal.
						expectedFailure = "loop_revision_stale"
					}
					beforeConsumption := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					defer func() {
						afterConsumption := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
						for _, protected := range protectedRuns {
							if !reflect.DeepEqual(forkContentionRowsForRun(t, beforeConsumption, protected), forkContentionRowsForRun(t, afterConsumption, protected)) {
								t.Errorf("child ordinal/barrier consumption changed source run %s", protected)
							}
						}
					}()
					consumeForkFanOutEmissions(t, fixture, backend.name, source, ctx, copied, claim, emissions, expectedFailure, completeBarrier)
				})
			}
		})
	}
}
