package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/google/uuid"
)

// Reuse the ordinary fork/ordinal journey, without its downstream execution and
// hostile-publication matrix. No event origin, outcome, or fork ancestry is
// synthesized: materialization, activation, evaluation and grouped commit own it.
func seedOperatorSnapshotInheritedOrdinal(t *testing.T, f operatorSnapshotFixture, postgres bool) (context.Context, events.Event) {
	t.Helper()
	f.db.SetMaxOpenConns(8) // The real process/publication owners retain connections.
	backend := "sqlite"
	if postgres {
		backend = "postgres"
	}
	repo := canonicalrouting.RepoRoot(t)
	root := canonicalrouting.CopyForkFanOutConsumer(t, false, false)
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
	ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, f.authorActivityReceiptFixture, runID, source), runID)
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	node := mustPersistenceRootNode("fan-out-source")
	plans := source.FanOutPlansForHandler(node, "items.ready")
	if len(plans) != 1 {
		t.Fatalf("expected one compiled fan-out plan: %d", len(plans))
	}
	seedWorkflowTargetStateForTransition(t, backend, f.db, runID, runID, runID, "pending", 1, at)
	if _, err := f.db.ExecContext(ctx, `UPDATE entity_state SET entity_type='root' WHERE run_id=$1`, runID); err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: ".", FlowInstance: runID, EntityID: runID})}
	trigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "items.ready", "operator", "", []byte(`{"items":["one"]}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
	selected := f.store.(storeTestDurableEventBusStore)
	if err := commitSemanticEventFixtureWithRoutes(ctx, selected, trigger, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(ctx, selected, trigger, route)
	if err != nil {
		t.Fatal(err)
	}
	intent := fanoutobligation.IntentRequest{
		Key:     fanoutobligation.IntentKey{RunID: runID, TriggeringDeliveryID: claimed.Claim.DeliveryID(), ElementRef: plans[0].Ref.ElementRef},
		PlanRef: plans[0].Ref, Source: fanoutobligation.SourceRef{Kind: fanoutobligation.SourceEventPayloadField, EventID: trigger.ID(), Field: "items"}, Cardinality: 1,
		Capsule: fanoutobligation.Capsule{NodeKey: node.Key(), ExecutionFlowID: ".", Route: flowidentity.StoredRoute(".", runID, runID), EntityID: runID,
			HandlerEventKey: "items.ready", CurrentState: "review", ProducerSource: trigger.RoutingSource(), Receiver: &fanoutobligation.ExecutionReceiver{Node: node, Target: route.Target}, Lineage: events.LineageFromEvent(trigger),
			Entity: map[string]any{}, StateFields: map[string]any{}},
	}
	state := stateOnlyWorkflowEngineMutationRecord(t, runID, ".", runID, runID, "pending", 1, at)
	state.CurrentState, state.EntityType, state.Mode = "review", "root", "static"
	if _, err := f.store.(pipeline.WorkflowEngineMutationOwner).CommitWorkflowEngineMutation(ctx, pipeline.WorkflowEngineMutationCommand{
		State: state, FanOutIntent: &intent, DeliverySuccess: &pipeline.WorkflowEngineDeliverySuccess{Claim: claimed.Claim, SideEffects: []string{"handler_completed"}, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleSelection()},
	}); err != nil {
		t.Fatal(err)
	}
	point := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.checkpoint", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at.Add(2*time.Minute))
	if err := insertCanonicalEventRecordFixture(ctx, f.store, point); err != nil {
		t.Fatal(err)
	}
	captureFanOutBarrierForkRevision(t, ctx, f.db, runID, postgres)
	forkOwner := f.store.(runForkSelectedLifecycleStore)
	child, err := forkOwner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: point.ID(), OriginalLoopCarriage: original})
	if err != nil {
		t.Fatal(err)
	}
	activation, err := forkOwner.ActivateRunFork(ctx, runfork.RunForkActivateRequest{ForkRunID: child.ForkRunID, AllowSourceFreeze: true, OriginalLoopCarriage: original, HistoricalReplayExecutionAdmitter: runforkexecution.HistoricalReplayExecutionAdmitter{}})
	if err != nil || !activation.Activated || !activation.SourceFrozen {
		t.Fatalf("ordinary fork activation: %+v err=%v", activation, err)
	}
	key := fanoutobligation.IntentKey{RunID: child.ForkRunID, ElementRef: intent.Key.ElementRef}
	if err := f.db.QueryRowContext(ctx, `SELECT triggering_delivery_id FROM fan_out_intents WHERE run_id=$1`, child.ForkRunID).Scan(&key.TriggeringDeliveryID); err != nil {
		t.Fatal(err)
	}
	owner, capability, _, _ := grantedFanOutOwnerForTest(t, ctx, f.store.(selectedFanOutOwner), fanOutOwnerFixture{bundleHash: intent.PlanRef.BundleHash, runID: child.ForkRunID, deliveryID: key.TriggeringDeliveryID})
	copied, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{Owner: "snapshot-lineage-proof", BundleHash: intent.PlanRef.BundleHash, Candidate: &key, Now: time.Now().UTC(), Lease: time.Minute})
	if err != nil || !found {
		t.Fatalf("claim lawful fork ordinal: found=%v err=%v", found, err)
	}
	input, err := owner.LoadFanOutEvaluation(ctx, claim)
	if err != nil || len(input.Items) != 1 {
		t.Fatalf("load lawful fork ordinal: count=%d err=%v", len(input.Items), err)
	}
	unused := forkOrdinalUnusedDependencies{}
	executor, err := engine.NewExecutor(engine.RuntimeDependencies{Source: source, StateRepo: unused, MutationOwner: unused, Locker: unused}, nil)
	if err != nil {
		t.Fatal(err)
	}
	emit, err := executor.EvaluateFanOutOrdinal(ctx, copied, input.Trigger, input.Items[0], input.StartOrdinal)
	if err != nil {
		t.Fatal(err)
	}
	descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
	if err != nil {
		t.Fatal(err)
	}
	scope, ok := authoractivity.ScopeFromContext(ctx)
	if !ok {
		t.Fatal("missing author scope")
	}
	lease, err := f.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	fact, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok {
		t.Fatal("missing source artifact fact")
	}
	eventBus, err := newStoreTestEventBus(t, selected, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, WorkOwner: storeTestWorkOwner(t)})
	if err != nil {
		t.Fatal(err)
	}
	ctx = correlation.WithRunID(ctx, child.ForkRunID)
	group, err := owner.BeginFanOutPublicationGroup(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := group.Close(context.WithoutCancel(ctx)); err != nil {
			t.Error(err)
		}
	}()
	prepared, err := eventBus.PrepareFanOutPublications(ctx, group, []pipeline.FanOutPublicationRequest{{Ordinal: 0, Intent: emit}})
	if err != nil || len(prepared) != 1 || prepared[0].Err != nil {
		t.Fatalf("prepare lawful inherited ordinal: %+v err=%v", prepared, err)
	}
	publication, ok := prepared[0].Publication.(bus.EnginePublicationPlan)
	if !ok {
		t.Fatalf("unexpected publication %T", prepared[0].Publication)
	}
	event := publication.PublicationCommand().Commit.Event.Event()
	if origin, found := event.InheritedFanOutOrigin(); !found || origin.SourceRunID() != runID || event.RunID() != child.ForkRunID {
		t.Fatalf("evaluator did not preserve lawful fork provenance: %+v", event)
	}
	if err := eventBus.SealFanOutPublications(ctx, group, 1, []engine.DurablePublicationPlan{prepared[0].Publication}); err != nil {
		t.Fatal(err)
	}
	committed, err := owner.CommitFanOutChunk(ctx, pipeline.FanOutChunkCommand{Claim: claim, Outcomes: []pipeline.FanOutChunkOutcome{{Ordinal: 0, Publication: prepared[0].Publication}}, Now: time.Now().UTC()})
	if err != nil || committed.Intent.Cursor != 1 {
		t.Fatalf("commit lawful inherited ordinal: cursor=%d err=%v", committed.Intent.Cursor, err)
	}
	if err := eventBus.FinalizeFanOutPublications(ctx, group, committed.Publications); err != nil {
		t.Fatal(err)
	}
	if err := group.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := capability.Release(ctx); err != nil {
		t.Fatal(err)
	}
	f.db.SetMaxOpenConns(1)
	return ctx, event
}

func TestOperatorEventSnapshotInheritedLineageInterleavingBothStores(t *testing.T) {
	for _, postgres := range []bool{false, true} {
		t.Run(fmt.Sprintf("postgres_%v", postgres), func(t *testing.T) {
			f := newOperatorSnapshotFixture(t, postgres)
			ctx, event := seedOperatorSnapshotInheritedOrdinal(t, f, postgres)
			store := f.store.(routeSettlementOperatorStore)
			before, err := store.LoadOperatorEvent(ctx, event.ID())
			if err != nil || before.InheritedFanOutOrigin == nil || len(before.Deliveries) != 1 {
				t.Fatalf("lawful committed inherited event: %+v err=%v", before, err)
			}
			origin := *before.InheritedFanOutOrigin
			var forkPoint string
			if err := f.db.QueryRowContext(ctx, `SELECT CAST(forked_from_event_id AS TEXT) FROM runs WHERE run_id=$1`, event.RunID()).Scan(&forkPoint); err != nil {
				t.Fatal(err)
			}
			opts := operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: event.RunID(), EventName: string(event.Type())}, Order: "asc", Limit: 1}
			for _, operation := range []string{"list", "get"} {
				t.Run(operation, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
					defer cancel()
					entered, release := f.probe.arm("event", 1, nil, nil)
					defer release()
					var page operatorread.OperatorEventListResult
					var full operatorread.OperatorEventFull
					done := make(chan error, 1)
					go func() {
						var err error
						if operation == "list" {
							page, err = store.ListOperatorEvents(ctx, opts)
						} else {
							full, err = store.LoadOperatorEvent(ctx, event.ID())
						}
						done <- err
					}()
					awaitOperatorSnapshotBarrier(t, ctx, entered)
					f.probe.mu.Lock()
					ownerBefore, lineageBefore := f.probe.seen["inherited-owner"], f.probe.seen["inherited-lineage"]
					f.probe.mu.Unlock()
					if ownerBefore != 0 || lineageBefore != 0 {
						t.Fatalf("barrier must precede owner and lineage reads: %d/%d", ownerBefore, lineageBefore)
					}
					// The fixture was created by real owners. This one hostile mutation
					// redirects ancestry to an unrelated, real run/checkpoint, preserving
					// schema constraints without changing the ordinal owner or origin.
					writer, err := f.writer.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer writer.Rollback()
					result, err := writer.ExecContext(ctx, `UPDATE runs SET forked_from_run_id=$3,forked_from_event_id=$4 WHERE run_id=$1 AND forked_from_run_id=$2`, event.RunID(), origin.SourceRunID(), f.opts.Filter.RunID, f.ids[0])
					if err != nil {
						t.Fatal(err)
					}
					if n, err := result.RowsAffected(); err != nil || n != 1 {
						t.Fatalf("real fork ancestry changed: rows=%d err=%v", n, err)
					}
					if err := writer.Commit(); err != nil {
						t.Fatalf("writer must commit before snapshot resumes (SQLite WAL): %v", err)
					}
					defer func() {
						f.probe.disarm()
						if _, err := f.writer.ExecContext(context.WithoutCancel(ctx), `UPDATE runs SET forked_from_run_id=$1,forked_from_event_id=$3 WHERE run_id=$2`, origin.SourceRunID(), event.RunID(), forkPoint); err != nil {
							t.Error(err)
						}
					}()
					release()
					if err := <-done; err != nil {
						t.Fatalf("pinned owner/lineage must remain coherently valid: %v", err)
					}
					assertOperatorSnapshotTransaction(t, f.probe, postgres)
					if f.probe.seen["inherited-owner"] == 0 || f.probe.seen["inherited-lineage"] == 0 {
						t.Fatalf("transitive owner/SourceRunInLineage not executed: %v", f.probe.seen)
					}
					if operation == "list" {
						if len(page.Events) != 1 || page.NextCursor != "" {
							t.Fatalf("exact inherited page: %+v", page)
						}
						full = page.Events[0]
					}
					assertOperatorBatchScalarEqual(t, full, before)
					t.Logf("committed ancestry removal preceded successful owner=%d lineage=%d snapshot reads", f.probe.seen["inherited-owner"], f.probe.seen["inherited-lineage"])
					f.probe.disarm()
					_, freshRelease := f.probe.arm("none", 1, nil, nil)
					defer freshRelease()
					page, full = operatorread.OperatorEventListResult{}, operatorread.OperatorEventFull{}
					if operation == "list" {
						page, err = store.ListOperatorEvents(ctx, opts)
					} else {
						full, err = store.LoadOperatorEvent(ctx, event.ID())
					}
					if !errors.Is(err, eventrecord.ErrCorrupt) || !strings.Contains(err.Error(), "outside the destination fork lineage") || !reflect.DeepEqual(page, operatorread.OperatorEventListResult{}) || !reflect.DeepEqual(full, operatorread.OperatorEventFull{}) {
						t.Fatalf("fresh invocation must refuse changed ancestry without partial result: page=%+v event=%+v err=%v", page, full, err)
					}
					assertOperatorSnapshotTransaction(t, f.probe, postgres)
					if f.probe.seen["inherited-owner"] == 0 || f.probe.seen["inherited-lineage"] == 0 {
						t.Fatalf("fresh refusal must traverse actual owner and lineage: %v", f.probe.seen)
					}
				})
			}
		})
	}
}
