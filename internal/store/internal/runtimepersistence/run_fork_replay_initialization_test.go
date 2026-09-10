package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestOrdinaryReplayInitializedReceiversBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			source := replayInitializedReceiverSource(t)
			runID := uuid.NewString()
			ctx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, runID, source), runID)
			fact, found := correlation.SourceArtifactFactFromContext(ctx)
			if !found {
				t.Fatal("missing source fact")
			}
			descriptors, err := runtimepkg.AuthorActivityEventDescriptors(source)
			if err != nil {
				t.Fatal(err)
			}
			scope, found := authoractivity.ScopeFromContext(ctx)
			if !found {
				t.Fatal("missing author scope")
			}
			lease, err := fixture.store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope, descriptors)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(lease.Release)
			selected := fixture.store.(storeTestDurableEventBusStore)
			workflow := fixture.store.(workflowTestSelectedStore)
			work := storeTestWorkOwner(t)
			eventBus, err := newStoreTestEventBus(t, selected, bus.EventBusOptions{ContractBundle: source, SourceArtifactFact: fact, WorkOwner: work})
			if err != nil {
				t.Fatal(err)
			}
			nodes, err := pipeline.LoadWorkflowNodes(source)
			if err != nil {
				t.Fatal(err)
			}
			coordinator := pipeline.NewPipelineCoordinatorWithOptions(eventBus, pipeline.PipelineCoordinatorOptions{
				Module: forkFanOutConsumerModule{runForkGateWorkflowModule{source: source}, nodes}, Persistence: pipeline.NewWorkflowPersistence(workflow), DeliveryStore: workflow,
				DeadLetters: workflow, PipelineObligations: workflow.PipelineObligations(), DecisionCards: workflow, ProposedEffects: workflow, HumanTasks: workflow,
				DecisionCardDraftExpiry: workflow, HumanTaskExpiry: workflow, DeliveryRuntime: eventBus, RunLifecycle: workflow,
				SourceArtifactFact: fact, ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(), WorkOwner: work,
			})
			if coordinator == nil {
				t.Fatal("real coordinator unavailable")
			}
			parent := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "start", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
			if err := insertCanonicalEventRecordFixture(ctx, fixture.store, parent); err != nil {
				t.Fatal(err)
			}
			trigger := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "work.ready", eventtest.Producer(events.EventProducerNode, mustPersistenceRootNode("source").Key()), "", []byte(`{}`), 0, events.LineageFromEvent(parent), events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
			plans, err := eventBus.PrepareEnginePublications(ctx, []engine.EmitIntent{{Event: trigger}})
			if err != nil || len(plans) != 1 {
				t.Fatalf("prepare root multi-receiver publication: %v", err)
			}
			t.Cleanup(func() { _ = eventBus.ReleaseEnginePublications(ctx, plans) })
			command := plans[0].(bus.EnginePublicationPlan).PublicationCommand()
			if len(command.Commit.DeliveryRoutes) != 4 {
				t.Fatalf("want two exact initializer/agent pairs, got %+v", command.Commit.DeliveryRoutes)
			}
			owner := fixture.store.(interface {
				runForkSelectedLifecycleStore
				CommitPublication(context.Context, bus.PublicationCommand) (bus.CommittedPublication, error)
			})
			committed, err := owner.CommitPublication(ctx, command)
			if err != nil {
				t.Fatal(err)
			}
			if err := eventBus.AcceptCommittedDeliveryHandoffs(committed.DeliveryHandoffs); err != nil {
				t.Fatal(err)
			}
			trigger = command.Commit.Event.Event()
			t.Logf("source aggregate flow=%q producer=%+v", trigger.FlowInstance(), trigger.RoutingSource().Route())
			for _, route := range command.Commit.DeliveryRoutes {
				if !route.Initialization.NodeDelivery() || route.Initialization.ValidateEvent(trigger) != nil {
					t.Fatal("publication lost canonical node initialization")
				}
				if route.Recipient.IsAgent() {
					if route.Materialization.Empty() {
						t.Fatal("agent lacks exact source dependency")
					}
					continue
				}
				delivery, err := events.NewDeliveryEvent(trigger, route)
				if err != nil {
					t.Fatal(err)
				}
				receiverCtx, err := eventreceiver.NormalExecution().Bind(ctx, trigger.ExecutionMode())
				if err != nil {
					t.Fatal(err)
				}
				receiverCtx = deliverylifecycle.WithRoute(events.WithDeliveryContext(receiverCtx, route.Context), route)
				_, _, outcome, err := coordinator.InterceptDeliveryRoute(receiverCtx, delivery, route)
				if err != nil || !outcome.ContinueDispatch() {
					disposition, _ := outcome.Disposition()
					t.Fatalf("execute initializer: reason=%s failure=%+v err=%v", disposition.ReasonCode(), disposition.Failure(), err)
				}
			}
			captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, backend.name == "postgres")
			point := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "fork.checkpoint", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
			if err := insertCanonicalEventRecordFixture(ctx, fixture.store, point); err != nil {
				t.Fatal(err)
			}
			captureFanOutBarrierForkRevision(t, ctx, fixture.db, runID, backend.name == "postgres")
			plan, err := owner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: point.ID()})
			if err != nil {
				t.Fatal(err)
			}
			completed, pending := 0, 0
			for _, item := range plan.PendingWork {
				if item.SubscriberType == "node" && item.Status == "delivered" {
					completed++
				}
				if item.SubscriberType == "agent" && item.Status == "pending" {
					pending++
				}
			}
			if len(plan.Entities) != 2 || completed != 2 || pending != 2 {
				t.Fatalf("source proof: entities=%d completed nodes=%d pending agents=%d", len(plan.Entities), completed, pending)
			}
			if !plan.ExecutionReady || !plan.ReplayResumeAdmission.DeliveryEventReplayReady {
				t.Fatalf("lawful source replay eligibility refused: %+v", plan.UnsupportedBlockers)
			}
			original, err := semanticview.CompileOriginalLoopCarriage(source)
			if err != nil {
				t.Fatal(err)
			}
			child, err := owner.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: runID, At: point.ID(), OriginalLoopCarriage: original})
			if err != nil {
				t.Fatal(err)
			}
			reader := fixture.store.(interface {
				DeliverySnapshotsForEvent(context.Context, string) ([]deliverylifecycle.Snapshot, error)
			})
			sourceDeliveries, err := reader.DeliverySnapshotsForEvent(ctx, trigger.ID())
			if err != nil {
				t.Fatal(err)
			}
			admitter := &fakeRunForkHistoricalReplayExecutionAdmitter{work: func(req runfork.RunForkHistoricalReplayExecutionRequest) []runfork.RunForkHistoricalReplayExecutableWork {
				var work []runfork.RunForkHistoricalReplayExecutableWork
				for _, item := range req.PendingWork {
					if item.SubscriberType == "agent" && item.Status == "pending" {
						work = append(work, runForkHistoricalReplayWorkFromPending(item))
					}
				}
				return work
			}}
			request := runfork.RunForkActivateRequest{ForkRunID: child.ForkRunID, AllowSourceFreeze: true, HistoricalReplayExecutionAdmitter: admitter, OriginalLoopCarriage: original}
			// A valid source snapshot is not proof that child reconstruction is intact.
			receiver := command.Commit.DeliveryRoutes[0].Target.Route()
			if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET flow_instance=$1 WHERE run_id=$2 AND entity_id=$3`, "foreign", child.ForkRunID, receiver.EntityID); err != nil {
				t.Fatal(err)
			}
			corruptBefore := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			if _, err := owner.ActivateRunFork(ctx, request); err == nil || !strings.Contains(err.Error(), "reconstructed replay receiver contradicts") {
				t.Fatalf("corrupt child ownership must reject before replay: %v", err)
			}
			if !reflect.DeepEqual(corruptBefore, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
				t.Fatal("rejected replay changed execution history")
			}
			if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET flow_instance=$1 WHERE run_id=$2 AND entity_id=$3`, receiver.FlowInstance, child.ForkRunID, receiver.EntityID); err != nil {
				t.Fatal(err)
			}
			before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			activation, err := owner.ActivateRunFork(ctx, request)
			if err != nil {
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("failed activation mutated execution history")
				}
				t.Fatalf("replay initialized source receivers: %v", err)
			}
			if !activation.Activated || activation.DeliveryEventReplay == nil || activation.DeliveryEventReplay.ReplayedDeliveryCount != 2 {
				t.Fatalf("missing exact replay deliveries: %+v", activation)
			}
			var childEventID string
			if err := fixture.db.QueryRowContext(ctx, `SELECT event_id FROM events WHERE run_id=$1 AND event_name=$2`, child.ForkRunID, "work.ready").Scan(&childEventID); err != nil {
				t.Fatal(err)
			}
			children, err := reader.DeliverySnapshotsForEvent(ctx, childEventID)
			if err != nil || len(children) != 2 {
				t.Fatalf("replay readback: %d deliveries, %v", len(children), err)
			}
			matched := map[string]bool{}
			for _, delivery := range children {
				if delivery.EventID != childEventID || delivery.RunID != child.ForkRunID || delivery.Status != deliverylifecycle.StatusPending {
					t.Fatalf("invalid child obligation: %+v", delivery)
				}
				for _, sourceDelivery := range sourceDeliveries {
					if !sourceDelivery.Route.Recipient.IsAgent() || sourceDelivery.Route.AgentIdentity.Name != delivery.Route.AgentIdentity.Name {
						continue
					}
					want := sourceDelivery.Route
					want.AgentIdentity.RunID = child.ForkRunID
					want.Target = events.MustExistingEntityTarget(want.Target.Route())
					want.Initialization = events.ReceiverInitialization{}
					want.Materialization = events.ReceiverMaterializationPlan{}
					if !reflect.DeepEqual(want, delivery.Route) {
						t.Fatalf("child route differs from exact initialized owner:\nwant=%+v\ngot=%+v", want, delivery.Route)
					}
					matched[sourceDelivery.DeliveryID] = true
				}
			}
			if len(matched) != 2 {
				t.Fatalf("same-slug receivers collapsed: %+v", children)
			}
			restarted := restartedEventDeliveryRouteReadbackStore(t, ctx, fixture)
			restoredRoutes, err := restarted.ListEventDeliveryRoutes(ctx, childEventID)
			if err != nil {
				t.Fatal(err)
			}
			var expectedRoutes []events.DeliveryRoute
			for _, delivery := range children {
				expectedRoutes = append(expectedRoutes, delivery.Route)
			}
			if !reflect.DeepEqual(events.NormalizeDeliveryRoutes(restoredRoutes), events.NormalizeDeliveryRoutes(expectedRoutes)) {
				t.Fatal("reconstructed store lost exact receiver ownership")
			}
			prepared, found, err := restarted.(preparedPublishEventReadbackStore).LoadPreparedPublishEvent(ctx, childEventID)
			if err != nil || !found {
				t.Fatalf("reconstructed publication: found=%v err=%v", found, err)
			}
			if err := prepared.Validate(); err != nil {
				t.Fatalf("reconstructed child aggregate: %v", err)
			}
			after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			if _, err := owner.ActivateRunFork(ctx, request); err == nil || !strings.Contains(err.Error(), `fork activation requires materialized fork status "paused"; got "running"`) {
				t.Fatalf("duplicate activation must retain exact lifecycle refusal: %v", err)
			}
			if !reflect.DeepEqual(after, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
				t.Fatal("duplicate activation mutated durable history")
			}
			sourceAfter, err := reader.DeliverySnapshotsForEvent(ctx, trigger.ID())
			if err != nil || !reflect.DeepEqual(sourceDeliveries, sourceAfter) {
				t.Fatalf("child replay changed source deliveries: %v\nbefore=%+v\nafter=%+v", err, sourceDeliveries, sourceAfter)
			}
		})
	}
}

func replayInitializedReceiverSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := canonicalrouting.CopyReplayInitializedReceivers(t)
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOptions(repo, root, contracts.DefaultPlatformSpecFile(repo), contracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}
