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
			bundle, _ := semanticview.Bundle(source)
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
			construction := constructHistoricalSourceFixture(t, ctx, fixture.store.(agentFixtureFlowStore), sqliteFlowActivationRequest(bundle, ".", runID, "", runID))
			constructed := map[string]pipeline.FlowInstanceActivationPlan{}
			for _, item := range construction.ConstructionPlans() {
				constructed[item.Identity.TemplateID] = item
			}
			if len(constructed) != 3 {
				t.Fatalf("want exact root and two receiver constructions, got %d", len(constructed))
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
			var receiver events.RouteIdentity
			for _, route := range command.Commit.DeliveryRoutes {
				captured, found := constructed[route.Target.Route().FlowID]
				if !found || captured.Identity.TemplateID == "." {
					t.Fatalf("delivery lacks its exact captured receiver construction: %+v", route)
				}
				header := events.RouteIdentity{FlowID: captured.Identity.TemplateID, FlowInstance: captured.Identity.InstancePath, EntityID: captured.Identity.EntityID}
				if route.Recipient.IsAgent() {
					actor := header
					actor.EntityID = ""
					agentRoute, err := captured.Identity.Route().AgentIdentityRoute()
					if err != nil {
						t.Fatal(err)
					}
					dependency, err := events.AdmitStoredFlowReceiverInitialization(runID, captured.CreatingInput.EventID, events.MustExistingEntityTarget(header), agentRoute)
					if err != nil || route.AgentIdentity.RunID != runID || route.AgentIdentity.Route != agentRoute ||
						!events.SameDeliveryTargetOwnership(route.Target, events.MustEntitylessReceiverTarget(actor)) || !route.Initialization.Equal(dependency) {
						t.Fatalf("agent must retain empty ownership and exact original construction: route=%+v dependency=%+v err=%v", route, dependency, err)
					}
					continue
				}
				if !events.SameDeliveryTargetOwnership(route.Target, events.MustExistingEntityTarget(header)) || !route.Initialization.Empty() {
					t.Fatal("handler delivery must consume its exact already-constructed receiver")
				}
				if receiver.Empty() {
					receiver = header
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
			if receiver.Empty() {
				t.Fatal("missing constructed initializer receiver for replay corruption controls")
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
			if len(plan.Entities) != 3 || completed != 2 || pending != 2 {
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
			if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET flow_instance=$1 WHERE run_id=$2 AND entity_id=$3`, "foreign", child.ForkRunID, receiver.EntityID); err != nil {
				t.Fatal(err)
			}
			corruptBefore := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			if _, err := owner.ActivateRunFork(ctx, request); err == nil || !strings.Contains(err.Error(), "constructed header projection disagrees with its declared field row") {
				t.Fatalf("corrupt child ownership must reject before replay: %v", err)
			}
			if !reflect.DeepEqual(corruptBefore, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
				t.Fatal("rejected replay changed execution history")
			}
			if _, err := fixture.db.ExecContext(ctx, `UPDATE entity_state SET flow_instance=$1 WHERE run_id=$2 AND entity_id=$3`, receiver.FlowInstance, child.ForkRunID, receiver.EntityID); err != nil {
				t.Fatal(err)
			}
			for _, corrupt := range []struct{ name, column, value, refusal string }{
				{"wrong_path", "instance_path", "foreign", "constructed header projection disagrees with its declared field row"},
				{"foreign_template", "flow_template", "unrelated", "reconstructed replay receiver contradicts fixed-revision ownership"},
				{"wrong_stage", "current_state", "done", "historical workflow state"},
				{"malformed_bookkeeping", "bookkeeping", "[]", "requires persisted JSON object"},
			} {
				t.Run(corrupt.name, func(t *testing.T) {
					var original string
					if err := fixture.db.QueryRowContext(ctx, `SELECT `+corrupt.column+` FROM flow_instances WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, receiver.EntityID).Scan(&original); err != nil {
						t.Fatal(err)
					}
					if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET `+corrupt.column+`=$1 WHERE run_id=$2 AND entity_id=$3`, corrupt.value, child.ForkRunID, receiver.EntityID); err != nil {
						t.Fatal(err)
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					if _, err := owner.ActivateRunFork(ctx, request); err == nil || !strings.Contains(err.Error(), corrupt.refusal) {
						t.Fatalf("corrupt receiver %s must refuse: %v", corrupt.name, err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatal("rejected corrupt receiver changed durable execution history")
					}
					if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET `+corrupt.column+`=$1 WHERE run_id=$2 AND entity_id=$3`, original, child.ForkRunID, receiver.EntityID); err != nil {
						t.Fatal(err)
					}
				})
			}
			t.Run("wrong_run", func(t *testing.T) {
				foreignRun := uuid.NewString()
				seedSelectedActivitySourceRun(t, fixture, foreignRun, source)
				if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET run_id=$1 WHERE run_id=$2 AND entity_id=$3`, foreignRun, child.ForkRunID, receiver.EntityID); err != nil {
					t.Fatal(err)
				}
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				if _, err := owner.ActivateRunFork(ctx, request); err == nil || !strings.Contains(err.Error(), "complete workflow target persistence is required") {
					t.Fatalf("foreign-run header must refuse: %v", err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("foreign-run header refusal changed durable history")
				}
				if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET run_id=$1 WHERE run_id=$2 AND entity_id=$3`, child.ForkRunID, foreignRun, receiver.EntityID); err != nil {
					t.Fatal(err)
				}
			})
			for _, missing := range []struct{ name, table, refusal string }{
				{"state_only_receiver", "flow_instances", "complete workflow target persistence is required"},
				{"missing_declared_fields", "entity_state", "constructed header projection disagrees with its declared field row"},
			} {
				t.Run(missing.name, func(t *testing.T) {
					saved := "saved_replay_receiver"
					if _, err := fixture.db.ExecContext(ctx, `CREATE TABLE `+saved+` AS SELECT * FROM `+missing.table+` WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, receiver.EntityID); err != nil {
						t.Fatal(err)
					}
					if _, err := fixture.db.ExecContext(ctx, `DELETE FROM `+missing.table+` WHERE run_id=$1 AND entity_id=$2`, child.ForkRunID, receiver.EntityID); err != nil {
						t.Fatal(err)
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					if _, err := owner.ActivateRunFork(ctx, request); err == nil || !strings.Contains(err.Error(), missing.refusal) {
						t.Fatalf("incomplete receiver must refuse: %v", err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatal("incomplete receiver refusal changed durable execution history")
					}
					if _, err := fixture.db.ExecContext(ctx, `INSERT INTO `+missing.table+` SELECT * FROM `+saved); err != nil {
						t.Fatal(err)
					}
					if _, err := fixture.db.ExecContext(ctx, `DROP TABLE `+saved); err != nil {
						t.Fatal(err)
					}
				})
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
					header := sourceDelivery.Route.Initialization.ConstructionTarget().Route()
					projected, err := runfork.ProjectEntityOwnership(runID, child.ForkRunID, header.EntityID, header.FlowInstance)
					if err != nil {
						t.Fatal(err)
					}
					header.EntityID, header.FlowInstance = projected.Fork.EntityID, projected.Fork.FlowInstance
					actor := header
					actor.EntityID = ""
					want.Target = events.MustEntitylessReceiverTarget(actor)
					want.Initialization, err = events.AdmitHistoricalFlowReceiverInitialization(child.ForkRunID, events.MustExistingEntityTarget(header), want.AgentIdentity.Route,
						events.HistoricalReceiverConstruction{SourceRunID: runID, SourceRevision: child.ForkPoint.Revision, SourceReceiver: sourceDelivery.Route.AgentIdentity.Route})
					if err != nil {
						t.Fatal(err)
					}
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
