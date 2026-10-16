package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
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
			constructHistoricalSourceFixture(t, ctx, fixture.store.(agentFixtureFlowStore), sqliteFlowActivationRequest(bundle, ".", runID, "", runID))
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
				construction := route.Target
				if !route.Initialization.Empty() {
					if err := route.Initialization.ValidateEvent(trigger); err != nil {
						t.Fatalf("source construction publication: %v", err)
					}
					if err := route.Initialization.ValidateRoute(route); err != nil {
						t.Fatalf("source construction/actor coordinates: %v", err)
					}
					construction = route.Initialization.ConstructionTarget()
				}
				if !construction.ExistingEntity() {
					t.Fatalf("handler delivery must consume already-constructed flow: %+v", route)
				}
				if route.Recipient.IsAgent() {
					if !route.Target.EntitylessReceiver() || route.Target.Route().EntityID != "" || route.Initialization.Empty() {
						t.Fatalf("static actor must be exactly entityless with its independent construction: %+v", route)
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
			receiver := command.Commit.DeliveryRoutes[0].Initialization.ConstructionTarget().Route()
			if receiver.EntityID == "" {
				t.Fatal("corruption proof requires the exact existing flow construction, not an entityless actor")
			}
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
				foreignSource := stateOnlyRootAcquisitionSource(t, "foreign-replay-parent")
				foreignBundle, _ := semanticview.Bundle(foreignSource)
				foreignCtx := correlation.WithRunID(seedSelectedActivitySourceRun(t, fixture, foreignRun, foreignSource), foreignRun)
				constructHistoricalSourceFixture(t, foreignCtx, fixture.store.(agentFixtureFlowStore), sqliteFlowActivationRequest(foreignBundle, ".", foreignRun, "", foreignRun))
				if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET run_id=$1, parent_instance=$4 WHERE run_id=$2 AND entity_id=$3`, foreignRun, child.ForkRunID, receiver.EntityID, foreignRun); err != nil {
					t.Fatal(err)
				}
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				if _, err := owner.ActivateRunFork(ctx, request); err == nil || !strings.Contains(err.Error(), "complete workflow target persistence is required") {
					t.Fatalf("foreign-run header must refuse: %v", err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("foreign-run header refusal changed durable history")
				}
				if _, err := fixture.db.ExecContext(ctx, `UPDATE flow_instances SET run_id=$1, parent_instance=$4 WHERE run_id=$2 AND entity_id=$3`, child.ForkRunID, foreignRun, receiver.EntityID, child.ForkRunID); err != nil {
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
			t.Run("rollback_after_initial_delivery_before_complete_lineage", func(t *testing.T) {
				before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				// The lineage's delivery FK proves that the initial publication and
				// delivery have been inserted before this existing fault fires.
				remove := installReceiverComposedFault(t, fixture.db, backend.name, "run_fork_delivery_event_replays", "INSERT", "")
				activation, err := owner.ActivateRunFork(ctx, request)
				remove()
				if err == nil || !strings.Contains(err.Error(), "typed_receiver_owner_fault") || activation.Activated {
					t.Fatalf("lineage fault must refuse after initial delivery: %+v err=%v", activation, err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("failed replay leaked event, scope, delivery, lineage, source freeze or revision effects")
				}
			})
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
					construction := sourceDelivery.Route.Initialization.ConstructionTarget()
					want.Initialization, err = events.AdmitHistoricalFlowReceiverInitialization(child.ForkRunID, construction, want.AgentIdentity.Route,
						events.HistoricalReceiverConstruction{SourceRunID: runID, SourceRevision: plan.ForkPoint.Revision, SourceReceiver: sourceDelivery.Route.AgentIdentity.Route})
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
			adapter := sqliteDeliveryAdapter
			if backend.name == "postgres" {
				adapter = postgresDeliveryAdapter
			}
			publicationOwner := fixture.store.(eventCommitTxStore)
			for _, test := range []struct {
				name    string
				prepare func() bus.PreparedPublishEvent
				refusal error
				message string
			}{
				{"exact_duplicate", func() bus.PreparedPublishEvent { return prepared }, nil, ""},
				{"crossed_route_aggregate", func() bus.PreparedPublishEvent {
					candidate := prepared
					candidate.DeliveryRoutes = append([]events.DeliveryRoute(nil), prepared.DeliveryRoutes...)
					candidate.DeliveryRoutes[0].PayloadProjection, err = events.NewDeliveryPayloadProjection(map[string]string{"hostile_receipt": "crossed"})
					if err != nil {
						t.Fatal(err)
					}
					return candidate
				}, nil, "historical replay duplicate crosses its complete durable delivery aggregate"},
				{"conflicting_duplicate_event", func() bus.PreparedPublishEvent {
					candidate := prepared
					record, err := eventrecord.FromAdmitted(prepared.Event, prepared.Settlement)
					if err != nil {
						t.Fatal(err)
					}
					record.TaskID = "foreign-replay-task"
					candidate.Event, err = record.Decode()
					if err != nil {
						t.Fatal(err)
					}
					return candidate
				}, events.ErrEventIdentityConflict, ""},
			} {
				t.Run(test.name, func(t *testing.T) {
					candidate := test.prepare()
					if err := candidate.Validate(); err != nil {
						t.Fatalf("hostile publication must reach the initial owner, not fail its unrelated setup: %v", err)
					}
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					var outcome bus.EventAppendOutcome
					var proofs []deliverylifecycle.DurableHandoffProof
					err := runSelectedFixtureMutation(ctx, fixture.store, "historical replay initial owner", func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
						var err error
						outcome, proofs, err = adapter.CommitHistoricalReplayInitialPublication(ctx, attempt, publicationOwner, candidate, children[0].Authority)
						return err
					})
					if test.refusal != nil {
						if !errors.Is(err, test.refusal) {
							t.Fatalf("duplicate identity refusal=%v, want %v", err, test.refusal)
						}
					} else if test.message != "" {
						if err == nil || !strings.Contains(err.Error(), test.message) {
							t.Fatalf("crossed aggregate refusal=%v", err)
						}
					} else if err != nil || outcome != bus.EventAppendExactDuplicate || len(proofs) != len(children) {
						t.Fatalf("exact duplicate lost its admitted aggregate: outcome=%v proofs=%d err=%v", outcome, len(proofs), err)
					}
					if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatal("duplicate replay changed durable event, scope, delivery, lineage, construction or revision evidence")
					}
				})
			}
			after := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
			if _, err := owner.ActivateRunFork(ctx, request); err == nil || !strings.Contains(err.Error(), `fork activation requires materialized fork status "paused"; got "running"`) {
				t.Fatalf("duplicate activation must retain exact lifecycle refusal: %v", err)
			}
			if !reflect.DeepEqual(after, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
				t.Fatal("duplicate activation mutated durable history")
			}
			t.Run("fresh_claim_recovery_and_exact_settlement", func(t *testing.T) {
				childCtx := correlation.WithRunID(ctx, child.ForkRunID)
				fresh := restartedEventDeliveryRouteReadbackStore(t, childCtx, fixture).(deliverylifecycle.Store)
				beforeHandoff := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				for _, pending := range children {
					observation, err := fresh.ObserveDeliveryContinuation(childCtx, pending.Authority, pending.DeliveryID)
					if err != nil || observation.Disposition != deliverylifecycle.ClaimDeferred || observation.Invariant != nil {
						t.Fatalf("historical replay escaped its pending pipeline handoff: %+v err=%v", observation, err)
					}
					refused, err := fresh.ClaimDelivery(childCtx, pending.Authority, prepared.Event.Event(), pending.Route)
					if _, acquired := refused.Acquired(); err != nil || acquired || refused.Disposition != deliverylifecycle.ClaimDeferred || refused.Renewal.Acknowledged {
						t.Fatalf("historical first claim bypassed the pipeline handoff: %+v err=%v", refused, err)
					}
				}
				if !reflect.DeepEqual(beforeHandoff, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
					t.Fatal("deferred historical claims changed durable evidence")
				}
				if err := acknowledgePipelineEventFixture(childCtx, fresh, childEventID); err != nil {
					t.Fatalf("accept replay through the existing pipeline settlement/handoff owner: %v", err)
				}
				children = admitHistoricalReplayReceiverClaimFixtures(t, childCtx, fresh, children, fact)
				immutableBefore := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				inventory, err := fresh.InspectDeliveryRecovery(childCtx, fact)
				if err != nil {
					t.Fatal(err)
				}
				foundFork := false
				for _, run := range inventory.Runs {
					if run.RunID == child.ForkRunID {
						foundFork = true
						if run.Pending != 2 || run.InProgress != 0 || run.Failed != 0 {
							t.Fatalf("fresh recovery lost the exact two historical obligations: %+v", run)
						}
					}
				}
				if !foundFork {
					t.Fatal("fresh recovery omitted the fork's historical obligations")
				}
				for _, pending := range children {
					if pending.Route.Target.Route().EntityID != "" || pending.Route.Initialization.Empty() {
						t.Fatal("recovery conflated the empty actor with historical construction")
					}
					single, err := fresh.ObserveDeliveryContinuation(childCtx, pending.Authority, pending.DeliveryID)
					if err != nil || single.Disposition != deliverylifecycle.ClaimAcquired || single.Invariant != nil || single.Wake != (deliverylifecycle.ContinuationWake{}) {
						t.Fatalf("fresh historical continuation lost exact eligibility: %+v err=%v", single, err)
					}
					batch, err := fresh.ObserveDeliveryContinuations(childCtx, pending.Authority, []string{pending.DeliveryID})
					if err != nil || len(batch) != 1 || !reflect.DeepEqual(single, batch[0]) {
						t.Fatalf("fresh historical batch changed exact eligibility: %+v err=%v", batch, err)
					}
					page, err := fresh.ScanDeliveryContinuations(childCtx, pending.Authority, deliverylifecycle.ContinuationCursor{}, 64)
					if err != nil {
						t.Fatal(err)
					}
					matched := 0
					for _, item := range page.Items {
						if item.DeliveryID != pending.DeliveryID {
							continue
						}
						matched++
						if item.Disposition != deliverylifecycle.ClaimAcquired || item.Invariant != nil || item.Wake != (deliverylifecycle.ContinuationWake{}) ||
							!reflect.DeepEqual(item.Snapshot, pending) || !reflect.DeepEqual(item.Event, prepared.Event.Event()) {
							t.Fatalf("fresh historical scan changed its event/route/authority: %+v", item)
						}
					}
					unchanged, err := fresh.Snapshot(childCtx, pending.DeliveryID)
					if err != nil || matched != 1 || !reflect.DeepEqual(pending, unchanged) || unchanged.ClaimVersion != 0 {
						t.Fatalf("fresh observations mutated or omitted the pending historical obligation: matches=%d %+v err=%v", matched, unchanged, err)
					}
					first, err := fresh.ClaimDelivery(childCtx, pending.Authority, prepared.Event.Event(), pending.Route)
					claimed, acquired := first.Acquired()
					if err != nil || !acquired || !first.Acknowledged || !first.Renewal.Acknowledged || claimed.Claim.Version() != 1 ||
						claimed.Snapshot.DeliveryID != pending.DeliveryID || !reflect.DeepEqual(claimed.Snapshot.Route, pending.Route) {
						t.Fatalf("fresh historical first claim: %+v err=%v", first, err)
					}
					if err := ExpireExactDeliveryClaimFaultForTest(childCtx, fresh, claimed.Claim); err != nil {
						t.Fatal(err)
					}
					recoveredStore := restartedEventDeliveryRouteReadbackStore(t, childCtx, fixture).(deliverylifecycle.Store)
					expired, err := recoveredStore.Snapshot(childCtx, pending.DeliveryID)
					if err != nil || expired.Status != deliverylifecycle.StatusInProgress || expired.ClaimVersion != 1 {
						t.Fatalf("fresh expired historical claim: %+v err=%v", expired, err)
					}
					assertClaimHistoryContinuations(t, childCtx, recoveredStore, expired, deliverylifecycle.ClaimReclaimable, false)
					second, err := recoveredStore.ClaimDelivery(childCtx, pending.Authority, prepared.Event.Event(), pending.Route)
					resumed, acquired := second.Acquired()
					if err != nil || !acquired || !second.Acknowledged || second.Previous != deliverylifecycle.ClaimReclaimable ||
						resumed.Claim.Version() != claimed.Claim.Version()+1 || resumed.Snapshot.DeliveryID != pending.DeliveryID ||
						!reflect.DeepEqual(resumed.Snapshot.Route, pending.Route) {
						t.Fatalf("fresh historical recovery changed its exact dependency/claim: %+v err=%v", second, err)
					}
					beforeStale := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
					if _, err := recoveredStore.SettleSuccess(childCtx, claimed.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); !errors.Is(err, deliverylifecycle.ErrConflict) {
						t.Fatalf("stale historical claim settled its successor: %v", err)
					}
					if !reflect.DeepEqual(beforeStale, snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")) {
						t.Fatal("stale historical settlement changed durable evidence")
					}
					settled, err := recoveredStore.SettleSuccess(childCtx, resumed.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
					if err != nil || !settled.MatchesSettlementClaim(resumed.Claim) || settled.Status != deliverylifecycle.StatusDelivered ||
						!reflect.DeepEqual(settled.Route, pending.Route) {
						t.Fatalf("exact recovered historical settlement: %+v err=%v", settled, err)
					}
					outcomes, err := recoveredStore.Outcomes(childCtx, pending.DeliveryID)
					if err != nil || len(outcomes) != 1 || outcomes[0].ClaimVersion != resumed.Claim.Version() || outcomes[0].Outcome != string(deliverylifecycle.StatusDelivered) {
						t.Fatalf("historical recovery did not settle exactly once: %+v err=%v", outcomes, err)
					}
				}
				immutableAfter := snapshotForkHistoricalExecutionTables(t, fixture.db, backend.name == "postgres")
				for _, table := range []string{"events", "committed_replay_scopes", "flow_instances", "entity_state", "workflow_instance_initial_materializations", "run_fork_delivery_event_replays"} {
					prior, existed := immutableBefore[table]
					current, exists := immutableAfter[table]
					if !existed || !exists || !reflect.DeepEqual(prior, current) {
						t.Fatalf("claim/recovery mutated immutable publication, lineage or construction table %s", table)
					}
				}
				exact, found, err := restartedEventDeliveryRouteReadbackStore(t, childCtx, fixture).(preparedPublishEventReadbackStore).LoadPreparedPublishEvent(childCtx, childEventID)
				if err != nil || !found || !reflect.DeepEqual(prepared, exact) {
					t.Fatalf("fresh recovery changed the original prepared publication: found=%t err=%v", found, err)
				}
			})
			sourceAfter, err := reader.DeliverySnapshotsForEvent(ctx, trigger.ID())
			if err != nil || !reflect.DeepEqual(sourceDeliveries, sourceAfter) {
				for _, prior := range sourceDeliveries {
					for _, current := range sourceAfter {
						if prior.DeliveryID == current.DeliveryID && !reflect.DeepEqual(prior, current) {
							t.Logf("source delivery %s status=%s->%s route_equal=%t authority=%s/%d->%s/%d claim_version=%d->%d updated=%s->%s",
								prior.DeliveryID, prior.Status, current.Status, reflect.DeepEqual(prior.Route, current.Route),
								prior.Authority.ExecutionID(), prior.Authority.Generation(), current.Authority.ExecutionID(), current.Authority.Generation(),
								prior.ClaimVersion, current.ClaimVersion, prior.UpdatedAt.Format(time.RFC3339Nano), current.UpdatedAt.Format(time.RFC3339Nano))
						}
					}
				}
				t.Fatalf("child replay changed source deliveries: %v\nbefore=%+v\nafter=%+v", err, sourceDeliveries, sourceAfter)
			}
		})
	}
}

func admitHistoricalReplayReceiverClaimFixtures(t *testing.T, ctx context.Context, selected deliverylifecycle.Store, pending []deliverylifecycle.Snapshot, source correlation.SourceArtifactFact) []deliverylifecycle.Snapshot {
	t.Helper()
	coordinate := agenttopology.SourceCoordinate{BundleHash: source.BundleHash()}
	records := make([]manager.PersistedAgent, len(pending))
	desired := make([]agenttopology.DesiredAgent, len(pending))
	for i, delivery := range pending {
		records[i] = agentFixtureStaticRecord(t, delivery.Route.AgentIdentity)
		records[i].Config.FlowID = delivery.Route.Initialization.ConstructionTarget().Route().FlowID
		records[i].Config = resolveNativeClaimFixtureActor(t, records[i].Config)
		if records[i].Config.EntityID != "" {
			t.Fatal("historical actor fixture must remain exactly entityless")
		}
		plan, err := delivery.Route.AgentIdentity.Plan()
		if err != nil {
			t.Fatal(err)
		}
		revision, err := manager.AgentConfigPlanRevision(records[i].Config, plan)
		if err != nil {
			t.Fatal(err)
		}
		desired[i] = agenttopology.DesiredAgent{Identity: plan, Source: coordinate, ConfigRevision: revision}
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, desired)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := agentfixture.AdmitGeneration(t, ctx, selected.(agentfixture.Store), plan, coordinate)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := agenttopology.StaticAdmission(plan.Revision, coordinate.BundleHash, agenttopology.LifetimeDurableManaged)
	if err != nil {
		t.Fatal(err)
	}
	for i, record := range records {
		record.Topology = admission
		registered, err := grant.CommitAgentLifecycleTransition(ctx, manager.AgentLifecycleTransition{
			DiagnosticOrigin: manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation},
			OperationID:      uuid.NewString(), OperationKind: "spawn", RequestHash: uuid.NewString(), Identity: record.Config.Identity, AgentID: record.Config.ID,
			Trigger: "historical-claim-proof", TargetEpoch: 1, TargetGeneration: 1, TargetPhase: manager.AgentLifecycleRegistered,
			ConfigRevision: desired[i].ConfigRevision, RunMode: manager.AgentRunModeStopped, Agent: &record, Topology: admission, Now: record.StartedAt,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := grant.CommitAgentLifecycleTransition(ctx, manager.AgentLifecycleTransition{
			DiagnosticOrigin: manager.LifecycleDiagnosticOrigin{Owner: manager.LifecycleDiagnosticNormal, Causality: manager.LifecycleDiagnosticObservation},
			OperationID:      uuid.NewString(), OperationKind: "start", RequestHash: uuid.NewString(), Identity: record.Config.Identity, AgentID: record.Config.ID,
			Trigger: "historical-claim-proof", ExpectedEpoch: registered.RuntimeEpoch, ExpectedGeneration: registered.Generation, ExpectedPhase: registered.Phase,
			TargetEpoch: registered.RuntimeEpoch, TargetGeneration: registered.Generation + 1, TargetPhase: manager.AgentLifecycleRunning,
			ConfigRevision: desired[i].ConfigRevision, RunMode: manager.AgentRunModeStandard, Topology: admission, Now: record.StartedAt.Add(time.Microsecond),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := grant.AdmitExecution(ctx); err != nil {
		t.Fatal(err)
	}
	evidence, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(source, evidence.GrantID, evidence.RuntimeGeneration)
	if err != nil {
		t.Fatal(err)
	}
	if err := selected.ActivateDeliveryAuthority(ctx, authority); err != nil {
		t.Fatal(err)
	}
	rebound := make([]deliverylifecycle.Snapshot, len(pending))
	for i, prior := range pending {
		rebound[i], err = selected.Snapshot(ctx, prior.DeliveryID)
		if err != nil || !rebound[i].Authority.Equal(authority) || !reflect.DeepEqual(prior.Route, rebound[i].Route) || rebound[i].ClaimVersion != 0 || rebound[i].Status != deliverylifecycle.StatusPending {
			t.Fatalf("existing authority activation changed historical obligation ownership: %+v err=%v", rebound[i], err)
		}
	}
	return rebound
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
