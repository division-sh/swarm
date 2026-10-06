package pipeline_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/testutil/flowroutefixture"
	"github.com/google/uuid"
)

// This starts from an existing receiver, not eager construction. C/E
// same-commit construction and boot activation remain separate proofs.
func TestA2KnownTargetUnarmedPublicationRetainsEarlyRefusalAfterArmAndRestartOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		for _, armBeforeExecution := range []bool{false, true} {
			ordering := "refuse_before_arm"
			if armBeforeExecution {
				ordering = "arm_before_execution"
			}
			t.Run(backend.name+"/"+ordering, func(t *testing.T) {
				selected := backend.open(t)
				runID, instanceID, earlyID := uuid.NewString(), uuid.NewString(), uuid.NewString()
				path := "orders/" + instanceID
				entityID := flowidentity.EntityID(path)
				owner := testRunScopedWorkflowInstanceForRun(runID, path)
				insertGateRecoveryRun(t, selected, runID)
				ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
				source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, a2KnownTargetBindingFiles()))
				collector := externalPipelineSourceNode(t, source, "orders", "collector")
				dispatcher := externalPipelineSourceNode(t, source, "orders", "dispatcher")
				module := proposedEffectProofModule{source: source, nodes: []runtimepipeline.WorkflowNode{
					{Node: collector, Subscriptions: []events.EventType{"orders/item.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
					{Node: dispatcher, Subscriptions: []events.EventType{"orders/dispatch.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
				}}
				probe := &a2HeldWorkerProbe{Probe: lifecycleprobe.New(), eventID: earlyID, release: make(chan struct{})}
				t.Cleanup(probe.resume)
				if !armBeforeExecution {
					probe.resume()
				}
				logger := &exactJoinRuntimeLogger{}
				newBus := func(observer lifecycleprobe.Observer) *runtimebus.EventBus {
					t.Helper()
					bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
						ContractBundle: source, TestLifecycleProbe: observer, Logger: logger,
					}, "platform.join_complete", "platform.join_timeout")
					if err != nil {
						t.Fatalf("new known-target EventBus: %v", err)
					}
					return bus
				}
				bus := newBus(probe)
				schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
				})
				bus.SetInterceptors(pc)
				now := time.Now().UTC()
				constructed := commitA2FixtureConstruction(t, pc, selected.events, ctx, owner, runtimepipeline.WorkflowInstance{
					InstanceID: instanceID, StorageRef: path, EntityID: entityID, WorkflowName: "orders", WorkflowVersion: source.WorkflowVersion(),
					CurrentState: "dispatching", EntityType: "order_state", Fields: map[string]any{"order_id": instanceID, "expected": []any{"a", "b"}},
				}, now)
				publishRoute := func() {
					t.Helper()
					if err := flowroutefixture.Publish(bus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: owner, Instance: constructed.Identity}); err != nil {
						t.Fatalf("publish existing receiver route: %v", err)
					}
				}
				publishRoute()
				load := func() runtimepipeline.WorkflowInstance {
					t.Helper()
					instance, found, err := pc.Load(ctx, owner)
					if err != nil || !found {
						t.Fatalf("load known receiver: found=%v err=%v", found, err)
					}
					return instance
				}
				unarmed := load()
				initialEntry, found, err := workflowlifecycle.LoadStageEntry(unarmed.Bookkeeping)
				if err != nil || !found || initialEntry.Stage != "dispatching" || initialEntry.Cause != "construction" || len(a2KnownTargetArms(t, unarmed)) != 0 {
					t.Fatalf("known receiver did not start outside the join stage: entry=%#v found=%v err=%v", initialEntry, found, err)
				}
				newEvent := func(id, name, payload string) events.Event {
					return eventtest.ExistingRunRootIngressWithRoutingSource(id, events.EventType(path+"/"+name), "operator", "", []byte(payload), 0,
						runID, events.EnvelopeForTargetRoute(events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), path),
							events.RouteIdentity{FlowID: "orders", FlowInstance: path, EntityID: entityID}),
						eventtest.ConcreteTemplateRoutingSource("orders", path, entityID), time.Now().UTC())
				}
				early := newEvent(earlyID, "item.completed", `{"member_id":"a","result":{"value":"early"}}`)
				if err := bus.PublishAcknowledged(ctx, early); err != nil {
					t.Fatalf("publish known-target unarmed arrival: %v", err)
				}
				prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, earlyID)
				if err != nil || !found || len(prepared.DeliveryRoutes) != 1 {
					t.Fatalf("read actual first-publication route: found=%v routes=%d err=%v", found, len(prepared.DeliveryRoutes), err)
				}
				route := prepared.DeliveryRoutes[0]
				if route.Recipient.ID() != collector.Key() || route.Target.Route() != (events.RouteIdentity{FlowID: "orders", FlowInstance: path, EntityID: entityID}) || len(route.Context.Joins) != 1 {
					t.Fatalf("publication lost its exact known receiver: %#v", route)
				}
				receipt := route.Context.Joins[0]
				if receipt.Disposition != events.JoinAdmissionEarly || !receipt.Ref.StageEntry().Empty() || receipt.Ref.HandlerEvent() != "item.completed" || receipt.Ref.Stage() != "awaiting" || !receipt.Ref.Node().Equal(collector) {
					t.Fatalf("unarmed publication did not retain a declaration-only early receipt: %#v", receipt)
				}
				if !armBeforeExecution {
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, early, collector.Key(), "failed", "dead_letter", logger)
					a2KnownTargetEarlyRefusal(t, selected, ctx, earlyID, collector.Key())
					if after := load(); !reflect.DeepEqual(unarmed, after) {
						t.Fatal("early refusal mutated the unarmed receiver")
					}
				} else {
					waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					_, err := probe.WaitForHandlerStarted(waitCtx, earlyID, collector.Key())
					cancel()
					if err != nil {
						t.Fatal(err)
					}
				}
				armEvent := newEvent(uuid.NewString(), "dispatch.completed", `{}`)
				if err := bus.PublishAcknowledged(ctx, armEvent); err != nil {
					t.Fatalf("publish actual join-stage transition: %v", err)
				}
				if armBeforeExecution {
					// The early handler is deliberately still active. Waiting for global
					// quiescence here would prevent the actual arming transition.
					waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					completed, err := probe.WaitForHandlerCompleted(waitCtx, armEvent.ID(), dispatcher.Key())
					cancel()
					if err != nil || completed.Status != "completed" {
						t.Fatalf("arm known target: status=%s err=%v logs=%s", completed.Status, err, logger.String())
					}
					assertExactJoinDeliveryStatus(t, selected, ctx, armEvent.ID(), dispatcher.Key(), "delivered")
				} else {
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, armEvent, dispatcher.Key(), "completed", "delivered", logger)
				}
				armed := load()
				entry, found, err := workflowlifecycle.LoadStageEntry(armed.Bookkeeping)
				arms := a2KnownTargetArms(t, armed)
				if err != nil || !found || entry.Stage != "awaiting" || entry.Cause != "delivery" || entry.EventID != armEvent.ID() || entry == initialEntry ||
					len(arms) != 1 || arms[0].Status != joinruntime.StatusOpen || arms[0].Completed() != 0 || arms[0].JoinRef().StageEntry() != entry ||
					!arms[0].JoinRef().Declaration().Equal(receipt.Ref) || len(armed.TransitionHistory) != 1 {
					t.Fatalf("actual transition did not arm an independent exact entry: entry=%#v arms=%#v err=%v", entry, arms, err)
				}
				if armBeforeExecution {
					probe.resume()
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, early, collector.Key(), "failed", "dead_letter", logger)
					if after := load(); !reflect.DeepEqual(armed, after) {
						t.Fatal("retained early receipt acquired the newly armed entry")
					}
				}
				failure := a2KnownTargetEarlyRefusal(t, selected, ctx, earlyID, collector.Key())

				// Rebuild EventBus and coordinator against the same durable store, not
				// their in-memory publication cache. This is not a process-crash test.
				stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err = schedules.Stop(stopCtx)
				cancel()
				if err != nil {
					t.Fatalf("stop prior schedule owner: %v", err)
				}
				restartedProbe := lifecycleprobe.New()
				bus = newBus(restartedProbe)
				schedules, _ = newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
				pc = newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
					Module: module, GenericSchedules: schedules, TestLifecycleProbe: restartedProbe,
				})
				bus.SetInterceptors(pc)
				publishRoute()
				beforeReplay := load()
				if err := bus.PublishAcknowledged(ctx, prepared.Event.Event()); err != nil {
					t.Fatalf("replay retained early publication through fresh EventBus: %v", err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				err = bus.WaitForQuiescence(waitCtx)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				replayed, found, err := selected.events.LoadPreparedPublishEvent(ctx, earlyID)
				if err != nil || !found || !reflect.DeepEqual(replayed.DeliveryRoutes, prepared.DeliveryRoutes) || !reflect.DeepEqual(beforeReplay, load()) ||
					!reflect.DeepEqual(a2KnownTargetEarlyRefusal(t, selected, ctx, earlyID, collector.Key()), failure) {
					t.Fatalf("restart replay rebound or reexecuted the early publication: found=%v err=%v", found, err)
				}
				assertExactJoinDeliveryCount(t, selected, ctx, earlyID, collector.Key(), 1)

				fresh := newEvent(uuid.NewString(), "item.completed", `{"member_id":"a","result":{"value":"fresh-after-restart"}}`)
				if err := bus.PublishAcknowledged(ctx, fresh); err != nil {
					t.Fatalf("publish fresh arrival to actual arm: %v", err)
				}
				a2KnownTargetWaitForSettlement(t, ctx, bus, restartedProbe, fresh, collector.Key(), "completed", "delivered", logger)
				freshPublication, found, err := selected.events.LoadPreparedPublishEvent(ctx, fresh.ID())
				if err != nil || !found || len(freshPublication.DeliveryRoutes) != 1 || len(freshPublication.DeliveryRoutes[0].Context.Joins) != 1 {
					t.Fatalf("fresh publication has no exact join receipt: found=%v err=%v", found, err)
				}
				freshReceipt := freshPublication.DeliveryRoutes[0].Context.Joins[0]
				final := load()
				finalArms := a2KnownTargetArms(t, final)
				if freshReceipt.Disposition != events.JoinAdmissionBound || !freshReceipt.Ref.Equal(arms[0].JoinRef()) || freshReceipt.Ref.Equal(receipt.Ref) ||
					len(finalArms) != 1 || finalArms[0].Completed() != 1 || finalArms[0].Status != joinruntime.StatusOpen ||
					!reflect.DeepEqual(finalArms[0].Outputs["a"].Value, map[string]any{"value": "fresh-after-restart"}) || len(final.TransitionHistory) != 1 {
					t.Fatalf("fresh arrival did not bind only the actual arm: receipt=%#v arms=%#v", freshReceipt, finalArms)
				}
			})
		}
	}
}

func TestA2KnownTargetWorkIssuedBeforeArmPublishesOutputBoundToActualArmOnBothStores(t *testing.T) {
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			runID, instanceID, triggerID := uuid.NewString(), uuid.NewString(), uuid.NewString()
			path := "orders/" + instanceID
			entityID := flowidentity.EntityID(path)
			owner := testRunScopedWorkflowInstanceForRun(runID, path)
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			files := canonicalrouting.ArrivalJoinRoutingFiles(t, canonicalrouting.ArrivalJoinPayloadDirectedBeforeArm)
			source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, files))
			worker := externalPipelineSourceNode(t, source, ".", "worker")
			collector := externalPipelineSourceNode(t, source, "orders", "collector")
			dispatcher := externalPipelineSourceNode(t, source, "orders", "dispatcher")
			probe := &a2HeldWorkerProbe{Probe: lifecycleprobe.New(), eventID: triggerID, release: make(chan struct{})}
			t.Cleanup(probe.resume)
			logger := &exactJoinRuntimeLogger{}
			module := proposedEffectProofModule{source: source, nodes: []runtimepipeline.WorkflowNode{
				{Node: worker, Subscriptions: []events.EventType{"work.requested"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
				{Node: collector, Subscriptions: []events.EventType{"orders/item.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
				{Node: dispatcher, Subscriptions: []events.EventType{"orders/dispatch.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
			}}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{
				ContractBundle: source, TestLifecycleProbe: probe, Logger: logger,
			}, "platform.join_complete", "platform.join_timeout")
			if err != nil {
				t.Fatal(err)
			}
			schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			pc := newGateRecoveryCoordinator(bus, selected, runtimepipeline.PipelineCoordinatorOptions{
				Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe,
			})
			bus.SetInterceptors(pc)
			now := time.Now().UTC()
			parent := commitA2FixtureConstruction(t, pc, selected.events, ctx, testRunScopedWorkflowInstanceForRun(runID, runID), runtimepipeline.WorkflowInstance{
				InstanceID: runID, StorageRef: runID, EntityID: runID, WorkflowName: source.WorkflowName(), WorkflowVersion: source.WorkflowVersion(),
				CurrentState: "active", EntityType: "root_state", Fields: map[string]any{"work_count": int64(0)},
			}, now)
			// Existing-receiver readiness/route fixtures do not prove C/E eager
			// construction or same-commit runtime boot activation.
			readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
				Identity: flowidentity.Instance{TemplateID: "orders", ScopeKey: "orders", InstanceID: instanceID, InstancePath: path, EntityID: entityID, HasStoredPath: true},
				RunID:    runID, BundleHash: authorActivityTestSourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
			}
			readiness.Identity.ParentRoute = flowidentity.ParentRoute{FlowID: parent.Identity.TemplateID, FlowInstance: parent.Identity.InstancePath, EntityID: parent.Identity.EntityID}
			readiness.Identity.ParentEntityID = parent.Identity.EntityID
			constructed := commitA2FixtureConstruction(t, pc, selected.events, ctx, owner, runtimepipeline.WorkflowInstance{
				InstanceID: instanceID, StorageRef: path, EntityID: entityID, WorkflowName: "orders", WorkflowVersion: source.WorkflowVersion(),
				ParentFlowID: parent.Identity.TemplateID, ParentFlowInstance: parent.Identity.InstancePath, ParentEntityID: parent.Identity.EntityID,
				Mode: "template", RuntimeReadiness: &readiness, CurrentState: "dispatching", EntityType: "order_state",
				Fields: map[string]any{"order_id": instanceID, "expected": []any{"a", "b"}},
			}, now)
			markGateRecoveryTopologyReadyFixture(t, selected, readiness, now)
			if err := flowroutefixture.Publish(bus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: owner, Instance: constructed.Identity}); err != nil {
				t.Fatal(err)
			}
			load := func() runtimepipeline.WorkflowInstance {
				t.Helper()
				instance, found, err := pc.Load(ctx, owner)
				if err != nil || !found {
					t.Fatalf("load existing output receiver: found=%v err=%v", found, err)
				}
				return instance
			}
			unarmed := load()
			initialEntry, found, err := workflowlifecycle.LoadStageEntry(unarmed.Bookkeeping)
			if err != nil || !found || initialEntry.Stage != "dispatching" || len(a2KnownTargetArms(t, unarmed)) != 0 {
				t.Fatalf("work receiver was already armed: entry=%#v found=%v err=%v", initialEntry, found, err)
			}
			payload, err := json.Marshal(map[string]any{"prefix": instanceID[:18], "suffix": instanceID[18:]})
			if err != nil {
				t.Fatal(err)
			}
			work := eventtest.ExistingRunRootIngressWithRoutingSource(triggerID, "work.requested", "operator", "", payload, 0, runID,
				events.EnvelopeForEntityID(events.EventEnvelope{}, runID), eventtest.RootRoutingSource(runID), now)
			if err := bus.PublishAcknowledged(ctx, work); err != nil {
				t.Fatalf("issue actual ordinary work while receiver is unarmed: %v", err)
			}
			waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err = probe.WaitForHandlerStarted(waitCtx, triggerID, worker.Key())
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			issued, found, err := selected.events.LoadPreparedPublishEvent(ctx, triggerID)
			if err != nil || !found || len(issued.DeliveryRoutes) != 1 || issued.DeliveryRoutes[0].Recipient.ID() != worker.Key() || len(issued.DeliveryRoutes[0].Context.Joins) != 0 || !reflect.DeepEqual(unarmed, load()) {
				t.Fatalf("ordinary work issuance was not independent of the unarmed receiver: found=%v routes=%#v err=%v", found, issued.DeliveryRoutes, err)
			}
			outputs := func() (int, string) {
				t.Helper()
				query := `SELECT COUNT(*), COALESCE(MIN(CAST(event_id AS TEXT)), '') FROM events WHERE run_id=? AND source_event_id=? AND event_name='item.completed'`
				if selected.postgres {
					query = `SELECT COUNT(*), COALESCE(MIN(event_id::text), '') FROM events WHERE run_id=$1::uuid AND source_event_id=$2::uuid AND event_name='item.completed'`
				}
				var count int
				var id string
				if err := selected.db.QueryRowContext(ctx, query, runID, triggerID).Scan(&count, &id); err != nil {
					t.Fatalf("read actual worker outputs: %v", err)
				}
				return count, id
			}
			if count, id := outputs(); count != 0 || id != "" {
				t.Fatalf("worker output was published before receiver arm: count=%d id=%q", count, id)
			}
			armEvent := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/dispatch.completed"), "operator", "", []byte(`{}`), 0,
				runID, events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), path),
				eventtest.ConcreteTemplateRoutingSource("orders", path, entityID), time.Now().UTC())
			if err := bus.PublishAcknowledged(ctx, armEvent); err != nil {
				t.Fatalf("arm existing receiver before first output publication: %v", err)
			}
			waitCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
			completed, err := probe.WaitForHandlerCompleted(waitCtx, armEvent.ID(), dispatcher.Key())
			cancel()
			if err != nil || completed.Status != "completed" {
				t.Fatalf("actual receiver arming: completed=%#v err=%v logs=%s", completed, err, logger.String())
			}
			assertExactJoinDeliveryStatus(t, selected, ctx, armEvent.ID(), dispatcher.Key(), "delivered")
			armed := load()
			entry, found, err := workflowlifecycle.LoadStageEntry(armed.Bookkeeping)
			arms := a2KnownTargetArms(t, armed)
			if err != nil || !found || entry.Stage != "awaiting" || entry.EventID != armEvent.ID() || entry.Cause != "delivery" || entry == initialEntry ||
				len(arms) != 1 || arms[0].JoinRef().StageEntry() != entry || arms[0].Status != joinruntime.StatusOpen || arms[0].Completed() != 0 {
				t.Fatalf("existing receiver did not acquire an actual arm before output: entry=%#v arms=%#v err=%v", entry, arms, err)
			}
			if count, id := outputs(); count != 0 || id != "" {
				t.Fatalf("output publication preceded worker release: count=%d id=%q", count, id)
			}
			probe.resume()
			a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, work, worker.Key(), "completed", "delivered", logger)
			count, outputID := outputs()
			if count != 1 || outputID == "" {
				t.Fatalf("ordinary worker did not commit exactly one real output: count=%d id=%q", count, outputID)
			}
			output, found, err := selected.events.LoadPreparedPublishEvent(ctx, outputID)
			if err != nil || !found || len(output.DeliveryRoutes) != 1 {
				t.Fatalf("load actual generated first publication: found=%v routes=%#v err=%v", found, output.DeliveryRoutes, err)
			}
			route := output.DeliveryRoutes[0]
			handlerNode, handlerEvent, authorized := route.ConnectClaim.NodeHandlerOwner()
			resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(source, handlerNode, string(handlerEvent))
			if route.Recipient.ID() != collector.Key() || route.Target.Route() != (events.RouteIdentity{FlowID: "orders", FlowInstance: path, EntityID: entityID}) ||
				len(route.Context.Joins) != 1 || route.Context.Joins[0].Disposition != events.JoinAdmissionBound || !route.Context.Joins[0].Ref.Equal(arms[0].JoinRef()) ||
				!authorized || !handlerNode.Equal(collector) || !resolved.Matched || resolved.HandlerEventKey != "item.completed" {
				t.Fatalf("generated output did not bind the actual arm at first publication: %#v", route)
			}
			a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, output.Event.Event(), collector.Key(), "completed", "delivered", logger)
			final := load()
			finalArms := a2KnownTargetArms(t, final)
			if len(finalArms) != 1 || finalArms[0].Completed() != 1 || finalArms[0].Status != joinruntime.StatusOpen || !finalArms[0].JoinRef().Equal(arms[0].JoinRef()) ||
				!reflect.DeepEqual(finalArms[0].Outputs["a"].Value, map[string]any{"value": "computed-by-worker"}) || len(final.TransitionHistory) != 1 {
				t.Fatalf("generated output did not contribute only to the actual armed entry: %#v", finalArms)
			}
			beforeReplay := load()
			if err := bus.PublishAcknowledged(ctx, output.Event.Event()); err != nil {
				t.Fatal(err)
			}
			waitCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
			err = bus.WaitForQuiescence(waitCtx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			replayed, found, err := selected.events.LoadPreparedPublishEvent(ctx, outputID)
			if err != nil || !found || !reflect.DeepEqual(replayed.DeliveryRoutes, output.DeliveryRoutes) || !reflect.DeepEqual(beforeReplay, load()) {
				t.Fatalf("generated output replay rebound or contributed twice: found=%v err=%v", found, err)
			}
			assertExactJoinDeliveryCount(t, selected, ctx, outputID, collector.Key(), 1)
		})
	}
}

func a2KnownTargetArms(t *testing.T, instance runtimepipeline.WorkflowInstance) []joinruntime.Activation {
	t.Helper()
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	arms, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	return arms
}

func a2KnownTargetWaitForSettlement(t *testing.T, ctx context.Context, bus *runtimebus.EventBus, probe *lifecycleprobe.Probe, event events.Event, nodeKey, handlerStatus, deliveryStatus string, logger *exactJoinRuntimeLogger) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	completed, cursor, handlerErr := probe.WaitAfter(waitCtx, lifecycleprobe.Cursor{}, lifecycleprobe.Signal{
		Kind: lifecycleprobe.HandlerCompleted, EventID: event.ID(), SubscriberType: "node", SubscriberID: nodeKey,
	})
	settled, _, settleErr := probe.WaitAfter(waitCtx, cursor, lifecycleprobe.Signal{
		Kind: lifecycleprobe.DeliveryStatusChanged, EventID: event.ID(), SubscriberType: "node", SubscriberID: nodeKey, Status: deliveryStatus,
	})
	quietErr := bus.WaitForQuiescence(waitCtx)
	if handlerErr != nil || completed.Status != handlerStatus || settleErr != nil || quietErr != nil {
		t.Fatalf("actual handler settlement: completed=%#v err=%v settled=%#v err=%v quiet=%v logs=%s", completed, handlerErr, settled, settleErr, quietErr, logger.String())
	}
}

func a2KnownTargetEarlyRefusal(t *testing.T, selected gateRecoveryStoreCase, ctx context.Context, eventID, nodeKey string) failures.Envelope {
	t.Helper()
	query := `SELECT d.status, CAST(COALESCE(d.failure, '{}') AS TEXT), COUNT(a.delivery_id), COALESCE(MAX(a.outcome), '')
		FROM event_deliveries d LEFT JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id AND a.closure_kind='settled'
		WHERE d.event_id=? AND d.subscriber_type='node' AND d.subscriber_id=? GROUP BY d.status, d.failure`
	if selected.postgres {
		query = `SELECT d.status, COALESCE(d.failure, '{}'::jsonb)::text, COUNT(a.delivery_id), COALESCE(MAX(a.outcome), '')
			FROM event_deliveries d LEFT JOIN event_delivery_attempts a ON a.delivery_id=d.delivery_id AND a.closure_kind='settled'
			WHERE d.event_id=$1::uuid AND d.subscriber_type='node' AND d.subscriber_id=$2 GROUP BY d.status, d.failure`
	}
	var status, raw, outcome string
	var attempts int
	if err := selected.db.QueryRowContext(ctx, query, eventID, nodeKey).Scan(&status, &raw, &attempts, &outcome); err != nil {
		t.Fatalf("read actual early refusal: %v", err)
	}
	var failure failures.Envelope
	if err := json.Unmarshal([]byte(raw), &failure); err != nil {
		t.Fatal(err)
	}
	if status != "dead_letter" || outcome != "dead_letter" || attempts != 1 || failure.Class != failures.ClassEarlyArrival || failure.Detail.Code != "join_not_armed" || failure.Retryable {
		t.Fatalf("early publication lost its durable refusal: status=%s outcome=%s settled_attempts=%d failure=%#v", status, outcome, attempts, failure)
	}
	return failure
}

func a2KnownTargetBindingFiles() map[string]string {
	return map[string]string{
		"schema.yaml": "name: a2-known-target-binding\nstages:\n  active: {}\n",
		"types.yaml":  "types:\n  JoinResult:\n    value: text\n",
		"orders/schema.yaml": `name: orders
instance: order_id
stages:
  dispatching: {}
  awaiting: {}
  ready: {final: true}
  attention: {final: true}
`,
		"orders/entities.yaml": "order_state:\n  order_id: {type: text, indexed: true}\n  expected: \"[text]\"\n",
		"orders/events.yaml":   "item.completed:\n  member_id: text\n  result: JoinResult\ndispatch.completed:\n",
		"orders/nodes.yaml": `collector:
  execution_type: system_node
  event_handlers:
    item.completed:
      join:
        stage: awaiting
        members: {from: state.expected, by: payload.member_id}
        output: payload.result
        deadline: {after: 1h, from: stage_entry}
        on_complete: {advances_to: ready}
        on_deadline: {advances_to: attention}
dispatcher:
  execution_type: system_node
  event_handlers:
    dispatch.completed: {advances_to: awaiting}
`,
	}
}
