package pipeline_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/division-sh/swarm/internal/testutil/flowroutefixture"
	"github.com/google/uuid"
)

// A real paired request creates return authority before the provider runs. This
// deliberately differs from the ordinary, previously unbound output journey.
func TestA2StageEntryBoundReturnIsolationOnBothStores(t *testing.T) {
	testA2BoundReplyJourney(t, "", "")
}

func TestA2BoundReplyCanonicalJoinOwnerRefusesCorruptedEntryOnBothStores(t *testing.T) {
	for _, field := range []string{"flow_scope", "instance_id"} {
		t.Run(field, func(t *testing.T) {
			testA2BoundReplyJourney(t, field, "")
		})
	}
}

func TestA2BoundReplyDoesNotLendAdmissionToLaterSiblingOnBothStores(t *testing.T) {
	testA2BoundReplyJourney(t, "", "requester")
}

func TestA2BoundReplyDoesNotExpandToLaterOrdinaryConnectObserverOnBothStores(t *testing.T) {
	testA2BoundReplyJourney(t, "", "observer")
}

func testA2BoundReplyJourney(t *testing.T, corruptEntryField, siblingFlow string) {
	t.Helper()
	lateSibling := siblingFlow != ""
	for _, backend := range []struct {
		name string
		open func(*testing.T) gateRecoveryStoreCase
	}{{"sqlite", openSQLiteGateRecoveryStore}, {"postgres", openPostgresGateRecoveryStore}} {
		t.Run(backend.name, func(t *testing.T) {
			selected := backend.open(t)
			runID, key := uuid.NewString(), uuid.NewString()
			insertGateRecoveryRun(t, selected, runID)
			ctx := withLiveGateExecution(correlation.WithRunID(testAuthorActivityContext(t, context.Background()), runID))
			variant := canonicalrouting.ArrivalJoinBoundReply
			if siblingFlow == "observer" {
				variant = canonicalrouting.ArrivalJoinBoundReplyObserver
			}
			files := canonicalrouting.ArrivalJoinRoutingFiles(t, variant)
			source := semanticview.Wrap(loadPipelineLifecycleFixtureBundle(t, files))
			if siblingFlow == "observer" {
				if issues := pinrouting.CompileConnectGraph(source).Issues(); len(issues) != 0 {
					t.Fatalf("ordinary observer fixture is not a compiler-admitted Connect surface: %#v", issues)
				}
			}
			var commits *a2SameCommitObservedPersistence
			if corruptEntryField != "" {
				commits = &a2SameCommitObservedPersistence{WorkflowPersistenceOwner: selected.events.(runtimepipeline.WorkflowPersistenceOwner)}
				selected.persistence = runtimepipeline.NewWorkflowPersistence(commits)
			}
			requester := externalPipelineSourceNode(t, source, "requester", "requester")
			collector := externalPipelineSourceNode(t, source, "requester", "collector")
			dispatcher := externalPipelineSourceNode(t, source, "requester", "dispatcher")
			provider := externalPipelineSourceNode(t, source, "provider", "provider")
			siblingNode := collector
			if siblingFlow == "observer" {
				siblingNode = externalPipelineSourceNode(t, source, "observer", "collector")
			}
			probe := &a2HeldWorkerProbe{Probe: lifecycleprobe.New(), nodeID: provider.Key(), started: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{})}
			t.Cleanup(probe.resume)
			logger := &exactJoinRuntimeLogger{}
			module := proposedEffectProofModule{source: source, nodes: []runtimepipeline.WorkflowNode{
				{Node: requester, Subscriptions: []events.EventType{"requester/request.send"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
				{Node: collector, Subscriptions: []events.EventType{"requester/provider.replied"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
				{Node: dispatcher, Subscriptions: []events.EventType{"requester/manual.abort", "requester/dispatch.completed"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
				{Node: provider, Subscriptions: []events.EventType{"provider/provider.requested"}, ExecutionType: runtimecontracts.SystemNodeExecutionType},
			}}
			if siblingFlow == "observer" {
				module.nodes[3].Subscriptions = append(module.nodes[3].Subscriptions, "provider/ordinary.requested")
				module.nodes = append(module.nodes, runtimepipeline.WorkflowNode{Node: siblingNode,
					Subscriptions: []events.EventType{"observer/provider.replied", "observer/provider.notified"}, ExecutionType: runtimecontracts.SystemNodeExecutionType})
			}
			bus, err := newScopedTestEventBus(t, selected.events, runtimebus.EventBusOptions{ContractBundle: source, TestLifecycleProbe: probe, Logger: logger},
				"platform.join_complete", "platform.join_timeout")
			if err != nil {
				t.Fatal(err)
			}
			schedules, _ := newExactJoinScheduleLifecycleForTest(t, ctx, selected, bus)
			options := runtimepipeline.PipelineCoordinatorOptions{Module: module, GenericSchedules: schedules, TestLifecycleProbe: probe}
			pc := newGateRecoveryCoordinator(bus, selected, options)
			bus.SetInterceptors(pc)
			commitKeylessConstructorComponent(t, ctx, selected, pc, source)
			parent, found, err := pc.Load(ctx, testRunScopedWorkflowInstanceForRun(runID, runID))
			if err != nil || !found {
				t.Fatalf("load initialized requester parent: found=%v err=%v", found, err)
			}
			path := "requester/" + key
			owner := testRunScopedWorkflowInstanceForRun(runID, path)
			readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
				Identity: flowidentity.Instance{TemplateID: "requester", ScopeKey: "requester", InstanceID: key, InstancePath: path, EntityID: flowidentity.EntityID(path), HasStoredPath: true},
				RunID:    runID, BundleHash: authorActivityTestSourceArtifactFact.BundleHash(), WorkflowVersion: source.WorkflowVersion(), ExecutionMode: executionmode.Live,
			}
			readiness.Identity.ParentRoute = flowidentity.ParentRoute{FlowID: parent.WorkflowName, FlowInstance: parent.StorageRef, EntityID: parent.EntityID}
			readiness.Identity.ParentEntityID = parent.EntityID
			now := time.Now().UTC()
			constructed := commitA2FixtureConstruction(t, pc, selected.events, ctx, owner, runtimepipeline.WorkflowInstance{
				InstanceID: key, StorageRef: path, EntityID: flowidentity.EntityID(path), WorkflowName: "requester", WorkflowVersion: source.WorkflowVersion(),
				ParentFlowID: parent.WorkflowName, ParentFlowInstance: parent.StorageRef, ParentEntityID: parent.EntityID,
				Mode: "template", RuntimeReadiness: &readiness, CurrentState: "awaiting", EntityType: "request_state",
				Fields: map[string]any{"order_id": key, "expected": []any{"a", "b"}},
			}, now)
			markGateRecoveryTopologyReadyFixture(t, selected, readiness, now)
			if err := flowroutefixture.Publish(bus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: owner, Instance: constructed.Identity}); err != nil {
				t.Fatal(err)
			}
			load := func() runtimepipeline.WorkflowInstance {
				t.Helper()
				instance, found, err := pc.Load(ctx, owner)
				if err != nil || !found {
					t.Fatalf("load requester: found=%v err=%v", found, err)
				}
				return instance
			}
			first, found, err := workflowlifecycle.LoadStageEntry(load().Bookkeeping)
			if err != nil || !found {
				t.Fatalf("initial entry: found=%v err=%v", found, err)
			}
			ingress := func(name string) events.Event {
				return eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(path+"/"+name), "operator", "", []byte(`{}`), 0,
					runID, events.EnvelopeForEntityID(events.EventEnvelope{}, flowidentity.EntityID(path)), eventtest.ConcreteTemplateRoutingSource("requester", path, flowidentity.EntityID(path)), now)
			}
			trigger := ingress("request.send")
			if err := bus.PublishAcknowledged(ctx, trigger); err != nil {
				t.Fatal(err)
			}
			var requestID string
			waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			select {
			case signal := <-probe.started:
				requestID = signal.EventID
			case <-waitCtx.Done():
				err = waitCtx.Err()
			}
			cancel()
			if err != nil {
				t.Fatalf("provider did not receive real request: %v logs=%s", err, logger.String())
			}
			request, found, err := selected.events.LoadPreparedPublishEvent(ctx, requestID)
			if err != nil || !found || len(request.DeliveryRoutes) != 1 || request.Event.Event().ParentEventID() != trigger.ID() {
				t.Fatalf("request publication: found=%v err=%v", found, err)
			}
			recordID := request.DeliveryRoutes[0].Context.ReplyContextID()
			record, err := selected.events.LoadReplyContext(ctx, recordID)
			if err != nil || len(record.ReturnJoins) != 1 || record.ReturnJoins[0].Ref.StageEntry() != first {
				t.Fatalf("request did not freeze actual return obligation: %#v err=%v", record, err)
			}
			var loadSibling func() runtimepipeline.WorkflowInstance
			var siblingBefore runtimepipeline.WorkflowInstance
			var siblingTarget events.RouteIdentity
			if lateSibling {
				siblingKey := uuid.NewString()
				if siblingFlow == "observer" {
					siblingKey = key
				}
				siblingPath := siblingFlow + "/" + siblingKey
				siblingEntity := flowidentity.EntityID(siblingPath)
				siblingOwner := testRunScopedWorkflowInstanceForRun(runID, siblingPath)
				siblingReadiness := readiness
				siblingReadiness.Identity = flowidentity.Instance{TemplateID: siblingFlow, ScopeKey: siblingFlow, InstanceID: siblingKey,
					InstancePath: siblingPath, EntityID: siblingEntity, HasStoredPath: true,
					ParentRoute: readiness.Identity.ParentRoute, ParentEntityID: readiness.Identity.ParentEntityID}
				siblingConstruction := commitA2FixtureConstruction(t, pc, selected.events, ctx, siblingOwner, runtimepipeline.WorkflowInstance{
					InstanceID: siblingKey, StorageRef: siblingPath, EntityID: siblingEntity, WorkflowName: siblingFlow, WorkflowVersion: source.WorkflowVersion(),
					ParentFlowID: parent.WorkflowName, ParentFlowInstance: parent.StorageRef, ParentEntityID: parent.EntityID,
					Mode: "template", RuntimeReadiness: &siblingReadiness, CurrentState: "awaiting", EntityType: "request_state",
					Fields: map[string]any{"order_id": siblingKey, "expected": []any{"a", "b"}},
				}, now)
				markGateRecoveryTopologyReadyFixture(t, selected, siblingReadiness, now)
				if err := flowroutefixture.Publish(bus, runtimebus.FlowInstanceRouteMaterializationRequest{Identity: siblingOwner, Instance: siblingConstruction.Identity}); err != nil {
					t.Fatal(err)
				}
				loadSibling = func() runtimepipeline.WorkflowInstance {
					t.Helper()
					instance, found, err := pc.Load(ctx, siblingOwner)
					if err != nil || !found {
						t.Fatalf("load real later sibling: found=%v err=%v", found, err)
					}
					return instance
				}
				initialSibling := loadSibling()
				initialArms := a2KnownTargetArms(t, initialSibling)
				if len(initialArms) != 1 || initialArms[0].Status != joinruntime.StatusOpen || initialArms[0].Completed() != 0 ||
					initialArms[0].JoinRef().StageEntry() == first {
					t.Fatalf("later sibling did not have an independent real arm: %#v", initialArms)
				}
				// A new directed arrival proves B's matching ordinary subscription
				// actually executes and receives B's own admission, never A's E1.
				controlPayload, err := json.Marshal(map[string]any{"order_id": siblingKey, "member_id": "b", "result": map[string]any{"value": "sibling-control"}})
				if err != nil {
					t.Fatal(err)
				}
				control := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), events.EventType(siblingPath+"/provider.replied"), "operator", "", controlPayload, 0,
					runID, events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, siblingEntity), siblingPath),
					eventtest.ConcreteTemplateRoutingSource(siblingFlow, siblingPath, siblingEntity), now)
				if err := bus.PublishAcknowledged(ctx, control); err != nil {
					t.Fatalf("publish real independent sibling arrival control: %v", err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				completed, cursor, completedErr := probe.WaitAfter(waitCtx, lifecycleprobe.Cursor{}, lifecycleprobe.Signal{
					Kind: lifecycleprobe.HandlerCompleted, EventID: control.ID(), SubscriberType: "node", SubscriberID: siblingNode.Key(),
				})
				_, _, settledErr := probe.WaitAfter(waitCtx, cursor, lifecycleprobe.Signal{
					Kind: lifecycleprobe.DeliveryStatusChanged, EventID: control.ID(), SubscriberType: "node", SubscriberID: siblingNode.Key(), Status: "delivered",
				})
				cancel()
				if completedErr != nil || completed.Status != "completed" || settledErr != nil {
					t.Fatalf("actual sibling control failed: completed=%#v err=%v settled=%v logs=%s", completed, completedErr, settledErr, logger.String())
				}
				controlPublication, found, err := selected.events.LoadPreparedPublishEvent(ctx, control.ID())
				if err != nil || !found || len(controlPublication.DeliveryRoutes) != 1 || controlPublication.DeliveryRoutes[0].Recipient.ID() != siblingNode.Key() ||
					controlPublication.DeliveryRoutes[0].Target.Route() != (events.RouteIdentity{FlowID: siblingFlow, FlowInstance: siblingPath, EntityID: siblingEntity}) ||
					len(controlPublication.DeliveryRoutes[0].Context.Joins) != 1 || controlPublication.DeliveryRoutes[0].Context.Joins[0].Disposition != events.JoinAdmissionBound ||
					!controlPublication.DeliveryRoutes[0].Context.Joins[0].Ref.Equal(initialArms[0].JoinRef()) || controlPublication.DeliveryRoutes[0].Context.ReplyContextID() != "" {
					t.Fatalf("independent sibling control acquired unrelated return authority: found=%v routes=%#v err=%v", found, controlPublication.DeliveryRoutes, err)
				}
				siblingTarget = controlPublication.DeliveryRoutes[0].Target.Route()
				siblingBefore = loadSibling()
				siblingArms := a2KnownTargetArms(t, siblingBefore)
				if len(siblingArms) != 1 || siblingArms[0].Completed() != 1 || siblingArms[0].Status != joinruntime.StatusOpen ||
					!reflect.DeepEqual(siblingArms[0].Outputs["b"].Value, map[string]any{"value": "sibling-control"}) {
					t.Fatalf("ordinary sibling control did not contribute exactly once to B: %#v", siblingArms)
				}
				retained, err := selected.events.LoadReplyContext(ctx, recordID)
				if err != nil || !reflect.DeepEqual(retained, record) {
					t.Fatalf("later sibling/control expanded frozen A return authority: %#v err=%v", retained, err)
				}
			}
			for _, name := range []string{"manual.abort", "dispatch.completed"} {
				event := ingress(name)
				if err := bus.PublishAcknowledged(ctx, event); err != nil {
					t.Fatal(err)
				}
				waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				completed, err := probe.WaitForHandlerCompleted(waitCtx, event.ID(), dispatcher.Key())
				cancel()
				if err != nil || completed.Status != "completed" {
					t.Fatalf("reentry: status=%s err=%v", completed.Status, err)
				}
				assertExactJoinDeliveryStatus(t, selected, ctx, event.ID(), dispatcher.Key(), "delivered")
			}
			before := load()
			second, found, err := workflowlifecycle.LoadStageEntry(before.Bookkeeping)
			if err != nil || !found || second == first {
				t.Fatalf("new requester entry: found=%v err=%v", found, err)
			}
			if corruptEntryField != "" {
				corrupted := a2CorruptBoundReplyEntry(t, ctx, selected, record, corruptEntryField)
				entry := record.ReturnJoins[0].Ref.StageEntry()
				if err := entry.RequireOwner(entry.RunID, entry.FlowScope, entry.InstanceID, entry.InstancePath, entry.EntityID, entry.Stage); err != nil {
					t.Fatalf("uncorrupted return entry is not owner-admitted: %v", err)
				}
				ownerErr := corrupted.ReturnJoins[0].Ref.StageEntry().RequireOwner(entry.RunID, entry.FlowScope, entry.InstanceID, entry.InstancePath, entry.EntityID, entry.Stage)
				if ownerErr == nil || ownerErr.Error() != "stage entry disagrees with its lifecycle owner" {
					t.Fatalf("corruption did not reach the canonical owner refusal: %v", ownerErr)
				}
				providerTarget := request.DeliveryRoutes[0].Target.Route()
				providerBefore := a2BoundReplySourceSnapshot(t, ctx, selected, runID, providerTarget.FlowInstance)
				beforeCommits := commits.witnesses()
				probe.resume()
				a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, request.Event.Event(), provider.Key(), "failed", "dead_letter", logger)
				wantFailure := failures.FromError(ownerErr, "workflow-runtime", "execute_handler").Failure
				failureLogs := logger.failuresFor("handler_error", requestID)
				if len(failureLogs) != 1 || !reflect.DeepEqual(failureLogs[0], wantFailure) {
					t.Fatalf("handler diagnostic discarded or changed canonical refusal: got=%+v want=%+v", failureLogs, wantFailure)
				}
				var replies int
				if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM events WHERE source_event_id=$1 AND event_name=$2", requestID, source.ResolveFlowEventReference("provider", "provider.replied")).Scan(&replies); err != nil || replies != 0 {
					t.Fatalf("refused provider output persisted a fresh reply: count=%d err=%v", replies, err)
				}
				retained, err := selected.events.LoadReplyContext(ctx, recordID)
				if err != nil || retained.State != replycontext.StateOpen || retained.AcceptedReplyEventID != "" || retained.TerminalAt != nil || !reflect.DeepEqual(retained, corrupted) {
					t.Fatalf("refusal accepted or rebound retained E1 authority: %#v err=%v", retained, err)
				}
				providerAfter := a2BoundReplySourceSnapshot(t, ctx, selected, runID, providerTarget.FlowInstance)
				if !reflect.DeepEqual(providerAfter, providerBefore) || !reflect.DeepEqual(before, load()) || !reflect.DeepEqual(beforeCommits, commits.witnesses()) {
					t.Fatal("refused reply changed source/receiver business state or committed a mutation")
				}
				unchangedRequest, found, err := selected.events.LoadPreparedPublishEvent(ctx, requestID)
				if err != nil || !found || !reflect.DeepEqual(unchangedRequest.DeliveryRoutes, request.DeliveryRoutes) || record.ReturnJoins[0].Ref.StageEntry() != first {
					t.Fatalf("hostility rewrote the original request/E1 binding: found=%v err=%v", found, err)
				}
				arms := a2KnownTargetArms(t, load())
				var retainedE1, currentE2 bool
				for _, arm := range arms {
					if arm.Completed() != 0 {
						t.Fatal("refused reply contributed to a retained or current arm")
					}
					retainedE1 = retainedE1 || arm.JoinRef().StageEntry() == first && arm.Status == joinruntime.StatusClosed
					currentE2 = currentE2 || arm.JoinRef().StageEntry() == second && arm.Status == joinruntime.StatusOpen
				}
				if !retainedE1 || !currentE2 || len(arms) != 2 {
					t.Fatalf("hostility erased/rebound the real E1/E2 arms: %#v", arms)
				}
				return
			}
			probe.resume()
			waitCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			err = bus.WaitForQuiescence(waitCtx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			var replyID string
			if err := selected.db.QueryRowContext(ctx, "SELECT event_id FROM events WHERE source_event_id=$1 AND event_name=$2", requestID, source.ResolveFlowEventReference("provider", "provider.replied")).Scan(&replyID); err != nil {
				t.Fatalf("provider response absent: %v logs=%s", err, logger.String())
			}
			reply, found, err := selected.events.LoadPreparedPublishEvent(ctx, replyID)
			if err != nil || !found || len(reply.DeliveryRoutes) != 1 || !reflect.DeepEqual(reply.DeliveryRoutes[0].Context.Joins, record.ReturnJoins) {
				t.Fatalf("response rebound the return obligation: found=%v routes=%#v err=%v", found, reply.DeliveryRoutes, err)
			}
			if lateSibling {
				route := reply.DeliveryRoutes[0]
				if route.Recipient.ID() != collector.Key() || route.Target.Route() != record.Origin || !reflect.DeepEqual(siblingBefore, loadSibling()) {
					t.Fatalf("exact A reply expanded to a later independently subscribed B: %#v", route)
				}
				var deliveries int
				if err := selected.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node'", replyID).Scan(&deliveries); err != nil || deliveries != 1 {
					t.Fatalf("exact reply persisted extra sibling deliveries: count=%d err=%v", deliveries, err)
				}
			}
			assertExactJoinDeliveryStatus(t, selected, ctx, replyID, collector.Key(), "dead_letter")
			var raw string
			if err := selected.db.QueryRowContext(ctx, "SELECT CAST(failure AS TEXT) FROM event_deliveries WHERE event_id=$1 AND subscriber_id=$2", replyID, collector.Key()).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var refusal failures.Envelope
			if err := json.Unmarshal([]byte(raw), &refusal); err != nil || refusal.Class != failures.ClassStaleArrival {
				t.Fatalf("late exact return disposition=%s err=%v", raw, err)
			}
			if after := load(); after.Revision != before.Revision || !reflect.DeepEqual(after.StateBuckets, before.StateBuckets) {
				t.Fatal("E1 return changed E2 state")
			}
			current := load()
			state, err := runtimeengine.StateCarrierFromPersisted(current.Fields, current.Bookkeeping, current.Gates, current.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			arms, err := joinruntime.List(state.StateBuckets)
			if err != nil {
				t.Fatal(err)
			}
			for _, arm := range arms {
				if arm.Completed() != 0 {
					t.Fatal("late return contributed to a retained or current join")
				}
			}
			terminal, err := selected.events.LoadReplyContext(ctx, recordID)
			if err != nil || terminal.State != replycontext.StateTerminal || terminal.AcceptedReplyEventID != replyID || !terminal.SameIdentity(record) {
				t.Fatalf("reply authority settlement=%#v err=%v", terminal, err)
			}
			pc = newGateRecoveryCoordinator(bus, selected, options)
			bus.SetInterceptors(pc)
			if err := bus.PublishAcknowledged(ctx, reply.Event.Event()); err != nil {
				t.Fatalf("exact return replay after restart: %v", err)
			}
			waitCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
			err = bus.WaitForQuiescence(waitCtx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			assertExactJoinDeliveryCount(t, selected, ctx, replyID, collector.Key(), 1)
			if after := load(); after.Revision != before.Revision || !reflect.DeepEqual(after.StateBuckets, before.StateBuckets) {
				t.Fatal("restored E1 reply altered E2")
			}
			if lateSibling && !reflect.DeepEqual(siblingBefore, loadSibling()) {
				t.Fatal("restarted exact A reply acquired or contributed to B")
			}
			if siblingFlow == "observer" {
				t.Run("independent_ordinary_B", func(t *testing.T) {
					providerTarget := request.DeliveryRoutes[0].Target.Route()
					// The real provider emits identical business data on a distinct
					// declared ordinary edge, with no retained reply authority.
					ordinaryTrigger := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(),
						events.EventType(source.ResolveFlowEventReference("provider", "ordinary.requested")), "operator", "", reply.Event.Event().Payload(), 0,
						runID, events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, providerTarget.EntityID), providerTarget.FlowInstance),
						eventtest.StaticFlowRoutingSource(providerTarget.FlowID, providerTarget.FlowInstance, providerTarget.EntityID), now)
					if err := bus.PublishAcknowledged(ctx, ordinaryTrigger); err != nil {
						t.Fatalf("publish real provider ordinary trigger: %v", err)
					}
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, ordinaryTrigger, provider.Key(), "completed", "delivered", logger)
					var ordinaryID string
					if err := selected.db.QueryRowContext(ctx, "SELECT event_id FROM events WHERE source_event_id=$1 AND event_name=$2",
						ordinaryTrigger.ID(), source.ResolveFlowEventReference("provider", "provider.notified")).Scan(&ordinaryID); err != nil {
						t.Fatalf("real provider ordinary output absent: %v logs=%s", err, logger.String())
					}
					ordinary, found, err := selected.events.LoadPreparedPublishEvent(ctx, ordinaryID)
					if err != nil || !found || len(ordinary.DeliveryRoutes) != 1 {
						t.Fatalf("ordinary provider publication: found=%v routes=%#v err=%v", found, ordinary.DeliveryRoutes, err)
					}
					route := ordinary.DeliveryRoutes[0]
					if route.Recipient.ID() != siblingNode.Key() || route.Target.Route() != siblingTarget ||
						len(route.Context.Joins) != 0 || route.Context.ReplyContextID() != "" {
						t.Fatalf("ordinary B output borrowed exact A return authority: %#v", route)
					}
					a2KnownTargetWaitForSettlement(t, ctx, bus, probe.Probe, ordinary.Event.Event(), siblingNode.Key(), "completed", "delivered", logger)
					var expectedPayload, actualPayload map[string]any
					if err := json.Unmarshal(reply.Event.Event().Payload(), &expectedPayload); err != nil {
						t.Fatal(err)
					}
					if err := json.Unmarshal(ordinary.Event.Event().Payload(), &actualPayload); err != nil || !reflect.DeepEqual(actualPayload, expectedPayload) {
						t.Fatalf("ordinary control changed business payload: %#v want=%#v err=%v", actualPayload, expectedPayload, err)
					}
					expectedBookkeeping := make(map[string]any, len(siblingBefore.Bookkeeping)+1)
					for key, value := range siblingBefore.Bookkeeping {
						expectedBookkeeping[key] = value
					}
					expectedBookkeeping["last_data_accumulation_event"] = string(ordinary.Event.Event().Type())
					afterOrdinary := loadSibling()
					if afterOrdinary.Revision != siblingBefore.Revision+1 || afterOrdinary.CurrentState != siblingBefore.CurrentState ||
						!reflect.DeepEqual(afterOrdinary.StateBuckets, siblingBefore.StateBuckets) || !reflect.DeepEqual(afterOrdinary.Bookkeeping, expectedBookkeeping) ||
						!reflect.DeepEqual(afterOrdinary.Fields["ordinary_result"], expectedPayload["result"]) || !reflect.DeepEqual(before, load()) {
						t.Fatalf("ordinary B handler did not execute independently without changing A/its join: before=%#v after=%#v", siblingBefore, afterOrdinary)
					}
					retained, err := selected.events.LoadReplyContext(ctx, recordID)
					if err != nil || !reflect.DeepEqual(retained, terminal) {
						t.Fatalf("ordinary B publication rewrote immutable/settled A E1 return authority: %#v err=%v", retained, err)
					}
					assertExactJoinDeliveryCount(t, selected, ctx, ordinaryID, siblingNode.Key(), 1)
					assertExactJoinDeliveryCount(t, selected, ctx, replyID, collector.Key(), 1)
				})
			}
		})
	}
}

func a2BoundReplySourceSnapshot(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, runID, concretePath string) map[string][][]any {
	t.Helper()
	if concretePath == "" {
		t.Fatal("source snapshot requires the actual delivery's concrete path")
	}
	// A fieldless source may have a header without state. Measure both persisted
	// halves without inventing an entity ID or deriving a semantic owner from path.
	snapshot := make(map[string][][]any)
	for _, table := range []struct{ name, path, order string }{{"flow_instances", "instance_path", "instance_path"}, {"entity_state", "flow_instance", "entity_id"}} {
		rows, err := selected.db.QueryContext(ctx, "SELECT * FROM "+table.name+" WHERE run_id=$1 AND "+table.path+"=$2 ORDER BY "+table.order, runID, concretePath)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		snapshot[table.name] = nil
		for rows.Next() {
			values, destinations := make([]any, len(columns)), make([]any, len(columns))
			for index := range values {
				destinations[index] = &values[index]
			}
			if err := rows.Scan(destinations...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			for index, value := range values {
				if raw, ok := value.([]byte); ok {
					values[index] = string(raw)
				}
			}
			snapshot[table.name] = append(snapshot[table.name], values)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if err := rows.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
}

func a2CorruptBoundReplyEntry(t *testing.T, ctx context.Context, selected gateRecoveryStoreCase, original replycontext.Record, field string) replycontext.Record {
	t.Helper()
	raw, err := original.EncodeOrigin()
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	entry := wire["join_admissions"].([]any)[0].(map[string]any)["ref"].(map[string]any)["stage_entry"].(map[string]any)
	expectedEntry := original.ReturnJoins[0].Ref.StageEntry()
	switch field {
	case "flow_scope":
		entry[field] = "foreign"
		expectedEntry.FlowScope = "foreign"
	case "instance_id":
		foreignID := uuid.NewString()
		entry[field] = foreignID
		expectedEntry.InstanceID = foreignID
	default:
		t.Fatalf("unsupported reply entry corruption %q", field)
	}
	raw, err = json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	dialect := authoractivityfixture.DialectSQLite
	query := `UPDATE reply_contexts SET origin_route=? WHERE reply_context_id=? AND run_id=? AND state='open' AND accepted_reply_event_id IS NULL`
	if selected.postgres {
		dialect = authoractivityfixture.DialectPostgres
		query = `UPDATE reply_contexts SET origin_route=$1::jsonb WHERE reply_context_id=$2 AND run_id=$3::uuid AND state='open' AND accepted_reply_event_id IS NULL`
	}
	// Deliberate persisted hostility, not a replacement reader or claim closure.
	// The selected-store fixture owner commits the single corrupt row and its
	// existing reply-context revision fact through the native transaction protocol.
	if err := eventfixture.RunMutation(ctx, selected.db, dialect, func(ctx context.Context, attempt *eventfixture.Attempt) error {
		if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			result, err := tx.ExecContext(ctx, query, string(raw), original.ID, original.RunID)
			if err != nil {
				return err
			}
			rows, err := result.RowsAffected()
			if err != nil || rows != 1 {
				return fmt.Errorf("corrupt exact retained reply entry: rows=%d err=%v", rows, err)
			}
			return nil
		}); err != nil {
			return err
		}
		return attempt.AddFact(original.RunID, runforkrevisionfixture.FamilyReplyContexts, original.ID)
	}).Err(); err != nil {
		t.Fatalf("commit retained reply entry hostility: %v", err)
	}
	corrupted, err := selected.events.LoadReplyContext(ctx, original.ID)
	if err != nil {
		t.Fatalf("load real corrupted reply record: %v", err)
	}
	// Record has no Source: its shape gate must not be mistaken for the
	// source-aware lifecycle-owner check performed by real reply publication.
	if err := corrupted.Validate(); err != nil || len(corrupted.ReturnJoins) != 1 || corrupted.ReturnJoins[0].Ref.StageEntry() != expectedEntry ||
		!corrupted.ReturnJoins[0].Ref.Declaration().Equal(original.ReturnJoins[0].Ref.Declaration()) ||
		corrupted.ReturnJoins[0].Ref.Generation() != original.ReturnJoins[0].Ref.Generation() || corrupted.ReturnJoins[0].Disposition != original.ReturnJoins[0].Disposition {
		t.Fatalf("fixture did not isolate shape-valid %s owner hostility: %#v err=%v", field, corrupted, err)
	}
	restored := corrupted
	restored.ReturnJoins = original.ReturnJoins
	if !reflect.DeepEqual(restored, original) {
		t.Fatal("hostility changed reply authority beyond the one retained join entry field")
	}
	return corrupted
}
