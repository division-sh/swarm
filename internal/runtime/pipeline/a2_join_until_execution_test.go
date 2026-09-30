package pipeline

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestA2UntilClosesMultipleJoinsAndPreservesOrdinaryHandlerOnBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			h := newExactWorkflowJoinHarness(t, backend, "orders", "awaiting", []any{"a", "b"})
			files := workflowJoinLifecycleFixtureFiles(false, "")
			files["orders/events.yaml"] += "halt.requested: {}\nfirst.closed: {}\nsecond.closed: {}\nhalt.observed: {}\nalternate.completed:\n  member_id: text\n  result: ItemResult\n"
			files["orders/nodes.yaml"] = strings.Replace(files["orders/nodes.yaml"], "on_complete: {advances_to: ready}", "until: halt.requested\n        on_complete: {emit: {event: first.closed}}", 1)
			files["orders/nodes.yaml"] += `    alternate.completed:
      join:
        id: alternate
        stage: awaiting
        members: {count: 2, by: payload.member_id}
        output: payload.result
        until: halt.requested
        on_complete: {emit: {event: second.closed}}
    halt.requested:
      emit: {event: halt.observed}
`
			h.bundle = loadWorkflowTempBundle(t, files)
			h.source.Source = semanticview.Wrap(h.bundle)
			h.source.plans = nil
			node := pipelineNode(t, "orders", "join-node")
			for _, plan := range h.bundle.Semantics.Joins {
				if plan.Node.Equal(node) {
					h.source.plans = append(h.source.plans, plan)
				}
			}
			if len(h.source.plans) != 2 {
				t.Fatalf("compiled until consumers = %#v", h.source.plans)
			}
			h.pc = h.newCoordinator()
			if err := applyTestInitialEntryEffect(h.ctx, h.pc, h.route, h.entityID); err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "halt.requested", "operator", "", []byte(`{}`), 0,
				testRunScopedWorkflowRoute(h.ctx, h.route).RunID, h.envelope(), exactJoinRoutingSource("orders", h.path, h.entityID), time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
				FlowID: "orders", FlowInstance: h.path, EntityID: h.entityID,
			})}
			ctx, err := persistWorkflowJoinPublicationForTest(t, h.pc, h.ctx, event, route, true)
			if err != nil {
				t.Fatal(err)
			}
			if got := events.DeliveryContextFromContext(ctx).Joins; len(got) != 2 {
				t.Fatalf("until publication lacks per-join bindings: %#v", got)
			}
			retained, found := workflowNodeDeliveryRoute(ctx)
			if !found {
				t.Fatal("until publication lacks its exact retained route")
			}
			for _, corruption := range []string{"partial", "duplicate", "foreign_run"} {
				t.Run(corruption, func(t *testing.T) {
					hostile := retained
					hostile.Context.Joins = append([]events.JoinAdmissionReceipt(nil), retained.Context.Joins...)
					switch corruption {
					case "partial":
						hostile.Context.Joins = hostile.Context.Joins[:1]
					case "duplicate":
						hostile.Context.Joins[1] = hostile.Context.Joins[0]
					case "foreign_run":
						entry := hostile.Context.Joins[0].Ref.StageEntry()
						entry.RunID = uuid.NewString()
						ref, err := hostile.Context.Joins[0].Ref.Declaration().BindStageEntry(entry, hostile.Context.Joins[0].Ref.Generation())
						if err != nil {
							t.Fatal(err)
						}
						hostile.Context.Joins[0].Ref = ref
					}
					before := h.instance()
					resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(h.source, node, string(event.Type()))
					_, err := h.pc.prepareDeliveryTargetApplication(withWorkflowNodeDeliveryRoute(ctx, hostile), node.Key(),
						MustDeliveryTargetHandler(node).ForEvent(events.EventType(resolved.HandlerEventKey)), resolved.Handler, event, hostile.Target)
					if err == nil {
						t.Fatal("receiver preparation admitted corrupt multi-join evidence")
					}
					if !reflect.DeepEqual(before, h.instance()) {
						t.Fatal("corrupt admission changed persisted receiver state")
					}
				})
			}
			resolved := semanticview.ResolveExecutableNodeSubscriptionHandler(h.source, node, string(event.Type()))
			observed := events.EventType(h.path + "/halt.observed")
			firstClosed := events.EventType(h.path + "/first.closed")
			secondClosed := events.EventType(h.path + "/second.closed")
			if !resolved.Matched || len(resolved.Handler.JoinUntilPlans) != 2 || resolved.Handler.Emit.Event != h.source.ResolveExecutableNodeEventReference(node, "halt.observed") {
				t.Fatalf("ordinary and compiled consumers were not composed: matched=%v plans=%d emit=%q key=%q", resolved.Matched, len(resolved.Handler.JoinUntilPlans), resolved.Handler.Emit.Event, resolved.HandlerEventKey)
			}
			before := h.bus.outboxCount()
			result, err := executeClaimedWorkflowJoinForTest(t, h.pc, ctx, node, resolved.Handler, workflowTriggerContext{Event: event,
				State: mustCurrentWorkflowState(t, h.pc, ctx, h.route, h.entityID), HandlerEventKey: resolved.HandlerEventKey})
			if err != nil {
				t.Fatal(err)
			}
			if h.bus.outboxCount() != before+1 || h.bus.outboxIntent(before).Event.Type() != observed {
				var emitted []events.EventType
				for i := before; i < h.bus.outboxCount(); i++ {
					emitted = append(emitted, h.bus.outboxIntent(i).Event.Type())
				}
				t.Fatalf("until suppressed or duplicated the ordinary handler: emitted=%v expected=%q committed=%v", emitted, observed, result.Committed)
			}
			readArms := func() []joinruntime.Activation {
				t.Helper()
				carrier, err := workflowInstanceStateCarrier(h.instance())
				if err != nil {
					t.Fatal(err)
				}
				arms, err := joinruntime.List(carrier.StateBuckets)
				if err != nil || len(arms) != 2 {
					t.Fatalf("until arms = %#v err=%v", arms, err)
				}
				return arms
			}
			arms := readArms()
			for _, arm := range arms {
				if arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonUntil || !arm.OutcomePending || arm.OutcomeFired || arm.Completed() != 0 {
					t.Fatalf("until did not retain independent incomplete closure: %#v", arm)
				}
			}
			h.restart()
			schedules, _ := committedWorkflowSchedulesForTest(t, h.store)
			for _, arm := range arms {
				fired := false
				for _, schedule := range schedules {
					if schedule.Command.TaskID != arm.TimerTaskID() {
						continue
					}
					control := h.scheduleEvent(schedule, uuid.NewString())
					if _, err := h.fire(control); err != nil {
						t.Fatal(err)
					}
					if _, err := h.fire(control); err != nil {
						t.Fatalf("exact duplicate continuation: %v", err)
					}
					fired = true
					break
				}
				if !fired {
					t.Fatalf("closed join %s lacks exact committed continuation", arm.Key())
				}
			}
			for _, arm := range readArms() {
				if !arm.OutcomeFired || arm.OutcomePending || arm.CloseReason != joinruntime.CloseReasonUntil {
					t.Fatalf("restart did not finish exact until outcome: %#v", arm)
				}
			}
			counts := map[events.EventType]int{}
			for i := 0; i < h.bus.outboxCount(); i++ {
				counts[h.bus.outboxIntent(i).Event.Type()]++
			}
			if counts[firstClosed] != 1 || counts[secondClosed] != 1 || counts[observed] != 1 {
				t.Fatalf("continuations were broadcast or repeated: %#v", counts)
			}
		})
	}
}
