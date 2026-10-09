package pipeline

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
)

func TestA2NonLoopStageReentryOnBothStores(t *testing.T) {
	for _, backend := range workflowJoinStoreCases() {
		for _, flow := range []string{"", "orders"} {
			t.Run(backend.name+"/"+pipelineDeclarationFlowPath(flow), func(t *testing.T) {
				h := newExactWorkflowJoinHarness(t, backend, flow, "awaiting", []any{"a", "b"})
				h.armInitial()
				first := h.activation()
				node := pipelineNode(t, flow, "join-node")
				handler := h.source.ExecutableNodeEventHandlers(node)["item.completed"]
				owner := testRunScopedWorkflowRoute(h.ctx, h.route)
				route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: pipelineDeclarationFlowPath(flow), FlowInstance: h.path, EntityID: h.entityID,
				})}
				oldEvent := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "item.completed", "operator", "",
					[]byte(`{"member_id":"a","result":{"ok":true}}`), 0, owner.RunID, h.envelope(), exactJoinRoutingSource(flow, h.path, h.entityID), first.ArmedAt)
				oldContext, err := persistWorkflowJoinPublicationForTest(t, h.pc, h.ctx, oldEvent, route, true)
				if err != nil {
					t.Fatal(err)
				}
				retained := events.DeliveryContextFromContext(oldContext).Joins
				if len(retained) != 1 || !retained[0].Ref.Equal(first.JoinRef()) {
					t.Fatalf("original publication binding = %#v", retained)
				}
				// Equal timestamps cannot collapse distinct admitted delivery entries.
				transition := func(next, name string) {
					t.Helper()
					event := workflowLifecycleEventForTest(t, h.store, h.ctx, pipelineDeclarationFlowPath(flow), h.path, h.entityID, name, first.ArmedAt)
					ctx := correlation.WithInboundEvent(h.ctx, event)
					if err := persistAdmittedJoinTransitionForTest(t, h.pc, ctx, h.route, h.entityID, next, name); err != nil {
						t.Fatal(err)
					}
					committed := h.instance()
					if len(committed.TransitionHistory) != 1 || committed.TransitionHistory[0].TriggerEventID != event.ID() || committed.TransitionHistory[0].To != next {
						t.Fatalf("entry lost the exact transition: %+v", committed)
					}
				}
				transition("dispatching", "manual.abort")
				if err := h.store.mutate(h.ctx, owner, func(instance *WorkflowInstance) {
					instance.Fields["expected"] = []any{"c", "d"}
				}); err != nil {
					t.Fatal(err)
				}
				transition("awaiting", "dispatch.completed")
				secondInstance := h.instance()
				entry, found, err := workflowlifecycle.LoadStageEntry(secondInstance.Bookkeeping)
				if err != nil || !found || entry == first.JoinRef().StageEntry() || entry.Cause != "delivery" {
					t.Fatalf("second lifecycle entry = %#v found=%v err=%v", entry, found, err)
				}
				carrier, err := workflowInstanceStateCarrier(secondInstance)
				if err != nil {
					t.Fatal(err)
				}
				arms, err := joinruntime.List(carrier.StateBuckets)
				if err != nil || len(arms) != 2 {
					t.Fatalf("retained arms = %#v err=%v", arms, err)
				}
				var second joinruntime.Activation
				for _, arm := range arms {
					if arm.JoinRef().StageEntry() == entry {
						second = arm
					} else if !arm.JoinRef().Equal(first.JoinRef()) || arm.Status != joinruntime.StatusClosed || arm.CloseReason != joinruntime.CloseReasonStageExit || !reflect.DeepEqual(arm.Members, []string{"a", "b"}) {
						t.Fatalf("original arm was erased or rebound: %#v", arm)
					}
				}
				if second.Status != joinruntime.StatusOpen || second.Key() == first.Key() || !second.ArmedAt.Equal(first.ArmedAt) || !reflect.DeepEqual(second.Members, []string{"c", "d"}) {
					t.Fatalf("fresh same-stage arm = %#v", second)
				}
				h.restart()
				before := h.instance()
				_, err = executePublishedWorkflowJoinForTest(t, h.pc, h.ctx, node, handler, workflowTriggerContext{Event: oldEvent,
					State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID), HandlerEventKey: "item.completed"})
				envelope, typed := failures.EnvelopeFromError(err)
				if !typed || envelope.Class != failures.ClassStaleArrival {
					t.Fatalf("retained E1 arrival was not stale: %v", err)
				}
				if after := h.instance(); !exactJoinSemanticStateEqual(before, after) {
					t.Fatal("old retained arrival mutated E2 after restart")
				}
				for _, member := range []string{"c", "d"} {
					if err := deliverExactJoinMember(t, h.pc, h.store, h.ctx, h.source, exactJoinScope{
						declarationFlowID: flow, executionFlowID: pipelineDeclarationFlowPath(flow), path: h.path, route: h.route, entityID: h.entityID,
					}, member); err != nil {
						t.Fatalf("fresh E2 member %s: %v", member, err)
					}
				}
				final := h.instance()
				if final.CurrentState != "ready" || len(final.TransitionHistory) != 1 || final.Revision <= secondInstance.Revision || final.TransitionHistory[0].From != "awaiting" || final.TransitionHistory[0].To != "ready" {
					t.Fatalf("second entry completion = %q history=%#v", final.CurrentState, final.TransitionHistory)
				}
			})
		}
	}
}
