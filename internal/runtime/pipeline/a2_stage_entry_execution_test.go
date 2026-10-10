package pipeline

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/google/uuid"
	"strings"
)

func VerifyNativeA2NonLoopStageReentryOnBothStoresForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range workflowJoinStoreCases() {
		for _, flow := range []string{"", "orders"} {
			t.Run(backend.name+"/"+pipelineDeclarationFlowPath(flow), func(t *testing.T) {
				files := workflowJoinLifecycleFixtureFiles(false, "")
				for _, prefix := range []string{"", "orders/"} {
					files[prefix+"events.yaml"] = strings.Replace(files[prefix+"events.yaml"], "manual.abort:\n", "manual.abort:\n  expected: list<text>\n", 1)
					files[prefix+"nodes.yaml"] = strings.Replace(files[prefix+"nodes.yaml"], "    manual.abort:\n      advances_to: dispatching", "    manual.abort:\n      data_accumulation:\n        writes:\n          - target_field: expected\n            value: payload.expected\n      advances_to: dispatching", 1)
				}
				h := newNativeExactWorkflowJoinHarness(t, backend.name, flow, "awaiting", []any{"a", "b"}, loadWorkflowTempBundle(t, files), open)
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
				oldContext, err := nativeWorkflowJoinPublicationContextForTest(t, h.fixture, h.pc, h.ctx, oldEvent, route, true)
				if err != nil {
					t.Fatal(err)
				}
				retained := events.DeliveryContextFromContext(oldContext).Joins
				if len(retained) != 1 || !retained[0].Ref.Equal(first.JoinRef()) {
					t.Fatalf("original publication binding = %#v", retained)
				}
				// Equal timestamps cannot collapse distinct admitted delivery entries.
				transition := func(next, name string, payload []byte) {
					t.Helper()
					event := nativeWorkflowJoinEventForTest(h.ctx, pipelineDeclarationFlowPath(flow), h.path, h.entityID, name, payload, first.ArmedAt)
					dispatchNativeWorkflowJoinEventForTest(t, h.fixture, h.pc, h.ctx, event, "dispatcher")
					if h.instance().CurrentState != next {
						t.Fatalf("native transition did not reach %s", next)
					}
					committed := h.instance()
					if len(committed.TransitionHistory) != 1 || committed.TransitionHistory[0].TriggerEventID != event.ID() || committed.TransitionHistory[0].To != next {
						t.Fatalf("entry lost the exact transition: %+v", committed)
					}
				}
				transition("dispatching", "manual.abort", []byte(`{"expected":["c","d"]}`))
				transition("awaiting", "dispatch.completed", []byte(`{}`))

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
				retainedSnapshot, err := h.fixture.NodeDeliverySnapshot(h.ctx, oldEvent.RunID(), oldEvent.ID(), node.Key())
				if err != nil {
					t.Fatal(err)
				}
				proof, err := h.fixture.Store.ProveHandoff(h.ctx, oldEvent.ID(), retainedSnapshot.Route)
				if err != nil {
					t.Fatal(err)
				}
				if err := h.fixture.Continuations.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
					t.Fatal(err)
				}
				_, err = executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, h.pc, h.ctx, node, handler, workflowTriggerContext{Event: oldEvent,
					State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID), HandlerEventKey: "item.completed"})
				envelope, typed := failures.EnvelopeFromError(err)
				if !typed || envelope.Class != failures.ClassStaleArrival {
					t.Fatalf("retained E1 arrival was not stale: %v", err)
				}
				if after := h.instance(); !exactJoinSemanticStateEqual(before, after) {
					t.Fatal("old retained arrival mutated E2 after restart")
				}
				for _, member := range []string{"c", "d"} {
					arrival := nativeWorkflowJoinEventForTest(h.ctx, pipelineDeclarationFlowPath(flow), h.path, h.entityID, "item.completed", mustJSON(map[string]any{"member_id": member, "result": map[string]any{"ok": true}}), first.ArmedAt)
					if _, err := executeNativePublishedWorkflowJoinForTest(t, h.fixture, h.mutations, h.pc, h.ctx, node, handler, workflowTriggerContext{Event: arrival, State: mustCurrentWorkflowState(t, h.pc, h.ctx, h.route, h.entityID), HandlerEventKey: "item.completed"}); err != nil {
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
