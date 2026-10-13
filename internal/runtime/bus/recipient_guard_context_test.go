package bus

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestRecipientGuardObservesEffectiveDeliveryContext(t *testing.T) {
	inherited := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:inherited"}}
	explicit := events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:explicit"}}
	for _, tc := range []struct {
		name         string
		routeContext events.DeliveryContext
		want         events.DeliveryContext
		reject       bool
	}{
		{name: "inherits before authorization", want: inherited},
		{name: "preserves explicit context", routeContext: explicit, want: explicit},
		{name: "rejects effective context before publication", want: inherited, reject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			node := testFlowNode(t, "review", "target-node")
			source := semanticview.Wrap(materializedTargetBundle(t, "review", "target-node", "task.started"))
			wantRoutes := []events.DeliveryRoute{{
				Recipient: events.MustNodeDeliveryRecipient(node), Context: tc.want,
				Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1", EntityID: eventtest.UUID("review-entity")}),
			}}
			if tc.routeContext.Reply != nil {
				// The materializer adds its explicit route alongside the compiled
				// subscription's independently inherited context.
				ordinary := wantRoutes[0]
				ordinary.Context = inherited
				wantRoutes = append([]events.DeliveryRoute{ordinary}, wantRoutes...)
			}
			refusal := errors.New("effective reply context refused")
			calls := 0
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source,
				RecipientPlanMaterializer: func(context.Context, events.Event, PublishRecipientPlan) ([]DeliveryRouteBlueprint, error) {
					return []DeliveryRouteBlueprint{{
						Recipient: events.MustNodeDeliveryRecipient(node),
						Target:    events.RouteIdentity{FlowID: "review", FlowInstance: "review/inst-1"},
						Handler:   runtimepipeline.MustDeliveryTargetHandler(node).ForEvent("task.started"),
						Context:   tc.routeContext,
					}}, nil
				},
				RecipientPlanGuard: func(_ context.Context, _ events.Event, plan PublishRecipientPlan) error {
					calls++
					actuals, err := plan.RecipientActuals()
					var actualRoutes []events.DeliveryRoute
					for _, actual := range actuals {
						actualRoutes = append(actualRoutes, actual.Route())
					}
					if err != nil || !reflect.DeepEqual(actualRoutes, wantRoutes) {
						t.Fatalf("canonical guard actuals = %#v, error %v, want exact routes %#v", actuals, err, wantRoutes)
					}
					if !reflect.DeepEqual(plan.DeliveryRoutes, wantRoutes) {
						t.Fatalf("guard routes = %#v, want exact routes %#v", plan.DeliveryRoutes, wantRoutes)
					}
					if tc.reject {
						return refusal
					}
					return nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			evt := eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), "review/inst-1/task.started", eventtest.Producer(events.EventProducerNode, node.Key()), "", nil, 0,
				events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live}, events.EventEnvelope{},
				eventtest.ConcreteTemplateRoutingSource("review", "review/inst-1", eventtest.UUID("review-entity")), time.Now().UTC())
			instance := StoredFlowInstanceIdentityFixture(source, "review", "inst-1", evt.RunID(), eventtest.UUID("review-entity"))
			store.installIndexObservation(constructionIndexObservation(t, source, evt.RunID(), instance, "inst-1"))
			plan, err := eb.planSubscribedPublish(events.WithDeliveryContext(context.Background(), inherited), evt)
			if calls != 1 {
				t.Fatalf("guard calls = %d, want 1; planning error: %v", calls, err)
			}
			if tc.reject {
				if !errors.Is(err, refusal) {
					t.Fatalf("error = %v, want guard refusal", err)
				}
				if len(plan.DeliveryRoutes()) != 0 {
					t.Fatal("rejected publication returned usable delivery routes")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			routes := plan.DeliveryRoutes()
			if !reflect.DeepEqual(routes, wantRoutes) {
				t.Fatalf("returned routes differ from authorized effective context: %#v", routes)
			}
		})
	}
}

func TestConsumedReplyContextPreservesRecipientIntents(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "explicit"}[explicit], func(t *testing.T) {
			node := testFlowNode(t, "review", "target-node")
			contextBefore := events.DeliveryContext{}
			if explicit {
				contextBefore = events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:explicit"}}
			}
			plan := RoutePlan{ReplyContextConsumed: true, DeliveryIntents: []RoutePlanDeliveryIntent{{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Handler:   runtimepipeline.MustDeliveryTargetHandler(node).ForEvent("task.started"),
				Context:   contextBefore,
			}}}
			projected := plan.WithDefaultDeliveryContext(events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "reply-v1:consumed"}})
			if len(projected.DeliveryIntents) != 1 || !reflect.DeepEqual(projected.DeliveryIntents[0].Context, contextBefore) {
				t.Fatalf("consumed reply altered recipient context: %#v", projected.DeliveryIntents)
			}
		})
	}
}
