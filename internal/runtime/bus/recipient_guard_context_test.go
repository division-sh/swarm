package bus

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
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
			refusal := errors.New("effective reply context refused")
			calls := 0
			eb, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: semanticview.Wrap(materializedTargetBundle(t, "review", "target-node", "task.started")),
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
					if err != nil || len(actuals) != 1 || !reflect.DeepEqual(actuals[0].Route().Context, tc.want) {
						t.Fatalf("canonical guard actuals = %#v, error %v, want effective context %#v", actuals, err, tc.want)
					}
					if len(plan.DeliveryRoutes) != 1 || !reflect.DeepEqual(plan.DeliveryRoutes[0].Context, tc.want) {
						t.Fatalf("guard routes = %#v, want one route with effective context %#v", plan.DeliveryRoutes, tc.want)
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
			evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "review/inst-1/task.started", "", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
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
			if len(routes) != 1 || !reflect.DeepEqual(routes[0].Context, tc.want) {
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
