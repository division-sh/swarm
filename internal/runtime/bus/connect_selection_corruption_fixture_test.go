package bus

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func requireCorruptConnectSelectionNoMutation(t testing.TB, bus *EventBus, store *connectRoutePlanDescriptorStore, event events.Event) {
	t.Helper()
	observations := slices.Clone(store.instanceObservations)
	plan, err := bus.planSubscribedRoutePlan(context.Background(), event, false)
	var corruption *pipeline.FlowInstanceConstructionCorruption
	if !errors.As(err, &corruption) || !reflect.DeepEqual(plan, RoutePlan{}) {
		t.Fatalf("corrupt native selector exposed a route plan: %+v %v", plan, err)
	}
	preview, err := bus.CheckPublishRecipientPlan(context.Background(), event)
	if !errors.As(err, &corruption) || len(preview.DeliveryRoutes) != 0 || len(preview.Recipients) != 0 || len(preview.RoutedRecipients) != 0 || len(preview.SubscriptionRecipients) != 0 {
		t.Fatalf("corrupt native selector exposed preflight recipients: %+v %v", preview, err)
	}
	if err := bus.Publish(context.Background(), event); !errors.As(err, &corruption) {
		t.Fatalf("corrupt native selector did not refuse publication: %v", err)
	}
	if len(store.events) != 0 || len(store.routes) != 0 || len(store.settlements) != 0 || !reflect.DeepEqual(observations, store.instanceObservations) {
		t.Fatal("corrupt selection changed publication or construction evidence")
	}
}
