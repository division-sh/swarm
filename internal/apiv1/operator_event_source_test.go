package apiv1

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
)

func TestPublicEventConstructionBindsRootSourceNotDeliveryTarget(t *testing.T) {
	for _, newRun := range []bool{true, false} {
		runID := uuid.NewString()
		target := events.RouteIdentity{FlowID: "child", FlowInstance: "child/one", EntityID: uuid.NewString()}
		event, err := eventPublicationEvent(eventPublicationParams{
			EventID: uuid.NewString(), EventName: "work.requested", Emitter: "operator-api",
			RunID: runID, NewRunCreated: newRun, Payload: []byte(`{}`),
			EntityID: target.EntityID, FlowInstance: target.FlowInstance, TargetRoute: target, TargetRouteSet: true,
		}, time.Now().UTC(), executionposture.Live)
		if err != nil {
			t.Fatal(err)
		}
		if event.RoutingSource().Kind() != events.RoutingSourceRoot || event.RoutingSource().Route() != (events.RouteIdentity{EntityID: runID}) {
			t.Fatalf("new_run=%v: public source borrowed target authority: %#v", newRun, event.RoutingSource())
		}
		if event.TargetRoute() != target {
			t.Fatalf("new_run=%v: source binding rewrote delivery target: %#v", newRun, event.TargetRoute())
		}
	}
}
