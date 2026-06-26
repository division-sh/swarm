package bus

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
)

func TestRoutePlanDeliveryIntentsCarryTypedProducer(t *testing.T) {
	routes := []events.DeliveryRoute{{
		SubscriberType: "node",
		SubscriberID:   "consumer-node",
		Target: events.RouteIdentity{
			FlowInstance: "consumer/inst-1",
			EntityID:     "ent-consumer",
		},
	}}

	intents := routePlanDeliveryIntentsFromRoutes(routes, routeIntentProducerConnectRoutePlan)
	if got, want := len(intents), 1; got != want {
		t.Fatalf("delivery intents = %d, want %d", got, want)
	}
	intent := intents[0]
	if intent.Producer != routeIntentProducerConnectRoutePlan {
		t.Fatalf("intent producer = %s, want %s", intent.Producer, routeIntentProducerConnectRoutePlan)
	}
	if intent.Producer.Source() != routePlanSourceConnectRoutePlan || intent.Producer.Reason() != routePlanReasonLoweredConnectRoutePlan {
		t.Fatalf("intent producer source/reason = %s/%s, want connect route plan/lowered connect", intent.Producer.Source(), intent.Producer.Reason())
	}
}

func TestRoutePlanProducerHelpersRequireTypedProducer(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(testFile), "route_plan.go"))
	if err != nil {
		t.Fatalf("read route_plan.go: %v", err)
	}
	source := string(raw)
	for _, forbidden := range []string{
		"routePlanLiveRecipientsFromManifest(manifest deliveryRecipientManifest, source, reason string)",
		"routePlanDeliveryIntentsFromRoutes(routes []events.DeliveryRoute, source, reason string)",
		"routePlanFromManifest(evt events.Event, manifest deliveryRecipientManifest, source, reason string)",
		"type routeIntentProducer struct",
		"routeIntentProducer{source:",
		"AuthorityOwner       string",
		"Source            string",
		"Reason            string",
		"Source         string",
		"Reason         string",
	} {
		if strings.Contains(source, forbidden) {
			t.Fatalf("route_plan.go still exposes raw route-intent producer shape %q", forbidden)
		}
	}
}
