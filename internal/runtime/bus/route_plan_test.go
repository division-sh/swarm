package bus

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

type routeValidationPublishTransaction struct {
	begun     bool
	finalized bool
}

func (t *routeValidationPublishTransaction) BeginPreparedPublish(context.Context, PreparedPublishEvent) (EventAppendOutcome, error) {
	t.begun = true
	return EventAppendInserted, nil
}

func (t *routeValidationPublishTransaction) FinalizePreparedPublish(context.Context, PreparedPublishFinalization) error {
	t.finalized = true
	return nil
}

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

func TestRoutePlanRejectsMalformedOrUnpairedPersistentDeliveryIntent(t *testing.T) {
	plan := RoutePlan{
		LiveRecipients: []RoutePlanLiveRecipient{{
			RecipientID: "agent-a", SubscriberType: routePlanSubscriberAgent, PersistAsDelivery: true,
		}},
		DeliveryIntents: []RoutePlanDeliveryIntent{{
			SubscriberID: "agent-a", Persist: true,
		}},
	}
	if err := plan.ValidatePersistentDeliveries(); err == nil || !strings.Contains(err.Error(), "unsupported subscriber type") {
		t.Fatalf("malformed durable route validation = %v", err)
	}
	if routes := plan.DeliveryRoutes(); len(routes) != 1 || routes[0].SubscriberID != "agent-a" {
		t.Fatalf("malformed durable intent was silently filtered: %#v", routes)
	}

	plan.DeliveryIntents = []RoutePlanDeliveryIntent{{
		SubscriberType: routePlanSubscriberAgent, SubscriberID: "agent-b", Persist: true,
	}}
	if err := plan.ValidatePersistentDeliveries(); err == nil || !strings.Contains(err.Error(), "has no exact durable delivery route") {
		t.Fatalf("live/durable recipient mismatch validation = %v", err)
	}
}

func TestEventBusPreparationRejectsMalformedDurableRouteBeforeFinalization(t *testing.T) {
	evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "input.received", "gateway", "", nil, 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	admitted, err := events.AdmitForPublish(evt, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	transaction := &routeValidationPublishTransaction{}
	ctx := WithCommitPublishTransaction(context.Background(), transaction)
	_, err = (&EventBus{}).prepareAdmittedPublishInMutation(ctx, admitted, nil, "subscribed", func(context.Context, events.Event) (RoutePlan, error) {
		return RoutePlan{
			LiveRecipients:  []RoutePlanLiveRecipient{{RecipientID: "agent-a", SubscriberType: routePlanSubscriberAgent, PersistAsDelivery: true}},
			DeliveryIntents: []RoutePlanDeliveryIntent{{SubscriberID: "agent-a", Persist: true}},
		}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "validate durable route plan") {
		t.Fatalf("prepare malformed route error = %v", err)
	}
	if !transaction.begun || transaction.finalized {
		t.Fatalf("malformed route transaction = begun:%v finalized:%v", transaction.begun, transaction.finalized)
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

func TestRoutePlanDoesNotExposeLegacyEventDeliveryPlanCompatibility(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller unavailable")
	}
	dir := filepath.Dir(testFile)
	files := []string{
		"eventbus.go",
		"route_plan.go",
		"delivery_planner.go",
		"eventbus_publish.go",
	}
	for _, file := range files {
		raw, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		source := string(raw)
		for _, forbidden := range []string{
			"type eventDeliveryPlan struct",
			"func (p RoutePlan) EventDeliveryPlan()",
			"CanonicalRoutePlan(",
			"WithCanonicalRoutePlan(",
			"routePlanFromLegacyEventDeliveryPlan",
		} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("%s still exposes legacy eventDeliveryPlan compatibility %q", file, forbidden)
			}
		}
	}
}
