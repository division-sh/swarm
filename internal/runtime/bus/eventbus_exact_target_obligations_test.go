package bus

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

func TestEventBusExactTargetObligationMatrix(t *testing.T) {
	dispatchModes := []string{"direct", "subscribed_outbox", "committed_replay"}
	scenarios := []struct {
		name       string
		installB   bool
		prepare    func(*testing.T, *EventBus)
		context    func() context.Context
		wantDetail string
	}{
		{
			name:     "both_live",
			installB: true,
			prepare:  func(*testing.T, *EventBus) {},
			context:  context.Background,
		},
		{
			name:       "b_missing",
			prepare:    func(*testing.T, *EventBus) {},
			context:    context.Background,
			wantDetail: "review/inst-b",
		},
		{
			name:     "b_inactive",
			installB: true,
			prepare: func(t *testing.T, bus *EventBus) {
				bus.mu.RLock()
				handle := bus.agentRouteHandles[testAgentRouteIdentity(t, "worker", "review/inst-b")]
				bus.mu.RUnlock()
				if handle == nil {
					t.Fatal("missing exact sibling B route")
				}
				handle.deactivate()
			},
			context:    context.Background,
			wantDetail: "review/inst-b",
		},
		{
			name:     "b_timeout",
			installB: true,
			prepare: func(t *testing.T, bus *EventBus) {
				fillExactSiblingChannel(t, bus, "review/inst-b")
			},
			context:    context.Background,
			wantDetail: "review/inst-b",
		},
		{
			name:     "context_cancel",
			installB: true,
			prepare: func(t *testing.T, bus *EventBus) {
				fillExactSiblingChannel(t, bus, "review/inst-a")
				fillExactSiblingChannel(t, bus, "review/inst-b")
			},
			context: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			wantDetail: "review/inst-a",
		},
	}

	for _, dispatchMode := range dispatchModes {
		for _, scenario := range scenarios {
			t.Run(dispatchMode+"/"+scenario.name, func(t *testing.T) {
				var store *targetRouteMemoryStore
				if dispatchMode != "direct" {
					store = newTargetRouteMemoryStore()
				}
				bus, channels, recipients, routes := exactSiblingObligationBus(t, store, scenario.installB)
				scenario.prepare(t, bus)
				ctx := scenario.context()
				evt := exactSiblingObligationEvent(dispatchMode + "/" + scenario.name)
				if store != nil {
					store.events[evt.ID()] = evt
					store.settlements[evt.ID()] = exactSiblingDeliverySettlement(t)
					store.scopes[evt.ID()] = runtimepipelineobligation.ScopeSubscribed
					store.routes[evt.ID()] = append([]events.DeliveryRoute(nil), routes...)
				}

				var err error
				switch dispatchMode {
				case "direct":
					err = bus.deliverLiveRecipientsWithRoutes(ctx, evt, recipients, routes)
				case "subscribed_outbox":
					err = bus.EngineDispatcher().DispatchPostCommit(ctx, []runtimeengine.EmitIntent{{Event: evt}})
				case "committed_replay":
					var live []string
					live, _, routes, err = bus.replayRecipientsForCommittedEvent(
						ctx,
						evt,
						[]string{"worker"},
						runtimepipelineobligation.ScopeSubscribed,
					)
					if err == nil {
						err = bus.deliverToRecipientsWithRoutes(ctx, evt, live, routes)
					}
				default:
					t.Fatalf("unsupported dispatch mode %q", dispatchMode)
				}

				if scenario.wantDetail == "" {
					if err != nil {
						t.Fatalf("%s delivery: %v", dispatchMode, err)
					}
					completeExactSiblingDeliveries(t, channels)
					return
				}
				failure, ok := runtimefailures.As(err)
				if !ok || failure.Failure.Detail.Code != "authoritative_delivery_incomplete" {
					t.Fatalf("delivery failure = %#v, want authoritative_delivery_incomplete", failure)
				}
				details := append(failureDetailStrings(failure.Failure.Detail.Attributes, "missing_recipients"),
					failureDetailStrings(failure.Failure.Detail.Attributes, "timed_out_recipients")...)
				if !strings.Contains(strings.Join(details, ","), scenario.wantDetail) {
					t.Fatalf("delivery details = %#v, want exact target %q", details, scenario.wantDetail)
				}
				drainExactSiblingDeliveries(channels)
			})
		}
	}
}

func exactSiblingObligationBus(t *testing.T, store *targetRouteMemoryStore, installB bool) (*EventBus, map[string]<-chan *LocalDelivery, []RoutePlanLiveRecipient, []events.DeliveryRoute) {
	t.Helper()
	bus, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatal(err)
	}
	tokenA := testAgentLifecycleToken(t, "worker", "review/inst-a", 1, 1)
	tokenB := testAgentLifecycleToken(t, "worker", "review/inst-b", 1, 1)
	channels := map[string]<-chan *LocalDelivery{}
	channels["review/inst-a"] = bus.ReplaceAgentRoute(tokenA, testAgentSubscriptionAdmissionForFlow(t, "worker", "review/inst-a", events.EventType("test.work")))
	if installB {
		channels["review/inst-b"] = bus.ReplaceAgentRoute(tokenB, testAgentSubscriptionAdmissionForFlow(t, "worker", "review/inst-b", events.EventType("test.work")))
	}
	recipients := []RoutePlanLiveRecipient{
		{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: tokenA.Identity, PersistAsDelivery: true},
		{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: tokenB.Identity, PersistAsDelivery: true},
	}
	routes := []events.DeliveryRoute{
		exactAgentDeliveryRoute(tokenA.Identity),
		exactAgentDeliveryRoute(tokenB.Identity),
	}
	return bus, channels, recipients, routes
}

func exactAgentDeliveryRoute(identity agentidentity.Identity) events.DeliveryRoute {
	return events.DeliveryRoute{
		Recipient:     events.MustAgentDeliveryRecipient(identity.AgentID()),
		AgentIdentity: identity,
	}
}

func exactSiblingObligationEvent(label string) events.Event {
	return eventtest.RunCreatingRootIngress(
		uuid.NewString(),
		events.EventType("test.work"),
		"",
		"",
		[]byte(`{}`),
		0,
		eventtest.UUID("exact-target-obligation-run-"+label),
		"",
		events.EventEnvelope{},
		time.Now(),
	)
}

func exactSiblingDeliverySettlement(t testing.TB) events.RouteSettlement {
	t.Helper()
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatalf("admit exact sibling evaluation ledger: %v", err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatalf("admit exact sibling delivery settlement: %v", err)
	}
	return settlement
}

func failureDetailStrings(attributes map[string]any, key string) []string {
	switch values := attributes[key].(type) {
	case []string:
		return append([]string(nil), values...)
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if text, ok := value.(string); ok {
				out = append(out, text)
			}
		}
		return out
	default:
		return nil
	}
}

func completeExactSiblingDeliveries(t *testing.T, channels map[string]<-chan *LocalDelivery) {
	t.Helper()
	for _, flowInstance := range []string{"review/inst-a", "review/inst-b"} {
		select {
		case delivery := <-channels[flowInstance]:
			if delivery == nil {
				t.Fatalf("%s delivery is nil", flowInstance)
			}
			if err := delivery.Complete(); err != nil {
				t.Fatalf("complete %s delivery: %v", flowInstance, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for %s", flowInstance)
		}
	}
}

func fillExactSiblingChannel(t *testing.T, bus *EventBus, flowInstance string) {
	t.Helper()
	identity := testAgentRouteIdentity(t, "worker", flowInstance)
	bus.mu.RLock()
	handle := bus.agentRouteHandles[identity]
	bus.mu.RUnlock()
	if handle == nil {
		t.Fatalf("missing route handle for %s", flowInstance)
	}
	send := handle.ch
	for len(send) < cap(send) {
		send <- nil
	}
}

func drainExactSiblingDeliveries(channels map[string]<-chan *LocalDelivery) {
	for _, receive := range channels {
		for {
			select {
			case delivery := <-receive:
				if delivery != nil {
					_ = delivery.Complete()
				}
			default:
				goto next
			}
		}
	next:
	}
}
