package bus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/deliverycontinuation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/google/uuid"
)

type electionNodeInterceptor struct {
	resolve func(context.Context, events.Event, events.DeliveryRoute) error
	settled bool
}

func (e electionNodeInterceptor) Intercept(context.Context, events.Event) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	return true, nil, pipelineobligation.Continue(), nil
}

func (e electionNodeInterceptor) InterceptDeliveryRoute(ctx context.Context, event events.DeliveryEvent, route events.DeliveryRoute) (bool, []events.Event, pipelineobligation.ExecutionOutcome, error) {
	outcome := pipelineobligation.Continue()
	if e.settled {
		outcome = pipelineobligation.DeadLetterExecution("handler_terminal_failure", nil)
	}
	return false, nil, outcome, e.resolve(ctx, event.Event(), route)
}

func TestDirectNodeDispatchRequiresActualCarrierResolution(t *testing.T) {
	for _, mode := range []string{"consumed", "returned", "terminal", "unresolved", "failure", "settled-consumed", "settled-returned", "settled-terminal", "settled-unresolved"} {
		t.Run(mode, func(t *testing.T) {
			eb, err := newScopedTestEventBus(InMemoryEventStore{})
			if err != nil {
				t.Fatal(err)
			}
			authority, err := eb.DeliveryAuthority()
			if err != nil {
				t.Fatal(err)
			}
			c, err := deliverycontinuation.New(struct{ runtimedelivery.Store }{}, unexpectedDurableTestRoles{}, authority, eb.workOwner, eb, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := eb.SetDeliveryContinuationOwner(c); err != nil {
				t.Fatal(err)
			}
			evt := deliveryContinuationProjectionEvent("direct-election-"+mode, "custom.election")
			route := nodeOnlyDeliveryPlan(t, evt, "election-node").DeliveryRoutes()[0]
			id, err := runtimedelivery.DeliveryID(evt.ID(), route)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := route.Identity()
			if err != nil {
				t.Fatal(err)
			}
			proof, err := runtimedelivery.AdmitDurableHandoffProof(id, evt.ID(), events.EncodeDeliveryRouteIdentity(identity), authority)
			if err != nil {
				t.Fatal(err)
			}
			if err := c.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
				t.Fatal(err)
			}
			eventSettled := mode == "settled-consumed" || mode == "settled-returned" || mode == "settled-terminal" || mode == "settled-unresolved"
			eb.SetInterceptors(electionNodeInterceptor{settled: eventSettled, resolve: func(ctx context.Context, _ events.Event, _ events.DeliveryRoute) error {
				guard, ok, err := worklifetime.DirectDeliveryCarrier(ctx, id)
				if err != nil || !ok {
					return fmt.Errorf("missing exact borrowed carrier: %w", err)
				}
				switch mode {
				case "consumed", "settled-consumed":
					_, err = guard.Consume(nil)
				case "returned", "settled-returned":
					_, err = guard.Complete(nil)
				case "terminal", "settled-terminal":
					if err = c.Release(id); err == nil {
						_, err = guard.Complete(nil)
					}
				case "failure":
					return fmt.Errorf("injected node failure")
				}
				return err
			}})
			result := eb.DispatchDeliveryContinuation(context.Background(), evt, route)
			want := deliverycontinuation.DispatchFatal
			switch mode {
			case "consumed", "settled-consumed":
				want = deliverycontinuation.DispatchTransferred
			case "returned":
				want = deliverycontinuation.DispatchDeferred
			case "terminal", "settled-terminal":
				want = deliverycontinuation.DispatchTerminal
			}
			if err := result.Validate(); err != nil || result.Disposition() != want {
				t.Fatalf("dispatch=%+v err=%v", result, err)
			}
			if mode == "returned" || mode == "unresolved" || mode == "failure" || mode == "settled-returned" || mode == "settled-unresolved" {
				cap, err := acquireTestDeliveryCapability(c, id)
				if err != nil {
					t.Fatalf("unconsumed carrier was orphaned: %v", err)
				}
				if _, err := cap.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestDeliveryDispatchPartialAcquisitionReturnsAcceptedPrefix(t *testing.T) {
	eb, err := newScopedTestEventBus(InMemoryEventStore{})
	if err != nil {
		t.Fatal(err)
	}
	authority, err := eb.DeliveryAuthority()
	if err != nil {
		t.Fatal(err)
	}
	c, err := deliverycontinuation.New(struct{ runtimedelivery.Store }{}, unexpectedDurableTestRoles{}, authority, eb.workOwner, eb, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := eb.SetDeliveryContinuationOwner(c); err != nil {
		t.Fatal(err)
	}
	evt := deliveryContinuationProjectionEvent("partial-election", "custom.election")
	routes := []events.DeliveryRoute{
		{Recipient: events.MustAgentDeliveryRecipient("a"), AgentIdentity: agentidentitytest.RootRuntime(t, "a", "election")},
		{Recipient: events.MustAgentDeliveryRecipient("z"), AgentIdentity: agentidentitytest.RootRuntime(t, "z", "election")},
	}
	routes = events.NormalizeDeliveryRoutes(routes)
	id, err := runtimedelivery.DeliveryID(evt.ID(), routes[0])
	if err != nil {
		t.Fatal(err)
	}
	identity, err := routes[0].Identity()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := runtimedelivery.AdmitDurableHandoffProof(id, evt.ID(), events.EncodeDeliveryRouteIdentity(identity), authority)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := eb.beginDeliveryDispatch(context.Background(), evt, routes); err == nil {
		t.Fatal("missing second continuation admitted")
	}
	cap, err := acquireTestDeliveryCapability(c, id)
	if err != nil {
		t.Fatalf("accepted prefix was orphaned: %v", err)
	}
	if _, err := cap.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil {
		t.Fatal(err)
	}
}

func TestDeliveryElectionLoserSkipsInterceptorsAndQueue(t *testing.T) {
	for _, kind := range []string{"agent", "node"} {
		for _, held := range []string{"carrier", "attempt", "terminal"} {
			t.Run(kind+"/"+held, func(t *testing.T) {
				eb, err := newScopedTestEventBus(InMemoryEventStore{})
				if err != nil {
					t.Fatal(err)
				}
				authority, err := eb.DeliveryAuthority()
				if err != nil {
					t.Fatal(err)
				}
				// This test exercises the real process-local election and bus
				// dispatch. Durable claim execution is covered by runtime parity.
				coordinator, err := deliverycontinuation.New(struct{ runtimedelivery.Store }{}, unexpectedDurableTestRoles{}, authority, eb.workOwner, eb, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := eb.SetDeliveryContinuationOwner(coordinator); err != nil {
					t.Fatal(err)
				}
				evt := eventtest.RunCreatingRootIngress(uuid.NewString(), "custom.election", "", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
				route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("election-agent"), AgentIdentity: agentidentitytest.RootRuntime(t, "election-agent", "election")}
				live := RoutePlanLiveRecipient{Recipient: route.Recipient, AgentIdentity: route.AgentIdentity, PersistAsDelivery: true, liveAuthority: liveRecipientAuthorityIdentity}
				if kind == "node" {
					route = nodeOnlyDeliveryPlan(t, evt, "election-node").DeliveryRoutes()[0]
					live = RoutePlanLiveRecipient{InternalID: route.Recipient.ID()}
				}
				id, err := runtimedelivery.DeliveryID(evt.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				identity, err := route.Identity()
				if err != nil {
					t.Fatal(err)
				}
				proof, err := runtimedelivery.AdmitDurableHandoffProof(id, evt.ID(), events.EncodeDeliveryRouteIdentity(identity), authority)
				if err != nil {
					t.Fatal(err)
				}
				if err := coordinator.AcceptCommitted([]runtimedelivery.DurableHandoffProof{proof}); err != nil {
					t.Fatal(err)
				}
				winner, err := acquireTestDeliveryCapability(coordinator, id)
				if err != nil {
					t.Fatal(err)
				}
				if held == "attempt" {
					if _, err := winner.Resolve(context.Background(), worklifetime.DeliveryContinuationConsume); err != nil {
						t.Fatal(err)
					}
				}
				if held == "terminal" {
					if err := coordinator.Release(id); err != nil {
						t.Fatal(err)
					}
				}
				interceptor := &nodeRouteConsumingInterceptor{}
				eb.SetInterceptors(interceptor)
				ctx, _, closeScope, err := eb.beginDeliveryDispatch(context.Background(), evt, []events.DeliveryRoute{route})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := eb.runInterceptorsForDeliveryRoutes(ctx, evt, []events.DeliveryRoute{route}); err != nil {
					t.Fatal(err)
				}
				dispatch, err := eb.dispatchLiveRecipientsWithRoutes(ctx, evt, []RoutePlanLiveRecipient{live}, []events.DeliveryRoute{route})
				if err != nil || !dispatch.complete() || len(dispatch.delivered) != 0 || len(dispatch.alreadyOwned) != 1 || interceptor.nodeCalls != 0 {
					t.Fatalf("loser dispatched or fabricated missing/delivered evidence: %+v calls=%d err=%v", dispatch, interceptor.nodeCalls, err)
				}
				if err := closeScope(); err != nil {
					t.Fatal(err)
				}
				recovery := eb.DispatchDeliveryContinuation(context.Background(), evt, route)
				want := deliverycontinuation.DispatchAlreadyOwned
				if held == "terminal" {
					want = deliverycontinuation.DispatchTerminal
				}
				if recovery.Disposition() != want {
					t.Fatalf("recovery loser: %+v", recovery)
				}
				if held != "attempt" {
					if _, err := winner.Resolve(context.Background(), worklifetime.DeliveryContinuationReturn); err != nil {
						t.Fatalf("loser stole winner settlement: %v", err)
					}
				}
			})
		}
	}
}
