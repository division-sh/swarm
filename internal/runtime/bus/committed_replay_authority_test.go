package bus

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

type preparedPublishEventReaderFunc func(context.Context, string) (PreparedPublishEvent, bool, error)

func (f preparedPublishEventReaderFunc) LoadPreparedPublishEvent(ctx context.Context, eventID string) (PreparedPublishEvent, bool, error) {
	return f(ctx, eventID)
}

func TestCommittedReplayPropagatesPreparedEventReadFailureBeforeDispatch(t *testing.T) {
	readFailure := errors.New("prepared event read failed")
	for _, consumer := range []string{"committed_replay", "post_commit_dispatch"} {
		t.Run(consumer, func(t *testing.T) {
			store := newTargetRouteMemoryStore()
			bus, err := newScopedTestEventBus(store)
			if err != nil {
				t.Fatal(err)
			}
			evt := exactSiblingObligationEvent(consumer)
			deliveries := subscribeInternalDeliveriesForTest(t, bus, "current-only", evt.Type())
			bus.durable.PreparedEvents = preparedPublishEventReaderFunc(func(context.Context, string) (PreparedPublishEvent, bool, error) {
				return PreparedPublishEvent{}, false, readFailure
			})

			switch consumer {
			case "committed_replay":
				_, _, _, err = bus.replayRecipientsForCommittedEvent(
					context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed,
				)
			case "post_commit_dispatch":
				_, _, err = (engineDispatcher{bus: bus}).dispatchIntent(
					context.Background(), runtimeengine.EmitIntent{Event: evt},
				)
			default:
				t.Fatalf("unsupported consumer %q", consumer)
			}
			if !errors.Is(err, readFailure) {
				t.Fatalf("%s error = %v, want prepared read failure", consumer, err)
			}
			assertNoCommittedAuthorityDelivery(t, deliveries)
		})
	}
}

func TestCommittedReplayRejectsCorruptPreparedSettlementAcrossConsumers(t *testing.T) {
	evt := exactSiblingObligationEvent("corrupt-settlement")
	admitted, err := events.RevalidatePersistedEvent(evt)
	if err != nil {
		t.Fatalf("admit persisted event: %v", err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatalf("admit evaluation ledger: %v", err)
	}
	settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
	if err != nil {
		t.Fatalf("admit delivery settlement: %v", err)
	}

	for _, corruption := range []struct {
		name   string
		routes []events.DeliveryRoute
		want   string
	}{
		{name: "missing_delivery_routes", want: "delivery settlement requires at least one durable route"},
		{name: "malformed_delivery_route", routes: []events.DeliveryRoute{{}}, want: "unsupported subscriber type"},
	} {
		for _, consumer := range []string{"committed_replay", "post_commit_dispatch"} {
			t.Run(corruption.name+"/"+consumer, func(t *testing.T) {
				bus, err := newScopedTestEventBus(newTargetRouteMemoryStore())
				if err != nil {
					t.Fatal(err)
				}
				bus.durable.PreparedEvents = preparedPublishEventReaderFunc(func(context.Context, string) (PreparedPublishEvent, bool, error) {
					return PreparedPublishEvent{Event: admitted, Settlement: settlement, DeliveryRoutes: corruption.routes}, true, nil
				})

				switch consumer {
				case "committed_replay":
					_, _, _, err = bus.replayRecipientsForCommittedEvent(
						context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed,
					)
				case "post_commit_dispatch":
					_, _, err = (engineDispatcher{bus: bus}).dispatchIntent(
						context.Background(), runtimeengine.EmitIntent{Event: evt},
					)
				}
				if err == nil || !strings.Contains(err.Error(), corruption.want) {
					t.Fatalf("%s error = %v, want corruption rejection containing %q", consumer, err, corruption.want)
				}
			})
		}
	}
}

func TestCommittedNoDeliveryNeverConsultsCurrentTopology(t *testing.T) {
	store := newTargetRouteMemoryStore()
	bus, err := newScopedTestEventBus(store)
	if err != nil {
		t.Fatal(err)
	}
	evt := exactSiblingObligationEvent("committed-no-delivery")
	deliveries := subscribeInternalDeliveriesForTest(t, bus, "current-only", evt.Type())
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatalf("admit evaluation ledger: %v", err)
	}
	settlement, err := events.NewNoDeliverySettlement(
		events.EventWriteNormalPublication,
		events.NoDeliveryDeclaredConsumerNoPlan,
		ledger,
	)
	if err != nil {
		t.Fatalf("admit no-delivery settlement: %v", err)
	}
	store.events[evt.ID()] = evt
	store.settlements[evt.ID()] = settlement
	store.routes[evt.ID()] = nil

	live, internal, routes, err := bus.replayRecipientsForCommittedEvent(
		context.Background(), evt, nil, runtimepipelineobligation.ScopeSubscribed,
	)
	if err != nil {
		t.Fatalf("replay explicit no-delivery: %v", err)
	}
	if len(live) != 0 || len(internal) != 0 || len(routes) != 0 {
		t.Fatalf("replay recipients = live:%v internal:%v routes:%v, want explicit no-delivery", live, internal, routes)
	}
	if _, _, err := (engineDispatcher{bus: bus}).dispatchIntent(
		context.Background(), runtimeengine.EmitIntent{Event: evt},
	); err != nil {
		t.Fatalf("post-commit explicit no-delivery: %v", err)
	}
	assertNoCommittedAuthorityDelivery(t, deliveries)
}

func assertNoCommittedAuthorityDelivery(t testing.TB, deliveries <-chan *LocalDelivery) {
	t.Helper()
	select {
	case delivery := <-deliveries:
		t.Fatalf("unexpected current-topology delivery: %#v", delivery)
	default:
	}
}

func seedCommittedNoDeliveryForTest(t testing.TB, store *targetRouteMemoryStore, evt events.Event) {
	t.Helper()
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatalf("admit committed no-delivery ledger: %v", err)
	}
	settlement, err := events.NewNoDeliverySettlement(
		events.EventWriteNormalPublication,
		events.NoDeliveryDeclaredConsumerNoPlan,
		ledger,
	)
	if err != nil {
		t.Fatalf("admit committed no-delivery settlement: %v", err)
	}
	store.events[evt.ID()] = evt
	store.settlements[evt.ID()] = settlement
	store.routes[evt.ID()] = nil
	store.scopes[evt.ID()] = runtimepipelineobligation.ScopeSubscribed
}
