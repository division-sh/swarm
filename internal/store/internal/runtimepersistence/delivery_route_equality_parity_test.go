package runtimepersistence

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestDeliveryRouteEqualityValidationStoreParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			t.Run("contradictory_node_ownership_rolls_back", func(t *testing.T) {
				event := deliveryRouteEqualityEvent(runID, "contradictory-node")
				target := events.RouteIdentity{
					FlowID: "review", FlowInstance: "review/one", EntityID: uuid.NewString(),
				}
				routes := []events.DeliveryRoute{
					{Recipient: events.MustNodeDeliveryRecipient("validator"), Target: events.MustExistingEntityTarget(target)},
					{Recipient: events.MustNodeDeliveryRecipient("validator"), Target: events.MustMaterializingEntityTarget(target)},
				}
				err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, routes)
				if err == nil || !strings.Contains(err.Error(), "conflicting target ownership kinds") {
					t.Fatalf("commit contradictory ownership error = %v", err)
				}
				assertDeliveryRouteEqualityCommitCounts(t, ctx, fixture, event.ID(), 0, 0)
			})

			t.Run("same_exact_agent_projection_conflict_rolls_back", func(t *testing.T) {
				event := deliveryRouteEqualityEvent(runID, "exact-agent-conflict")
				identity := testAgentIdentity(t, "worker", "review/one")
				first, _ := events.NewDeliveryPayloadProjection(map[string]string{"case": "one"})
				second, _ := events.NewDeliveryPayloadProjection(map[string]string{"case": "two"})
				routes := []events.DeliveryRoute{
					{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: identity, PayloadProjection: first},
					{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: identity, PayloadProjection: second},
				}
				err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, routes)
				if err == nil || !strings.Contains(err.Error(), "conflicting synthetic payload projections") {
					t.Fatalf("commit exact agent projection conflict error = %v", err)
				}
				assertDeliveryRouteEqualityCommitCounts(t, ctx, fixture, event.ID(), 0, 0)
			})

			t.Run("distinct_exact_agents_preserve_independent_projections", func(t *testing.T) {
				event := deliveryRouteEqualityEvent(runID, "agent-siblings")
				firstIdentity := testAgentIdentity(t, "worker", "review/one")
				secondIdentity := testAgentIdentity(t, "worker", "review/two")
				firstProjection, _ := events.NewDeliveryPayloadProjection(map[string]string{"case": "one"})
				secondProjection, _ := events.NewDeliveryPayloadProjection(map[string]string{"case": "two"})
				routes := []events.DeliveryRoute{
					{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: firstIdentity, PayloadProjection: firstProjection},
					{Recipient: events.MustAgentDeliveryRecipient("worker"), AgentIdentity: secondIdentity, PayloadProjection: secondProjection},
				}
				if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, routes); err != nil {
					t.Fatalf("commit distinct exact agent routes: %v", err)
				}
				assertDeliveryRouteEqualityCommitCounts(t, ctx, fixture, event.ID(), 1, 2)
				for _, route := range routes {
					snapshot := loadDeliverySnapshotFixture(t, ctx, fixture.store.(deliveryFixtureStore), event.ID(), route)
					if !events.SameDeliveryRouteIdentity(snapshot.Route, route) {
						t.Fatalf("persisted route = %#v, want exact route %#v", snapshot.Route, route)
					}
				}
			})
		})
	}
}

func TestMixedPubsubConnectCompositionPublicReadbackOnBothBackends(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(interface {
				deliveryFixtureStore
				ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
				LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
			})
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			producerTarget := events.RouteIdentity{
				FlowID: "producer", FlowInstance: "producer", EntityID: uuid.NewString(),
			}.Normalized()
			consumerTarget := events.RouteIdentity{
				FlowID: "consumer", FlowInstance: "consumer", EntityID: uuid.NewString(),
			}.Normalized()
			connectRecipient := events.MustNodeDeliveryRecipient("consumer-node")
			connectClaim, err := events.AdmitConnectExecutionClaim(
				sha256.Sum256([]byte("producer.ready->consumer.ready")),
				sha256.Sum256([]byte("consumer.ready")),
				connectRecipient,
				"consumer",
				"consumer-node",
				"work.accepted",
			)
			if err != nil {
				t.Fatalf("admit connect execution claim: %v", err)
			}
			routes := []events.DeliveryRoute{
				{
					Recipient: events.MustNodeDeliveryRecipient("producer-local"),
					Target:    events.MustExistingEntityTarget(producerTarget),
				},
				{
					Recipient:    connectRecipient,
					Target:       events.MustExistingEntityTarget(consumerTarget),
					ConnectClaim: connectClaim,
				},
			}
			event := eventtest.PersistedProjection(
				uuid.NewString(), "producer/work.ready", "producer-node", "", json.RawMessage(`{"work_id":"one"}`), 0,
				runID, "", events.EnvelopeForTargetSet(events.EventEnvelope{Source: producerTarget}, []events.RouteIdentity{consumerTarget, producerTarget}), time.Now().UTC(),
			)
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
				t.Fatalf("commit mixed local/connect routes: %v", err)
			}
			assertMixedDeliveryRouteReadback(t, ctx, selected, event.ID(), routes)
			assertMixedEventReadback(t, ctx, selected, event.ID(), producerTarget, consumerTarget, 2)

			localClaim, err := claimDeliveryFixture(ctx, selected, event, routes[0])
			if err != nil {
				t.Fatalf("claim local branch: %v", err)
			}
			if _, err := selected.SettleSuccess(ctx, localClaim.Claim, nil, 0); err != nil {
				t.Fatalf("settle local branch: %v", err)
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, routes); err != nil {
				t.Fatalf("exact duplicate after partial settlement: %v", err)
			}
			localSnapshot := loadDeliverySnapshotFixture(t, ctx, selected, event.ID(), routes[0])
			connectSnapshot := loadDeliverySnapshotFixture(t, ctx, selected, event.ID(), routes[1])
			if localSnapshot.State() != runtimedelivery.StateDelivered || connectSnapshot.State() != runtimedelivery.StateQueued {
				t.Fatalf("partial settlement duplicate states = local:%s connect:%s, want delivered/queued", localSnapshot.State(), connectSnapshot.State())
			}
			assertMixedDeliveryRouteReadback(t, ctx, selected, event.ID(), routes)

			connectClaimed, err := claimDeliveryFixture(ctx, selected, event, routes[1])
			if err != nil {
				t.Fatalf("claim connect branch: %v", err)
			}
			if _, err := selected.SettleSuccess(ctx, connectClaimed.Claim, nil, 0); err != nil {
				t.Fatalf("settle connect branch: %v", err)
			}
			assertMixedEventReadback(t, ctx, selected, event.ID(), producerTarget, consumerTarget, 2)
		})
	}
}

func deliveryRouteEqualityEvent(runID, label string) events.Event {
	return eventtest.PersistedProjection(
		uuid.NewString(), events.EventType("delivery.equality."+label), "fixture", "", json.RawMessage(`{}`), 0,
		runID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
}

func assertDeliveryRouteEqualityCommitCounts(t testing.TB, ctx context.Context, fixture authorActivityReceiptFixture, eventID string, wantEvents, wantDeliveries int) {
	t.Helper()
	eventQuery := `SELECT COUNT(*) FROM events WHERE event_id=?`
	deliveryQuery := `SELECT COUNT(*) FROM event_deliveries WHERE event_id=?`
	if fixture.dialect == "postgres" {
		eventQuery = `SELECT COUNT(*) FROM events WHERE event_id=$1::uuid`
		deliveryQuery = `SELECT COUNT(*) FROM event_deliveries WHERE event_id=$1::uuid`
	}
	var eventCount, deliveryCount int
	if err := fixture.db.QueryRowContext(ctx, eventQuery, eventID).Scan(&eventCount); err != nil {
		t.Fatalf("count persisted events: %v", err)
	}
	if err := fixture.db.QueryRowContext(ctx, deliveryQuery, eventID).Scan(&deliveryCount); err != nil {
		t.Fatalf("count persisted deliveries: %v", err)
	}
	if eventCount != wantEvents || deliveryCount != wantDeliveries {
		t.Fatalf("persisted counts = event:%d deliveries:%d, want event:%d deliveries:%d", eventCount, deliveryCount, wantEvents, wantDeliveries)
	}
}

func assertMixedDeliveryRouteReadback(t testing.TB, ctx context.Context, selected interface {
	ListEventDeliveryRoutes(context.Context, string) ([]events.DeliveryRoute, error)
}, eventID string, want []events.DeliveryRoute) {
	t.Helper()
	got, err := selected.ListEventDeliveryRoutes(ctx, eventID)
	if err != nil {
		t.Fatalf("list mixed delivery routes: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("mixed delivery routes = %#v, want %#v", got, want)
	}
	for _, wantRoute := range want {
		matched := false
		for _, gotRoute := range got {
			if events.SameDeliveryRouteIdentity(gotRoute, wantRoute) {
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("mixed delivery routes = %#v, missing exact route %#v", got, wantRoute)
		}
	}
}

func assertMixedEventReadback(t testing.TB, ctx context.Context, selected interface {
	LoadOperatorEvent(context.Context, string) (operatorread.OperatorEventFull, error)
}, eventID string, producerTarget, consumerTarget events.RouteIdentity, wantDeliveries int) {
	t.Helper()
	view, err := selected.LoadOperatorEvent(ctx, eventID)
	if err != nil {
		t.Fatalf("load mixed operator event: %v", err)
	}
	snapshot, err := view.EventSnapshot()
	if err != nil {
		t.Fatalf("decode mixed event snapshot: %v", err)
	}
	targets := snapshot.TargetRoutes()
	if len(targets) != 2 || !containsExactRoute(targets, producerTarget) || !containsExactRoute(targets, consumerTarget) {
		t.Fatalf("mixed event targets = %#v, want producer %#v and consumer %#v", targets, producerTarget, consumerTarget)
	}
	if len(view.Deliveries) != wantDeliveries || view.NoDelivery != nil || len(view.DeadLetters) != 0 {
		t.Fatalf("mixed public settlement = deliveries:%#v no_delivery:%#v dead_letters:%#v", view.Deliveries, view.NoDelivery, view.DeadLetters)
	}
}

func containsExactRoute(routes []events.RouteIdentity, want events.RouteIdentity) bool {
	for _, route := range routes {
		if events.SameRouteIdentity(route, want) {
			return true
		}
	}
	return false
}
