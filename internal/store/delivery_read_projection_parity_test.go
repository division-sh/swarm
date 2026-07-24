package store

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

type deliveryReadProjectionStore interface {
	authorActivityReceiptStore
	ListPendingAgentDeliveryFacts(context.Context, []string, time.Time) (map[string]PendingAgentDeliveryFacts, error)
	ListPendingAgentDeliveryDetails(context.Context, PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error)
	ListAgentDeliveryLifecycleFacts(context.Context, []string) (map[string]AgentDeliveryLifecycleFacts, error)
}

func TestDeliveryReadProjectionBoundsAndExactIdentityParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(deliveryReadProjectionStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			base := time.Date(2026, 7, 14, 11, 0, 0, 0, time.UTC)
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			siblingEvent := eventtest.PersistedProjection(
				uuid.NewString(), "projection.siblings", "gateway", "", json.RawMessage(`{"kind":"siblings"}`), 0,
				runID, "", events.EventEnvelope{}, base,
			)
			pageAgent := "page-agent"
			siblingRoutes := []events.DeliveryRoute{
				{
					SubscriberType: string(runtimedelivery.SubscriberAgent),
					SubscriberID:   pageAgent,
					Target:         events.RouteIdentity{FlowID: "delivery-projection", FlowInstance: "delivery-projection/one", EntityID: uuid.NewString()},
				},
				{
					SubscriberType: string(runtimedelivery.SubscriberAgent),
					SubscriberID:   pageAgent,
					Target:         events.RouteIdentity{FlowID: "delivery-projection", FlowInstance: "delivery-projection/two", EntityID: uuid.NewString()},
				},
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, siblingEvent, siblingRoutes); err != nil {
				t.Fatalf("commit sibling delivery event: %v", err)
			}
			siblingIDs := make([]string, 0, len(siblingRoutes))
			for _, route := range siblingRoutes {
				snapshot := loadDeliverySnapshotFixture(t, ctx, selected, siblingEvent.ID(), route)
				setDeliveryReadProjectionFixtureTimes(t, ctx, fixture, snapshot, base)
				siblingIDs = append(siblingIDs, snapshot.DeliveryID)
			}
			sort.Strings(siblingIDs)

			tailEvent := eventtest.PersistedProjection(
				uuid.NewString(), "projection.malformed_tail", "gateway", "", json.RawMessage(`{"kind":"tail"}`), 0,
				runID, "", events.EventEnvelope{}, base.Add(time.Minute),
			)
			tailRoute := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: pageAgent}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, tailEvent, []events.DeliveryRoute{tailRoute}); err != nil {
				t.Fatalf("commit malformed-tail event: %v", err)
			}
			tailSnapshot := loadDeliverySnapshotFixture(t, ctx, selected, tailEvent.ID(), tailRoute)
			setDeliveryReadProjectionFixtureTimes(t, ctx, fixture, tailSnapshot, base.Add(time.Minute))
			corruptOperatorAgentDeliveryTail(t, ctx, fixture, tailSnapshot.DeliveryID)

			facts, err := selected.ListPendingAgentDeliveryFacts(ctx, []string{pageAgent}, base.Add(-time.Minute))
			if err != nil {
				t.Fatalf("load pending aggregate: %v", err)
			}
			if facts[pageAgent].PendingCount != 3 || facts[pageAgent].OldestPendingAgeSec <= 0 {
				t.Fatalf("pending aggregate = %#v, want three obligations with positive age", facts[pageAgent])
			}

			first, err := selected.ListPendingAgentDeliveryDetails(ctx, PendingAgentDeliveryListOptions{
				AgentID: pageAgent, Since: base.Add(-time.Minute), Limit: 1,
			})
			if err != nil {
				t.Fatalf("load first pending page: %v", err)
			}
			if len(first.PendingDeliveries) != 1 || first.PendingDeliveries[0].DeliveryID != siblingIDs[0] || first.NextCursor == "" {
				t.Fatalf("first pending page = %#v, want first exact sibling plus cursor", first)
			}
			second, err := selected.ListPendingAgentDeliveryDetails(ctx, PendingAgentDeliveryListOptions{
				AgentID: pageAgent, Since: base.Add(-time.Minute), Limit: 1, Cursor: first.NextCursor,
			})
			if err != nil {
				t.Fatalf("load second pending page with malformed row beyond lookahead: %v", err)
			}
			if len(second.PendingDeliveries) != 1 || second.PendingDeliveries[0].DeliveryID != siblingIDs[1] || second.NextCursor == "" {
				t.Fatalf("second pending page = %#v, want second exact sibling plus cursor", second)
			}

			currentAgent := "current-agent"
			currentEvent := eventtest.PersistedProjection(
				uuid.NewString(), "projection.current", "gateway", "", json.RawMessage(`{"kind":"current"}`), 0,
				runID, "", events.EventEnvelope{}, base.Add(2*time.Minute),
			)
			currentRoute := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: currentAgent}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, currentEvent, []events.DeliveryRoute{currentRoute}); err != nil {
				t.Fatalf("commit current lifecycle event: %v", err)
			}
			currentSnapshot := loadDeliverySnapshotFixture(t, ctx, selected, currentEvent.ID(), currentRoute)
			setDeliveryReadProjectionFixtureTimes(t, ctx, fixture, currentSnapshot, base.Add(2*time.Minute))

			historyEvent := eventtest.PersistedProjection(
				uuid.NewString(), "projection.delivered_history", "gateway", "", json.RawMessage(`{"kind":"history"}`), 0,
				runID, "", events.EventEnvelope{}, base.Add(3*time.Minute),
			)
			historyRoute := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: currentAgent}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, historyEvent, []events.DeliveryRoute{historyRoute}); err != nil {
				t.Fatalf("commit delivered history event: %v", err)
			}
			historySnapshot := seedDeliveryStateFixture(t, ctx, selected, historyEvent, historyRoute, runtimedelivery.StateDelivered, nil)
			setDeliveryReadProjectionFixtureTimes(t, ctx, fixture, historySnapshot, base.Add(3*time.Minute))
			corruptOperatorAgentDeliveryTail(t, ctx, fixture, historySnapshot.DeliveryID)

			lifecycle, err := selected.ListAgentDeliveryLifecycleFacts(ctx, []string{currentAgent})
			if err != nil {
				t.Fatalf("load batched current lifecycle with malformed delivered history: %v", err)
			}
			if got := lifecycle[currentAgent]; got.CurrentState != string(runtimedelivery.StateQueued) || got.BlockingLayer != "delivery_queue" {
				t.Fatalf("current lifecycle = %#v, want queued delivery_queue", got)
			}
		})
	}
}

func setDeliveryReadProjectionFixtureTimes(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	snapshot runtimedelivery.Snapshot,
	at time.Time,
) {
	t.Helper()
	if fixture.dialect == "postgres" {
		setPostgresDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, at, at)
		return
	}
	setSQLiteDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, at, at)
}
