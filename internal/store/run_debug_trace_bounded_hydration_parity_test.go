package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

type boundedRunDebugTraceStore interface {
	authorActivityReceiptStore
	LoadRunDebugTracePage(context.Context, string, RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error)
}

func TestRunDebugTracePageBoundsCanonicalHydrationParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(boundedRunDebugTraceStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			base := time.Date(2026, 7, 23, 3, 30, 0, 0, time.UTC)
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			filteredEvent, filteredDelivery := seedRunDebugTraceBoundedDelivery(t, ctx, selected, runID, "trace.filtered", "filtered-agent", base)
			filteredClaim, err := selected.ClaimAgentDelivery(ctx, filteredEvent, events.DeliveryRoute{SubscriberType: "agent", SubscriberID: "filtered-agent"})
			if err != nil {
				t.Fatalf("claim filtered delivery: %v", err)
			}
			if _, err := selected.SettleSuccess(ctx, filteredClaim.Claim, nil, 0); err != nil {
				t.Fatalf("settle filtered delivery: %v", err)
			}

			targetEvents := make([]events.Event, 0, 3)
			targetDeliveries := make([]string, 0, 3)
			for index := 0; index < 3; index++ {
				event, deliveryID := seedRunDebugTraceBoundedDelivery(
					t, ctx, selected, runID, "trace.target", "target-agent", base.Add(time.Duration(index+1)*time.Second),
				)
				targetEvents = append(targetEvents, event)
				targetDeliveries = append(targetDeliveries, deliveryID)
			}

			corruptOperatorAgentDeliveryTail(t, ctx, fixture, filteredDelivery)
			corruptOperatorAgentDeliveryTail(t, ctx, fixture, targetDeliveries[2])

			opts := RunDebugTraceQueryOptions{
				Limit: 1,
				Filter: RunDebugTraceFilter{
					EventNames:       []string{"trace.target"},
					DeliveryStatuses: []string{"pending"},
					SubscriberIDs:    []string{"target-agent"},
					SubscriberTypes:  []string{"agent"},
				},
			}
			first, next, err := selected.LoadRunDebugTracePage(ctx, runID, opts)
			if err != nil {
				t.Fatalf("load first bounded trace page: %v", err)
			}
			assertBoundedRunDebugTracePage(t, first, next, targetEvents[0].ID(), targetDeliveries[0])

			opts.Cursor = next
			second, next, err := selected.LoadRunDebugTracePage(ctx, runID, opts)
			if err != nil {
				t.Fatalf("load second bounded trace page: %v", err)
			}
			assertBoundedRunDebugTracePage(t, second, next, targetEvents[1].ID(), targetDeliveries[1])
		})
	}
}

func seedRunDebugTraceBoundedDelivery(
	t *testing.T,
	ctx context.Context,
	store boundedRunDebugTraceStore,
	runID string,
	eventName string,
	agentID string,
	createdAt time.Time,
) (events.Event, string) {
	t.Helper()
	event := eventtest.PersistedProjection(
		uuid.NewString(), events.EventType(eventName), "runtime", "", json.RawMessage(fmt.Sprintf(`{"index":%d}`, createdAt.Unix())), 0,
		runID, "", events.EventEnvelope{}, createdAt,
	)
	route := events.DeliveryRoute{SubscriberType: string(runtimedelivery.SubscriberAgent), SubscriberID: agentID}
	if err := commitSemanticEventFixtureWithRoutes(ctx, store, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatalf("commit bounded trace event %s: %v", event.ID(), err)
	}
	proof, err := store.ProveHandoff(ctx, event.ID(), route)
	if err != nil {
		t.Fatalf("prove bounded trace delivery %s: %v", event.ID(), err)
	}
	return event, proof.DeliveryID()
}

func assertBoundedRunDebugTracePage(t *testing.T, rows []RunDebugTraceRow, next, eventID, deliveryID string) {
	t.Helper()
	if len(rows) != 1 || rows[0].EventID != eventID || rows[0].DeliveryID != deliveryID || next == "" {
		t.Fatalf("bounded trace page = %#v next=%q, want %s/%s and cursor", rows, next, eventID, deliveryID)
	}
}
