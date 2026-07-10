package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
)

func TestCanonicalDeliveryOwnerInvariant_PendingSurfacesAgree_V2(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID string)
		wantPending bool
	}{
		{
			name:        "pending delivery remains pending everywhere",
			setup:       func(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID string) {},
			wantPending: true,
		},
		{
			name: "in progress delivery remains pending everywhere",
			setup: func(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID string) {
				t.Helper()
				if err := pg.MarkEventDeliveryInProgress(ctx, eventID, agentID, ""); err != nil {
					t.Fatalf("MarkEventDeliveryInProgress: %v", err)
				}
			},
			wantPending: true,
		},
		{
			name: "retryable aged failure remains pending everywhere",
			setup: func(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID string) {
				t.Helper()
				if err := pg.UpsertEventReceipt(ctx, eventID, agentID, runtimemanager.ReceiptStatusError, testRetryableFailure()); err != nil {
					t.Fatalf("UpsertEventReceipt(error): %v", err)
				}
				rewindCanonicalDeliveryAttempt(t, ctx, pg, eventID, agentID, time.Now().Add(-2*time.Minute))
			},
			wantPending: true,
		},
		{
			name: "retryable fresh failure stays out of pending surfaces until backoff elapses",
			setup: func(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID string) {
				t.Helper()
				if err := pg.UpsertEventReceipt(ctx, eventID, agentID, runtimemanager.ReceiptStatusError, testRetryableFailure()); err != nil {
					t.Fatalf("UpsertEventReceipt(error): %v", err)
				}
			},
			wantPending: false,
		},
		{
			name: "processed delivery leaves all pending surfaces",
			setup: func(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID string) {
				t.Helper()
				if err := pg.UpsertEventReceipt(ctx, eventID, agentID, runtimemanager.ReceiptStatusProcessed, nil); err != nil {
					t.Fatalf("UpsertEventReceipt(processed): %v", err)
				}
			},
			wantPending: false,
		},
		{
			name: "dead letter leaves all pending surfaces",
			setup: func(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID string) {
				t.Helper()
				if err := pg.UpsertEventReceipt(ctx, eventID, agentID, runtimemanager.ReceiptStatusError, testRetryableFailure()); err != nil {
					t.Fatalf("UpsertEventReceipt(first error): %v", err)
				}
				if err := pg.UpsertEventReceipt(ctx, eventID, agentID, runtimemanager.ReceiptStatusError, testRetryableFailure()); err != nil {
					t.Fatalf("UpsertEventReceipt(second error): %v", err)
				}
			},
			wantPending: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg, cleanup := newTestPostgresStore(t)
			defer cleanup()

			ctx := context.Background()
			entityID, agentID := seedEntityAndAgent(t, ctx, pg)
			evt := seedEvent(t, ctx, pg, entityID, "test.delivery_receipt.invariant."+strings.ReplaceAll(tt.name, " ", "_"))
			if err := pg.InsertEventDeliveries(ctx, evt.ID(), []string{agentID}); err != nil {
				t.Fatalf("InsertEventDeliveries: %v", err)
			}

			tt.setup(t, ctx, pg, evt.ID(), agentID)
			assertCanonicalPendingTruthAcrossSurfaces(t, ctx, pg, agentID, evt, tt.wantPending)
		})
	}
}

func TestCanonicalDeliveryOwnerInvariant_ReceiptRowsRemainOutcomeOnly_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.delivery_receipt.invariant.legacy_receipt_only")
	insertLegacyAgentReceiptState(t, ctx, pg, evt.ID(), agentID, runtimemanager.ReceiptStatusError, 1, "handler_error", "boom", time.Now().Add(-2*time.Minute))

	assertCanonicalPendingTruthAcrossSurfaces(t, ctx, pg, agentID, evt, false)

	err := pg.UpsertEventReceipt(ctx, evt.ID(), agentID, runtimemanager.ReceiptStatusError, testRetryableFailure())
	if err == nil {
		t.Fatal("expected receipt-only state to fail closed without a canonical delivery row")
	}
	if !strings.Contains(err.Error(), "delivery row required") {
		t.Fatalf("UpsertEventReceipt receipt-only error = %v, want delivery row required", err)
	}
}

func assertCanonicalPendingTruthAcrossSurfaces(
	t *testing.T,
	ctx context.Context,
	pg *store.PostgresStore,
	agentID string,
	evt events.Event,
	wantPending bool,
) {
	t.Helper()

	since := time.Now().Add(-2 * time.Hour)

	direct, err := pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	assertPendingEventPresence(t, "direct pending reads", direct, evt.ID(), wantPending)

	subscribed, err := pg.ListPendingSubscribedEvents(ctx, agentID, []events.EventType{evt.Type()}, since, 100)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	assertPendingEventPresence(t, "subscribed pending reads", subscribed, evt.ID(), wantPending)

	factsByAgent, err := pg.ListPendingAgentDeliveryFacts(ctx, []string{agentID}, since)
	if err != nil {
		t.Fatalf("ListPendingAgentDeliveryFacts: %v", err)
	}
	facts := factsByAgent[agentID]
	if wantPending {
		if facts.PendingCount != 1 {
			t.Fatalf("pending delivery facts count = %d, want 1", facts.PendingCount)
		}
		if facts.OldestPendingAgeSec <= 0 {
			t.Fatalf("oldest pending age = %d, want > 0", facts.OldestPendingAgeSec)
		}
		return
	}
	if facts.PendingCount != 0 || facts.OldestPendingAgeSec != 0 {
		t.Fatalf("pending delivery facts = %+v, want zero pending truth", facts)
	}
}

func assertPendingEventPresence(t *testing.T, surface string, evts []events.Event, eventID string, want bool) {
	t.Helper()
	found := false
	for _, evt := range evts {
		if strings.TrimSpace(evt.ID()) == strings.TrimSpace(eventID) {
			found = true
			break
		}
	}
	if found != want {
		t.Fatalf("%s presence mismatch for %s: found=%v want=%v", surface, eventID, found, want)
	}
}
