package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestDeliveryLifecycleConformance(t *testing.T) {
	ctx := testAuthorActivityContext(context.Background())

	cases := []struct {
		name   string
		seed   func(t *testing.T, ctx context.Context, fx *deliveryLifecycleFixture) string
		expect deliveryLifecycleExpectation
	}{
		{
			name: "pending_after_direct_publish",
			seed: func(t *testing.T, ctx context.Context, fx *deliveryLifecycleFixture) string {
				t.Helper()
				return fx.publishDirectEvent(t, ctx)
			},
			expect: deliveryLifecycleExpectation{
				deliveryStatus:     "pending",
				deliveryRetryCount: 0,
				directPending:      true,
				subscribedPending:  true,
				receiptFound:       false,
				recoveryReplays:    true,
			},
		},
		{
			name: "retryable_failed_delivery_after_backoff",
			seed: func(t *testing.T, ctx context.Context, fx *deliveryLifecycleFixture) string {
				t.Helper()
				eventID := fx.publishDirectEvent(t, ctx)
				if err := fx.pg.UpsertEventReceipt(ctx, eventID, fx.agentID, runtimemanager.ReceiptStatusError, testFailure("handler_failed")); err != nil {
					t.Fatalf("UpsertEventReceipt(retryable error): %v", err)
				}
				fx.rewindDeliveryAttempt(t, ctx, eventID, time.Now().Add(-2*time.Minute))
				return eventID
			},
			expect: deliveryLifecycleExpectation{
				deliveryStatus:     "failed",
				deliveryRetryCount: 1,
				directPending:      true,
				subscribedPending:  true,
				receiptFound:       true,
				receiptStatus:      runtimemanager.ReceiptStatusError,
				receiptRetryCount:  1,
				recoveryReplays:    true,
			},
		},
		{
			name: "dead_letter_is_terminal_everywhere",
			seed: func(t *testing.T, ctx context.Context, fx *deliveryLifecycleFixture) string {
				t.Helper()
				eventID := fx.publishDirectEvent(t, ctx)
				if err := fx.pg.UpsertEventReceipt(ctx, eventID, fx.agentID, runtimemanager.ReceiptStatusError, testFailure("handler_failed")); err != nil {
					t.Fatalf("UpsertEventReceipt(first error): %v", err)
				}
				if err := fx.pg.UpsertEventReceipt(ctx, eventID, fx.agentID, runtimemanager.ReceiptStatusError, testFailure("handler_failed")); err != nil {
					t.Fatalf("UpsertEventReceipt(second error): %v", err)
				}
				return eventID
			},
			expect: deliveryLifecycleExpectation{
				deliveryStatus:     "dead_letter",
				deliveryRetryCount: 2,
				directPending:      false,
				subscribedPending:  false,
				receiptFound:       true,
				receiptStatus:      runtimemanager.ReceiptStatusDeadLetter,
				receiptRetryCount:  2,
				recoveryReplays:    false,
			},
		},
		{
			name: "stranded_in_progress_without_receipt_remains_replay_eligible",
			seed: func(t *testing.T, ctx context.Context, fx *deliveryLifecycleFixture) string {
				t.Helper()
				eventID := fx.publishDirectEvent(t, ctx)
				if err := fx.pg.MarkEventDeliveryInProgress(ctx, eventID, fx.agentID, ""); err != nil {
					t.Fatalf("MarkEventDeliveryInProgress: %v", err)
				}
				return eventID
			},
			expect: deliveryLifecycleExpectation{
				deliveryStatus:     "in_progress",
				deliveryRetryCount: 0,
				directPending:      true,
				subscribedPending:  true,
				receiptFound:       false,
				recoveryReplays:    true,
			},
		},
		{
			name: "delivery_dead_letter_overrides_retryable_receipt_drift",
			seed: func(t *testing.T, ctx context.Context, fx *deliveryLifecycleFixture) string {
				t.Helper()
				eventID := fx.publishDirectEvent(t, ctx)
				if err := fx.pg.UpsertEventReceipt(ctx, eventID, fx.agentID, runtimemanager.ReceiptStatusError, testFailure("handler_failed")); err != nil {
					t.Fatalf("UpsertEventReceipt(first error): %v", err)
				}
				if err := fx.pg.UpsertEventReceipt(ctx, eventID, fx.agentID, runtimemanager.ReceiptStatusError, testFailure("handler_failed")); err != nil {
					t.Fatalf("UpsertEventReceipt(second error): %v", err)
				}
				fx.forceReceiptState(t, ctx, eventID, runtimemanager.ReceiptStatusError, 1, time.Now().Add(-2*time.Minute), "stale-retry")
				return eventID
			},
			expect: deliveryLifecycleExpectation{
				deliveryStatus:     "dead_letter",
				deliveryRetryCount: 2,
				directPending:      false,
				subscribedPending:  false,
				receiptFound:       true,
				receiptStatus:      runtimemanager.ReceiptStatusDeadLetter,
				receiptRetryCount:  2,
				recoveryReplays:    false,
			},
		},
		{
			name: "retryable_delivery_overrides_dead_letter_receipt_drift",
			seed: func(t *testing.T, ctx context.Context, fx *deliveryLifecycleFixture) string {
				t.Helper()
				eventID := fx.publishDirectEvent(t, ctx)
				if err := fx.pg.UpsertEventReceipt(ctx, eventID, fx.agentID, runtimemanager.ReceiptStatusError, testFailure("handler_failed")); err != nil {
					t.Fatalf("UpsertEventReceipt(retryable error): %v", err)
				}
				fx.rewindDeliveryAttempt(t, ctx, eventID, time.Now().Add(-2*time.Minute))
				fx.forceReceiptState(t, ctx, eventID, runtimemanager.ReceiptStatusDeadLetter, 2, time.Now().Add(-2*time.Minute), "stale-dead-letter")
				return eventID
			},
			expect: deliveryLifecycleExpectation{
				deliveryStatus:     "failed",
				deliveryRetryCount: 1,
				directPending:      true,
				subscribedPending:  true,
				receiptFound:       true,
				receiptStatus:      runtimemanager.ReceiptStatusError,
				receiptRetryCount:  1,
				recoveryReplays:    true,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			fx := newDeliveryLifecycleFixture(t, ctx)
			eventID := tc.seed(t, ctx, fx)
			got := fx.snapshot(t, ctx, eventID)
			assertDeliveryLifecycleExpectation(t, got, tc.expect, eventID)
		})
	}
}

type deliveryLifecycleExpectation struct {
	deliveryStatus     string
	deliveryRetryCount int
	directPending      bool
	subscribedPending  bool
	receiptFound       bool
	receiptStatus      runtimemanager.ReceiptStatus
	receiptRetryCount  int
	recoveryReplays    bool
}

type deliveryLifecycleSnapshot struct {
	deliveryStatus     string
	deliveryRetryCount int
	directPendingIDs   []string
	subscribedPending  []string
	receipt            runtimemanager.EventReceipt
	receiptFound       bool
	recoveredEventIDs  []string
}

type deliveryLifecycleFixture struct {
	pg           *store.PostgresStore
	bus          *runtimebus.EventBus
	workOwner    *worklifetime.RuntimeOccurrence
	agentID      string
	subscription events.EventType
}

func newDeliveryLifecycleFixture(t *testing.T, ctx context.Context) *deliveryLifecycleFixture {
	t.Helper()
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	requireCanonicalDeliveryLifecycleSurface(t, ctx, pg)

	workOwner := conformanceTestRuntimeOccurrence(t, authorActivityTestBundleSourceFact.BundleHash)
	bus, err := newScopedTestEventBus(t, pg, runtimebus.EventBusOptions{WorkOwner: workOwner}, "review.requested")
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	agentID := "delivery-conformance-agent"
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            agentID,
			Role:          "tester",
			Type:          "stub",
			FlowID:        "global",
			Model:         "regular",
			ExecutionMode: "live",
			Subscriptions: []string{"review.*"},
			Config:        []byte(`{"system_prompt":"x"}`),
		},
		Status:    "active",
		HiredBy:   "delivery-conformance",
		StartedAt: time.Now().Add(-24 * time.Hour).UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent(%s): %v", agentID, err)
	}

	return &deliveryLifecycleFixture{
		pg:           pg,
		bus:          bus,
		workOwner:    workOwner,
		agentID:      agentID,
		subscription: events.EventType("review.*"),
	}
}

func requireCanonicalDeliveryLifecycleSurface(t *testing.T, ctx context.Context, pg *store.PostgresStore) {
	t.Helper()
	storetest.BootstrapPostgresRuntimeStore(t, pg)
	requireTableColumns(t, ctx, pg.DB, "event_deliveries", "delivery_id", "event_id", "subscriber_type", "subscriber_id", "status", "retry_count", "created_at", "delivered_at")
	requireTableColumns(t, ctx, pg.DB, "event_receipts", "event_id", "subscriber_type", "subscriber_id", "outcome", "side_effects", "processed_at")
}

func (fx *deliveryLifecycleFixture) publishDirectEvent(t *testing.T, ctx context.Context) string {
	t.Helper()
	eventID := uuid.NewString()
	ch := fx.bus.Subscribe(fx.agentID)
	defer fx.bus.Unsubscribe(fx.agentID)

	evt := eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("review.requested"),
		"runtime",
		"",
		[]byte(`{"ok":true}`),
		0,
		"",
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, uuid.NewString()),
		time.Now().Add(-2*time.Hour).UTC(),
	)

	if err := fx.bus.PublishDirect(ctx, evt, []string{fx.agentID}); err != nil {
		t.Fatalf("PublishDirect: %v", err)
	}
	select {
	case delivered := <-ch:
		if delivered.ID() != eventID {
			t.Fatalf("delivered event id = %q, want %q", delivered.ID(), eventID)
		}
		if err := delivered.Complete(); err != nil {
			t.Fatalf("complete local delivery ownership: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("expected direct publish to fan out to live subscriber")
	}
	return eventID
}

func (fx *deliveryLifecycleFixture) rewindDeliveryAttempt(t *testing.T, ctx context.Context, eventID string, when time.Time) {
	t.Helper()
	if _, err := fx.pg.DB.ExecContext(ctx, `
		UPDATE event_deliveries
		SET delivered_at = $3
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, fx.agentID, when.UTC()); err != nil {
		t.Fatalf("rewind event_deliveries.delivered_at: %v", err)
	}
	if _, err := fx.pg.DB.ExecContext(ctx, `
		UPDATE event_receipts
		SET processed_at = $3
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, fx.agentID, when.UTC()); err != nil {
		t.Fatalf("rewind event_receipts.processed_at: %v", err)
	}
}

func (fx *deliveryLifecycleFixture) forceReceiptState(
	t *testing.T,
	ctx context.Context,
	eventID string,
	status runtimemanager.ReceiptStatus,
	retryCount int,
	processedAt time.Time,
	errText string,
) {
	t.Helper()
	sideEffects, err := json.Marshal(map[string]any{
		"manager_status": strings.TrimSpace(string(status)),
		"retry_count":    retryCount,
		"error":          strings.TrimSpace(errText),
	})
	if err != nil {
		t.Fatalf("marshal receipt side effects: %v", err)
	}
	if _, err := fx.pg.DB.ExecContext(ctx, `
		UPDATE event_receipts
		SET outcome = $3,
			side_effects = $4::jsonb,
			processed_at = $5
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, fx.agentID, managerReceiptOutcome(status), string(sideEffects), processedAt.UTC()); err != nil {
		t.Fatalf("force receipt state: %v", err)
	}
}

func managerReceiptOutcome(status runtimemanager.ReceiptStatus) string {
	switch status {
	case runtimemanager.ReceiptStatusError, runtimemanager.ReceiptStatusDeadLetter:
		return "dead_letter"
	default:
		return "success"
	}
}

func (fx *deliveryLifecycleFixture) snapshot(t *testing.T, ctx context.Context, eventID string) deliveryLifecycleSnapshot {
	t.Helper()

	var got deliveryLifecycleSnapshot
	if err := fx.pg.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(retry_count, 0)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`, eventID, fx.agentID).Scan(&got.deliveryStatus, &got.deliveryRetryCount); err != nil {
		t.Fatalf("load event_deliveries state: %v", err)
	}

	direct, err := fx.pg.ListPendingEventsForAgent(ctx, fx.agentID, time.Now().Add(-24*time.Hour), 20)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	for _, evt := range direct {
		got.directPendingIDs = append(got.directPendingIDs, evt.ID())
	}

	subscribed, err := fx.pg.ListPendingSubscribedEvents(ctx, fx.agentID, []events.EventType{fx.subscription}, time.Now().Add(-24*time.Hour), 20)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	for _, evt := range subscribed {
		got.subscribedPending = append(got.subscribedPending, evt.ID())
	}

	got.receipt, got.receiptFound, err = fx.pg.GetEventReceipt(ctx, eventID, fx.agentID)
	if err != nil {
		t.Fatalf("GetEventReceipt: %v", err)
	}

	got.recoveredEventIDs = fx.recoverSeenEventIDs(t, ctx)
	return got
}

func (fx *deliveryLifecycleFixture) recoverSeenEventIDs(t *testing.T, ctx context.Context) []string {
	t.Helper()
	ctx = managedConformanceExecutionContext(t, ctx, "delivery-lifecycle-conformance")
	var (
		mu   sync.Mutex
		seen []string
	)
	am := runtimemanager.NewAgentManagerWithOptions(fx.bus, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		subscriptions := make([]events.EventType, 0, len(cfg.Subscriptions))
		for _, raw := range cfg.Subscriptions {
			raw = strings.TrimSpace(raw)
			if raw != "" {
				subscriptions = append(subscriptions, events.EventType(raw))
			}
		}
		return &deliveryLifecycleRecordingAgent{
			id:            cfg.ID,
			subscriptions: subscriptions,
			record: func(evt events.Event) {
				mu.Lock()
				seen = append(seen, evt.ID())
				mu.Unlock()
			},
		}, nil
	}, runtimemanager.AgentManagerOptions{WorkOwner: fx.workOwner}, fx.pg)
	if err := am.Recover(ctx); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), seen...)
}

type deliveryLifecycleRecordingAgent struct {
	id            string
	subscriptions []events.EventType
	record        func(events.Event)
}

func (a *deliveryLifecycleRecordingAgent) ID() string { return a.id }

func (*deliveryLifecycleRecordingAgent) Type() string { return "stub" }

func (a *deliveryLifecycleRecordingAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}

func (a *deliveryLifecycleRecordingAgent) OnEvent(_ context.Context, evt events.Event) ([]events.Event, error) {
	if a.record != nil {
		a.record(evt)
	}
	return nil, errors.New("session currently leased")
}

func assertDeliveryLifecycleExpectation(t *testing.T, got deliveryLifecycleSnapshot, want deliveryLifecycleExpectation, eventID string) {
	t.Helper()

	if got.deliveryStatus != want.deliveryStatus || got.deliveryRetryCount != want.deliveryRetryCount {
		t.Fatalf("delivery state = status:%q retry:%d, want status:%q retry:%d", got.deliveryStatus, got.deliveryRetryCount, want.deliveryStatus, want.deliveryRetryCount)
	}

	assertPendingContainsEvent(t, "direct pending", got.directPendingIDs, eventID, want.directPending)
	assertPendingContainsEvent(t, "subscribed pending", got.subscribedPending, eventID, want.subscribedPending)

	if got.receiptFound != want.receiptFound {
		t.Fatalf("receiptFound = %v, want %v (receipt=%+v)", got.receiptFound, want.receiptFound, got.receipt)
	}
	if want.receiptFound {
		if got.receipt.Status != want.receiptStatus {
			t.Fatalf("receipt status = %q, want %q", got.receipt.Status, want.receiptStatus)
		}
		if got.receipt.RetryCount != want.receiptRetryCount {
			t.Fatalf("receipt retry_count = %d, want %d", got.receipt.RetryCount, want.receiptRetryCount)
		}
	}

	assertPendingContainsEvent(t, "recovered events", got.recoveredEventIDs, eventID, want.recoveryReplays)
}

func assertPendingContainsEvent(t *testing.T, label string, ids []string, eventID string, want bool) {
	t.Helper()
	has := slices.Contains(ids, eventID)
	if has != want {
		t.Fatalf("%s contains %s = %v, want %v (ids=%v)", label, eventID, has, want, ids)
	}
	if want && countMatches(ids, eventID) != 1 {
		t.Fatalf("%s contains %s %d times, want 1 (ids=%v)", label, eventID, countMatches(ids, eventID), ids)
	}
}

func countMatches(ids []string, want string) int {
	count := 0
	for _, id := range ids {
		if strings.TrimSpace(id) == strings.TrimSpace(want) {
			count++
		}
	}
	return count
}

func (fx *deliveryLifecycleFixture) String() string {
	return fmt.Sprintf("agent=%s subscription=%s", fx.agentID, fx.subscription)
}
