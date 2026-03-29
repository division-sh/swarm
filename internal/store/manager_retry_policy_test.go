package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
	runtimemanager "swarm/internal/runtime/manager"
	"swarm/internal/store"
	"swarm/internal/testutil"
)

func TestUpsertEventReceipt_DeadLettersAfterOneRetry_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.retry_upsert")

	for i := 1; i <= 2; i++ {
		if err := pg.UpsertEventReceipt(ctx, evt.ID, agentID, "error", "boom"); err != nil {
			t.Fatalf("upsert receipt error #%d: %v", i, err)
		}

		var status string
		var retryCount int
		if err := pg.DB.QueryRowContext(ctx, `
			SELECT COALESCE(side_effects->>'manager_status', ''), COALESCE((side_effects->>'retry_count')::int, 0)
			FROM event_receipts
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'agent'
			  AND subscriber_id = $2
		`, evt.ID, agentID).Scan(&status, &retryCount); err != nil {
			t.Fatalf("query receipt after #%d: %v", i, err)
		}

		wantStatus := "error"
		if i == 2 {
			wantStatus = "dead_letter"
		}
		if status != wantStatus || retryCount != i {
			t.Fatalf("after %d errors: got status=%q retry_count=%d, want status=%q retry_count=%d", i, status, retryCount, wantStatus, i)
		}
	}
}

func TestListPendingEventsForAgent_RetryBackoff_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.pending_direct")
	if err := pg.InsertEventDeliveries(ctx, evt.ID, []string{agentID}); err != nil {
		t.Fatalf("insert deliveries: %v", err)
	}

	since := time.Now().Add(-2 * time.Hour)

	// No receipt: should be immediately pending.
	evts, err := pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (no receipt): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list pending (no receipt): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 1, time.Now().Add(-30*time.Second))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=1 not ready): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=1 not ready): got %d events, want 0", len(evts))
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 1, time.Now().Add(-2*time.Minute))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=1 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list pending (retry=1 ready): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	// After retries are exhausted, the event should not be pending.
	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 2, time.Now().Add(-2*time.Hour))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (retry=2 exhausted): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (retry=2 exhausted): got %d events, want 0", len(evts))
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "dead_letter", 2, time.Now().Add(-2*time.Hour))
	evts, err = pg.ListPendingEventsForAgent(ctx, agentID, since, 100)
	if err != nil {
		t.Fatalf("list pending (dead_letter): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list pending (dead_letter): got %d events, want 0", len(evts))
	}
}

func TestListPendingSubscribedEvents_RetryBackoff_V2(t *testing.T) {
	pg, cleanup := newTestPostgresStore(t)
	defer cleanup()

	ctx := context.Background()
	entityID, agentID := seedEntityAndAgent(t, ctx, pg)
	evt := seedEvent(t, ctx, pg, entityID, "test.pending_subscribed")

	since := time.Now().Add(-2 * time.Hour)
	subs := []events.EventType{evt.Type}

	evts, err := pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (no receipt): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list subscribed pending (no receipt): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "error", 1, time.Now().Add(-2*time.Minute))
	evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (retry=1 ready): %v", err)
	}
	if len(evts) != 1 || evts[0].ID != evt.ID {
		t.Fatalf("list subscribed pending (retry=1 ready): got %v events, want 1 (%s)", len(evts), evt.ID)
	}

	insertOrUpdateReceipt(t, ctx, pg, evt.ID, agentID, "dead_letter", 2, time.Now().Add(-2*time.Hour))
	evts, err = pg.ListPendingSubscribedEvents(ctx, agentID, subs, since, 100)
	if err != nil {
		t.Fatalf("list subscribed pending (dead_letter): %v", err)
	}
	if len(evts) != 0 {
		t.Fatalf("list subscribed pending (dead_letter): got %d events, want 0", len(evts))
	}
}

func insertOrUpdateReceipt(t *testing.T, ctx context.Context, pg *store.PostgresStore, eventID, agentID, status string, retryCount int, processedAt time.Time) {
	t.Helper()
	// Upsert-style helper for tests; the production upsert also mutates retry_count which isn't what we want
	// for time-window filtering tests.
	const q = `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, side_effects, processed_at
		)
		SELECT
			e.event_id, 'agent', $2, e.entity_id, e.flow_instance, $3, $4::jsonb, $5
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			processed_at = EXCLUDED.processed_at,
			outcome = EXCLUDED.outcome,
			side_effects = EXCLUDED.side_effects
	`
	sideEffects, err := json.Marshal(map[string]any{
		"manager_status": status,
		"retry_count":    retryCount,
		"error":          "boom",
	})
	if err != nil {
		t.Fatalf("marshal side effects: %v", err)
	}
	outcome := "success"
	switch status {
	case "error", "dead_letter":
		outcome = status
	}
	if outcome == "error" {
		outcome = "dead_letter"
	}
	if _, err := pg.DB.ExecContext(ctx, q, eventID, agentID, outcome, string(sideEffects), processedAt); err != nil {
		t.Fatalf("upsert receipt: %v", err)
	}
}

func newTestPostgresStore(t *testing.T) (*store.PostgresStore, func()) {
	t.Helper()
	dsn, _, cleanup := testutil.StartPostgres(t)
	appDSN := dsn
	pg, err := store.NewPostgresStore(appDSN)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := pg.Ping(context.Background()); err != nil {
		_ = pg.DB.Close()
		t.Fatalf("ping app db: %v", err)
	}
	return pg, func() {
		_ = pg.DB.Close()
		cleanup()
	}
}

func seedEntityAndAgent(t *testing.T, ctx context.Context, pg *store.PostgresStore) (entityID, agentID string) {
	t.Helper()

	entityID = uuid.NewString()
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('retry-policy-entity', 'test', 'static', '{"instance_kind":"entity","workflow_version":"v1"}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := pg.DB.ExecContext(ctx, `
		INSERT INTO entity_state (
			entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		)
		VALUES ($1::uuid, 'retry-policy-entity', 'default', 'retry-policy', 'Store Retry Policy Test', 'approved',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now())
	`, entityID); err != nil {
		t.Fatalf("seed entity: %v", err)
	}

	agentID = "agent-" + uuid.NewString()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:       agentID,
			Type:     "test",
			Role:     "test",
			Mode:     "worker",
			EntityID: entityID,
			Config:   []byte(`{"system_prompt":"x"}`),
		},
		Status:    "active",
		HiredBy:   "test",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	return entityID, agentID
}

func seedEvent(t *testing.T, ctx context.Context, pg *store.PostgresStore, entityID, eventType string) events.Event {
	t.Helper()

	payload, _ := json.Marshal(map[string]any{"k": "v"})
	evt := (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType(eventType),
		SourceAgent: "store-test",
		Payload:     payload,
		CreatedAt:   time.Now().Add(-1 * time.Hour),
	}).WithEntityID(entityID)
	if err := pg.AppendEvent(ctx, evt); err != nil {
		t.Fatalf("append event: %v", err)
	}
	return evt
}
