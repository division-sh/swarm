package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestSubscriptionMatchPatterns(t *testing.T) {
	if !subscriptionMatch("", "x.y") {
		t.Fatalf("empty should match all")
	}
	if !subscriptionMatch("*", "x.y") {
		t.Fatalf("* should match all")
	}
	if !subscriptionMatch("inbound.*", "inbound.a") {
		t.Fatalf("prefix star should match")
	}
	if subscriptionMatch("inbound.*", "board.chat") {
		t.Fatalf("prefix star should not match other prefix")
	}
	if !subscriptionMatch("board.chat", "board.chat") {
		t.Fatalf("exact should match")
	}
	if subscriptionMatch("board.chat", "board.chats") {
		t.Fatalf("exact should not match different")
	}
	if !matchesAnySubscription("inbound.a", []events.EventType{"board.*", "inbound.*"}) {
		t.Fatalf("matchesAnySubscription expected true")
	}
}

func TestSchedules_UpsertLoadCancelAndMarkFired(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	// Seed minimal agent for FK.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('a1', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	once := runtime.Schedule{
		AgentID:   "a1",
		EventType: "system.directive",
		Mode:      "once",
		At:        time.Now().Add(1 * time.Hour).UTC(),
		Payload:   []byte(`{"x":1}`),
	}
	if err := pg.UpsertSchedule(ctx, once); err != nil {
		t.Fatalf("upsert once: %v", err)
	}
	active, err := pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules: %v", err)
	}
	if len(active) == 0 {
		t.Fatalf("expected active schedule")
	}

	// Mark once schedule fired -> becomes inactive.
	if err := pg.MarkScheduleFired(ctx, once); err != nil {
		t.Fatalf("mark fired: %v", err)
	}
	active, err = pg.LoadActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("load schedules after fired: %v", err)
	}
	for _, sc := range active {
		if sc.AgentID == "a1" && sc.EventType == "system.directive" && sc.Mode == "once" {
			t.Fatalf("expected once schedule to deactivate after fired")
		}
	}

	recurring := runtime.Schedule{
		AgentID:   "a1",
		EventType: "system.started",
		Mode:      "cron",
		Cron:      "0 9 * * *",
		Payload:   nil, // ensures payload default branch
	}
	if err := pg.UpsertSchedule(ctx, recurring); err != nil {
		t.Fatalf("upsert recurring: %v", err)
	}
	if err := pg.MarkScheduleFired(ctx, recurring); err != nil {
		t.Fatalf("mark recurring fired: %v", err)
	}
	// Cancel should deactivate it.
	if err := pg.CancelSchedule(ctx, "a1", "system.started"); err != nil {
		t.Fatalf("cancel schedule: %v", err)
	}
}

func TestEventReceipts_RetryToDeadLetter_AndPendingQueries(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES ('a1', 'stub', 'empire-coordinator', 'holding', NULL, 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'inbound.test', 'inbound', $2::uuid, '{}'::jsonb, now())
	`, eventID, verticalID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, 'a1', now())
		ON CONFLICT DO NOTHING
	`, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	// No receipt -> should be pending.
	pending, err := pg.ListPendingEventsForAgent(ctx, "a1", time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 pending, got %d", len(pending))
	}

	// Error receipts should increase retry count and eventually dead-letter.
	for i := 0; i < 4; i++ {
		if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "error", "boom"); err != nil {
			t.Fatalf("upsert receipt: %v", err)
		}
	}
	r, ok, err := pg.GetEventReceipt(ctx, eventID, "a1")
	if err != nil || !ok {
		t.Fatalf("GetEventReceipt ok=%v err=%v", ok, err)
	}
	if strings.TrimSpace(r.Status) != "dead_letter" {
		t.Fatalf("expected dead_letter, got %q retry=%d", r.Status, r.RetryCount)
	}

	// Subscribed pending events: receipt is dead_letter, should not be returned.
	subscribed, err := pg.ListPendingSubscribedEvents(ctx, "a1", []events.EventType{"inbound.*"}, time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	if len(subscribed) != 0 {
		t.Fatalf("expected no subscribed pending events after dead_letter, got %d", len(subscribed))
	}

	// Mark processed -> still not pending.
	if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "processed", ""); err != nil {
		t.Fatalf("upsert processed: %v", err)
	}
	subscribed, err = pg.ListPendingSubscribedEvents(ctx, "a1", []events.EventType{"inbound.*"}, time.Now().Add(-1*time.Hour), 10)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents processed: %v", err)
	}
	if len(subscribed) != 0 {
		t.Fatalf("expected no pending after processed, got %d", len(subscribed))
	}
}

func TestListPendingSubscribedEvents_RespectsDirectDeliveryScope(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES
			('a1', 'stub', 'market-research-agent', 'factory', 'active', '{}'::jsonb, now(), now()),
			('a2', 'stub', 'market-research-agent', 'factory', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	broadcastID := uuid.NewString()
	directOtherID := uuid.NewString()
	directSelfID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES
			($1::uuid, 'inbound.alert', 'runtime', '{}'::jsonb, now() - interval '3 minute'),
			($2::uuid, 'inbound.alert', 'runtime', '{}'::jsonb, now() - interval '2 minute'),
			($3::uuid, 'inbound.alert', 'runtime', '{}'::jsonb, now() - interval '1 minute')
	`, broadcastID, directOtherID, directSelfID); err != nil {
		t.Fatalf("seed events: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES
			($1::uuid, 'a2', now()),
			($2::uuid, 'a1', now())
	`, directOtherID, directSelfID); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}

	got, err := pg.ListPendingSubscribedEvents(
		ctx,
		"a1",
		[]events.EventType{"inbound.*"},
		time.Now().Add(-1*time.Hour),
		20,
	)
	if err != nil {
		t.Fatalf("ListPendingSubscribedEvents: %v", err)
	}
	gotSet := map[string]struct{}{}
	for _, evt := range got {
		gotSet[strings.TrimSpace(evt.ID)] = struct{}{}
	}
	if _, ok := gotSet[broadcastID]; !ok {
		t.Fatalf("expected broadcast event %s in subscribed backlog, got=%v", broadcastID, gotSet)
	}
	if _, ok := gotSet[directSelfID]; !ok {
		t.Fatalf("expected direct-self event %s in subscribed backlog, got=%v", directSelfID, gotSet)
	}
	if _, ok := gotSet[directOtherID]; ok {
		t.Fatalf("did not expect direct-other event %s in subscribed backlog, got=%v", directOtherID, gotSet)
	}
}
