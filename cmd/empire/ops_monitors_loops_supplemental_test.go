package main

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPortfolioDigestLoop_EmitsCompiledEvent(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	// Seed one active vertical so digest has content.
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	bus := runtime.NewEventBus(pg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go portfolioDigestLoop(ctx, bus, pg, pg)
	time.Sleep(50 * time.Millisecond) // allow subscriptions to attach

	// Trigger compile via timer event.
	_ = bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("timer.portfolio_digest"),
		SourceAgent: "empire-coordinator",
		Payload:     []byte(`{"trigger":"test"}`),
		CreatedAt:   time.Now(),
	})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		_ = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE type='portfolio.digest_compiled'`).Scan(&n)
		if n > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("expected portfolio.digest_compiled event")
}

func TestVerticalHealthMonitorLoop_EmitsWarningAndSteadyState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	verticalID := uuid.NewString()
	launchedAt := time.Now().Add(-11 * 7 * 24 * time.Hour).UTC()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, launched_at, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', $2, now(), now())
	`, verticalID, launchedAt); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	// verticalHealthMonitorLoop subscribes as an "agent" on the event bus; opco.* events
	// persist deliveries (FK -> agents.id), so the subscriber must exist.
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('vertical-health-monitor', 'stub', 'vertical-health-monitor', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed monitor agent: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO vertical_metrics (vertical_id, period_start, period_end, users_total, users_new, users_churned, mrr_cents, created_at)
		VALUES ($1::uuid, $2::date, $3::date, 1, 0, 0, 100, now())
	`, verticalID, time.Now().Add(-48*time.Hour), time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}

	bus := runtime.NewEventBus(pg)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go verticalHealthMonitorLoop(ctx, bus, db, pg)
	time.Sleep(50 * time.Millisecond) // let subscription attach

	// Publish report with vertical id in payload (evt.VerticalID empty) to cover payload extraction.
	_ = bus.Publish(context.Background(), (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.ceo_report"),
		SourceAgent: "opco-ceo-" + verticalID,
		Payload:     []byte(`{"vertical_id":"` + verticalID + `"}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(verticalID))

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var warn int
		_ = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE type='vertical.health_warning' AND payload->>'vertical_id'=$1`, verticalID).Scan(&warn)
		var steady int
		_ = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE type='opco.steady_state_reached' AND vertical_id=$1::uuid`, verticalID).Scan(&steady)
		var mb int
		_ = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM mailbox WHERE type='escalation' AND vertical_id=$1::uuid`, verticalID).Scan(&mb)
		if warn > 0 && steady > 0 && mb > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("expected health warning + steady_state + mailbox item")
}
