package main

import (
	"context"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestOpsMonitors_Loops_PortfolioDigest_And_HealthMonitor(t *testing.T) {
	dsn, _, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	cfgPath := writeTempConfig(t, dsn)
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("load cfg: %v", err)
	}

	stores := buildStores(context.Background(), "postgres", cfg, false, "migrations/001_initial.sql")
	if stores.SQLDB == nil || stores.EventStore == nil || stores.DigestStore == nil || stores.MailboxStore == nil {
		t.Fatalf("expected postgres stores")
	}
	db := stores.SQLDB
	bus := runtime.NewEventBus(stores.EventStore)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, launched_at, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','operating','operating', now() - interval '80 day', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO vertical_metrics (id, vertical_id, period_start, period_end, users_total, users_churned, mrr_cents, csat_avg, created_at)
		VALUES ($1::uuid,$2::uuid, now() - interval '1 day', now(), 2, 1, 100, 2.0, now())
	`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
			INSERT INTO spend_ledger (id, vertical_id, category, amount_cents, created_at)
			VALUES ($1::uuid,$2::uuid,'api',10000, now())
		`, uuid.NewString(), verticalID); err != nil {
		t.Fatalf("seed spend: %v", err)
	}
	// The opco.* publish path persists deliveries with an FK to agents(id).
	// Any subscriber to opco.* must exist in agents, otherwise Publish() fails
	// before in-memory delivery.
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES
		  ('portfolio-digest-manager', 'system', 'monitor', 'holding', 'active', '{}'::jsonb),
		  ('vertical-health-monitor', 'system', 'monitor', 'holding', 'active', '{}'::jsonb)
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed monitor agent: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Portfolio digest loop: trigger compilation.
	digestCh := bus.Subscribe("t-digest", events.EventType("portfolio.digest_compiled"))
	go portfolioDigestLoop(ctx, bus, stores.DigestStore, stores.MailboxStore)
	time.Sleep(30 * time.Millisecond)
	_ = bus.Publish(context.Background(), events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("timer.portfolio_digest"),
		SourceAgent: "tester",
		Payload:     []byte(`{"trigger":"test"}`),
		CreatedAt:   time.Now(),
	})
	select {
	case <-digestCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected portfolio.digest_compiled")
	}

	// Health monitor loop: publish a report to trigger evaluation.
	healthCh := bus.Subscribe("t-health", events.EventType("vertical.health_warning"))
	go verticalHealthMonitorLoop(ctx, bus, db, stores.MailboxStore)
	time.Sleep(30 * time.Millisecond)
	_ = bus.Publish(context.Background(), (events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.ceo_report"),
		SourceAgent: "opco-ceo-" + verticalID,
		Payload:     []byte(`{"vertical_id":"` + verticalID + `"}`),
		CreatedAt:   time.Now(),
	}).WithEntityID(verticalID))
	select {
	case <-healthCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected vertical.health_warning")
	}
}
