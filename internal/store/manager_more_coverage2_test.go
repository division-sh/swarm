package store

import (
	"context"
	"testing"
	"time"

	"empireai/internal/runtime"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestManagerStore_EventReceiptBranches(t *testing.T) {
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
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('a1', 'stub', 'a1', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'system.started', 'runtime', $2::uuid, '{}'::jsonb, now())
	`, eventID, verticalID); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	// Guardrail: empty ids no-op.
	if err := pg.UpsertEventReceipt(ctx, "", "a1", "processed", ""); err != nil {
		t.Fatalf("UpsertEventReceipt empty event: %v", err)
	}

	// Default status = processed.
	if err := pg.UpsertEventReceipt(ctx, eventID, "a1", "", ""); err != nil {
		t.Fatalf("UpsertEventReceipt default: %v", err)
	}
	r, ok, err := pg.GetEventReceipt(ctx, eventID, "a1")
	if err != nil || !ok || r.Status != "processed" {
		t.Fatalf("GetEventReceipt got ok=%v err=%v rec=%+v", ok, err, r)
	}

	// Missing args validation.
	if _, _, err := pg.GetEventReceipt(ctx, "", "a1"); err == nil {
		t.Fatalf("expected required args error")
	}
	if _, _, err := pg.GetEventReceipt(ctx, eventID, ""); err == nil {
		t.Fatalf("expected required args error")
	}

	// Not found returns ok=false.
	if _, ok, err := pg.GetEventReceipt(ctx, uuid.NewString(), "a1"); err != nil || ok {
		t.Fatalf("expected not found ok=false err=%v", err)
	}
}

func TestManagerStore_LoadRoutingRules_AndDeactivateValidation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('sub', 'stub', 'sub', 'holding', 'active', '{}'::jsonb, now(), now()),
		       ('inst', 'stub', 'inst', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agents: %v", err)
	}
	// One active and one deactivated.
	if err := pg.UpsertRoutingRule(ctx, runtime.PersistedRoutingRule{
		VerticalID:   verticalID,
		EventPattern: "x.*",
		SubscriberID: "sub",
		InstalledBy:  "inst",
		Status:       "active",
		Source:       "discovered",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule: %v", err)
	}
	if err := pg.UpsertRoutingRule(ctx, runtime.PersistedRoutingRule{
		VerticalID:   verticalID,
		EventPattern: "y.*",
		SubscriberID: "sub",
		InstalledBy:  "inst",
		Status:       "deactivated",
		Source:       "discovered",
	}); err != nil {
		t.Fatalf("UpsertRoutingRule deactivated: %v", err)
	}
	rules, err := pg.LoadRoutingRules(ctx)
	if err != nil {
		t.Fatalf("LoadRoutingRules: %v", err)
	}
	if len(rules) != 1 || rules[0].EventPattern != "x.*" {
		t.Fatalf("expected only active/proposed rules, got %#v", rules)
	}
	if err := pg.DeactivateRoutingRulesByVertical(ctx, ""); err == nil {
		t.Fatalf("expected vertical_id required")
	}

	// MarkAgentTerminated validation.
	if err := pg.MarkAgentTerminated(ctx, " "); err == nil {
		t.Fatalf("expected agent_id required")
	}

	// Schedule cancel/load error branches aren't easy to force; cover a normal cancel.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO schedules (id, agent_id, event_type, mode, next_fire_at, created_at)
		VALUES ($1::uuid, 'sub', 'timer.portfolio_digest', 'cron', now(), now())
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed schedule: %v", err)
	}
	if err := pg.CancelSchedule(ctx, "sub", "timer.portfolio_digest"); err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
	_ = time.Second
}

