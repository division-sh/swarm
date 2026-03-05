package main

import (
	"context"
	"testing"
	"time"

	"empireai/internal/mailbox"
	"empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestEmitMailboxDecisionSideEffects_Branches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	// Recipients must exist for delivery insertion.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES
			('empire-coordinator', 'stub', 'empire-coordinator', 'holding', NULL, 'active', '{}'::jsonb, now(), now()),
			($1, 'stub', 'opco-ceo', 'operating', $2::uuid, 'active', '{}'::jsonb, now(), now()),
			('holding-devops', 'stub', 'holding-devops', 'holding', NULL, 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`, "opco-ceo-"+verticalID, verticalID); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	stores := storeBundle{SQLDB: db, EventStore: pg, ScanCampaignStore: pg}

	// more_data -> vertical.needs_more_data targeted to empire-coordinator.
	if err := emitMailboxDecisionSideEffects(ctx, stores, runtime.MailboxItem{
		ID:         uuid.NewString(),
		VerticalID: verticalID,
		Type:       "product_spec_review",
		Context:    []byte(`{"a":1}`),
	}, mailbox.DecisionOutcome{Status: "more_data", Decision: "more_data"}, "need more"); err != nil {
		t.Fatalf("emit more_data: %v", err)
	}

	// spend approved -> spend.approved targeted to opco-ceo.
	if err := emitMailboxDecisionSideEffects(ctx, stores, runtime.MailboxItem{
		ID:         uuid.NewString(),
		VerticalID: verticalID,
		Type:       "budget_increase",
		Context:    []byte(`{}`),
	}, mailbox.DecisionOutcome{Status: "approved", Decision: "approve"}, "ok"); err != nil {
		t.Fatalf("emit spend approved: %v", err)
	}

	// spend rejected with holding FromAgent routes back to requester.
	if err := emitMailboxDecisionSideEffects(ctx, stores, runtime.MailboxItem{
		ID:        uuid.NewString(),
		Type:      "devops.capacity_warning",
		FromAgent: "holding-devops",
		Context:   []byte(`{}`),
	}, mailbox.DecisionOutcome{Status: "rejected", Decision: "reject"}, "no"); err != nil {
		t.Fatalf("emit spend rejected holding: %v", err)
	}

	// founder_input.response -> opco-ceo recipient.
	if err := emitMailboxDecisionSideEffects(ctx, stores, runtime.MailboxItem{
		ID:         uuid.NewString(),
		VerticalID: verticalID,
		Type:       "review",
		Context:    []byte(`{"review_type":"founder_input"}`),
	}, mailbox.DecisionOutcome{Status: "approved", Decision: "approve"}, "x"); err != nil {
		t.Fatalf("emit founder_input: %v", err)
	}

	// escalation response -> opco-ceo recipient.
	if err := emitMailboxDecisionSideEffects(ctx, stores, runtime.MailboxItem{
		ID:         uuid.NewString(),
		VerticalID: verticalID,
		Type:       "escalation",
		Context:    []byte(`{}`),
	}, mailbox.DecisionOutcome{Status: "approved", Decision: "approve"}, "do it"); err != nil {
		t.Fatalf("emit escalation: %v", err)
	}

	// geography expansion approval -> geography + scan campaign + coordinator event.
	if err := emitMailboxDecisionSideEffects(ctx, stores, runtime.MailboxItem{
		ID:         uuid.NewString(),
		VerticalID: verticalID,
		Type:       "domain_approval",
		Context:    []byte(`{"review_type":"geography_expansion","geography":"Cancun, Mexico","country":"MX","mode":"local_services","categories":["tourism","hospitality"],"priority":"high"}`),
	}, mailbox.DecisionOutcome{Status: "approved", Decision: "approve"}, "expand"); err != nil {
		t.Fatalf("emit geography expansion: %v", err)
	}

	// Verify at least one targeted delivery for each branch.
	wantTypes := []string{
		"vertical.needs_more_data",
		"spend.approved",
		"spend.rejected",
		"founder_input.response",
		"opco.escalation_response",
		"geography.expansion_queued",
	}
	for _, typ := range wantTypes {
		var n int
		_ = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_deliveries d
			JOIN events e ON e.id = d.event_id
			WHERE e.type = $1
		`, typ).Scan(&n)
		if n < 1 {
			t.Fatalf("expected deliveries for %s", typ)
		}
	}
	var decidedCount int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type = 'mailbox.item_decided'`).Scan(&decidedCount)
	if decidedCount < 6 {
		t.Fatalf("expected mailbox.item_decided events, got %d", decidedCount)
	}
	var geographies int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM geographies`).Scan(&geographies)
	if geographies < 1 {
		t.Fatalf("expected geography row inserted")
	}
	var campaigns int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_campaigns`).Scan(&campaigns)
	if campaigns < 1 {
		t.Fatalf("expected scan campaign queued")
	}
}

func TestBuildWorkspaceLifecycle_EnvDisables(t *testing.T) {
	// No DB -> nil.
	if got := buildWorkspaceLifecycle(context.Background(), nil); got != nil {
		t.Fatalf("expected nil workspace lifecycle when db nil")
	}
	// Disable via env.
	t.Setenv("EMPIREAI_ENABLE_DOCKER_WORKSPACES", "off")
	dsn, db, _ := testutil.StartPostgres(t)
	_ = dsn
	if got := buildWorkspaceLifecycle(context.Background(), db); got != nil {
		t.Fatalf("expected nil when docker workspaces disabled")
	}
}

func TestEmitMailboxDecisionSideEffects_VerticalApprovalRoutesToCoordinator(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v2', 'us', 'branding', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed empire-coordinator: %v", err)
	}

	stores := storeBundle{SQLDB: db, EventStore: pg}
	if err := emitMailboxDecisionSideEffects(ctx, stores, runtime.MailboxItem{
		ID:         uuid.NewString(),
		VerticalID: verticalID,
		Type:       "vertical_approval",
		Context:    []byte(`{"kit":"ready"}`),
	}, mailbox.DecisionOutcome{Status: "approved", Decision: "approve"}, "ship"); err != nil {
		t.Fatalf("approved side effects: %v", err)
	}
	if err := emitMailboxDecisionSideEffects(ctx, stores, runtime.MailboxItem{
		ID:         uuid.NewString(),
		VerticalID: verticalID,
		Type:       "vertical_approval",
		Context:    []byte(`{"kit":"nope"}`),
	}, mailbox.DecisionOutcome{Status: "rejected", Decision: "reject"}, "kill"); err != nil {
		t.Fatalf("rejected side effects: %v", err)
	}

	for _, typ := range []string{"vertical.approved", "vertical.killed"} {
		var n int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_deliveries d
			JOIN events e ON e.id = d.event_id
			WHERE e.type = $1 AND d.agent_id = 'empire-coordinator'
		`, typ).Scan(&n); err != nil {
			t.Fatalf("count %s deliveries: %v", typ, err)
		}
		if n < 1 {
			t.Fatalf("expected delivery for %s to empire-coordinator", typ)
		}
	}
}

func TestRunOperatorActions_FlagValidation(t *testing.T) {
	if err := runOperatorActions(context.Background(), storeBundle{}, operatorOptions{mailboxListCritical: true}); err == nil {
		t.Fatalf("expected flag dependency error")
	}
	// Digest requires stores.
	if err := runOperatorActions(context.Background(), storeBundle{}, operatorOptions{digestGenerate: true, digestTopN: 5}); err == nil {
		t.Fatalf("expected digest requires stores error")
	}
	// Mailbox decide requires decision.
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	mbID, _ := pg.InsertMailboxItem(context.Background(), runtime.MailboxItem{Type: "budget_increase", Status: "pending", Context: []byte(`{}`)})
	if err := runOperatorActions(context.Background(), storeBundle{SQLDB: db, MailboxStore: pg, EventStore: pg}, operatorOptions{mailboxDecideID: mbID}); err == nil {
		t.Fatalf("expected mailbox decision required error")
	}
	_ = time.Second
}
