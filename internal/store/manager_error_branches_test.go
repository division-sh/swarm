package store

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Manager_ErrorBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	// UpsertAgent: missing id.
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{Config: models.AgentConfig{}}); err == nil {
		t.Fatal("expected missing agent id error")
	}

	// EnsureVerticalSchema: invalid/missing slug.
	vid := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'Bad','', 'us','operating','operating', now(), now())
	`, vid); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if err := pg.EnsureVerticalSchema(ctx, vid); err == nil {
		t.Fatal("expected EnsureVerticalSchema to fail for empty slug")
	}
	if err := pg.EnsureVerticalSchema(ctx, ""); err == nil {
		t.Fatal("expected EnsureVerticalSchema to require vertical_id")
	}

	// LoadLatestOrgTemplate: no rows should error.
	if _, err := pg.LoadLatestOrgTemplate(ctx); err == nil {
		t.Fatal("expected LoadLatestOrgTemplate to error without templates")
	}

	// SetVerticalTemplateVersion: validation.
	if err := pg.SetVerticalTemplateVersion(ctx, "", "v1"); err == nil {
		t.Fatal("expected SetVerticalTemplateVersion to require vertical_id")
	}
	if err := pg.SetVerticalTemplateVersion(ctx, vid, ""); err == nil {
		t.Fatal("expected SetVerticalTemplateVersion to require version")
	}

	// MarkAgentTerminated: validation.
	if err := pg.MarkAgentTerminated(ctx, ""); err == nil {
		t.Fatal("expected MarkAgentTerminated validation error")
	}

	// Routing rule required fields.
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{}); err == nil {
		t.Fatal("expected UpsertRoutingRule validation error")
	}

	// UpsertEventReceipt should accept empty errText; also exercise invalid status guardrails indirectly.
	aid := "a1"
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: models.AgentConfig{ID: aid, Role: "r", Mode: "holding", Type: "stub", Config: []byte(`{"subscriptions":["*"]}`)},
		Status: "active", HiredBy: "t", StartedAt: time.Now(),
	})
	evtID := uuid.NewString()
	if err := pg.AppendEvent(ctx, events.Event{
		ID:          evtID,
		Type:        "test.event",
		SourceAgent: "tester",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evtID, []string{aid}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, evtID, aid, "processed", ""); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
}
