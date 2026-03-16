package store

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	runtimeactors "empireai/internal/runtime/core/actors"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Manager_ErrorBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}
	ctx := context.Background()

	// UpsertAgent: missing id.
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{Config: runtimeactors.AgentConfig{}}); err == nil {
		t.Fatal("expected missing agent id error")
	}

	// EnsureEntitySchema: invalid/missing slug.
	vid := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('entity-no-slug', 'test', 'static', '{}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, 'entity-no-slug', 'default', 'operating',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, vid); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
	if err := pg.EnsureEntitySchema(ctx, vid); err == nil {
		t.Fatal("expected EnsureEntitySchema to fail for empty slug")
	}
	if err := pg.EnsureEntitySchema(ctx, "ent-001"); err != nil {
		t.Fatalf("expected EnsureEntitySchema to ignore non-UUID entity ids: %v", err)
	}
	if err := pg.EnsureEntitySchema(ctx, ""); err == nil {
		t.Fatal("expected EnsureEntitySchema to require entity_id")
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
		Config: runtimeactors.AgentConfig{ID: aid, Role: "r", Mode: "global", Type: "stub", Config: []byte(`{"subscriptions":["*"]}`)},
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
