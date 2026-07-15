package store

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresStore_Manager_ErrorBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	const runID = "44444444-4444-4444-4444-444444444444"
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)

	// UpsertAgent: missing id.
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{Config: runtimeactors.AgentConfig{ExecutionMode: "live"}}); err == nil {
		t.Fatal("expected missing agent id error")
	}

	// EnsureEntitySchema: invalid/missing slug.
	vid := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at)
		VALUES ('entity-no-slug', 'test', 'static', '{}'::jsonb, 'active', now())
	`); err != nil {
		t.Fatalf("seed flow instance: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'entity-no-slug', 'default', 'operating',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, runID, vid); err != nil {
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

	// Canonical lifecycle transition: validation.
	if _, err := pg.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{}); err == nil {
		t.Fatal("expected lifecycle transition validation error")
	}

	// Routing rule required fields.
	if err := pg.UpsertRoutingRule(ctx, runtimemanager.PersistedRoutingRule{}); err == nil {
		t.Fatal("expected UpsertRoutingRule validation error")
	}

	// UpsertEventReceipt should accept empty errText; also exercise invalid status guardrails indirectly.
	aid := "a1"
	_ = pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: aid, Role: "r", FlowID: "global", Type: "stub", Model: "regular", Config: []byte(`{"subscriptions":["*"]}`)},
		Status: "active", HiredBy: "t", StartedAt: time.Now(),
	})
	evtID := uuid.NewString()
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(evtID,
		"test.event",
		"tester", "", []byte(`{}`), 0, "", "", events.EventEnvelope{}, time.Now())); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := pg.InsertEventDeliveries(ctx, evtID, []string{aid}); err != nil {
		t.Fatalf("InsertEventDeliveries: %v", err)
	}
	if err := pg.UpsertEventReceipt(ctx, evtID, aid, "processed", nil); err != nil {
		t.Fatalf("UpsertEventReceipt: %v", err)
	}
}
