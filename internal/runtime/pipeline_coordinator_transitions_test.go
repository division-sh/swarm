package runtime_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

func TestFactoryPipelineCoordinator_RecordsConsumedTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS pipeline_transitions (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			event_id UUID NOT NULL REFERENCES events(id),
			event_type TEXT NOT NULL,
			handler TEXT NOT NULL,
			pipeline_type TEXT NOT NULL,
			pipeline_id UUID NOT NULL,
			action TEXT NOT NULL,
			state_before JSONB,
			state_after JSONB,
			events_emitted TEXT[],
			drop_reason TEXT,
			error TEXT,
			duration_us INT,
			created_at TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create pipeline_transitions: %v", err)
	}

	pg := &store.PostgresStore{DB: db}
	bus := runtime.NewEventBus(pg)
	pc := runtime.NewFactoryPipelineCoordinator(bus, db)
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS shards (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			root_task_id UUID NOT NULL,
			scan_id UUID,
			stage TEXT NOT NULL,
			shard_index INT NOT NULL,
			shard_count INT NOT NULL,
			shard_key TEXT NOT NULL,
			scope JSONB NOT NULL,
			agent_id TEXT,
			status TEXT NOT NULL DEFAULT 'pending',
			deadline_at TIMESTAMPTZ NOT NULL,
			budget_cents INT NOT NULL,
			spend_cents INT NOT NULL DEFAULT 0,
			retry_count INT NOT NULL DEFAULT 0,
			error TEXT,
			assigned_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE UNIQUE INDEX IF NOT EXISTS idx_shards_idempotent ON shards(root_task_id, shard_key);
	`); err != nil {
		t.Fatalf("create shards table: %v", err)
	}
	cfg := &config.Config{}
	cfg.LLM.RuntimeMode = "api"
	cfg.LLM.Session.LockTTL = time.Second
	cfg.LLM.Session.RotateAfterTurns = 1
	cfg.LLM.Session.RotateOnParseFailures = 1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("config validate: %v", err)
	}
	pc.SetShardPlanner(runtime.NewShardPlanner(cfg.Sharding))
	bus.SetInterceptors(pc)

	scanID := uuid.NewString()
	campaignID := uuid.NewString()
	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "empire-coordinator",
		Payload: mustJSON(t, map[string]any{
			"scan_id":     scanID,
			"campaign_id": campaignID,
			"mode":        "saas_gap",
			"geography":   "Asuncion, Paraguay",
		}),
		CreatedAt: time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var action, pipelineType, pipelineID string
	var emitted []string
	if err := db.QueryRowContext(context.Background(), `
		SELECT action, pipeline_type, pipeline_id::text, COALESCE(events_emitted, ARRAY[]::text[])
		FROM pipeline_transitions
		WHERE event_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT 1
	`, evt.ID).Scan(&action, &pipelineType, &pipelineID, pq.Array(&emitted)); err != nil {
		t.Fatalf("query transition: %v", err)
	}
	if action != "consumed" {
		t.Fatalf("expected action=consumed, got %s", action)
	}
	if pipelineType != "campaign" {
		t.Fatalf("expected pipeline_type=campaign, got %s", pipelineType)
	}
	if pipelineID != campaignID {
		t.Fatalf("expected pipeline_id=%s, got %s", campaignID, pipelineID)
	}
	for _, e := range emitted {
		if strings.TrimSpace(e) == "market_research.scan_assigned" {
			t.Fatalf("did not expect inline market_research.scan_assigned in sharded mode, got %#v", emitted)
		}
	}
	var shardCount int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM shards WHERE root_task_id = $1::uuid`, evt.ID).Scan(&shardCount); err != nil {
		t.Fatalf("query shards: %v", err)
	}
	if shardCount != 4 {
		t.Fatalf("expected 4 persisted shards, got %d", shardCount)
	}
}

func TestFactoryPipelineCoordinator_RecordsDroppedTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS pipeline_transitions (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			event_id UUID NOT NULL REFERENCES events(id),
			event_type TEXT NOT NULL,
			handler TEXT NOT NULL,
			pipeline_type TEXT NOT NULL,
			pipeline_id UUID NOT NULL,
			action TEXT NOT NULL,
			state_before JSONB,
			state_after JSONB,
			events_emitted TEXT[],
			drop_reason TEXT,
			error TEXT,
			duration_us INT,
			created_at TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create pipeline_transitions: %v", err)
	}

	pg := &store.PostgresStore{DB: db}
	bus := runtime.NewEventBus(pg)
	pc := runtime.NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.validation_passed"),
		SourceAgent: "spec-auditor",
		Payload:     mustJSON(t, map[string]any{"status": "pass"}),
		CreatedAt:   time.Now(),
	}
	if err := bus.Publish(context.Background(), evt); err != nil {
		t.Fatalf("publish: %v", err)
	}

	var action, dropReason string
	if err := db.QueryRowContext(context.Background(), `
		SELECT action, COALESCE(drop_reason, '')
		FROM pipeline_transitions
		WHERE event_id = $1::uuid
		ORDER BY created_at DESC
		LIMIT 1
	`, evt.ID).Scan(&action, &dropReason); err != nil {
		t.Fatalf("query transition: %v", err)
	}
	if action != "dropped" {
		t.Fatalf("expected action=dropped, got %s", action)
	}
	if dropReason != "missing vertical_id" {
		t.Fatalf("expected drop_reason=missing vertical_id, got %q", dropReason)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return b
}
