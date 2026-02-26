package runtime

import (
	"context"
	"testing"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestFactoryPipelineCoordinator_ShardsHelpersAndAsFloat(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	if _, err := db.ExecContext(ctx, `
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
		)
	`); err != nil {
		t.Fatalf("create shards table: %v", err)
	}

	scanID := "scan-runtime-helper"
	scanUUID := stableUUID(scanID).String()
	shardID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, deadline_at, budget_cents, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 1, 'finance',
			'{}'::jsonb, 'agent-a', 'assigned', now() + interval '15 minute', 100, now()
		)
	`, shardID, uuid.NewString(), scanUUID); err != nil {
		t.Fatalf("seed shard: %v", err)
	}

	total, completed, failed, ok := pc.shardTerminalProgress(ctx, scanID)
	if !ok || total != 1 || completed != 0 || failed != 0 {
		t.Fatalf("unexpected shard progress before completion: total=%d completed=%d failed=%d ok=%v", total, completed, failed, ok)
	}

	if got := pc.markShardCompletedByAgent(ctx, "agent-a"); got == "" {
		t.Fatal("expected markShardCompletedByAgent to return completed shard id")
	}
	if got := pc.markShardCompletedByAgent(ctx, "agent-a"); got != "" {
		t.Fatalf("expected no second completion id after terminal update, got %q", got)
	}

	total, completed, failed, ok = pc.shardTerminalProgress(ctx, scanID)
	if !ok || total != 1 || completed != 1 || failed != 0 {
		t.Fatalf("unexpected shard progress after completion: total=%d completed=%d failed=%d ok=%v", total, completed, failed, ok)
	}

	if got := asFloat("12.5"); got != 12.5 {
		t.Fatalf("asFloat string parse mismatch: %v", got)
	}
	if got := asFloat(7); got != 7 {
		t.Fatalf("asFloat int parse mismatch: %v", got)
	}
	if got := asFloat(nil); got != 0 {
		t.Fatalf("asFloat nil should be zero, got %v", got)
	}
}

func TestFactoryPipelineCoordinator_InterceptPolicyAndRunMaintenance(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)

	if consume, handled := pc.interceptPolicy("scan.requested", events.Event{ID: uuid.NewString()}); !consume || !handled {
		t.Fatalf("scan.requested should be intercepted and consumed; got consume=%v handled=%v", consume, handled)
	}
	if consume, handled := pc.interceptPolicy("vertical.shortlisted", events.Event{ID: uuid.NewString(), VerticalID: uuid.NewString()}); !consume || !handled {
		t.Fatalf("vertical.shortlisted should be intercepted and consumed; got consume=%v handled=%v", consume, handled)
	}
	if consume, handled := pc.interceptPolicy("spec.validation_passed", events.Event{ID: uuid.NewString()}); consume || handled {
		t.Fatalf("spec.validation_passed without vertical should not be handled; got consume=%v handled=%v", consume, handled)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pc.RunMaintenance(ctx)
}
