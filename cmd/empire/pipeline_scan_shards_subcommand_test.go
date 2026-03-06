package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func ensurePipelineTransitionsTable(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS pipeline_transitions (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			event_id UUID NOT NULL,
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
	`)
	if err != nil {
		t.Fatalf("create pipeline_transitions: %v", err)
	}
}

func ensureShardsTableCLI(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, `
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
	`)
	if err != nil {
		t.Fatalf("create shards table: %v", err)
	}
}

func TestPipelineSubcommand_EndToEnd(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	cfgPath := writeTempConfig(t, dsn)

	ensurePipelineTransitionsTable(t, ctx, db)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'PipeCo', 'pipeco', 'argentina', 'discovered', 'factory', now() - interval '3 hour', now() - interval '3 hour')
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	eventID1 := uuid.NewString()
	eventID2 := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES
			($1::uuid, 'spec.validation_passed', 'spec-auditor', '{}'::jsonb, now() - interval '30 minute'),
			($2::uuid, 'vertical.shortlisted', 'pipeline-coordinator', '{}'::jsonb, now() - interval '20 minute')
	`, eventID1, eventID2); err != nil {
		t.Fatalf("seed events for transitions: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO pipeline_transitions (
			id, event_id, event_type, handler, pipeline_type, pipeline_id, action, drop_reason, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'spec.validation_passed', 'factory.interceptor', 'vertical', $3::uuid, 'dropped', 'missing vertical_id', now() - interval '30 minute'
		), (
			$4::uuid, $5::uuid, 'vertical.shortlisted', 'factory.interceptor', 'vertical', $3::uuid, 'consumed', NULL, now() - interval '20 minute'
		)
	`, uuid.NewString(), eventID1, verticalID, uuid.NewString(), eventID2); err != nil {
		t.Fatalf("seed transitions: %v", err)
	}

	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, scan_config, created_at)
		VALUES ($1::uuid, 'Argentina', 'Argentina', NULL, '{}'::jsonb, now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, priority, status, discoveries, created_at, started_at, completed_at)
		VALUES
			($1::uuid, $4::uuid, 'saas_gap', 'normal', 'completed', 2, now() - interval '2 hour', now() - interval '2 hour', now() - interval '90 minute'),
			($2::uuid, $4::uuid, 'saas_trend', 'high', 'active', 0, now() - interval '1 hour', now() - interval '45 minute', NULL),
			($3::uuid, $4::uuid, 'local_services', 'low', 'queued', 0, now() - interval '20 minute', NULL, NULL)
	`, uuid.NewString(), uuid.NewString(), uuid.NewString(), geoID); err != nil {
		t.Fatalf("seed campaigns: %v", err)
	}

	if err := runPipelineSubcommand([]string{"status", "--config", cfgPath, "--store", "postgres", "pipeco"}); err != nil {
		t.Fatalf("pipeline status: %v", err)
	}
	if err := runPipelineSubcommand([]string{"trace", "--config", cfgPath, "--store", "postgres", "--last", "20", verticalID}); err != nil {
		t.Fatalf("pipeline trace: %v", err)
	}
	if err := runPipelineSubcommand([]string{"campaigns", "--config", cfgPath, "--store", "postgres", "--limit", "10"}); err != nil {
		t.Fatalf("pipeline campaigns: %v", err)
	}
	if err := runPipelineSubcommand([]string{"stuck", "--config", cfgPath, "--store", "postgres", "--threshold", "1h"}); err != nil {
		t.Fatalf("pipeline stuck: %v", err)
	}
	if err := runPipelineSubcommand([]string{"drops", "--config", cfgPath, "--store", "postgres", "--last", "48h", "--vertical", "pipeco", "--limit", "50"}); err != nil {
		t.Fatalf("pipeline drops: %v", err)
	}

	if err := runPipelineSubcommand([]string{"stuck", "--config", cfgPath, "--store", "postgres", "--threshold", "bogus"}); err == nil {
		t.Fatal("expected invalid threshold error")
	}
	if err := runPipelineSubcommand([]string{"drops", "--config", cfgPath, "--store", "postgres", "--last", "bad"}); err == nil {
		t.Fatal("expected invalid --last error")
	}
}

func TestScanShardsSubcommands_EndToEnd(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	cfgPath := writeTempConfig(t, dsn)

	ensureShardsTableCLI(t, ctx, db)

	scanID := uuid.NewString()
	rootTaskID := uuid.NewString()
	shardID := uuid.NewString()
	scope := map[string]any{
		"scan_id":             scanID,
		"mode":                "saas_gap",
		"geography":           "argentina",
		"taxonomy_categories": []string{"finance"},
	}
	scopeRaw, _ := json.Marshal(scope)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ($1, 'market-research-agent', 'market-research-agent', 'factory', 'active', '{"system_prompt":"x"}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`, "market-research-agent-shard-0"); err != nil {
		t.Fatalf("seed shard agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, deadline_at, budget_cents, spend_cents, retry_count,
			error, assigned_at, completed_at, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 1, 'finance',
			$4::jsonb, 'market-research-agent-shard-0', 'failed', now() + interval '1 hour', 2500, 1200, 2,
			'transient timeout', now() - interval '30 minute', now() - interval '10 minute', now() - interval '40 minute'
		)
	`, shardID, rootTaskID, scanID, string(scopeRaw)); err != nil {
		t.Fatalf("seed shard: %v", err)
	}

	eventPayload1 := map[string]any{
		"scan_id":           scanID,
		"reports_count":     2,
		"high_signal_count": 1,
		"signal_strength":   73,
		"shard": map[string]any{
			"terminal": false,
		},
	}
	eventPayload2 := map[string]any{
		"scan_id":         scanID,
		"reports_count":   1,
		"signal_strength": 81,
		"shard": map[string]any{
			"terminal": false,
		},
	}
	raw1, _ := json.Marshal(eventPayload1)
	raw2, _ := json.Marshal(eventPayload2)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES
			($1::uuid, 'market_research.scan_complete', 'market-research-agent-shard-0', $2::jsonb, now() - interval '5 minute'),
			($3::uuid, 'market_research.scan_complete', 'market-research-agent-shard-0', $4::jsonb, now() - interval '2 minute')
	`, uuid.NewString(), string(raw1), uuid.NewString(), string(raw2)); err != nil {
		t.Fatalf("seed shard events: %v", err)
	}

	if err := runScanShardsSubcommand([]string{"--config", cfgPath, "--store", "postgres", scanID}); err != nil {
		t.Fatalf("scan shards list: %v", err)
	}
	if err := runScanShardSubcommand([]string{"--config", cfgPath, "--store", "postgres", shardID}); err != nil {
		t.Fatalf("scan shard detail: %v", err)
	}
	if err := runScanShardSubcommand([]string{"retry", "--config", cfgPath, "--store", "postgres", shardID}); err != nil {
		t.Fatalf("scan shard retry: %v", err)
	}
	if err := runScanShardSubcommand([]string{"cancel", "--config", cfgPath, "--store", "postgres", shardID}); err != nil {
		t.Fatalf("scan shard cancel: %v", err)
	}

	if err := runScanShardActionSubcommand("noop", []string{"--config", cfgPath, "--store", "postgres", shardID}); err == nil {
		t.Fatal("expected unsupported action error")
	}

	// No-shards path still succeeds and covers stable UUID normalization.
	if err := runScanShardsSubcommand([]string{"--config", cfgPath, "--store", "postgres", "SaaS in Argentina"}); err != nil {
		t.Fatalf("scan shards no-rows path: %v", err)
	}
}

func TestScanShardHelpers(t *testing.T) {
	valid := uuid.NewString()
	if got := stableUUIDLikeRuntime(valid); got != valid {
		t.Fatalf("expected same uuid, got %q", got)
	}
	if got := stableUUIDLikeRuntime("not-a-uuid"); got == "" {
		t.Fatal("expected deterministic hash uuid")
	}
	if nullableTime(nil) != "-" {
		t.Fatalf("expected nil time to render '-'")
	}
	now := time.Now().UTC()
	if nullableTime(&now) == "-" {
		t.Fatalf("expected concrete time rendering")
	}
	if shardCompletionEventTypeForStage("market_research") != "market_research.scan_complete" {
		t.Fatalf("unexpected stage mapping")
	}
	if shardCompletionEventTypeForStage("unknown") != "" {
		t.Fatalf("unknown stage should map to empty event type")
	}
	if asFloatAny("12.5") != 12.5 {
		t.Fatalf("string parse failed")
	}
	if asFloatAny(7) != 7 {
		t.Fatalf("int parse failed")
	}
}
