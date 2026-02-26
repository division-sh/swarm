package runtime

import (
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestMCPStallDiagnosticsPass_EmitsRuntimeLogForStalledAgent(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	// Ensure minimal tables for diagnostic pass in case migrations lag in tests.
	_, _ = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS agents (
			id TEXT PRIMARY KEY,
			type TEXT,
			role TEXT,
			mode TEXT,
			vertical_id UUID,
			status TEXT,
			config JSONB,
			created_at TIMESTAMPTZ DEFAULT now(),
			updated_at TIMESTAMPTZ DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS events (
			id UUID PRIMARY KEY,
			type TEXT,
			source_agent TEXT,
			task_id UUID,
			vertical_id UUID,
			payload JSONB,
			created_at TIMESTAMPTZ DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS event_deliveries (
			event_id UUID,
			agent_id TEXT,
			created_at TIMESTAMPTZ DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS event_receipts (
			event_id UUID,
			agent_id TEXT,
			processed_at TIMESTAMPTZ DEFAULT now(),
			status TEXT,
			retry_count INT,
			error TEXT
		);
		CREATE TABLE IF NOT EXISTS agent_turns (
			id BIGSERIAL PRIMARY KEY,
			agent_id TEXT,
			session_row_id UUID,
			turn_index INT,
			request_payload JSONB,
			response_payload JSONB,
			parse_ok BOOLEAN,
			latency_ms INT,
			retry_count INT,
			error TEXT,
			created_at TIMESTAMPTZ DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS agent_sessions (
			id UUID PRIMARY KEY,
			agent_id TEXT,
			runtime_mode TEXT,
			provider TEXT,
			session_id TEXT,
			status TEXT,
			turn_count INT,
			lock_owner TEXT,
			lock_expires_at TIMESTAMPTZ,
			last_used_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS runtime_log (
			id BIGSERIAL PRIMARY KEY,
			ts TIMESTAMPTZ NOT NULL DEFAULT now(),
			level TEXT NOT NULL DEFAULT 'info',
			component TEXT NOT NULL DEFAULT 'runtime',
			action TEXT NOT NULL DEFAULT 'unknown',
			event_id UUID,
			event_type TEXT,
			agent_id TEXT,
			vertical_id UUID,
			campaign_id UUID,
			scan_id UUID,
			session_id UUID,
			detail JSONB NOT NULL DEFAULT '{}'::jsonb,
			error TEXT,
			duration_us BIGINT
		)
	`)

	agentID := "market-research-agent-shard-0-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config)
		VALUES ($1, 'stub', 'market-research-agent', 'factory', 'active', '{}'::jsonb)
	`, agentID); err != nil {
		t.Fatalf("insert agent: %v", err)
	}
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'market_research.scan_assigned', 'shard-dispatcher', '{}'::jsonb, now() - interval '12 minutes')
	`, eventID); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now() - interval '12 minutes')
	`, eventID, agentID); err != nil {
		t.Fatalf("insert delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (id, agent_id, runtime_mode, provider, session_id, status, turn_count, created_at, last_used_at)
		VALUES ($1::uuid, $2, 'cli_test', 'anthropic', 'sess-stall', 'active', 0, now() - interval '12 minutes', now() - interval '12 minutes')
	`, uuid.NewString(), agentID); err != nil {
		t.Fatalf("insert session: %v", err)
	}

	logger := NewRuntimeLogger(db)
	cfg := DefaultMCPStallDiagnosticConfig()
	cfg.MinPending = 1
	cfg.PendingAge = 2 * time.Minute
	cfg.ArtifactLines = 5
	runMCPStallDiagnosticsPass(ctx, db, logger, cfg)

	var (
		action string
		errTxt string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(action, ''), COALESCE(error, '')
		FROM runtime_log
		WHERE component = 'mcp-diagnostics'
		  AND action = 'auto_diagnostic_stall'
		ORDER BY ts DESC
		LIMIT 1
	`).Scan(&action, &errTxt); err != nil {
		t.Fatalf("expected emitted diagnostic runtime_log: %v", err)
	}
	if action != "auto_diagnostic_stall" {
		t.Fatalf("unexpected action: %q", action)
	}
	if !strings.Contains(errTxt, "code="+ErrCodeMCPStallDetected) {
		t.Fatalf("expected stall code in error envelope, got %q", errTxt)
	}
}
