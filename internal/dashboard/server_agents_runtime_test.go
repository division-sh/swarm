package dashboard

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboard_Agents_StateClassificationAndMetrics(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v1', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	now := time.Now().UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES
			('terminated-agent', 'stub', 'terminated', 'holding', NULL, 'terminated', '{}'::jsonb, now() - interval '2 hours', now() - interval '2 hours'),
			('idle-agent', 'stub', 'idle', 'holding', NULL, 'active', '{}'::jsonb, now() - interval '2 hours', now() - interval '1 hours'),
			('lock-agent', 'stub', 'lock', 'holding', NULL, 'active', '{}'::jsonb, now() - interval '2 hours', now()),
			('pending-stuck-agent', 'stub', 'pending', 'operating', $1::uuid, 'active', '{}'::jsonb, now() - interval '2 hours', now() - interval '30 minutes'),
			('breaker-stuck-agent', 'stub', 'breaker', 'operating', $1::uuid, 'active', '{}'::jsonb, now() - interval '2 hours', $2)
	`, verticalID, now.Add(-5*time.Minute)); err != nil {
		t.Fatalf("seed agents: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (agent_id, runtime_mode, provider, session_id, status, turn_count, lock_owner, lock_expires_at, last_used_at, created_at)
		VALUES
			('lock-agent', 'cli_test', 'cli', 's-lock', 'active', 1, 'human', now() + interval '10 minutes', now(), now()),
			('pending-stuck-agent', 'cli_test', 'cli', 's-pending', 'active', 2, NULL, NULL, now() - interval '30 minutes', now()),
			('breaker-stuck-agent', 'cli_test', 'cli', 's-breaker', 'active', 39, NULL, NULL, now(), now())
	`); err != nil {
		t.Fatalf("seed sessions: %v", err)
	}

	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'system.directive', 'human', $2::uuid, '{}'::jsonb, now() - interval '20 minutes')
	`, eventID, verticalID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, 'pending-stuck-agent', now() - interval '20 minutes')
	`, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	var sessRowID string
	if err := db.QueryRowContext(ctx, `
		SELECT id::text FROM agent_sessions WHERE agent_id='breaker-stuck-agent' AND status='active' LIMIT 1
	`).Scan(&sessRowID); err != nil {
		t.Fatalf("lookup session row id: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (agent_id, session_row_id, turn_index, request_payload, response_payload, parse_ok, latency_ms, retry_count, error, created_at)
		VALUES (
			'breaker-stuck-agent', $1::uuid, 1,
			'{}'::jsonb,
			'{"usage":{"input_tokens": "12", "output_tokens": "34"}}'::jsonb,
			false,
			10,
			0,
			'parse failed',
			now()
		)
		ON CONFLICT DO NOTHING
	`, sessRowID); err != nil {
		t.Fatalf("seed agent_turns: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'agent.tool_execution', 'breaker-stuck-agent', '{"tool_name":"sql_execute","ok":"false","error":"boom","result":"nope"}'::jsonb, now())
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed tool event: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'agent.started', 'lock-agent', '{}'::jsonb, now() - interval '1 hour')
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed creation event: %v", err)
	}

	srv := NewServer(db, &config.Config{LLM: config.LLMConfig{Session: config.LLMSessionConfig{RotateAfterTurns: 40}}}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/agents", nil)
	req.Header.Set("X-Empire-Key", "test-key")
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("agents status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestDashboard_APIVerticalAgents_AndAPIDirective(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v1', 'us', 'operating', 'operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES ($1, 'stub', 'opco-ceo', 'operating', $2::uuid, 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`, "opco-ceo-"+verticalID, verticalID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/verticals/"+verticalID+"/agents", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("vertical agents status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"agents"`)) {
			t.Fatalf("expected agents list: %s", w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/directive", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/directive", bytes.NewReader([]byte(`{"directive_text":"do it"}`)))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusConflict {
			t.Fatalf("expected 409 before system.started, got %d body=%s", w.Code, w.Body.String())
		}
	}

	{
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (id, type, source_agent, payload, created_at)
			VALUES ($1::uuid, 'system.started', 'runtime', '{}'::jsonb, now())
		`, uuid.NewString()); err != nil {
			t.Fatalf("seed system.started: %v", err)
		}
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/directive", bytes.NewReader([]byte(`{"directive_text":"do it"}`)))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 without runtime manager, got %d body=%s", w.Code, w.Body.String())
		}
	}
}
