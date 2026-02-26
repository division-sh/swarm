package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboard_Agents_Turns24hAndTokenAggregation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	agentSpend := "agent-spend-priority"
	agentFallback := "agent-turn-fallback"
	for _, id := range []string{agentSpend, agentFallback} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
			VALUES ($1, 'stub', $1, 'holding', 'active', '{}'::jsonb, now(), now())
		`, id); err != nil {
			t.Fatalf("seed agent %s: %v", id, err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agent_sessions (agent_id, runtime_mode, provider, session_id, status, turn_count, created_at, last_used_at)
			VALUES ($1, 'cli_test', 'cli', $2, 'active', 0, now(), now())
		`, id, uuid.NewString()); err != nil {
			t.Fatalf("seed session %s: %v", id, err)
		}
	}

	var spendSess string
	if err := db.QueryRowContext(ctx, `SELECT id::text FROM agent_sessions WHERE agent_id = $1 LIMIT 1`, agentSpend).Scan(&spendSess); err != nil {
		t.Fatalf("lookup spend session row: %v", err)
	}
	var fallbackSess string
	if err := db.QueryRowContext(ctx, `SELECT id::text FROM agent_sessions WHERE agent_id = $1 LIMIT 1`, agentFallback).Scan(&fallbackSess); err != nil {
		t.Fatalf("lookup fallback session row: %v", err)
	}

	// Two turns in last 24h for spend-priority agent.
	for i := 0; i < 2; i++ {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO agent_turns (agent_id, session_row_id, turn_index, request_payload, response_payload, parse_ok, latency_ms, retry_count, error, created_at)
			VALUES ($1, $2::uuid, $3, '{}'::jsonb, '{"usage":{"input_tokens":"9","output_tokens":"10"}}'::jsonb, true, 1, 0, '', now())
		`, agentSpend, spendSess, i+1); err != nil {
			t.Fatalf("seed spend agent_turn %d: %v", i, err)
		}
	}
	// One turn in last 24h for fallback agent.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (agent_id, session_row_id, turn_index, request_payload, response_payload, parse_ok, latency_ms, retry_count, error, created_at)
		VALUES ($1, $2::uuid, 1, '{}'::jsonb, '{"usage":{"input_tokens":"7","output_tokens":"8"}}'::jsonb, true, 1, 0, '', now())
	`, agentFallback, fallbackSess); err != nil {
		t.Fatalf("seed fallback agent_turn: %v", err)
	}

	// spend_ledger should be the primary source when present.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO spend_ledger (agent_id, category, amount_cents, currency, source, metadata, created_at)
		VALUES ($1, 'llm', 123, 'USD', 'estimated', '{"input_tokens":"1234","output_tokens":"321"}'::jsonb, now())
	`, agentSpend); err != nil {
		t.Fatalf("seed spend_ledger: %v", err)
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

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	items, _ := body["agents"].([]any)
	if len(items) == 0 {
		t.Fatalf("expected agents in response")
	}

	find := func(id string) map[string]any {
		for _, raw := range items {
			m, _ := raw.(map[string]any)
			if m != nil && m["id"] == id {
				return m
			}
		}
		return nil
	}

	spendAgent := find(agentSpend)
	if spendAgent == nil {
		t.Fatalf("missing %s in response", agentSpend)
	}
	if got := int(asFloat64(spendAgent["turn_count"])); got != 0 {
		t.Fatalf("turn_count mismatch: got %d want %d", got, 0)
	}
	if got := int(asFloat64(spendAgent["turns_24h"])); got != 2 {
		t.Fatalf("turns_24h mismatch: got %d want %d", got, 2)
	}
	if got := int64(asFloat64(spendAgent["input_tokens_24h"])); got != 1234 {
		t.Fatalf("input_tokens_24h mismatch: got %d want %d", got, 1234)
	}
	if got := int64(asFloat64(spendAgent["output_tokens_24h"])); got != 321 {
		t.Fatalf("output_tokens_24h mismatch: got %d want %d", got, 321)
	}
	if got := int64(asFloat64(spendAgent["total_tokens_24h"])); got != 1555 {
		t.Fatalf("total_tokens_24h mismatch: got %d want %d", got, 1555)
	}

	fallbackAgent := find(agentFallback)
	if fallbackAgent == nil {
		t.Fatalf("missing %s in response", agentFallback)
	}
	if got := int(asFloat64(fallbackAgent["turns_24h"])); got != 1 {
		t.Fatalf("fallback turns_24h mismatch: got %d want %d", got, 1)
	}
	if got := int64(asFloat64(fallbackAgent["input_tokens_24h"])); got != 7 {
		t.Fatalf("fallback input_tokens_24h mismatch: got %d want %d", got, 7)
	}
	if got := int64(asFloat64(fallbackAgent["output_tokens_24h"])); got != 8 {
		t.Fatalf("fallback output_tokens_24h mismatch: got %d want %d", got, 8)
	}
}

func asFloat64(v any) float64 {
	f, _ := v.(float64)
	return f
}
