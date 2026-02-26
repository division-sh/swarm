package dashboard

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestDashboardRuntimeIncidents_AggregatesMCPErrorCodes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	if _, err := db.ExecContext(context.Background(), `
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
	`); err != nil {
		t.Fatalf("create runtime_log: %v", err)
	}

	now := time.Now().UTC()
	rows := []struct {
		level     string
		component string
		action    string
		agentID   string
		errorText string
		ts        time.Time
	}{
		{"warn", "mcp-gateway", "mcp.tools.call.context_error", "market-research-agent-shard-0", "runtime_error code=mcp_context_token_missing component=mcp-gateway operation=mcp.context.resolve retryable=true: missing", now.Add(-2 * time.Minute)},
		{"warn", "mcp-gateway", "mcp.tools.call.context_error", "market-research-agent-shard-1", "runtime_error code=mcp_context_token_missing component=mcp-gateway operation=mcp.context.resolve retryable=true: missing", now.Add(-90 * time.Second)},
		{"warn", "mcp-gateway", "mcp.authorize_failed", "market-research-agent-shard-2", "runtime_error code=mcp_auth_invalid_bearer component=mcp-gateway operation=mcp.authorize retryable=false: invalid", now.Add(-40 * time.Second)},
		{"warn", "runtime", "other", "x", "runtime_error code=other_code component=runtime operation=x retryable=false: x", now.Add(-30 * time.Second)},
	}
	for _, r := range rows {
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO runtime_log (ts, level, component, action, agent_id, detail, error)
			VALUES ($1, $2, $3, $4, $5, '{}'::jsonb, $6)
		`, r.ts, r.level, r.component, r.action, r.agentID, r.errorText); err != nil {
			t.Fatalf("insert runtime_log: %v", err)
		}
	}

	srv := NewServer(db, &config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test"}}, pg, pg, nil)
	h := srv.Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/runtime/incidents?since_hours=24&mcp_only=true", nil)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, `"code":"mcp_context_token_missing"`) {
		t.Fatalf("expected mcp_context_token_missing incident: %s", body)
	}
	if !strings.Contains(body, `"count":2`) {
		t.Fatalf("expected grouped count=2 for context missing: %s", body)
	}
	if strings.Contains(body, `"code":"other_code"`) {
		t.Fatalf("expected non-mcp code excluded when mcp_only=true: %s", body)
	}
}

func TestDashboardRuntimeLogs_FilterByErrorCode(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	if _, err := db.ExecContext(context.Background(), `
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
	`); err != nil {
		t.Fatalf("create runtime_log: %v", err)
	}
	_, _ = db.ExecContext(context.Background(), `
		INSERT INTO runtime_log (ts, level, component, action, detail, error)
		VALUES
			(now(), 'warn', 'mcp-gateway', 'mcp.tools.call.context_error', '{}'::jsonb, 'runtime_error code=mcp_context_token_missing component=mcp-gateway operation=mcp.context.resolve retryable=true: missing'),
			(now(), 'warn', 'mcp-gateway', 'mcp.authorize_failed', '{}'::jsonb, 'runtime_error code=mcp_auth_invalid_bearer component=mcp-gateway operation=mcp.authorize retryable=false: invalid')
	`)

	srv := NewServer(db, &config.Config{LLM: config.LLMConfig{RuntimeMode: "cli_test"}}, pg, pg, nil)
	h := srv.Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/runtime/logs?error_code=mcp_context_token_missing", nil)
	req.Header.Set("X-Empire-Key", "test-key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	logs, _ := payload["runtime_logs"].([]any)
	if len(logs) != 1 {
		t.Fatalf("expected exactly 1 runtime log by error_code filter, got %d payload=%s", len(logs), w.Body.String())
	}
}
