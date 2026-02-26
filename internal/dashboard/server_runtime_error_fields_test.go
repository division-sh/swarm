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

func TestDashboard_Agents_LastToolRuntimeErrorFields(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('agent-runtime-error', 'stub', 'agent-runtime-error', 'holding', 'active', '{}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	errText := "runtime_error code=schema_validation_failed component=tool_executor operation=emit_event retryable=false: schema validation failed"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'agent.tool_execution', 'agent-runtime-error', $2::jsonb, now())
	`, uuid.NewString(), `{"tool_name":"emit_vertical_scored","ok":"false","error":"`+errText+`","result":"{}"}`); err != nil {
		t.Fatalf("seed tool execution: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
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
	agents, _ := body["agents"].([]any)
	var found map[string]any
	for _, raw := range agents {
		item, _ := raw.(map[string]any)
		if item != nil && item["id"] == "agent-runtime-error" {
			found = item
			break
		}
	}
	if found == nil {
		t.Fatalf("agent not found in response: %s", w.Body.String())
	}
	lastTool, _ := found["last_tool"].(map[string]any)
	if lastTool == nil {
		t.Fatalf("last_tool missing in response: %s", w.Body.String())
	}
	if got := asString(lastTool["error_code"]); got != "schema_validation_failed" {
		t.Fatalf("error_code mismatch: got=%q", got)
	}
	if got := asString(lastTool["error_component"]); got != "tool_executor" {
		t.Fatalf("error_component mismatch: got=%q", got)
	}
	if got := asString(lastTool["error_operation"]); got != "emit_event" {
		t.Fatalf("error_operation mismatch: got=%q", got)
	}
	if got, ok := lastTool["error_retryable"].(bool); !ok || got {
		t.Fatalf("error_retryable mismatch: got=%v ok=%t", lastTool["error_retryable"], ok)
	}
}

func TestDashboard_EventDetail_DeliveryRuntimeErrorFields(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('agent-delivery-runtime-error', 'stub', 'agent-delivery-runtime-error', 'holding', 'active', '{}'::jsonb, now(), now())
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'system.directive', 'human', '{}'::jsonb, now())
	`, eventID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, 'agent-delivery-runtime-error', now())
	`, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	errText := "runtime_error code=event_publish_failed component=manager operation=process_event retryable=true: publish timeout"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, 'agent-delivery-runtime-error', now(), 'error', 1, $2)
	`, eventID, errText); err != nil {
		t.Fatalf("seed receipt: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/events/"+eventID, nil)
	req.Header.Set("X-Empire-Key", "test-key")
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("event detail status=%d body=%s", w.Code, w.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	deliveries, _ := body["deliveries"].([]any)
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 delivery, got=%d", len(deliveries))
	}
	d, _ := deliveries[0].(map[string]any)
	if d == nil {
		t.Fatalf("delivery payload invalid: %v", deliveries[0])
	}
	if got := asString(d["error_code"]); got != "event_publish_failed" {
		t.Fatalf("error_code mismatch: got=%q", got)
	}
	if got := asString(d["error_component"]); got != "manager" {
		t.Fatalf("error_component mismatch: got=%q", got)
	}
	if got := asString(d["error_operation"]); got != "process_event" {
		t.Fatalf("error_operation mismatch: got=%q", got)
	}
	if got, ok := d["error_retryable"].(bool); !ok || !got {
		t.Fatalf("error_retryable mismatch: got=%v ok=%t", d["error_retryable"], ok)
	}
}

func TestDashboard_RuntimeLogs_RuntimeErrorFields(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS runtime_log (
			id BIGSERIAL PRIMARY KEY,
			ts TIMESTAMPTZ NOT NULL DEFAULT now(),
			level TEXT NOT NULL,
			component TEXT NOT NULL,
			action TEXT NOT NULL,
			event_id UUID,
			event_type TEXT,
			agent_id TEXT,
			vertical_id UUID,
			campaign_id UUID,
			scan_id UUID,
			session_id UUID,
			detail JSONB,
			error TEXT,
			duration_us INT
		)
	`); err != nil {
		t.Fatalf("create runtime_log: %v", err)
	}
	errText := "runtime_error code=event_publish_failed component=eventbus operation=publish retryable=true: queue full"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runtime_log (ts, level, component, action, event_type, detail, error, duration_us)
		VALUES (now(), 'error', 'eventbus', 'publish', 'scan.requested', '{}'::jsonb, $1, 123)
	`, errText); err != nil {
		t.Fatalf("seed runtime_log: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/runtime/logs?component=eventbus&limit=5", nil)
	req.Header.Set("X-Empire-Key", "test-key")
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("runtime logs status=%d body=%s", w.Code, w.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	logs, _ := body["runtime_logs"].([]any)
	if len(logs) == 0 {
		t.Fatalf("expected runtime logs, got empty")
	}
	item, _ := logs[0].(map[string]any)
	if item == nil {
		t.Fatalf("runtime log payload invalid: %v", logs[0])
	}
	if got := asString(item["error_code"]); got != "event_publish_failed" {
		t.Fatalf("error_code mismatch: got=%q", got)
	}
	if got := asString(item["error_component"]); got != "eventbus" {
		t.Fatalf("error_component mismatch: got=%q", got)
	}
	if got := asString(item["error_operation"]); got != "publish" {
		t.Fatalf("error_operation mismatch: got=%q", got)
	}
	if got, ok := item["error_retryable"].(bool); !ok || !got {
		t.Fatalf("error_retryable mismatch: got=%v ok=%t", item["error_retryable"], ok)
	}
}
