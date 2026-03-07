package dashboard

import (
	"bytes"
	"context"
	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/runtime"
	rt "empireai/internal/runtime"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"encoding/json"
	"github.com/google/uuid"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"
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

	// Failure + token usage for breaker-stuck-agent.
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

type chatStubAgent struct{ id string }

func (a *chatStubAgent) ID() string { return a.id }

func (a *chatStubAgent) Type() string { return "stub" }

func (a *chatStubAgent) Subscriptions() []events.EventType {
	return []events.EventType{"board.chat", "board.directive"}
}

func (a *chatStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func (a *chatStubAgent) BoardStep(context.Context, string) (string, error) { return "OK", nil }

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

func TestDashboard_ControlChat_LiveAndAsyncPaths(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	bus := rt.NewEventBus(pg)
	manager := runtimemanager.NewAgentManager(bus, func(cfg models.AgentConfig) (rt.Agent, error) {
		return &chatStubAgent{id: cfg.ID}, nil
	}, pg)
	manager.Run(ctx)
	t.Cleanup(func() { _ = manager.Shutdown() })
	_ = manager.SpawnAgent(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", Type: "stub"})

	srv := NewServer(db, &config.Config{}, pg, pg, manager)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		body := []byte(`{"agent_id":"empire-coordinator","message":"hi","mode":"live"}`)
		req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/chat", bytes.NewReader(body))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("control chat live status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"response"`)) {
			t.Fatalf("expected response in live chat: %s", w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		body := []byte(`{"agent_id":"empire-coordinator","message":"hi","mode":"async"}`)
		req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/chat", bytes.NewReader(body))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("control chat async status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Ensure a receipt was written for the live chat delivery.
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE agent_id='empire-coordinator' AND status='processed'`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected processed receipt after live chat")
	}
	_ = time.Second
}

type controlStubAgent struct{ id string }

func (a *controlStubAgent) ID() string { return a.id }

func (a *controlStubAgent) Type() string { return "stub" }

func (a *controlStubAgent) Subscriptions() []events.EventType {
	return []events.EventType{"system.directive"}
}

func (a *controlStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func (a *controlStubAgent) BoardStep(context.Context, string) (string, error) { return "ACK", nil }

func repoRootControl(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func authed(t *testing.T, method, path string, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("X-Empire-Key", "test-key")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func TestDashboardServer_ControlPlane_MoreBranches(t *testing.T) {
	root := repoRootControl(t)
	_ = root

	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	t.Setenv("EMPIREAI_API_KEY", "test-key")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               10 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				OutputFormat: "json",
				Timeout:      1 * time.Second,
				Retries:      1,
			},
		},
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{BudgetReset: "monday", MaxTasksPerWeek: 3, AutoExpireHours: 168},
		},
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v1', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', NULL, 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent row: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'system.started', 'runtime', '{}'::jsonb, now())
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed system.started: %v", err)
	}

	bus := rt.NewEventBus(pg)
	factory := func(cfg models.AgentConfig) (rt.Agent, error) {
		return &controlStubAgent{id: cfg.ID}, nil
	}
	manager := runtimemanager.NewAgentManager(bus, factory, pg)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	_ = manager.SpawnAgent(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", Type: "stub"})

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodGet, "/dashboard/api/control/targets", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("targets status=%d body=%s", w.Code, w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/agents/restart", []byte(`{"agent_id":"empire-coordinator"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("restart status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/agents/replay", []byte(`{"agent_id":"empire-coordinator"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("replay status=%d body=%s", w.Code, w.Body.String())
		}
	}

	{
		eventID := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
			VALUES ($1::uuid, 'system.directive', 'human', $2::uuid, '{}'::jsonb, now())
		`, eventID, verticalID); err != nil {
			t.Fatalf("seed event: %v", err)
		}
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO event_deliveries (event_id, agent_id, created_at)
			VALUES ($1::uuid, 'empire-coordinator', now())
			ON CONFLICT DO NOTHING
		`, eventID); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}

		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"event_id":"`+eventID+`","agent_id":"empire-coordinator"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("requeue single status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"event_id":"`+eventID+`"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("requeue all status=%d body=%s", w.Code, w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/directive", []byte(`{"agent_id":"empire-coordinator","message":"do it"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("control directive status=%d body=%s", w.Code, w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodGet, "/api/tasks/not-a-uuid", nil))
		if w.Code != http.StatusBadRequest && w.Code != http.StatusNotFound && w.Code != http.StatusInternalServerError {

			t.Fatalf("expected 400/404/500 for invalid task id, got %d body=%s", w.Code, w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodGet, "/api/mailbox/abc/decide", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected method not allowed, got %d body=%s", w.Code, w.Body.String())
		}
	}

	if asString(nil) != "" || asString(123) == "" {
		t.Fatalf("asString helper unexpected")
	}
	if clamp(5, 0, 3) != 3 || clamp(-1, 0, 3) != 0 || clamp(2, 0, 3) != 2 {
		t.Fatalf("clamp helper unexpected")
	}
	if strings.TrimSpace(truncate("hello", 2)) != "he..." {
		t.Fatalf("truncate helper unexpected")
	}

	if parseInt("x", 7) != 7 || parseInt("3", 7) != 3 {
		t.Fatalf("parseInt helper unexpected")
	}

	_ = json.Valid(mustJSON(map[string]any{"k": "v"}))
}

func authedReqAny(t *testing.T, method, path string, body any) *http.Request {
	t.Helper()
	var b []byte
	if body != nil {
		b, _ = json.Marshal(body)
	}
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("X-Empire-Key", "test-key")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func TestDashboard_EventDetail_WithDeliveries(t *testing.T) {
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
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	eventID := uuid.NewString()
	createdAt := time.Now().Add(-10 * time.Second).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
		VALUES ($1::uuid, 'board.chat', 'human', $2::uuid, $3::jsonb, $4)
	`, eventID, verticalID, `{"hello":"world"}`, createdAt); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, 'empire-coordinator', now())
	`, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}
	processedAt := time.Now().UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (event_id, agent_id, processed_at, status, retry_count, error)
		VALUES ($1::uuid, 'empire-coordinator', $2, 'processed', 0, NULL)
	`, eventID, processedAt); err != nil {
		t.Fatalf("seed receipt: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/events/"+uuid.NewString(), nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d body=%s", w.Code, w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/events/"+eventID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("detail status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"deliveries"`)) {
			t.Fatalf("expected deliveries in response: %s", w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/api/events/"+eventID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("alias detail status=%d body=%s", w.Code, w.Body.String())
		}
	}
}

func TestDashboard_Funnel_ThroughputAndStuck(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour).UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES
			($1::uuid, 'A', 'a', 'us', 'discovered', 'factory', now() - interval '2 days', $4),
			($2::uuid, 'B', 'b', 'us', 'operating', 'operating', now() - interval '1 days', now()),
			($3::uuid, 'C', 'c', 'us', 'killed', 'factory', now() - interval '1 days', now())
	`, uuid.NewString(), uuid.NewString(), uuid.NewString(), old); err != nil {
		t.Fatalf("seed verticals: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/funnel", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("funnel status=%d body=%s", w.Code, w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"stage_counts"`)) || !bytes.Contains(w.Body.Bytes(), []byte(`"stuck"`)) {
		t.Fatalf("unexpected funnel response: %s", w.Body.String())
	}
}

func TestDashboard_PipelineShardsEndpoint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

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
			agent_id TEXT REFERENCES agents(id),
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

	scanID := uuid.NewString()
	scopeA := `{"mode":"saas_gap","geography":"Argentina","taxonomy_categories":["financial_ops","commerce_payments"]}`
	scopeB := `{"mode":"saas_gap","geography":"Argentina","taxonomy_categories":["customer_ops","marketing_sales"]}`
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, status, deadline_at, budget_cents, spend_cents, created_at
		)
		VALUES
			($1::uuid, $3::uuid, $2::uuid, 'market_research', 0, 2, 'financial_ops+commerce_payments', $4::jsonb, 'assigned', now() + interval '20 minutes', 50, 31, now() - interval '15 minutes'),
			($5::uuid, $3::uuid, $2::uuid, 'market_research', 1, 2, 'customer_ops+marketing_sales', $6::jsonb, 'completed', now() + interval '20 minutes', 50, 25, now() - interval '8 minutes')
	`, uuid.NewString(), scanID, uuid.NewString(), scopeA, uuid.NewString(), scopeB); err != nil {
		t.Fatalf("seed shards: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/pipeline/shards", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("pipeline shards status=%d body=%s", w.Code, w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"scans"`)) || !bytes.Contains(w.Body.Bytes(), []byte(`"scan_id"`)) {
		t.Fatalf("unexpected pipeline shards response: %s", w.Body.String())
	}
	if !bytes.Contains(w.Body.Bytes(), []byte(`"shards_total":2`)) {
		t.Fatalf("expected shards_total=2 response: %s", w.Body.String())
	}
}

func TestDashboard_PipelineShardDetailAndActions(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

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
			agent_id TEXT REFERENCES agents(id),
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

	scanID := uuid.NewString()
	rootTaskID := uuid.NewString()
	failedShardID := uuid.NewString()
	pendingShardID := uuid.NewString()
	failedAgentID := "market-research-agent-shard-0-a1b2c3d4"
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ($1, 'stub', 'market-research-agent', 'factory', 'active', '{}'::jsonb, now(), now())
	`, failedAgentID); err != nil {
		t.Fatalf("seed shard agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, deadline_at, budget_cents, spend_cents, error, assigned_at, completed_at, created_at
		)
		VALUES
			($1::uuid, $3::uuid, $2::uuid, 'market_research', 0, 2, 'financial_ops+commerce', '{"mode":"saas_gap","geography":"Argentina","taxonomy_categories":["financial_ops","commerce_payments"]}'::jsonb, $5, 'timed_out', now() - interval '15 minutes', 50, 41, 'timeout', now() - interval '35 minutes', now() - interval '5 minutes', now() - interval '40 minutes'),
			($4::uuid, $3::uuid, $2::uuid, 'market_research', 1, 2, 'customer_ops+marketing', '{"mode":"saas_gap","geography":"Argentina","taxonomy_categories":["customer_ops","marketing_sales"]}'::jsonb, NULL, 'pending', now() + interval '20 minutes', 50, 12, NULL, NULL, NULL, now() - interval '3 minutes')
	`, failedShardID, scanID, rootTaskID, pendingShardID, failedAgentID); err != nil {
		t.Fatalf("seed shards: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES
			($1::uuid, 'market_research.scan_complete', $4, $2::jsonb, now() - interval '7 minutes'),
			($3::uuid, 'market_research.scan_complete', $4, $5::jsonb, now() - interval '6 minutes'),
			($6::uuid, 'market_research.scan_complete', $4, $7::jsonb, now() - interval '5 minutes'),
			($8::uuid, 'market_research.scan_complete', $4, $9::jsonb, now() - interval '4 minutes')
	`,
		uuid.NewString(),
		`{"scan_id":"`+scanID+`","signal_strength":82}`,
		uuid.NewString(),
		failedAgentID,
		`{"scan_id":"`+scanID+`","signal_strength":55}`,
		uuid.NewString(),
		`{"scan_id":"`+scanID+`","high_signal_count":2}`,
		uuid.NewString(),
		`{"scan_id":"`+scanID+`","shard":{"terminal":true},"signal_strength":99}`,
	); err != nil {
		t.Fatalf("seed shard events: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/api/pipeline/shards/"+scanID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("detail status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"scan_id":"`+scanID+`"`)) {
			t.Fatalf("expected scan id in detail response: %s", w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"id":"`+failedShardID+`"`)) {
			t.Fatalf("expected failed shard id in detail response: %s", w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"reports_count":3`)) {
			t.Fatalf("expected reports_count=3 in detail response: %s", w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"high_signal_count":3`)) {
			t.Fatalf("expected high_signal_count=3 in detail response: %s", w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/api/pipeline/shards/"+failedShardID+"/retry", map[string]any{}))
		if w.Code != http.StatusOK {
			t.Fatalf("retry status=%d body=%s", w.Code, w.Body.String())
		}
		var status, errText string
		if err := db.QueryRowContext(ctx, `SELECT status, COALESCE(error, '') FROM shards WHERE id = $1::uuid`, failedShardID).Scan(&status, &errText); err != nil {
			t.Fatalf("query retried shard: %v", err)
		}
		if status != "pending" || errText != "" {
			t.Fatalf("expected retried shard pending with empty error, got status=%s error=%q", status, errText)
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/pipeline/shards/"+pendingShardID+"/cancel", map[string]any{}))
		if w.Code != http.StatusOK {
			t.Fatalf("cancel status=%d body=%s", w.Code, w.Body.String())
		}
		var status, errText string
		if err := db.QueryRowContext(ctx, `SELECT status, COALESCE(error, '') FROM shards WHERE id = $1::uuid`, pendingShardID).Scan(&status, &errText); err != nil {
			t.Fatalf("query canceled shard: %v", err)
		}
		if status != "failed" {
			t.Fatalf("expected canceled shard failed, got status=%s", status)
		}
		if errText == "" {
			t.Fatalf("expected canceled shard error text, got empty")
		}
	}
}

func TestDashboard_PipelineHealthEndpoint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, scan_config, created_at)
		VALUES ($1::uuid, 'Argentina', 'AR', 'latam', '{}'::jsonb, now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
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
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, status, priority, created_at, started_at, completed_at)
		VALUES
			($1::uuid, $4::uuid, 'saas_gap', 'active', 'high', now() - interval '2 hours', now() - interval '2 hours', NULL),
			($2::uuid, $4::uuid, 'saas_trend', 'paused', 'normal', now() - interval '8 hours', now() - interval '8 hours', NULL),
			($3::uuid, $4::uuid, 'local_services', 'completed', 'normal', now() - interval '1 days', now() - interval '1 days', now() - interval '30 minutes')
	`, uuid.NewString(), uuid.NewString(), uuid.NewString(), geoID); err != nil {
		t.Fatalf("seed scan campaigns: %v", err)
	}
	verticalResearch := uuid.NewString()
	verticalReady := uuid.NewString()
	verticalMarginal := uuid.NewString()
	verticalApproved := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, parked_at, created_at, updated_at)
		VALUES
			($1::uuid, 'Research V', 'research-v', 'ar', 'researching', 'factory', NULL, now() - interval '1 days', now() - interval '3 hours'),
			($2::uuid, 'Ready V', 'ready-v', 'ar', 'ready_for_review', 'factory', NULL, now() - interval '1 days', now() - interval '3 hours'),
			($3::uuid, 'Marginal V', 'marginal-v', 'ar', 'marginal_review', 'factory', now() - interval '9 days', now() - interval '10 days', now() - interval '9 days'),
			($4::uuid, 'Approved V', 'approved-v', 'ar', 'approved', 'factory', NULL, now() - interval '2 days', now() - interval '1 hours')
	`, verticalResearch, verticalReady, verticalMarginal, verticalApproved); err != nil {
		t.Fatalf("seed verticals: %v", err)
	}
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'scan.completed', 'scan-runner', '{}'::jsonb, now() - interval '1 hour')
	`, eventID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pipeline_transitions (
			event_id, event_type, handler, pipeline_type, pipeline_id, action, error, created_at
		)
		VALUES ($1::uuid, 'scan.completed', 'scanRunner.handleScanRequested', 'scan', $2::uuid, 'error', 'request timeout', now() - interval '20 minutes')
	`, eventID, uuid.NewString()); err != nil {
		t.Fatalf("seed pipeline transition: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health/pipeline", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/health/pipeline status=%d body=%s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal /health/pipeline: %v", err)
	}
	if _, ok := body["campaigns"]; !ok {
		t.Fatalf("expected campaigns in response: %s", w.Body.String())
	}
	if _, ok := body["alerts"]; !ok {
		t.Fatalf("expected alerts in response: %s", w.Body.String())
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/api/health/pipeline", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("/api/health/pipeline status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestDashboard_ControlMailboxDecide_EmitsSideEffects(t *testing.T) {
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
		t.Fatalf("seed opco ceo agent: %v", err)
	}

	mbID, err := pg.InsertMailboxItem(ctx, runtime.MailboxItem{
		VerticalID: verticalID,
		FromAgent:  "operations-analyst",
		Type:       "escalation",
		Priority:   "normal",
		Status:     "pending",
		Context:    []byte(`{"x":1}`),
		Summary:    "need direction",
	})
	if err != nil {
		t.Fatalf("InsertMailboxItem: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{"mailbox_id": mbID}))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{
			"mailbox_id": mbID,
			"action":     "approve",
			"notes":      "do the thing",
		}))
		if w.Code != http.StatusOK {
			t.Fatalf("decide status=%d body=%s", w.Code, w.Body.String())
		}
	}
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='opco.escalation_response' AND vertical_id=$1::uuid`, verticalID).Scan(&n)
	if n < 1 {
		t.Fatalf("expected opco.escalation_response event")
	}
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries d
		JOIN events e ON e.id = d.event_id
		WHERE e.type='opco.escalation_response' AND d.agent_id=$1
	`, "opco-ceo-"+verticalID).Scan(&n)
	if n < 1 {
		t.Fatalf("expected delivery to opco ceo")
	}
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='mailbox.item_decided'`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected mailbox.item_decided event")
	}
}

func TestDashboard_ControlMailboxDecide_GeographyExpansionQueuesCampaign(t *testing.T) {
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
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed empire coordinator: %v", err)
	}

	mbID, err := pg.InsertMailboxItem(ctx, runtime.MailboxItem{
		VerticalID: verticalID,
		FromAgent:  "opco-ceo-" + verticalID,
		Type:       "domain_approval",
		Priority:   "normal",
		Status:     "pending",
		Context:    []byte(`{"review_type":"geography_expansion","geography":"Asuncion, Paraguay","country":"PY","mode":"saas_gap","categories":["financial_ops"],"priority":"high"}`),
		Summary:    "expand to Paraguay",
	})
	if err != nil {
		t.Fatalf("InsertMailboxItem: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{
		"mailbox_id": mbID,
		"action":     "approve",
		"notes":      "run validation",
	}))
	if w.Code != http.StatusOK {
		t.Fatalf("decide status=%d body=%s", w.Code, w.Body.String())
	}

	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM geographies WHERE lower(name)=lower('Asuncion, Paraguay')`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected geography inserted")
	}
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_campaigns WHERE mode='saas_gap'`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected scan campaign queued")
	}
	_ = db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries d
		JOIN events e ON e.id = d.event_id
		WHERE e.type='geography.expansion_queued' AND d.agent_id='empire-coordinator'
	`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected geography.expansion_queued delivery to empire-coordinator")
	}
}

func TestDashboard_ControlMailboxDecide_VerticalApprovalEmitsLifecycleEvents(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v3', 'us', 'branding', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed empire coordinator: %v", err)
	}

	makeMailbox := func(summary string) string {
		id, err := pg.InsertMailboxItem(ctx, runtime.MailboxItem{
			VerticalID: verticalID,
			FromAgent:  "validation-coordinator",
			Type:       "vertical_approval",
			Priority:   "high",
			Status:     "pending",
			Context:    []byte(`{"validation_kit":"ok"}`),
			Summary:    summary,
		})
		if err != nil {
			t.Fatalf("InsertMailboxItem(%s): %v", summary, err)
		}
		return id
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	approvedID := makeMailbox("approve path")
	w1 := httptest.NewRecorder()
	h.ServeHTTP(w1, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{
		"mailbox_id": approvedID,
		"action":     "approve",
		"notes":      "approved",
	}))
	if w1.Code != http.StatusOK {
		t.Fatalf("approve decide status=%d body=%s", w1.Code, w1.Body.String())
	}

	rejectedID := makeMailbox("reject path")
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{
		"mailbox_id": rejectedID,
		"action":     "reject",
		"notes":      "rejected",
	}))
	if w2.Code != http.StatusOK {
		t.Fatalf("reject decide status=%d body=%s", w2.Code, w2.Body.String())
	}

	for _, typ := range []string{"vertical.approved", "vertical.killed"} {
		var n int
		if err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM event_deliveries d
			JOIN events e ON e.id = d.event_id
			WHERE e.type = $1 AND d.agent_id = 'empire-coordinator'
		`, typ).Scan(&n); err != nil {
			t.Fatalf("count %s deliveries: %v", typ, err)
		}
		if n < 1 {
			t.Fatalf("expected %s delivery to empire-coordinator", typ)
		}
	}
}

func TestDashboard_TaskView_FoundAndNotFound(t *testing.T) {
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

	taskID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, priority, status, assigned_to, reviewed_at, completed_at, result, outcome, follow_up_needed, created_at)
		VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'call someone', 'high', 'completed', 'founder', now(), now(), 'done', 'success', false, now())
	`, taskID, verticalID); err != nil {
		t.Fatalf("seed task: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/tasks/"+uuid.NewString(), nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d body=%s", w.Code, w.Body.String())
		}
	}
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReqAny(t, http.MethodGet, "/dashboard/api/tasks/"+taskID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("task view status=%d body=%s", w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"task"`)) {
			t.Fatalf("expected task payload, got %s", w.Body.String())
		}
	}
}
