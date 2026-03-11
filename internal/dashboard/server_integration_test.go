package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	empireconfig "empireai/internal/empire/config"
	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

type stubAgent struct {
	id   string
	subs []events.EventType
}

func (a *stubAgent) ID() string { return a.id }
func (a *stubAgent) Type() string {
	return "stub"
}
func (a *stubAgent) Subscriptions() []events.EventType { return a.subs }
func (a *stubAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	// Minimal behavior: mark activity and emit nothing.
	_ = ctx
	_ = evt
	return nil, nil
}
func (a *stubAgent) BoardStep(ctx context.Context, directive string) (string, error) {
	_ = ctx
	_ = directive
	return "ACK", nil
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	// internal/dashboard -> repo root
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func authedReq(t *testing.T, method, path string, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("X-Empire-Key", "test-key")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func TestDashboardServer_EndToEndCoreAPIs(t *testing.T) {
	root := repoRoot(t)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	t.Setenv("EMPIREAI_API_KEY", "test-key")
	t.Setenv("EMPIREAI_GLOBAL_AGENTS_DIR", filepath.Join(root, "configs", "agents"))
	t.Setenv("EMPIREAI_TEMPLATE_AGENTS_DIR", filepath.Join(root, "configs", "agents", "templates"))
	t.Setenv("EMPIREAI_TEMPLATE_ROUTES_YAML", filepath.Join(root, "configs", "agents", "templates", "routes.yaml"))
	t.Setenv("EMPIREAI_INITIAL_TEMPLATE_VERSION", "2.0.1")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               30 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				OutputFormat: "json",
				Timeout:      10 * time.Second,
				Retries:      1,
			},
		},
		Extensions: map[string]any{"budget": empireconfig.BudgetConfig{
			FactoryMonthlyCap:     50000,
			PerVerticalMonthlyCap: 20000,
			PortfolioMonthlyCap:   100000,
			HumanTasks: empireconfig.HumanTasksConfig{
				MaxTasksPerWeek: 3,
				BudgetReset:     "monday",
				AutoExpireHours: 168,
			},
		}},
	}

	bus := rt.NewEventBus(pg)
	factory := func(cfg models.AgentConfig) (runtimemanager.Agent, error) {
		// Use configured subscriptions when present.
		subs := make([]events.EventType, 0, len(cfg.Subscriptions))
		for _, s := range cfg.Subscriptions {
			subs = append(subs, events.EventType(strings.TrimSpace(s)))
		}
		return &stubAgent{id: cfg.ID, subs: subs}, nil
	}
	manager := runtimemanager.NewAgentManager(bus, factory, pg)
	manager.Run(context.Background())

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	// Dashboard page should not require auth.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/dashboard", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("dashboard page status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Basic health should require auth and work with key.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/dashboard/api/health", nil))
		if w.Code != http.StatusUnauthorized && w.Code != http.StatusInternalServerError {
			t.Fatalf("expected auth failure code, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Reset DB + seed org (exercises template compile + publish + global roster spawn).
	{
		body := []byte(`{"action":"reset_db","confirm":"RESET","seed_org":true,"template_version":"2.0.1"}`)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/runtime", body))
		if w.Code != http.StatusOK {
			t.Fatalf("reset_db failed: status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Overview + agents.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/overview", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("overview status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/agents", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("agents status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Graphs: holding + template.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/graph?mode=holding", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("holding graph status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/graph?mode=template", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("template graph status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Create a vertical + spawn OpCo.
	var verticalID, verticalSlug string
	{
		body := []byte(`{"name":"TestCo","geography":"us","slug":"testco"}`)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/verticals/create", body))
		if w.Code != http.StatusOK {
			t.Fatalf("create vertical status=%d body=%s", w.Code, w.Body.String())
		}
		var resp struct {
			VerticalID string `json:"vertical_id"`
			Slug       string `json:"slug"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("unmarshal create vertical: %v", err)
		}
		verticalID = resp.VerticalID
		verticalSlug = resp.Slug
		if verticalID == "" || verticalSlug == "" {
			t.Fatalf("expected vertical_id and slug in response: %s", w.Body.String())
		}
	}

	// OpCo graph should resolve by slug.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/graph?mode=opco&vertical="+verticalSlug, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("opco graph status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Seed a human task row and a conversation row to exercise listing + detail endpoints.
	taskID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
		VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'call a customer', 'pending_review', now())
	`, taskID, verticalID); err != nil {
		t.Fatalf("seed human task: %v", err)
	}
	convID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO conversations (id, agent_id, mode, messages, summary, turn_count, status, created_at, updated_at)
		VALUES ($1::uuid, 'empire-coordinator', 'session', $2::jsonb, 'test conversation', 1, 'active', now(), now())
	`, convID, `["hello"]`); err != nil {
		t.Fatalf("seed conversation: %v", err)
	}
	// Seed a session + turn so conversation detail includes tool artifacts.
	{
		sessRowID := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO agent_sessions (id, agent_id, runtime_mode, provider, session_id, status, turn_count, created_at, last_used_at)
			VALUES ($1::uuid, 'empire-coordinator', 'cli_test', 'anthropic_cli', 's1', 'active', 1, now(), now())
		`, sessRowID); err != nil {
			t.Fatalf("seed agent session: %v", err)
		}
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO agent_turns (agent_id, session_row_id, turn_index, request_payload, response_payload, parse_ok, latency_ms, retry_count, created_at)
			VALUES (
				'empire-coordinator',
				$1::uuid,
				0,
				$2::jsonb,
				$3::jsonb,
				true,
				12,
				0,
				now()
			)
		`,
			sessRowID,
			`{"message":{"role":"tool","content":"tool_result_text"}}`,
			`{"content":[{"type":"text","text":"assistant_text"},{"type":"tool_use","name":"email_api","input":{"to":["a@example.com"]}}]}`,
		); err != nil {
			t.Fatalf("seed agent turn: %v", err)
		}
	}

	// Tasks + conversation endpoints.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/tasks?status=pending_review&limit=10", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("tasks list status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/tasks/"+taskID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("task detail status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/tasks/stats", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("task stats status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/conversations?limit=10", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("conversations list status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/conversations/empire-coordinator", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("conversation detail status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Human task actions: claim/complete/reject paths (coverage for 0% handlers).
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/tasks/"+taskID+"/claim", []byte(`{"assigned_to":"founder"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("task claim status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/tasks/"+taskID+"/complete", []byte(`{"result_text":"done","outcome":"success","follow_up_needed":true}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("task complete status=%d body=%s", w.Code, w.Body.String())
		}
		task2 := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
			VALUES ($1::uuid, 'empire-coordinator', $2::uuid, 'verification', 'call again', 'approved', now())
		`, task2, verticalID); err != nil {
			t.Fatalf("seed second human task: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/tasks/"+task2+"/reject", []byte(`{"reason":"not now"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("task reject status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Digest endpoint.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/digest?top=3", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("digest status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Event stream (SSE) coverage: use a real server so Flush works.
	{
		// Ensure at least one event exists to stream.
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO events (id, type, source_agent, payload, created_at)
			VALUES ($1::uuid, 'system.started', 'runtime', '{}'::jsonb, now())
			ON CONFLICT DO NOTHING
		`, uuid.NewString()); err != nil {
			t.Fatalf("seed stream event: %v", err)
		}
		ts := httptest.NewServer(h)
		defer ts.Close()
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/dashboard/api/events/stream?key=test-key", nil)
		cctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		req = req.WithContext(cctx)
		resp, err := ts.Client().Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
		}
	}

	// Control chat live should mark receipt processed.
	{
		body := []byte(`{"agent_id":"empire-coordinator","message":"ping","mode":"live"}`)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/chat", body))
		if w.Code != http.StatusOK {
			t.Fatalf("control chat status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Event APIs: list + detail + vertical trace.
	{
		evtID := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
			VALUES ($1::uuid, 'inbound.testco.email_received', 'inbound-gateway', $2::uuid, '{}'::jsonb, now())
		`, evtID, verticalID); err != nil {
			t.Fatalf("seed event: %v", err)
		}

		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/events?limit=5", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("events list status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/events/"+evtID, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("event detail status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/verticals/"+verticalSlug+"/trace", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("vertical trace status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Health + funnel + mailbox.
	{
		mbID := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, 'empire-coordinator', 'vertical_approval', 'critical', 'pending', '{}'::jsonb, 'test', now())
		`, mbID, verticalID); err != nil {
			t.Fatalf("seed mailbox: %v", err)
		}

		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/mailbox?status=pending&limit=10", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("mailbox status=%d body=%s", w.Code, w.Body.String())
		}
		decideBody := []byte(`{"mailbox_id":"` + mbID + `","action":"approve","notes":"ok"}`)
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", decideBody))
		if w.Code != http.StatusOK {
			t.Fatalf("mailbox decide status=%d body=%s", w.Code, w.Body.String())
		}

		// allowMethod coverage: wrong method.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/control/mailbox/decide", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected method not allowed, got %d body=%s", w.Code, w.Body.String())
		}

		// Side-effect branches: spend approval/rejection, founder input, escalation response, more-data.
		opcoCEO := "opco-ceo-" + verticalID
		// Ensure opco CEO exists (spawned by create vertical).
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
			VALUES ($1, 'stub', 'opco-ceo', 'operating', $2::uuid, 'active', '{}'::jsonb, now(), now())
			ON CONFLICT (id) DO NOTHING
		`, opcoCEO, verticalID); err != nil {
			t.Fatalf("ensure opco ceo: %v", err)
		}

		spendApprove := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, $3, 'spend_request', 'normal', 'pending', '{}'::jsonb, 'spend', now())
		`, spendApprove, verticalID, opcoCEO); err != nil {
			t.Fatalf("seed spend approve mailbox: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", []byte(`{"mailbox_id":"`+spendApprove+`","action":"approve","notes":"ok"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("spend approve decide status=%d body=%s", w.Code, w.Body.String())
		}

		spendReject := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, $3, 'spend_request', 'normal', 'pending', '{}'::jsonb, 'spend', now())
		`, spendReject, verticalID, opcoCEO); err != nil {
			t.Fatalf("seed spend reject mailbox: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", []byte(`{"mailbox_id":"`+spendReject+`","action":"reject","notes":"no"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("spend reject decide status=%d body=%s", w.Code, w.Body.String())
		}

		founderInput := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, $3, 'review', 'normal', 'pending', '{"review_type":"founder_input"}'::jsonb, 'input', now())
		`, founderInput, verticalID, opcoCEO); err != nil {
			t.Fatalf("seed founder input mailbox: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", []byte(`{"mailbox_id":"`+founderInput+`","action":"approve","notes":"yes"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("founder input decide status=%d body=%s", w.Code, w.Body.String())
		}

		escalation := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, $3, 'escalation', 'normal', 'pending', '{}'::jsonb, 'esc', now())
		`, escalation, verticalID, opcoCEO); err != nil {
			t.Fatalf("seed escalation mailbox: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", []byte(`{"mailbox_id":"`+escalation+`","action":"approve","notes":"do X"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("escalation decide status=%d body=%s", w.Code, w.Body.String())
		}

		moreData := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, $3, 'vertical_approval', 'normal', 'pending', '{}'::jsonb, 'more data', now())
		`, moreData, verticalID, opcoCEO); err != nil {
			t.Fatalf("seed more-data mailbox: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", []byte(`{"mailbox_id":"`+moreData+`","action":"more-data","notes":"need info"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("more-data decide status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/health", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("health status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/funnel", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("funnel status=%d body=%s", w.Code, w.Body.String())
		}

		// Control targets.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/control/targets", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("control targets status=%d body=%s", w.Code, w.Body.String())
		}

		// Control seed-org should be idempotent and return status.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/seed-org", []byte(`{}`)))
		if w.Code != http.StatusOK && w.Code != http.StatusMultiStatus {
			t.Fatalf("seed-org status=%d body=%s", w.Code, w.Body.String())
		}

		// Control agent actions: restart + replay.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/agents/restart", []byte(`{"agent_id":"empire-coordinator"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("agent restart status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/agents/replay", []byte(`{"agent_id":"empire-coordinator"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("agent replay status=%d body=%s", w.Code, w.Body.String())
		}

		// Control directive queues system.directive when targeting Empire Coordinator.
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO events (id, type, source_agent, payload, created_at)
			VALUES ($1::uuid, 'system.started', 'runtime', '{}'::jsonb, now())
		`, uuid.NewString()); err != nil {
			t.Fatalf("seed system.started: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/directive", []byte(`{"agent_id":"empire-coordinator","message":"hello"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("control directive status=%d body=%s", w.Code, w.Body.String())
		}

		// Control runtime: invalid confirm should reject; pause/resume/reset_state should work.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"reset_state","confirm":"NOPE"}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected bad confirm, got %d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"pause"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("pause status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"resume"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("resume status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"reset_state","confirm":"RESET"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("reset_state status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"nope"}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected invalid action badrequest, got %d body=%s", w.Code, w.Body.String())
		}

		// Control event requeue: by agent_id and all recipients.
		evtID := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
			VALUES ($1::uuid, 'board.directive', 'human', NULL, '{}'::jsonb, now())
		`, evtID); err != nil {
			t.Fatalf("seed event for requeue: %v", err)
		}
		// Ensure the recipient agent exists for FK constraints.
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
			VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
			ON CONFLICT (id) DO NOTHING
		`); err != nil {
			t.Fatalf("ensure empire-coordinator agent: %v", err)
		}
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO event_deliveries (event_id, agent_id, created_at)
			VALUES ($1::uuid, 'empire-coordinator', now())
			ON CONFLICT DO NOTHING
		`, evtID); err != nil {
			t.Fatalf("seed delivery: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"event_id":"`+evtID+`","agent_id":"empire-coordinator"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("requeue single status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"event_id":"`+evtID+`"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("requeue all status=%d body=%s", w.Code, w.Body.String())
		}

		// API alias endpoints.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/api/events?limit=3", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("api events status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/api/verticals", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("api verticals status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/api/verticals/"+verticalSlug+"/agents", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("api vertical agents status=%d body=%s", w.Code, w.Body.String())
		}
		// /api/chat async mode avoids calling manager.ChatWithAgent.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/api/chat/empire-coordinator", []byte(`{"message":"hi","mode":"async"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("api chat status=%d body=%s", w.Code, w.Body.String())
		}
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO events (id, type, source_agent, payload, created_at)
			VALUES ($1::uuid, 'system.started', 'runtime', '{}'::jsonb, now())
		`, uuid.NewString()); err != nil {
			t.Fatalf("seed system.started for api directive: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/api/directive", []byte(`{"directive_text":"do something"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("api directive status=%d body=%s", w.Code, w.Body.String())
		}

		// /api/mailbox decide path (use a fresh mailbox item).
		mbAPI := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, NULL, 'empire-coordinator', 'vertical_approval', 'normal', 'pending', '{}'::jsonb, 'api decide', now())
		`, mbAPI); err != nil {
			t.Fatalf("seed api mailbox: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/api/mailbox/"+mbAPI+"/decide", []byte(`{"action":"approve","notes":"ok"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("api mailbox decide status=%d body=%s", w.Code, w.Body.String())
		}

		// /api/budget should return caps and spent.
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO spend_ledger (id, category, amount_cents, created_at)
			VALUES ($1::uuid, 'api', 123, now())
		`, uuid.NewString()); err != nil {
			t.Fatalf("seed spend: %v", err)
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/api/budget", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("api budget status=%d body=%s", w.Code, w.Body.String())
		}

		// Graph: invalid mode.
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/graph?mode=unknown", nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected bad graph mode, got %d body=%s", w.Code, w.Body.String())
		}
	}
}
