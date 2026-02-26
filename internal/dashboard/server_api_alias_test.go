package dashboard

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func repoRootAlias(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

type aliasStubAgent struct{ id string }

func (a *aliasStubAgent) ID() string                        { return a.id }
func (a *aliasStubAgent) Type() string                      { return "stub" }
func (a *aliasStubAgent) Subscriptions() []events.EventType { return nil }
func (a *aliasStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (a *aliasStubAgent) BoardStep(context.Context, string) (string, error) { return "ACK", nil }

func apiReq(t *testing.T, method, path string, body []byte) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("X-Empire-Key", "test-key")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func seedAgentRow(t *testing.T, db *sql.DB, id, role, mode, status, verticalID string) {
	t.Helper()
	if verticalID == "" {
		verticalID = uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
			VALUES ($1::uuid, 'V', $2, 'us', 'discovered', 'factory', now(), now())
			ON CONFLICT (id) DO NOTHING
		`, verticalID, "v-"+verticalID[:8]); err != nil {
			t.Fatalf("seed vertical for agent: %v", err)
		}
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at)
		VALUES ($1, 'stub', $2, $3, $4::uuid, $5, '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`, id, role, mode, verticalID, status); err != nil {
		t.Fatalf("seed agent row: %v", err)
	}
}

func TestDashboardServer_APIAliases_Work(t *testing.T) {
	root := repoRootAlias(t)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	t.Setenv("EMPIREAI_API_KEY", "test-key")

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
		Budget: config.BudgetConfig{
			FactoryMonthlyCap:     50000,
			PerVerticalMonthlyCap: 20000,
			PortfolioMonthlyCap:   100000,
		},
	}

	// Seed required agents so event delivery FK constraints don't explode.
	seedAgentRow(t, db, "empire-coordinator", "empire-coordinator", "holding", "active", "")

	bus := rt.NewEventBus(pg)
	factory := func(cfg models.AgentConfig) (rt.Agent, error) { //nolint:revive
		return &aliasStubAgent{id: cfg.ID}, nil
	}
	manager := rt.NewAgentManager(bus, factory, pg)
	manager.Run(context.Background())
	// Also register the stub in memory so ChatWithAgent works.
	_ = manager.SpawnAgent(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", Type: "stub"})

	s := NewServer(db, cfg, pg, pg, manager)
	h := s.Handler()

	// Create one vertical and one mailbox item.
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Acme', 'acme', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	mbID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES ($1::uuid, $2::uuid, 'empire-coordinator', 'vertical_decision', 'critical', 'pending', '{}'::jsonb, 'test', now())
	`, mbID, verticalID); err != nil {
		t.Fatalf("seed mailbox: %v", err)
	}
	// Seed spend ledger for budget endpoint.
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO spend_ledger (vertical_id, category, amount_cents, created_at)
		VALUES ($1::uuid, 'api', 1234, now())
	`, verticalID); err != nil {
		t.Fatalf("seed spend: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
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
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO runtime_log (ts, level, component, action, event_type, vertical_id, detail)
		VALUES (now(), 'info', 'eventbus', 'published', 'system.started', $1::uuid, '{}'::jsonb)
	`, verticalID); err != nil {
		t.Fatalf("seed runtime_log: %v", err)
	}

	// /api/verticals
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodGet, "/api/verticals", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("verticals status=%d body=%s", w.Code, w.Body.String())
		}
	}
	// /api/verticals/:id/agents by slug + by id.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodGet, "/api/verticals/acme/agents", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("vertical agents by slug status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodGet, "/api/verticals/"+verticalID+"/agents", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("vertical agents by id status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// /api/budget
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodGet, "/api/budget", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("budget status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// /api/directive queues event + delivery.
	{
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO events (id, type, source_agent, payload, created_at)
			VALUES ($1::uuid, 'system.started', 'runtime', '{}'::jsonb, now())
		`, uuid.NewString()); err != nil {
			t.Fatalf("seed system.started: %v", err)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodPost, "/api/directive", []byte(`{"directive_text":"do thing"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("directive status=%d body=%s", w.Code, w.Body.String())
		}
		var resp struct {
			OK      bool   `json:"ok"`
			EventID string `json:"event_id"`
		}
		_ = json.Unmarshal(w.Body.Bytes(), &resp)
		if !resp.OK || strings.TrimSpace(resp.EventID) == "" {
			t.Fatalf("unexpected directive response: %s", w.Body.String())
		}
	}

	// /api/chat/:agent should also call ChatWithAgent (BoardStep) when mode is live.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodPost, "/api/chat/empire-coordinator", []byte(`{"message":"ping"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("chat status=%d body=%s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "ACK") {
			t.Fatalf("expected ACK in response: %s", w.Body.String())
		}
	}

	// /api/events list.
	{
		evtID := uuid.NewString()
		if _, err := db.ExecContext(context.Background(), `
			INSERT INTO events (id, type, source_agent, vertical_id, payload, created_at)
			VALUES ($1::uuid, 'system.started', 'runtime', $2::uuid, '{}'::jsonb, now())
		`, evtID, verticalID); err != nil {
			t.Fatalf("seed event: %v", err)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodGet, "/api/events?limit=5", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("events status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// /api/runtime/logs list.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodGet, "/api/runtime/logs?component=eventbus&limit=5", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("runtime logs status=%d body=%s", w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), `"runtime_logs"`) {
			t.Fatalf("expected runtime_logs payload: %s", w.Body.String())
		}
	}

	// /api/events?stream=true uses SSE path (query param key fallback also exercised).
	{
		ts := httptest.NewServer(h)
		defer ts.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, ts.URL+"/api/events?stream=true&key=test-key", nil)
		resp, err := ts.Client().Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
		}
	}

	// /api/mailbox/:id/decide
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, apiReq(t, http.MethodPost, "/api/mailbox/"+mbID+"/decide", []byte(`{"action":"approve","notes":"ok"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("mailbox decide status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Make sure the repo-root env vars are unused here but keep them available for future test expansions.
	_ = root
}
