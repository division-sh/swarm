package dashboard

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"empireai/internal/config"
	rt "empireai/internal/runtime"
	models "empireai/internal/runtime/actors"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestDashboard_APIAgentPromptLifecycle(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	bus := rt.NewEventBus(pg)
	manager := runtimemanager.NewAgentManager(bus, nil, pg)
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS prompt_overrides (
			agent_id TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
			prompt TEXT NOT NULL,
			previous_prompt TEXT,
			source TEXT,
			notes TEXT,
			created_at TIMESTAMPTZ DEFAULT now(),
			updated_at TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("ensure prompt_overrides table: %v", err)
	}
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:   "empire-coordinator",
		Type: "stub",
		Role: "empire-coordinator",
		Mode: "holding",
		Config: mustJSON(map[string]any{
			"system_prompt": "Template system prompt",
			"tools":         []string{"mailbox_send"},
			"subscriptions": []string{"system.started"},
		}),
	}); err != nil {
		t.Fatalf("spawn test agent: %v", err)
	}

	srv := NewServer(db, &config.Config{}, pg, pg, manager)
	h := srv.Handler()

	// GET current prompt.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/agents/empire-coordinator/prompt", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("get prompt status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// PUT validation error for empty prompt.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/api/agents/empire-coordinator/prompt", bytes.NewReader([]byte(`{"prompt":"  "}`)))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 on empty prompt, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// PUT valid override.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/api/agents/empire-coordinator/prompt", bytes.NewReader([]byte(`{"prompt":"Override prompt","source":"tests","notes":"coverage"}`)))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("put override status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// GET diff path exercises renderPromptDiff.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/agents/empire-coordinator/prompt/diff", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("diff status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// DELETE override.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/api/agents/empire-coordinator/prompt", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("delete override status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Dashboard-prefixed alias should route to the same handler.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/dashboard/api/agents/empire-coordinator/prompt", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("dashboard alias status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Invalid nested path => 404.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/agents/empire-coordinator/not-prompt", nil)
		req.Header.Set("X-Empire-Key", "test-key")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404 for invalid agent prompt path, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Method not allowed branch.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/agents/empire-coordinator/prompt", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 for unsupported method, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Ensure override row was removed by DELETE.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM prompt_overrides WHERE agent_id = 'empire-coordinator'`).Scan(&count); err != nil {
		t.Fatalf("count prompt overrides: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected no prompt override row after delete, got count=%d", count)
	}
}

func TestDashboard_APIAgentPrompt_ManagerUnavailable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents/empire-coordinator/prompt", nil)
	req.Header.Set("X-Empire-Key", "test-key")
	h.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 when manager unavailable, got %d body=%s", w.Code, w.Body.String())
	}
}
