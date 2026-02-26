package dashboard

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
)

func TestDashboardServer_ErrorBranches(t *testing.T) {
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
	}

	bus := rt.NewEventBus(pg)
	factory := func(cfg models.AgentConfig) (rt.Agent, error) {
		return &stubAgent{id: cfg.ID, subs: nil}, nil
	}
	manager := rt.NewAgentManager(bus, factory, pg)
	manager.Run(context.Background())

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	// Wrong method should 405.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/overview", []byte(`{}`)))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Invalid JSON bodies should 400.
	{
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/runtime", strings.NewReader("{"))
		req.Header.Set("X-Empire-Key", "test-key")
		req.Header.Set("Content-Type", "application/json")
		h.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Create vertical: missing required fields.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/verticals/create", []byte(`{"name":"","geography":""}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Agent restart: missing agent_id.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/agents/restart", []byte(`{}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Event requeue: missing event_id.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"agent_id":"empire-coordinator"}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Graph: missing/invalid params.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/graph", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("expected default graph ok, got %d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodGet, "/dashboard/api/graph?mode=opco", nil))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected opco missing vertical 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// API chat: missing message should 400.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/api/chat/empire-coordinator", []byte(`{"mode":"async"}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// API mailbox: invalid path and missing action.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authedReq(t, http.MethodPost, "/api/mailbox/xxx", []byte(`{}`)))
		if w.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d body=%s", w.Code, w.Body.String())
		}
	}
}
