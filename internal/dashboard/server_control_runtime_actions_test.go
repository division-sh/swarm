package dashboard

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	goruntime "runtime"
	"testing"
	"time"

	"empireai/internal/config"
	rt "empireai/internal/runtime"
	models "empireai/internal/runtime/actors"
	runtimemanager "empireai/internal/runtime/manager"
	"empireai/internal/store"
	"empireai/internal/testutil"
)

func repoRootRuntimeActions(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatalf("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

func keyReq(method, path string, body []byte) *http.Request {
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("X-Empire-Key", "test-key")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return req
}

func TestDashboardServer_ControlRuntime_Actions(t *testing.T) {
	root := repoRootRuntimeActions(t)
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
	}

	bus := rt.NewEventBus(pg)
	factory := func(cfg models.AgentConfig) (runtimemanager.Agent, error) { //nolint:revive
		return &controlStubAgent{id: cfg.ID}, nil
	}
	manager := runtimemanager.NewAgentManager(bus, factory, pg)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	// invalid action
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, keyReq(http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"nope"}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// pause + resume
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, keyReq(http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"pause"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("pause status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, keyReq(http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"resume"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("resume status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// reset_state confirm required
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, keyReq(http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"reset_state","confirm":"nope"}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("reset_state confirm status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, keyReq(http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"reset_state","confirm":"RESET"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("reset_state status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// reset_db confirm required + seed_org false path (no YAML agent seeding).
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, keyReq(http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"reset_db","confirm":"nope"}`)))
		if w.Code != http.StatusBadRequest {
			t.Fatalf("reset_db confirm status=%d body=%s", w.Code, w.Body.String())
		}
		w = httptest.NewRecorder()
		h.ServeHTTP(w, keyReq(http.MethodPost, "/dashboard/api/control/runtime", []byte(`{"action":"reset_db","confirm":"RESET","seed_org":false,"template_version":"2.0.1"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("reset_db seed_org=false status=%d body=%s", w.Code, w.Body.String())
		}
	}
}
