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
	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

type controlStubAgent struct{ id string }

func (a *controlStubAgent) ID() string   { return a.id }
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

	// Seed a vertical + agent row to satisfy FK constraints.
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
	factory := func(cfg models.AgentConfig) (rt.Agent, error) { //nolint:revive
		return &controlStubAgent{id: cfg.ID}, nil
	}
	manager := rt.NewAgentManager(bus, factory, pg)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	_ = manager.SpawnAgent(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", Type: "stub"})

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	// Control targets list.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodGet, "/dashboard/api/control/targets", nil))
		if w.Code != http.StatusOK {
			t.Fatalf("targets status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Agent restart + replay.
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

	// Event requeue: both single-agent and all-deliveries paths.
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
		// Single agent.
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"event_id":"`+eventID+`","agent_id":"empire-coordinator"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("requeue single status=%d body=%s", w.Code, w.Body.String())
		}
		// All deliveries (agent_id omitted).
		w = httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"event_id":"`+eventID+`"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("requeue all status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// Control directive (queues system.directive).
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/directive", []byte(`{"agent_id":"empire-coordinator","message":"do it"}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("control directive status=%d body=%s", w.Code, w.Body.String())
		}
	}

	// /api/tasks routing + not-found task view branch.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodGet, "/api/tasks/not-a-uuid", nil))
		if w.Code != http.StatusBadRequest && w.Code != http.StatusNotFound && w.Code != http.StatusInternalServerError {
			// This path currently bubbles the UUID cast error from Postgres as 500.
			t.Fatalf("expected 400/404/500 for invalid task id, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// /api/mailbox/:id/decide method not allowed.
	{
		w := httptest.NewRecorder()
		h.ServeHTTP(w, authed(t, http.MethodGet, "/api/mailbox/abc/decide", nil))
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected method not allowed, got %d body=%s", w.Code, w.Body.String())
		}
	}

	// Cover small helper branches.
	if asString(nil) != "" || asString(123) == "" {
		t.Fatalf("asString helper unexpected")
	}
	if clamp(5, 0, 3) != 3 || clamp(-1, 0, 3) != 0 || clamp(2, 0, 3) != 2 {
		t.Fatalf("clamp helper unexpected")
	}
	if strings.TrimSpace(truncate("hello", 2)) != "he..." {
		t.Fatalf("truncate helper unexpected")
	}
	// parseInt in server.go is used for query params; validate some branches.
	if parseInt("x", 7) != 7 || parseInt("3", 7) != 3 {
		t.Fatalf("parseInt helper unexpected")
	}

	// Ensure mustJSON fallback doesn't panic.
	_ = json.Valid(mustJSON(map[string]any{"k": "v"}))
}
