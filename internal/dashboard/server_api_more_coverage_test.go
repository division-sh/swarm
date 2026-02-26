package dashboard

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
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

type chatStubAgent struct{ id string }

func (a *chatStubAgent) ID() string   { return a.id }
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

	// /api/verticals/:id/agents
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

	// /api/directive missing text -> 400.
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

	// /api/directive before system.started -> 409.
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

	// /api/directive requires runtime manager.
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

	// Seed agent row for lookupControlTarget + FK deliveries.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed agent: %v", err)
	}

	bus := rt.NewEventBus(pg)
	manager := rt.NewAgentManager(bus, func(cfg models.AgentConfig) (rt.Agent, error) { //nolint:revive
		return &chatStubAgent{id: cfg.ID}, nil
	}, pg)
	manager.Run(ctx)
	t.Cleanup(func() { _ = manager.Shutdown() })
	_ = manager.SpawnAgent(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", Type: "stub"})

	srv := NewServer(db, &config.Config{}, pg, pg, manager)
	h := srv.Handler()

	// Live mode hits ChatWithAgent + upsertEventReceipt.
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

	// Async mode skips ChatWithAgent.
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
