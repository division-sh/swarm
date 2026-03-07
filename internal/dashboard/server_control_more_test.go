package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	runtimemanager "empireai/internal/runtime/manager"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

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
	manager := runtimemanager.NewAgentManager(bus, func(cfg models.AgentConfig) (runtimemanager.Agent, error) {
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

	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE agent_id='empire-coordinator' AND status='processed'`).Scan(&n)
	if n < 1 {
		t.Fatalf("expected processed receipt after live chat")
	}
}

func TestDashboardServer_ControlPlane_MoreBranches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{LockTTL: 10 * time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3},
			ClaudeCLI: config.ClaudeCLIConfig{Command: "true", OutputFormat: "json", Timeout: 1 * time.Second, Retries: 1},
		},
		Budget: config.BudgetConfig{HumanTasks: config.HumanTasksConfig{BudgetReset: "monday", MaxTasksPerWeek: 3, AutoExpireHours: 168}},
	}

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v1', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now())
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
	factory := func(cfg models.AgentConfig) (runtimemanager.Agent, error) { return &controlStubAgent{id: cfg.ID}, nil }
	manager := runtimemanager.NewAgentManager(bus, factory, pg)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	_ = manager.SpawnAgent(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", Type: "stub"})

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	{ w := httptest.NewRecorder(); h.ServeHTTP(w, authed(t, http.MethodGet, "/dashboard/api/control/targets", nil)); if w.Code != http.StatusOK { t.Fatalf("targets status=%d body=%s", w.Code, w.Body.String()) } }
	{ w := httptest.NewRecorder(); h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/agents/restart", []byte(`{"agent_id":"empire-coordinator"}`))); if w.Code != http.StatusOK { t.Fatalf("restart status=%d body=%s", w.Code, w.Body.String()) }; w = httptest.NewRecorder(); h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/agents/replay", []byte(`{"agent_id":"empire-coordinator"}`))); if w.Code != http.StatusOK { t.Fatalf("replay status=%d body=%s", w.Code, w.Body.String()) } }

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

		w := httptest.NewRecorder(); h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"event_id":"`+eventID+`","agent_id":"empire-coordinator"}`))); if w.Code != http.StatusOK { t.Fatalf("requeue single status=%d body=%s", w.Code, w.Body.String()) }
		w = httptest.NewRecorder(); h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/events/requeue", []byte(`{"event_id":"`+eventID+`"}`))); if w.Code != http.StatusOK { t.Fatalf("requeue all status=%d body=%s", w.Code, w.Body.String()) }
	}

	{ w := httptest.NewRecorder(); h.ServeHTTP(w, authed(t, http.MethodPost, "/dashboard/api/control/directive", []byte(`{"agent_id":"empire-coordinator","message":"do it"}`))); if w.Code != http.StatusOK { t.Fatalf("control directive status=%d body=%s", w.Code, w.Body.String()) } }
	{ w := httptest.NewRecorder(); h.ServeHTTP(w, authed(t, http.MethodGet, "/api/tasks/not-a-uuid", nil)); if w.Code != http.StatusBadRequest && w.Code != http.StatusNotFound && w.Code != http.StatusInternalServerError { t.Fatalf("expected 400/404/500 for invalid task id, got %d body=%s", w.Code, w.Body.String()) } }
	{ w := httptest.NewRecorder(); h.ServeHTTP(w, authed(t, http.MethodGet, "/api/mailbox/abc/decide", nil)); if w.Code != http.StatusMethodNotAllowed { t.Fatalf("expected method not allowed, got %d body=%s", w.Code, w.Body.String()) } }

	if asString(nil) != "" || asString(123) == "" { t.Fatalf("asString helper unexpected") }
	if clamp(5, 0, 3) != 3 || clamp(-1, 0, 3) != 0 || clamp(2, 0, 3) != 2 { t.Fatalf("clamp helper unexpected") }
	if strings.TrimSpace(truncate("hello", 2)) != "he..." { t.Fatalf("truncate helper unexpected") }
	if parseInt("x", 7) != 7 || parseInt("3", 7) != 3 { t.Fatalf("parseInt helper unexpected") }
	_ = json.Valid(mustJSON(map[string]any{"k": "v"}))
}

func TestDashboard_ControlMailboxDecide_EmitsSideEffects(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at) VALUES ($1::uuid, 'V', 'v1', 'us', 'operating', 'operating', now(), now())`, verticalID); err != nil { t.Fatalf("seed vertical: %v", err) }
	if _, err := db.ExecContext(ctx, `INSERT INTO agents (id, type, role, mode, vertical_id, status, config, started_at, last_active_at) VALUES ($1, 'stub', 'opco-ceo', 'operating', $2::uuid, 'active', '{}'::jsonb, now(), now()) ON CONFLICT (id) DO NOTHING`, "opco-ceo-"+verticalID, verticalID); err != nil { t.Fatalf("seed opco ceo agent: %v", err) }
	mbID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{VerticalID: verticalID, FromAgent: "operations-analyst", Type: "escalation", Priority: "normal", Status: "pending", Context: []byte(`{"x":1}`), Summary: "need direction"})
	if err != nil { t.Fatalf("InsertMailboxItem: %v", err) }
	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()
	{ w := httptest.NewRecorder(); h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{"mailbox_id": mbID})); if w.Code != http.StatusBadRequest { t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String()) } }
	{ w := httptest.NewRecorder(); h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{"mailbox_id": mbID, "action": "approve", "notes": "do the thing"})); if w.Code != http.StatusOK { t.Fatalf("decide status=%d body=%s", w.Code, w.Body.String()) } }
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='opco.escalation_response' AND vertical_id=$1::uuid`, verticalID).Scan(&n)
	if n < 1 { t.Fatalf("expected opco.escalation_response event") }
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.id = d.event_id WHERE e.type='opco.escalation_response' AND d.agent_id=$1`, "opco-ceo-"+verticalID).Scan(&n)
	if n < 1 { t.Fatalf("expected delivery to opco ceo") }
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE type='mailbox.item_decided'`).Scan(&n)
	if n < 1 { t.Fatalf("expected mailbox.item_decided event") }
}

func TestDashboard_ControlMailboxDecide_GeographyExpansionQueuesCampaign(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at) VALUES ($1::uuid, 'V', 'v1', 'us', 'operating', 'operating', now(), now())`, verticalID); err != nil { t.Fatalf("seed vertical: %v", err) }
	if _, err := db.ExecContext(ctx, `INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at) VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now()) ON CONFLICT (id) DO NOTHING`); err != nil { t.Fatalf("seed empire coordinator: %v", err) }
	mbID, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{VerticalID: verticalID, FromAgent: "opco-ceo-" + verticalID, Type: "domain_approval", Priority: "normal", Status: "pending", Context: []byte(`{"review_type":"geography_expansion","geography":"Asuncion, Paraguay","country":"PY","mode":"saas_gap","categories":["financial_ops"],"priority":"high"}`), Summary: "expand to Paraguay"})
	if err != nil { t.Fatalf("InsertMailboxItem: %v", err) }
	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()
	w := httptest.NewRecorder(); h.ServeHTTP(w, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{"mailbox_id": mbID, "action": "approve", "notes": "run validation"}))
	if w.Code != http.StatusOK { t.Fatalf("decide status=%d body=%s", w.Code, w.Body.String()) }
	var n int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM geographies WHERE lower(name)=lower('Asuncion, Paraguay')`).Scan(&n)
	if n < 1 { t.Fatalf("expected geography inserted") }
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_campaigns WHERE mode='saas_gap'`).Scan(&n)
	if n < 1 { t.Fatalf("expected scan campaign queued") }
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.id = d.event_id WHERE e.type='geography.expansion_queued' AND d.agent_id='empire-coordinator'`).Scan(&n)
	if n < 1 { t.Fatalf("expected geography.expansion_queued delivery to empire-coordinator") }
}

func TestDashboard_ControlMailboxDecide_VerticalApprovalEmitsLifecycleEvents(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	t.Setenv("EMPIREAI_API_KEY", "test-key")
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at) VALUES ($1::uuid, 'V', 'v3', 'us', 'branding', 'factory', now(), now())`, verticalID); err != nil { t.Fatalf("seed vertical: %v", err) }
	if _, err := db.ExecContext(ctx, `INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at) VALUES ('empire-coordinator', 'stub', 'empire-coordinator', 'holding', 'active', '{}'::jsonb, now(), now()) ON CONFLICT (id) DO NOTHING`); err != nil { t.Fatalf("seed empire coordinator: %v", err) }
	makeMailbox := func(summary string) string { id, err := pg.InsertMailboxItem(ctx, runtimetools.MailboxItem{VerticalID: verticalID, FromAgent: "validation-coordinator", Type: "vertical_approval", Priority: "high", Status: "pending", Context: []byte(`{"validation_kit":"ok"}`), Summary: summary}); if err != nil { t.Fatalf("InsertMailboxItem(%s): %v", summary, err) }; return id }
	srv := NewServer(db, &config.Config{}, pg, pg, nil)
	h := srv.Handler()
	approvedID := makeMailbox("approve path")
	w1 := httptest.NewRecorder(); h.ServeHTTP(w1, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{"mailbox_id": approvedID, "action": "approve", "notes": "approved"})); if w1.Code != http.StatusOK { t.Fatalf("approve decide status=%d body=%s", w1.Code, w1.Body.String()) }
	rejectedID := makeMailbox("reject path")
	w2 := httptest.NewRecorder(); h.ServeHTTP(w2, authedReqAny(t, http.MethodPost, "/dashboard/api/control/mailbox/decide", map[string]any{"mailbox_id": rejectedID, "action": "reject", "notes": "rejected"})); if w2.Code != http.StatusOK { t.Fatalf("reject decide status=%d body=%s", w2.Code, w2.Body.String()) }
	for _, typ := range []string{"vertical.approved", "vertical.killed"} { var n int; if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.id = d.event_id WHERE e.type = $1 AND d.agent_id = 'empire-coordinator'`, typ).Scan(&n); err != nil { t.Fatalf("count %s deliveries: %v", typ, err) }; if n < 1 { t.Fatalf("expected %s delivery to empire-coordinator", typ) } }
}
