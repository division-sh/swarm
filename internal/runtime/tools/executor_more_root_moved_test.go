package tools_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	runtimetestkit "empireai/internal/runtime/testkit"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestRuntimeToolExecutor_EndToEndActions_AgentMessage_Schedule_Routing_Hire_Fire_Reconfigure_Mailbox_HumanTasks(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', '{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	bus := NewEventBus(InMemoryEventStore{})
	storeStub := &managerStoreStub{}
	factory := func(cfg models.AgentConfig) (Agent, error) {
		return &stubAgent{id: cfg.ID, typ: cfg.Type, subs: []events.EventType{"*"}}, nil
	}
	manager := NewAgentManager(bus, factory, storeStub)

	schedStore := &scheduleStoreStub{}
	scheduler := NewScheduler(func(sc Schedule) {})
	defer scheduler.Stop()

	mailboxCap := &mailboxStoreCapture{}
	exec := NewRuntimeToolExecutor(bus, scheduler, manager, schedStore)
	exec.SetSQLDB(db)
	exec.SetMailboxStore(mailboxCap)
	exec.SetConfig(&config.Config{
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{
				MaxTasksPerWeek: 1,
				BudgetReset:     "monday",
				CategoriesEnabled: []string{
					"verification",
				},
			},
		},
	})

	targetID := "backend-agent-" + verticalID
	if err := manager.SpawnAgent(models.AgentConfig{
		ID:         targetID,
		Type:       "stub",
		Role:       "backend-agent",
		Mode:       "operating",
		VerticalID: verticalID,
		Config:     json.RawMessage(`{"system_prompt":"x","tools":["*"],"subscriptions":["*"]}`),
	}); err != nil {
		t.Fatalf("spawn target: %v", err)
	}
	targetCh := bus.Subscribe(targetID, "agent.message")

	actor := models.AgentConfig{
		ID:         "opco-ceo-" + verticalID,
		Type:       "stub",
		Role:       "opco-ceo",
		Mode:       "operating",
		VerticalID: verticalID,
		Config:     json.RawMessage(`{"system_prompt":"x","tools":["agent_message","schedule","configure_routing","agent_hire","agent_fire","agent_reconfigure","mailbox_send","human_task_request","human_task_decide"]}`),
	}
	if err := manager.SpawnAgent(actor); err != nil {
		t.Fatalf("spawn actor: %v", err)
	}
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_message", map[string]any{
		"to":      targetID,
		"message": "hi",
	}); err != nil {
		t.Fatalf("agent_message: %v", err)
	}
	select {
	case <-targetCh:
	case <-time.After(1 * time.Second):
		t.Fatal("expected message delivered to target")
	}

	if _, err := exec.Execute(WithActor(ctx, actor), "schedule", map[string]any{
		"action":        "timer.test",
		"delay_seconds": 10,
		"context":       map[string]any{"x": 1},
	}); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if schedStore.upserts == 0 {
		t.Fatal("expected scheduleStore upsert")
	}

	if _, err := exec.Execute(WithActor(ctx, actor), "configure_routing", map[string]any{
		"operation":     "add",
		"event_type":    "opco.*",
		"subscriber_id": targetID,
		"reason":        "test",
	}); err != nil {
		t.Fatalf("configure_routing: %v", err)
	}

	hiredID := "qa-agent-" + verticalID
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_hire", map[string]any{
		"agent_id":      hiredID,
		"role":          "qa-agent",
		"system_prompt": "x",
	}); err != nil {
		t.Fatalf("agent_hire: %v", err)
	}
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_reconfigure", map[string]any{
		"agent_id": hiredID,
		"config": map[string]any{
			"id":          hiredID,
			"type":        "stub",
			"role":        "qa-agent",
			"mode":        "operating",
			"vertical_id": verticalID,
			"config":      json.RawMessage(`{"system_prompt":"updated"}`),
		},
	}); err != nil {
		t.Fatalf("agent_reconfigure: %v", err)
	}
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_fire", map[string]any{"agent_id": hiredID, "reason": "test"}); err != nil {
		t.Fatalf("agent_fire: %v", err)
	}

	if _, err := exec.Execute(WithActor(ctx, actor), "mailbox_send", map[string]any{
		"type":     "review",
		"priority": "critical",
		"subject":  "please review",
		"payload":  map[string]any{"a": 1},
	}); err != nil {
		t.Fatalf("mailbox_send: %v", err)
	}
	if mailboxCap.last.Type != "review" || mailboxCap.last.Priority != "critical" {
		t.Fatalf("unexpected mailbox item: %+v", mailboxCap.last)
	}

	out, err := exec.Execute(WithActor(ctx, actor), "human_task_request", map[string]any{
		"category":    "verification",
		"description": "call someone",
		"priority":    "normal",
		"deadline":    time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
		"talking_points": []string{
			"x",
		},
	})
	if err != nil {
		t.Fatalf("human_task_request: %v", err)
	}
	taskID, _ := out.(map[string]any)["task_id"].(string)
	if taskID == "" {
		t.Fatalf("expected task_id in output, got %v", out)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, reviewed_at, created_at)
		VALUES ($1::uuid, $2, $3::uuid, 'verification', 'x', 'approved', now(), now())
	`, uuid.NewString(), actor.ID, verticalID); err != nil {
		t.Fatalf("seed approved task: %v", err)
	}

	coordinator := models.AgentConfig{
		ID:     "empire-coordinator",
		Type:   "stub",
		Role:   "empire-coordinator",
		Mode:   "holding",
		Config: json.RawMessage(`{"system_prompt":"x","tools":["human_task_decide"]}`),
	}
	if err := manager.SpawnAgent(coordinator); err != nil {
		t.Fatalf("spawn coordinator: %v", err)
	}
	decOut, err := exec.Execute(WithActor(ctx, coordinator), "human_task_decide", map[string]any{
		"task_id":      taskID,
		"decision":     "approve",
		"reason":       "ok",
		"requeue_date": time.Now().Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("human_task_decide: %v", err)
	}
	if status, _ := decOut.(map[string]any)["status"].(string); status == "approved" {
		t.Fatalf("expected budget enforcement to defer, got %v", decOut)
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()
	t.Setenv("EMPIREAI_EXTERNAL_PROXY_BASE_URL", ts.URL)
	_, _ = exec.Execute(WithActor(ctx, actor), "domain_availability_check", map[string]any{"domain": "example.com"})
}

func TestAuthorizeRouting_AllBranches(t *testing.T) {
	targetProduct := models.AgentConfig{Role: "backend-agent"}
	targetGrowth := models.AgentConfig{Role: "marketing-agent"}
	targetEng := models.AgentConfig{Role: "qa-agent"}
	targetBad := models.AgentConfig{Role: "random"}

	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "opco-ceo"}, targetBad, "active"); err != nil {
		t.Fatalf("opco-ceo should allow: %v", err)
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "empire-coordinator"}, targetBad, "active"); err != nil {
		t.Fatalf("empire-coordinator should allow: %v", err)
	}

	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "chief-of-staff"}, targetBad, "active"); err == nil {
		t.Fatalf("chief-of-staff should reject non-proposed")
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "chief-of-staff"}, targetBad, "proposed"); err != nil {
		t.Fatalf("chief-of-staff proposed should allow: %v", err)
	}

	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "vp-product"}, targetProduct, "active"); err != nil {
		t.Fatalf("vp-product product target should allow: %v", err)
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "vp-product"}, targetGrowth, "active"); err == nil {
		t.Fatalf("vp-product should reject growth target")
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "vp-growth"}, targetGrowth, "active"); err != nil {
		t.Fatalf("vp-growth growth target should allow: %v", err)
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "vp-growth"}, targetBad, "active"); err == nil {
		t.Fatalf("vp-growth should reject unknown target")
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "cto-agent"}, targetEng, "active"); err != nil {
		t.Fatalf("cto-agent eng target should allow: %v", err)
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "cto-agent"}, targetGrowth, "active"); err == nil {
		t.Fatalf("cto-agent should reject growth target")
	}

	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "support-agent"}, targetBad, "active"); err == nil {
		t.Fatalf("expected unauthorized routing role error")
	}
}

func TestAuthorizeManage_AllBranches(t *testing.T) {

	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "empire-coordinator"}, "anything", "v"); err != nil {
		t.Fatalf("empire-coordinator should allow: %v", err)
	}

	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v2"); err == nil {
		t.Fatalf("expected cross-vertical restriction")
	}

	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v1"); err != nil {
		t.Fatalf("opco-ceo should allow: %v", err)
	}

	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "vp-product"}, "backend-agent", ""); err != nil {
		t.Fatalf("vp-product should manage product agents: %v", err)
	}
	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "vp-product"}, "marketing-agent", ""); err == nil {
		t.Fatalf("vp-product should reject growth agent")
	}
	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "vp-growth"}, "marketing-agent", ""); err != nil {
		t.Fatalf("vp-growth should manage growth agents: %v", err)
	}
	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "vp-growth"}, "backend-agent", ""); err == nil {
		t.Fatalf("vp-growth should reject product agent")
	}
	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "cto-agent"}, "qa-agent", ""); err != nil {
		t.Fatalf("cto-agent should manage eng agents: %v", err)
	}
	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "cto-agent"}, "marketing-agent", ""); err == nil {
		t.Fatalf("cto-agent should reject non-eng")
	}

	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "support-agent"}, "support-agent", ""); err == nil {
		t.Fatalf("expected unauthorized manage role")
	}
}

func TestDecryptCredentialValue_AndLoadVerticalCredentials_Branches(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ex.SetSQLDB(db)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'operating', 'operating', $2::jsonb, now(), now())
	`, verticalID, `{"api_key":"enc::"}`); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "")
	if got := ex.DecryptCredentialValueForTest(ctx, "enc::AAAA"); got != "enc::AAAA" {
		t.Fatalf("expected passthrough without key, got %#v", got)
	}

	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")
	if got := ex.DecryptCredentialValueForTest(ctx, "enc::"); got != "" {
		t.Fatalf("expected empty encoded to return empty string, got %#v", got)
	}

	if got := ex.DecryptCredentialValueForTest(ctx, "enc::not-base64"); got != "enc::not-base64" {
		t.Fatalf("expected passthrough on decrypt error, got %#v", got)
	}

	// Valid encrypted value round-trip.
	var enc string
	if err := db.QueryRowContext(ctx, `SELECT encode(pgp_sym_encrypt('secret', 'k'), 'base64')`).Scan(&enc); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if got := ex.DecryptCredentialValueForTest(ctx, "enc::"+enc); got != "secret" {
		t.Fatalf("expected decrypted secret, got %#v", got)
	}

	ex2 := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	if _, err := ex2.LoadVerticalCredentialsForTest(ctx, verticalID); err == nil {
		t.Fatalf("expected sql db not configured")
	}
	ex2.SetSQLDB(db)
	if _, err := ex2.LoadVerticalCredentialsForTest(ctx, ""); err == nil {
		t.Fatalf("expected vertical_id required")
	}
	verticalBad := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'vb', 'us', 'operating', 'operating', $2::jsonb, now(), now())
	`, verticalBad, "\"x\""); err != nil {
		t.Fatalf("seed bad creds: %v", err)
	}
	if _, err := ex2.LoadVerticalCredentialsForTest(ctx, verticalBad); err == nil {
		t.Fatalf("expected decode vertical credentials error")
	}
}

func TestRuntimeToolExecutor_DecryptCredentialValue_DBNilBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ctx := context.Background()

	if got := ex.DecryptCredentialValueForTest(ctx, "plain"); got.(string) != "plain" {
		t.Fatalf("expected pass-through, got %#v", got)
	}

	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")
	if got := ex.DecryptCredentialValueForTest(ctx, "enc::AAAA"); got.(string) != "enc::AAAA" {
		t.Fatalf("expected encrypted passthrough when db missing, got %#v", got)
	}

	m := map[string]any{"a": "enc::AAAA", "b": []any{"enc::AAAA", "x"}, "c": 1}
	out := ex.DecryptCredentialValueForTest(ctx, m).(map[string]any)
	if out["a"].(string) != "enc::AAAA" {
		t.Fatalf("unexpected map decrypt: %#v", out)
	}
	if out["c"].(int) != 1 {
		t.Fatalf("unexpected passthrough: %#v", out)
	}
}

func TestToolExecutor_HelperFunctions_MoreBranches(t *testing.T) {

	txt := SafeTelemetryText(map[string]any{
		"token": "super-secret",
		"fn":    func() {},
	})
	if !strings.Contains(txt, "[REDACTED]") {
		t.Fatalf("expected redaction in telemetry, got %q", txt)
	}
	largePayload := map[string]any{}
	for i := 0; i < 80; i++ {
		largePayload[fmt.Sprintf("k_%d", i)] = strings.Repeat("x", 90)
	}
	largeText := SafeTelemetryText(largePayload)
	if len(largeText) <= 400 {
		t.Fatalf("expected telemetry truncation budget > 400 chars, got len=%d", len(largeText))
	}
	if len(largeText) > 1003 {
		t.Fatalf("expected telemetry capped at 1000 (+ellipsis), got len=%d", len(largeText))
	}

	if got := TruncateTelemetry("abc", 0); got != "abc" {
		t.Fatalf("expected no-op when max<=0, got %q", got)
	}
	if got := TruncateTelemetry("abc", 10); got != "abc" {
		t.Fatalf("expected no truncation, got %q", got)
	}
	if got := TruncateTelemetry("abcdef", 3); got != "abc..." {
		t.Fatalf("expected truncation, got %q", got)
	}

	if asString(nil) != "" {
		t.Fatalf("expected empty for nil")
	}
	if asString(123) != "123" {
		t.Fatalf("expected fmt fallback, got %q", asString(123))
	}

	if DefaultExternalMethod("domain_availability_check") != http.MethodGet {
		t.Fatalf("expected GET")
	}
	if DefaultExternalMethod("domain_purchase") != http.MethodPost {
		t.Fatalf("expected POST")
	}

	req, _ := http.NewRequest(http.MethodGet, "http://x", nil)
	ApplyExternalHeaders(req, map[string]any{
		" X ":  " y ",
		"":     "z",
		"noop": "",
	})
	if req.Header.Get("X") != "y" {
		t.Fatalf("expected header X=y, got %q", req.Header.Get("X"))
	}
	if req.Header.Get("noop") != "" {
		t.Fatalf("expected noop to be skipped")
	}

	req2, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	ApplyExternalCredentialHeaders(req2, map[string]any{
		"api_key": "k1",
		"headers": map[string]any{"X-Extra": "v"},
	}, "dns_configure")
	if req2.Header.Get("X-Extra") != "v" {
		t.Fatalf("expected X-Extra set")
	}
	if got := req2.Header.Get("Authorization"); got != "Bearer k1" {
		t.Fatalf("expected bearer auth, got %q", got)
	}

	req3, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	ApplyExternalCredentialHeaders(req3, map[string]any{
		"auth_header": "X-Api-Key",
		"token":       "t1",
	}, "whatsapp_business_api")
	if got := req3.Header.Get("X-Api-Key"); got != "t1" {
		t.Fatalf("expected custom auth header, got %q", got)
	}

	req4, _ := http.NewRequest(http.MethodPost, "http://x", nil)
	ApplyExternalCredentialHeaders(req4, map[string]any{
		"token": "Bearer z",
	}, "instagram_api")
	if got := req4.Header.Get("Authorization"); got != "Bearer z" {
		t.Fatalf("expected preserved bearer token, got %q", got)
	}

	if got := ParseExternalResponseBody(nil).(map[string]any); len(got) != 0 {
		t.Fatalf("expected empty map, got %#v", got)
	}
	if got := ParseExternalResponseBody([]byte(`{"ok":true}`)).(map[string]any)["ok"]; got != true {
		t.Fatalf("expected parsed json, got %#v", got)
	}
	if got := ParseExternalResponseBody([]byte(" hi ")).(string); got != "hi" {
		t.Fatalf("expected trimmed string, got %q", got)
	}
}

type captureEventStore struct {
	events     []events.Event
	deliveries int
}

func (c *captureEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	c.events = append(c.events, evt)
	return nil
}

func (c *captureEventStore) InsertEventDeliveries(_ context.Context, _ string, agentIDs []string) error {
	c.deliveries += len(agentIDs)
	return nil
}

func TestExecHumanTaskDecide_ValidationErrors(t *testing.T) {
	ctx := context.Background()

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	if _, err := ex.ExecHumanTaskDecideDirect(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "sql db is not configured") {
		t.Fatalf("expected db error, got %v", err)
	}

	_, db, _ := testutil.StartPostgres(t)
	ex2 := NewRuntimeToolExecutor(nil, nil, nil)
	ex2.SetSQLDB(db)
	if _, err := ex2.ExecHumanTaskDecideDirect(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "event bus is not configured") {
		t.Fatalf("expected bus error, got %v", err)
	}

	ex3 := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ex3.SetSQLDB(db)
	if _, err := ex3.ExecHumanTaskDecideDirect(ctx, models.AgentConfig{ID: "a", Role: "opco-ceo"}, map[string]any{
		"task_id": "t", "decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "not authorized") {
		t.Fatalf("expected auth error, got %v", err)
	}

	if _, err := ex3.ExecHumanTaskDecideDirect(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"decision": "approve",
	}); err == nil || !strings.Contains(err.Error(), "task_id is required") {
		t.Fatalf("expected task_id required, got %v", err)
	}
	if _, err := ex3.ExecHumanTaskDecideDirect(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t",
	}); err == nil || !strings.Contains(err.Error(), "decision is required") {
		t.Fatalf("expected decision required, got %v", err)
	}
	if _, err := ex3.ExecHumanTaskDecideDirect(ctx, models.AgentConfig{ID: "ec", Role: "empire-coordinator"}, map[string]any{
		"task_id": "t", "decision": "weird",
	}); err == nil || !strings.Contains(err.Error(), "unknown decision") {
		t.Fatalf("expected unknown decision, got %v", err)
	}
}

func TestExecHumanTaskDecide_ApproveRejectAndBudgetDeferral(t *testing.T) {
	ctx := context.Background()
	_, db, _ := testutil.StartPostgres(t)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid,'V','v','us','operating','operating','{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	store := &captureEventStore{}
	bus := NewEventBus(store)
	ex := NewRuntimeToolExecutor(bus, nil, nil)
	ex.SetSQLDB(db)
	ex.SetConfig(&config.Config{
		Budget: config.BudgetConfig{
			HumanTasks: config.HumanTasksConfig{
				MaxTasksPerWeek: 1,
				BudgetReset:     "monday",
			},
		},
	})

	task1 := uuid.NewString()
	task2 := uuid.NewString()
	task3 := uuid.NewString()
	for _, id := range []string{task1, task2, task3} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO human_tasks (id, requesting_agent, vertical_id, category, description, status, created_at)
			VALUES ($1::uuid, 'requester-1', $2::uuid, 'ops', 'do it', 'pending_review', now())
		`, id, verticalID); err != nil {
			t.Fatalf("seed task %s: %v", id, err)
		}
	}

	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator"}

	out, err := ex.ExecHumanTaskDecideDirect(ctx, actor, map[string]any{
		"task_id":       task1,
		"decision":      "approve",
		"reason":        "ok",
		"priority_rank": 2,
	})
	if err != nil {
		t.Fatalf("approve 1: %v", err)
	}
	if out.(map[string]any)["status"] != "approved" {
		t.Fatalf("unexpected approve out: %#v", out)
	}

	out2, err := ex.ExecHumanTaskDecideDirect(ctx, actor, map[string]any{
		"task_id":  task2,
		"decision": "approved",
	})
	if err != nil {
		t.Fatalf("approve 2: %v", err)
	}
	if out2.(map[string]any)["status"] != "deferred" {
		t.Fatalf("expected deferred, got %#v", out2)
	}

	out3, err := ex.ExecHumanTaskDecideDirect(ctx, actor, map[string]any{
		"task_id":      task3,
		"decision":     "reject",
		"reason":       "no",
		"requeue_date": time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("reject: %v", err)
	}
	if out3.(map[string]any)["status"] != "rejected" {
		t.Fatalf("expected rejected, got %#v", out3)
	}

	// Verify DB state updated.
	var s1, s2, s3 string
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task1).Scan(&s1)
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task2).Scan(&s2)
	_ = db.QueryRowContext(ctx, `SELECT status FROM human_tasks WHERE id = $1::uuid`, task3).Scan(&s3)
	if s1 != "approved" || s2 != "deferred" || s3 != "rejected" {
		t.Fatalf("unexpected statuses: %q %q %q", s1, s2, s3)
	}

	types := map[string]events.Event{}
	for _, evt := range store.events {
		types[string(evt.Type)] = evt
	}
	if _, ok := types["human_task.approved"]; !ok {
		t.Fatalf("expected approved event, have=%v", store.events)
	}
	if evt, ok := types["human_task.deferred"]; !ok {
		t.Fatalf("expected deferred event, have=%v", store.events)
	} else {
		var p map[string]any
		_ = json.Unmarshal(evt.Payload, &p)
		if strings.TrimSpace(asString(p["requeue_date"])) == "" {
			t.Fatalf("expected requeue_date in deferred payload, got %#v", p)
		}
		if !strings.Contains(strings.ToLower(asString(p["defer_reason"])), "budget") {
			t.Fatalf("expected defer reason to mention budget, got %#v", p)
		}
	}
	if evt, ok := types["human_task.rejected"]; !ok {
		t.Fatalf("expected rejected event, have=%v", store.events)
	} else {
		var p map[string]any
		_ = json.Unmarshal(evt.Payload, &p)
		if asString(p["rejection_reason"]) != "no" {
			t.Fatalf("expected rejection_reason, got %#v", p)
		}
	}
}

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestAuthorizeRoutingAndManage_Branches(t *testing.T) {
	target := models.AgentConfig{Role: "backend-agent"}

	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "chief-of-staff"}, target, "active"); err == nil {
		t.Fatalf("expected CoS to be blocked unless proposed")
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "chief-of-staff"}, target, "proposed"); err != nil {
		t.Fatalf("expected CoS proposed ok: %v", err)
	}

	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "vp-growth"}, target, "active"); err == nil {
		t.Fatalf("expected vp-growth to be blocked for eng target")
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "cto-agent"}, target, "active"); err != nil {
		t.Fatalf("expected cto-agent ok for eng target: %v", err)
	}

	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "vp-product", VerticalID: "v1"}, "vp-growth", "v1"); err == nil {
		t.Fatalf("expected vp-product blocked for growth role")
	}
	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "vp-product", VerticalID: "v1"}, "backend-agent", "v1"); err != nil {
		t.Fatalf("expected vp-product can manage product roles (backend is allowed list): %v", err)
	}
	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "opco-ceo", VerticalID: "v1"}, "vp-product", "v2"); err == nil {
		t.Fatalf("expected cross-vertical block")
	}
	if err := runtimetools.AuthorizeManageForTest(models.AgentConfig{Role: "empire-coordinator", VerticalID: "v1"}, "vp-product", "v2"); err != nil {
		t.Fatalf("coordinator bypass: %v", err)
	}
}

func TestDefaultExternalCredentialEnv_Branches(t *testing.T) {
	t.Setenv("REGISTRAR_API_ENDPOINT", "https://reg.example")
	t.Setenv("REGISTRAR_API_KEY", "rk")
	t.Setenv("CLOUDFLARE_API_ENDPOINT", "")
	t.Setenv("CLOUDFLARE_API_TOKEN", "cfk")
	t.Setenv("WHATSAPP_API_ENDPOINT", "https://wa.example")
	t.Setenv("WHATSAPP_API_KEY", "wak")
	t.Setenv("INSTAGRAM_API_ENDPOINT", "https://ig.example")
	t.Setenv("INSTAGRAM_API_KEY", "igk")

	if got := DefaultExternalCredentialEnv("domain_purchase"); got["endpoint"] != "https://reg.example" || got["api_key"] != "rk" {
		t.Fatalf("domain creds mismatch: %#v", got)
	}
	if got := DefaultExternalCredentialEnv("dns_configure"); !strings.Contains(got["endpoint"], "cloudflare.com") || got["api_key"] != "cfk" {
		t.Fatalf("dns creds mismatch: %#v", got)
	}
	if got := DefaultExternalCredentialEnv("whatsapp_business_api"); got["endpoint"] != "https://wa.example" || got["api_key"] != "wak" {
		t.Fatalf("wa creds mismatch: %#v", got)
	}
	if got := DefaultExternalCredentialEnv("instagram_api"); got["endpoint"] != "https://ig.example" || got["api_key"] != "igk" {
		t.Fatalf("ig creds mismatch: %#v", got)
	}
	if got := DefaultExternalCredentialEnv("unknown"); len(got) != 0 {
		t.Fatalf("expected empty map")
	}
}

func TestExecInstagramHandleCheck_Availability(t *testing.T) {
	orig := http.DefaultClient.Transport
	t.Cleanup(func() { http.DefaultClient.Transport = orig })

	http.DefaultClient.Transport = rtFunc(func(r *http.Request) (*http.Response, error) {
		if !strings.Contains(r.URL.Host, "www.instagram.com") {
			return nil, errors.New("unexpected host")
		}
		if strings.Contains(r.URL.Path, "available_handle") {
			return runtimetestkit.HTTPResponse(404, "not found"), nil
		}
		return runtimetestkit.HTTPResponse(200, "ok"), nil
	})

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	out, err := ex.ExecInstagramHandleCheckDirect(ctx, models.AgentConfig{ID: "a"}, map[string]any{"handle": "@available_handle"})
	if err != nil {
		t.Fatalf("expected ok: %v", err)
	}
	m := out.(map[string]any)
	if m["available"] != true {
		t.Fatalf("expected available=true, got %#v", m)
	}

	out, err = ex.ExecInstagramHandleCheckDirect(ctx, models.AgentConfig{ID: "a"}, map[string]any{"handle": "taken_handle"})
	if err != nil {
		t.Fatalf("expected ok: %v", err)
	}
	m = out.(map[string]any)
	if m["available"] != false {
		t.Fatalf("expected available=false, got %#v", m)
	}

	if _, err := ex.ExecInstagramHandleCheckDirect(ctx, models.AgentConfig{}, map[string]any{"handle": ""}); err == nil {
		t.Fatalf("expected handle required error")
	}
	if _, err := ex.ExecInstagramHandleCheckDirect(ctx, models.AgentConfig{}, map[string]any{"handle": "bad!!"}); err == nil {
		t.Fatalf("expected invalid format error")
	}
}

func TestExecEmailAPI_CredentialAndSendBranches(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	_ = dsn
	ctx := context.Background()

	verticalID := runtimetestkit.SeedVertical(t, ctx, db, "emailco", `{
		"email": {
			"smtp_addr": "127.0.0.1:1",
			"from": "noreply@example.com",
			"username": "u",
			"password": "p"
		}
	}`)

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	actor := models.AgentConfig{ID: "opco-ceo-" + verticalID, Role: "opco-ceo", VerticalID: verticalID}

	if _, err := ex.ExecEmailAPIDirect(ctx, actor, map[string]any{"to": []string{}}); err == nil {
		t.Fatalf("expected recipient error")
	}

	if _, err := ex.ExecEmailAPIDirect(ctx, actor, map[string]any{"to": []string{"a@example.com"}, "subject": "s", "body": "b"}); err == nil {
		t.Fatalf("expected send failure")
	}

	vertical2 := runtimetestkit.SeedVertical(t, ctx, db, "emailco2", `{"email":{}}`)
	actor2 := models.AgentConfig{ID: "a2", Role: "opco-ceo", VerticalID: vertical2}
	if _, err := ex.ExecEmailAPIDirect(ctx, actor2, map[string]any{"to": []string{"a@example.com"}, "subject": "s", "body": "b"}); err == nil {
		t.Fatalf("expected missing credential error")
	}
}

func TestRestrictedDevOpsTools_RejectNonHoldingDevOps(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo"}

	if _, err := ex.ExecNginxReloadDirect(ctx, actor, nil); err == nil {
		t.Fatalf("expected nginx restriction error")
	}
	if _, err := ex.ExecSystemdControlDirect(ctx, actor, map[string]any{"action": "restart", "unit": "empireai-x"}); err == nil {
		t.Fatalf("expected systemd restriction error")
	}
	if _, err := ex.ExecCertbotExecuteDirect(ctx, actor, map[string]any{"domain": "example.com"}); err == nil {
		t.Fatalf("expected certbot restriction error")
	}
}

func TestDecryptCredentialValue_NoKeyLeavesEncrypted(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	_ = dsn
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "")
	in := map[string]any{"token": "enc::abc"}
	out := ex.DecryptCredentialMapForTest(context.Background(), in)
	if out["token"].(string) != "enc::abc" {
		t.Fatalf("expected encrypted value to remain when key missing")
	}

	_ = os.ErrInvalid
}

func TestExecSystemdControl_ValidationBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "hd", Role: "holding-devops"}

	if _, err := ex.ExecSystemdControlDirect(ctx, actor, map[string]any{"action": "bogus", "unit": "empireai-x"}); err == nil {
		t.Fatalf("expected unsupported action error")
	}

	if _, err := ex.ExecSystemdControlDirect(ctx, actor, map[string]any{"action": "restart", "unit": "nginx"}); err == nil {
		t.Fatalf("expected unit prefix error")
	}
}

func TestExecCertbotExecute_ValidationBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ctx := context.Background()
	actor := models.AgentConfig{ID: "hd", Role: "holding-devops"}
	if _, err := ex.ExecCertbotExecuteDirect(ctx, actor, map[string]any{"domain": ""}); err == nil {
		t.Fatalf("expected domain required error")
	}
}

func TestRedactTelemetryValue_NestedAndSensitive(t *testing.T) {
	in := map[string]any{
		"token":    "secret-token-value",
		"password": "pw",
		"notes":    "payment confirmed ch_abcdef123456",
		"meta": map[string]any{
			"Authorization": "Bearer X",
			"count":         2,
			"items":         []any{"a", map[string]any{"api_key": "k"}},
		},
	}
	out := RedactTelemetryValue(in).(map[string]any)
	if out["token"] != "[REDACTED]" || out["password"] != "[REDACTED]" {
		t.Fatalf("expected sensitive keys redacted: %#v", out)
	}
	meta := out["meta"].(map[string]any)
	if meta["Authorization"] != "[REDACTED]" {
		t.Fatalf("expected nested auth redacted: %#v", meta)
	}
	if strings.Contains(asString(out["notes"]), "ch_abcdef123456") || !strings.Contains(asString(out["notes"]), "[PAYMENT_REF]") {
		t.Fatalf("expected payment ref redacted, got %#v", out["notes"])
	}
}

func TestLoadExternalCredentials_MergesSections(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	verticalID := runtimetestkit.SeedVertical(t, ctx, db, "credco", `{
		"whatsapp": {"endpoint":"w","token":"t"},
		"instagram": {"endpoint":"i","api_key":"k"},
		"registrar": {"endpoint":"r","api_key":"rk"},
		"dns": {"endpoint":"d","api_key":"dk"},
		"whatsapp_name_check": {"endpoint":"n","api_key":"nk"}
	}`)

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", VerticalID: verticalID}

	check := func(tool string, wantKey string) {
		creds, err := ex.LoadExternalCredentialsForTest(ctx, actor.VerticalID, tool)
		if err != nil {
			t.Fatalf("loadExternalCredentials %s: %v", tool, err)
		}
		if strings.TrimSpace(asString(creds[wantKey])) == "" {
			t.Fatalf("expected %s in creds for %s: %#v", wantKey, tool, creds)
		}
	}
	check("whatsapp_business_api", "endpoint")
	check("instagram_api", "api_key")
	check("domain_availability_check", "api_key")
	check("dns_configure", "endpoint")
	check("whatsapp_name_check", "api_key")
}

func TestDecryptCredentialValue_Success(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")

	var encoded string
	if err := db.QueryRowContext(context.Background(), `
		SELECT encode(pgp_sym_encrypt('plain', 'k'), 'base64')
	`).Scan(&encoded); err != nil {
		t.Fatalf("encode: %v", err)
	}
	got := ex.DecryptCredentialValueForTest(context.Background(), "enc::"+strings.TrimSpace(encoded))
	if got.(string) != "plain" {
		t.Fatalf("expected decrypted plain, got %#v", got)
	}
}

type mailboxStub struct {
	last MailboxItem
}

func (m *mailboxStub) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	m.last = item
	if strings.TrimSpace(item.ID) == "" {
		return "mb1", nil
	}
	return item.ID, nil
}

func (m *mailboxStub) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStub) CountMailboxItems(context.Context, string) (int, error) { return 0, nil }

func (m *mailboxStub) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}

func (m *mailboxStub) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}

func (m *mailboxStub) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStub) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStub) MarkMailboxItemNotified(context.Context, string) error { return nil }

type scheduleStoreStub2 struct{ upsert int }

func (s *scheduleStoreStub2) UpsertSchedule(context.Context, Schedule) error { s.upsert++; return nil }

func (s *scheduleStoreStub2) CancelSchedule(context.Context, string, string) error {
	return nil
}

func (s *scheduleStoreStub2) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}

func (s *scheduleStoreStub2) MarkScheduleFired(context.Context, Schedule) error { return nil }

func TestToolExecutor_AgentHireFireReconfigure_And_ScheduleAndMailbox(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	scheduler := NewScheduler(func(Schedule) {})
	t.Cleanup(func() { scheduler.Stop() })

	store := &scheduleStoreStub2{}
	ex := NewRuntimeToolExecutor(bus, scheduler, nil, store)
	mb := &mailboxStub{}
	ex.SetMailboxStore(mb)

	manager := NewAgentManager(bus, nil)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	ex.SetManager(manager)

	actor := models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", VerticalID: "v1"}

	out, err := ex.ExecAgentHireDirect(actor, map[string]any{"config": map[string]any{"id": "a1", "role": "vp-product"}})
	if err != nil {
		t.Fatalf("hire: %v", err)
	}
	if out.(map[string]any)["status"] != "hired" {
		t.Fatalf("unexpected hire out: %#v", out)
	}

	out, err = ex.ExecAgentReconfigureDirect(actor, map[string]any{"agent_id": "a1", "config": map[string]any{"mode": "holding"}})
	if err != nil {
		t.Fatalf("reconfigure: %v", err)
	}
	if out.(map[string]any)["status"] != "reconfigured" {
		t.Fatalf("unexpected reconfigure out: %#v", out)
	}

	out, err = ex.ExecAgentFireDirect(actor, map[string]any{"agent_id": "a1"})
	if err != nil {
		t.Fatalf("fire: %v", err)
	}
	if out.(map[string]any)["status"] != "fired" {
		t.Fatalf("unexpected fire out: %#v", out)
	}

	if _, err := ex.ExecScheduleDirect(actor, map[string]any{"event_type": "timer.x", "at": "bad"}); err == nil {
		t.Fatalf("expected invalid at error")
	}
	if _, err := ex.ExecScheduleDirect(actor, map[string]any{"agent_id": "other", "event_type": "timer.x"}); err == nil {
		t.Fatalf("expected schedule for self only error")
	}
	if _, err := ex.ExecScheduleDirect(models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}, map[string]any{"vertical_id": "v2", "event_type": "timer.x"}); err == nil {
		t.Fatalf("expected cross-vertical schedule error")
	}
	if _, err := ex.ExecScheduleDirect(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator", Mode: "holding", VerticalID: "v1"}, map[string]any{"event_type": "timer.x", "mode": "cron", "cron": "@every 1h"}); err != nil {
		t.Fatalf("schedule ok: %v", err)
	}
	if store.upsert == 0 {
		t.Fatalf("expected schedule store upsert")
	}

	if _, err := ex.ExecMailboxSendDirect(models.AgentConfig{ID: "x", Role: "qa-agent", VerticalID: "v1"}, map[string]any{"type": "review"}); err == nil {
		t.Fatalf("expected mailbox auth error")
	}
	if _, err := ex.ExecMailboxSendDirect(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"priority": "normal"}); err == nil {
		t.Fatalf("expected mailbox type required")
	}
	if _, err := ex.ExecMailboxSendDirect(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"vertical_id": "v2", "type": "review"}); err == nil {
		t.Fatalf("expected cross-vertical mailbox error")
	}
	out, err = ex.ExecMailboxSendDirect(models.AgentConfig{ID: "x", Role: "opco-ceo", VerticalID: "v1"}, map[string]any{"type": "review", "context": map[string]any{"token": "secret"}})
	if err != nil {
		t.Fatalf("mailbox send: %v", err)
	}
	if out.(map[string]any)["status"] != "queued" {
		t.Fatalf("unexpected mailbox out: %#v", out)
	}
	if mb.last.Type != "review" || mb.last.Status != "pending" {
		t.Fatalf("unexpected mailbox item: %#v", mb.last)
	}
}

func TestDecodeToolInput_ErrorBranch(t *testing.T) {
	var out struct{}
	if err := decodeToolInput(func() {}, &out); err == nil {
		t.Fatalf("expected marshal error for func")
	}
}

func TestNormalizeSQLValue_Bytes(t *testing.T) {
	if got := runtimetools.NormalizeSQLValueForTest([]byte("x")); got.(string) != "x" {
		t.Fatalf("expected string from bytes, got %#v", got)
	}
	if got := runtimetools.NormalizeSQLValueForTest(123); got.(int) != 123 {
		t.Fatalf("expected passthrough, got %#v", got)
	}
}

func TestExecSQLExecute_ReadOnly(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	verticalID := runtimetestkit.SeedVertical(t, ctx, db, "acme", `{}`)

	if _, err := db.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS "acme_schema"`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS "acme_schema".t (id INT PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), NewScheduler(func(Schedule) {}), nil)
	ex.SetSQLDB(db)

	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: verticalID}

	if _, err := ex.ExecSQLExecuteDirect(ctx, actor, map[string]any{"query": `INSERT INTO t (id,v) VALUES (1,'x')`}); err == nil {
		t.Fatalf("expected insert rejection")
	}

	if _, err := db.ExecContext(ctx, `INSERT INTO "acme_schema".t (id, v) VALUES (1, 'x')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}

	out, err := ex.ExecSQLExecuteDirect(ctx, actor, map[string]any{"query": `SELECT id, v FROM t ORDER BY id`})
	if err != nil {
		t.Fatalf("execSQLExecute select: %v", err)
	}
	rows := out.(map[string]any)["rows"].([]map[string]any)
	if len(rows) != 1 || rows[0]["v"] != "x" {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestExecAgentMessage_TargetValidationBranches(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	scheduler := NewScheduler(func(Schedule) {})
	ex := NewRuntimeToolExecutor(bus, scheduler, nil)
	t.Cleanup(func() { scheduler.Stop() })

	manager := NewAgentManager(bus, nil)
	_ = manager.SpawnAgent(models.AgentConfig{ID: "t1", Role: "vp-product", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "t2", Role: "vp-product", Mode: "operating", VerticalID: "v2"})
	ex.SetManager(manager)

	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}
	ctx := context.Background()

	if _, err := ex.ExecAgentMessageDirect(ctx, actor, map[string]any{"message": "hi"}); err == nil {
		t.Fatalf("expected missing target error")
	}

	if _, err := ex.ExecAgentMessageDirect(ctx, actor, map[string]any{"target_agent_id": "nope", "message": "hi"}); err == nil {
		t.Fatalf("expected unknown target error")
	}

	if _, err := ex.ExecAgentMessageDirect(ctx, actor, map[string]any{"target_agent_id": "t2", "message": "hi"}); err == nil {
		t.Fatalf("expected cross-vertical error")
	}

	if _, err := ex.ExecAgentMessageDirect(ctx, actor, map[string]any{"target_agent_ids": []string{"t1", "t1"}, "message": "hi"}); err != nil {
		t.Fatalf("expected ok: %v", err)
	}
}

func TestExecAgentMessage_AuthorityAndManagementChain(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	scheduler := NewScheduler(func(Schedule) {})
	ex := NewRuntimeToolExecutor(bus, scheduler, nil)
	t.Cleanup(func() { scheduler.Stop() })

	manager := NewAgentManager(bus, nil)
	_ = manager.SpawnAgent(models.AgentConfig{ID: "vp-product-v1", Role: "vp-product", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "vp-growth-v1", Role: "vp-growth", Mode: "operating", VerticalID: "v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "cto-v1", Role: "cto-agent", Mode: "operating", VerticalID: "v1", ParentAgent: "vp-product-v1"})
	_ = manager.SpawnAgent(models.AgentConfig{ID: "backend-v1", Role: "backend-agent", Mode: "operating", VerticalID: "v1", ParentAgent: "cto-v1"})
	ex.SetManager(manager)

	ctx := context.Background()

	actor := models.AgentConfig{ID: "vp-product-v1", Role: "vp-product", Mode: "operating", VerticalID: "v1"}
	if _, err := ex.ExecAgentMessageDirect(ctx, actor, map[string]any{"target_agent_id": "vp-growth-v1", "message": "sync"}); err == nil {
		t.Fatalf("expected authority rejection for vp-product -> vp-growth")
	}

	if _, err := ex.ExecAgentMessageDirect(ctx, actor, map[string]any{"target_agent_id": "backend-v1", "message": "prioritize bug"}); err != nil {
		t.Fatalf("expected management-chain authorization, got: %v", err)
	}

	worker := models.AgentConfig{ID: "backend-v1", Role: "backend-agent", Mode: "operating", VerticalID: "v1"}
	if _, err := ex.ExecAgentMessageDirect(ctx, worker, map[string]any{"target_agent_id": "vp-product-v1", "message": "blocked on product decision"}); err != nil {
		t.Fatalf("expected upward escalation authorization, got: %v", err)
	}
}

func TestAuthorizeToolUsage_AllowedToolsConfig(t *testing.T) {
	scheduler := NewScheduler(func(Schedule) {})
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), scheduler, nil)
	t.Cleanup(func() { scheduler.Stop() })
	actor := models.AgentConfig{
		ID:   "a",
		Role: "worker",
		Config: []byte(`{
			"allowed_tools": ["agent_message"]
		}`),
	}
	ctx := WithActor(context.Background(), actor)

	if _, err := ex.Execute(ctx, "agent_message", map[string]any{"target_agent_id": "x"}); err == nil {

	}

	if _, err := ex.Execute(ctx, "schedule", map[string]any{}); err == nil {
		t.Fatalf("expected tool not allowed error")
	}
}

func TestAuthorizeMailboxSend_RoleCoverage(t *testing.T) {
	for _, role := range []string{
		"validation-coordinator",
		"vp-growth",
		"support-agent",
		"marketing-agent",
	} {
		if err := runtimetools.AuthorizeMailboxSendForTest(models.AgentConfig{Role: role}); err != nil {
			t.Fatalf("expected role %s to be allowed mailbox_send: %v", role, err)
		}
	}
}

func TestRuntimeToolExecutor_SetManager_Instagram_Email_SystemTools(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, credentials, created_at, updated_at)
		VALUES ($1::uuid, 'TestCo', 'testco', 'us', 'operating', 'operating', '{}'::jsonb, now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	bus := NewEventBus(InMemoryEventStore{})
	manager := NewAgentManager(bus, nil)
	exec := NewRuntimeToolExecutor(bus, nil, nil)
	exec.SetSQLDB(db)
	exec.SetManager(manager)

	{
		actor := models.AgentConfig{
			ID:         "a",
			Role:       "vp-growth",
			Mode:       "operating",
			VerticalID: verticalID,
			Config:     json.RawMessage(`{"system_prompt":"x","tools":["instagram_handle_check"]}`),
		}
		_, err := exec.Execute(WithActor(ctx, actor), "instagram_handle_check", map[string]any{"handle": "bad!!"})
		if err == nil {
			t.Fatal("expected instagram handle format error")
		}
	}

	{
		actor := models.AgentConfig{
			ID:         "a",
			Role:       "opco-ceo",
			Mode:       "operating",
			VerticalID: verticalID,
			Config:     json.RawMessage(`{"system_prompt":"x","tools":["email_api"]}`),
		}
		_, err := exec.Execute(WithActor(ctx, actor), "email_api", map[string]any{
			"to":      []string{"a@example.com"},
			"subject": "s",
			"body":    "b",
		})
		if err == nil {
			t.Fatal("expected missing email credentials error")
		}
	}

	{
		actor := models.AgentConfig{
			ID:     "holding-devops",
			Role:   "holding-devops",
			Mode:   "holding",
			Config: json.RawMessage(`{"system_prompt":"x","tools":["nginx_reload","systemd_control","certbot_execute"]}`),
		}

		toolCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "nginx_reload", map[string]any{}); err == nil {
			t.Fatal("expected nginx_reload to fail in test environment")
		}
		cancel()

		toolCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "systemd_control", map[string]any{"action": "nope", "service": "empireai-x"}); err == nil {
			t.Fatal("expected systemd_control to reject invalid action")
		}
		cancel()

		toolCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
		if _, err := exec.Execute(WithActor(toolCtx, actor), "certbot_execute", map[string]any{"domain": ""}); err == nil {
			t.Fatal("expected certbot_execute to require domain")
		}
		cancel()
	}
}

func TestToolExecutor_AuthorizationHelpers(t *testing.T) {
	actor := models.AgentConfig{Role: "vp-product", VerticalID: "v1"}
	target := models.AgentConfig{Role: "marketing-agent", VerticalID: "v1"}
	if err := runtimetools.AuthorizeManageForTest(actor, target.Role, target.VerticalID); err == nil {
		t.Fatal("expected vp-product to be blocked from managing growth agents")
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "chief-of-staff"}, models.AgentConfig{Role: "vp-growth"}, "active"); err == nil {
		t.Fatal("expected chief-of-staff to only propose routing")
	}
	if err := runtimetools.AuthorizeRoutingForTest(models.AgentConfig{Role: "cto-agent"}, models.AgentConfig{Role: "backend-agent"}, "active"); err != nil {
		t.Fatalf("expected cto-agent to route eng agents, got %v", err)
	}
}

func TestToolExecutor_AgentManagement_AndMailbox_ErrorBranches(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	scheduler := NewScheduler(func(Schedule) {})
	t.Cleanup(func() { scheduler.Stop() })

	ex := NewRuntimeToolExecutor(bus, scheduler, nil)
	actor := models.AgentConfig{ID: "a", Role: "opco-ceo", Mode: "operating", VerticalID: "v1"}

	if _, err := ex.ExecAgentHireDirect(actor, map[string]any{"config": map[string]any{"id": "x", "role": "backend-agent"}}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}
	if _, err := ex.ExecAgentFireDirect(actor, map[string]any{"agent_id": "x"}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}
	if _, err := ex.ExecAgentReconfigureDirect(actor, map[string]any{"agent_id": "x", "config": map[string]any{"mode": "holding"}}); err == nil || !strings.Contains(err.Error(), "agent manager") {
		t.Fatalf("expected manager error, got %v", err)
	}

	manager := NewAgentManager(bus, nil)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })
	ex.SetManager(manager)

	if _, err := ex.ExecAgentHireDirect(actor, map[string]any{"config": map[string]any{"role": "backend-agent"}}); err == nil || !strings.Contains(err.Error(), "config.id is required") {
		t.Fatalf("expected config.id required, got %v", err)
	}

	if _, err := ex.ExecAgentHireDirect(models.AgentConfig{ID: "g", Role: "vp-growth", Mode: "operating", VerticalID: "v1"}, map[string]any{
		"config": map[string]any{"id": "x", "role": "backend-agent"},
	}); err == nil {
		t.Fatal("expected authorization error")
	}

	if _, err := ex.ExecAgentHireDirect(actor, map[string]any{"config": map[string]any{"id": "dup", "role": "backend-agent"}}); err != nil {
		t.Fatalf("expected initial hire ok: %v", err)
	}
	if _, err := ex.ExecAgentHireDirect(actor, map[string]any{"config": map[string]any{"id": "dup", "role": "backend-agent"}}); err == nil {
		t.Fatal("expected duplicate hire error")
	}

	if _, err := ex.ExecAgentFireDirect(actor, map[string]any{}); err == nil || !strings.Contains(err.Error(), "agent_id is required") {
		t.Fatalf("expected agent_id required, got %v", err)
	}
	if _, err := ex.ExecAgentFireDirect(actor, map[string]any{"agent_id": "nope"}); err == nil || !strings.Contains(err.Error(), "target agent not found") {
		t.Fatalf("expected not found, got %v", err)
	}
	if _, err := ex.ExecAgentReconfigureDirect(actor, map[string]any{"agent_id": "nope", "config": map[string]any{"mode": "holding"}}); err == nil || !strings.Contains(err.Error(), "target agent not found") {
		t.Fatalf("expected not found, got %v", err)
	}

	if _, err := ex.ExecMailboxSendDirect(actor, map[string]any{"type": "review"}); err == nil || !strings.Contains(err.Error(), "mailbox store is not configured") {
		t.Fatalf("expected mailbox store error, got %v", err)
	}
	ex.SetMailboxStore(&mailboxStub{})
	if _, err := ex.ExecMailboxSendDirect(actor, map[string]any{"type": "review", "timeout_at": "nope"}); err == nil || !strings.Contains(err.Error(), "invalid timeout_at") {
		t.Fatalf("expected timeout_at error, got %v", err)
	}
}

type scheduleStoreStub struct{ upserts int }

func (s *scheduleStoreStub) UpsertSchedule(context.Context, Schedule) error { s.upserts++; return nil }

func (s *scheduleStoreStub) CancelSchedule(context.Context, string, string) error { return nil }

func (s *scheduleStoreStub) LoadActiveSchedules(context.Context) ([]Schedule, error) {
	return nil, nil
}

func (s *scheduleStoreStub) MarkScheduleFired(context.Context, Schedule) error { return nil }

type mailboxStoreCapture struct {
	last MailboxItem
}

func (m *mailboxStoreCapture) InsertMailboxItem(_ context.Context, item MailboxItem) (string, error) {
	m.last = item
	if item.ID == "" {
		item.ID = uuid.NewString()
	}
	return item.ID, nil
}

func (m *mailboxStoreCapture) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStoreCapture) CountMailboxItems(context.Context, string) (int, error) { return 0, nil }

func (m *mailboxStoreCapture) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, nil
}

func (m *mailboxStoreCapture) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStoreCapture) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (m *mailboxStoreCapture) MarkMailboxItemNotified(context.Context, string) error { return nil }

func (m *mailboxStoreCapture) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}
