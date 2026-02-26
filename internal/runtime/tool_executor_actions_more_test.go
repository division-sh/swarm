package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

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
func (m *mailboxStoreCapture) GetMailboxItem(context.Context, string) (MailboxItem, error) { return MailboxItem{}, nil }
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

	// Spawn a target agent so agent_message can validate existence.
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

	// agent_message success.
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
		"target_agent_id": targetID,
		"message":         "hi",
	}); err != nil {
		t.Fatalf("agent_message: %v", err)
	}
	select {
	case <-targetCh:
	case <-time.After(1 * time.Second):
		t.Fatal("expected message delivered to target")
	}

	// schedule + scheduleStore upsert.
	if _, err := exec.Execute(WithActor(ctx, actor), "schedule", map[string]any{
		"event_type": "timer.test",
		"mode":       "once",
		"at":         time.Now().Add(10 * time.Second).UTC().Format(time.RFC3339),
		"payload":    map[string]any{"x": 1},
	}); err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if schedStore.upserts == 0 {
		t.Fatal("expected scheduleStore upsert")
	}

	// configure_routing.
	if _, err := exec.Execute(WithActor(ctx, actor), "configure_routing", map[string]any{
		"event_pattern": "opco.*",
		"subscriber_id": targetID,
		"reason":        "test",
		"status":        "active",
	}); err != nil {
		t.Fatalf("configure_routing: %v", err)
	}

	// agent_hire + agent_reconfigure + agent_fire.
	hiredID := "qa-agent-" + verticalID
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_hire", map[string]any{
		"config": map[string]any{
			"id":          hiredID,
			"type":        "stub",
			"role":        "qa-agent",
			"mode":        "operating",
			"vertical_id": verticalID,
			"config":      json.RawMessage(`{"system_prompt":"x","tools":["*"]}`),
		},
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
	if _, err := exec.Execute(WithActor(ctx, actor), "agent_fire", map[string]any{"agent_id": hiredID}); err != nil {
		t.Fatalf("agent_fire: %v", err)
	}

	// mailbox_send.
	if _, err := exec.Execute(WithActor(ctx, actor), "mailbox_send", map[string]any{
		"type":     "founder_review",
		"priority": "critical",
		"summary":  "please review",
		"context":  map[string]any{"a": 1},
	}); err != nil {
		t.Fatalf("mailbox_send: %v", err)
	}
	if mailboxCap.last.Type != "founder_review" || mailboxCap.last.Priority != "critical" {
		t.Fatalf("unexpected mailbox item: %+v", mailboxCap.last)
	}

	// human_task_request: create a task.
	out, err := exec.Execute(WithActor(ctx, actor), "human_task_request", map[string]any{
		"category":     "verification",
		"description":  "call someone",
		"priority":     "medium",
		"deadline":     time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339),
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

	// Create an already-approved task this week so approving again triggers budget exhaustion deferral.
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
		"task_id":   taskID,
		"decision":  "approve",
		"reason":    "ok",
		"requeue_date": time.Now().Add(7 * 24 * time.Hour).UTC().Format(time.RFC3339),
	})
	if err != nil {
		t.Fatalf("human_task_decide: %v", err)
	}
	if status, _ := decOut.(map[string]any)["status"].(string); status == "approved" {
		t.Fatalf("expected budget enforcement to defer, got %v", decOut)
	}

	// execExternalProxy coverage: basic path via test server (no credentials -> error already covered elsewhere).
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()
	t.Setenv("EMPIREAI_EXTERNAL_PROXY_BASE_URL", ts.URL)
	_, _ = exec.Execute(WithActor(ctx, actor), "domain_availability_check", map[string]any{"domain": "example.com"})
}
