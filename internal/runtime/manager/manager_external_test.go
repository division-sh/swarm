package manager_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	runtimebus "empireai/internal/runtime/bus"
	runtimemanager "empireai/internal/runtime/manager"
	storepkg "empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func mustJSONB(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return b
}

func TestAgentManager_PromptOverrideLifecycle(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &storepkg.PostgresStore{DB: db}
	bus := rt.NewEventBus(pg)
	am := runtimemanager.NewAgentManager(bus, nil, pg)
	if _, err := db.ExecContext(context.Background(), `
		CREATE TABLE IF NOT EXISTS prompt_overrides (
			agent_id        TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
			prompt          TEXT NOT NULL,
			previous_prompt TEXT,
			source          TEXT NOT NULL DEFAULT 'dashboard',
			notes           TEXT,
			created_at      TIMESTAMPTZ DEFAULT now(),
			updated_at      TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create prompt_overrides table: %v", err)
	}

	cfg := models.AgentConfig{
		ID:   "empire-coordinator",
		Type: "worker",
		Role: "empire-coordinator",
		Mode: "holding",
		Config: mustJSONB(t, map[string]any{
			"system_prompt": "template prompt",
		}),
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("spawn agent: %v", err)
	}

	state, err := am.GetAgentPromptState(context.Background(), cfg.ID)
	if err != nil {
		t.Fatalf("GetAgentPromptState initial: %v", err)
	}
	if state.TemplatePrompt != "template prompt" || state.EffectivePrompt != "template prompt" || state.Override != nil {
		t.Fatalf("unexpected initial prompt state: %+v", state)
	}

	if err := am.SetAgentPromptOverride(context.Background(), cfg.ID, "override prompt", "test", "notes"); err != nil {
		t.Fatalf("SetAgentPromptOverride: %v", err)
	}
	state2, err := am.GetAgentPromptState(context.Background(), cfg.ID)
	if err != nil {
		t.Fatalf("GetAgentPromptState overridden: %v", err)
	}
	if state2.Override == nil || state2.EffectivePrompt != "override prompt" {
		t.Fatalf("expected override active, got %+v", state2)
	}

	if err := am.RevertAgentPromptOverride(context.Background(), cfg.ID); err != nil {
		t.Fatalf("RevertAgentPromptOverride: %v", err)
	}
	state3, err := am.GetAgentPromptState(context.Background(), cfg.ID)
	if err != nil {
		t.Fatalf("GetAgentPromptState reverted: %v", err)
	}
	if state3.Override != nil || state3.EffectivePrompt != "template prompt" {
		t.Fatalf("expected override removed, got %+v", state3)
	}
}

func TestAgentManager_PublishEvent(t *testing.T) {
	bus := rt.NewEventBus(runtimebus.InMemoryEventStore{})
	am := runtimemanager.NewAgentManager(bus, nil, nil)
	ch := bus.Subscribe("watch-publish", events.EventType("system.directive"))

	evt := events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSONB(t, map[string]any{"directive_text": "SaaS in Argentina"}),
		CreatedAt:   time.Now(),
	}
	if err := am.PublishEvent(context.Background(), evt); err != nil {
		t.Fatalf("PublishEvent: %v", err)
	}
	select {
	case got := <-ch:
		if got.ID != evt.ID {
			t.Fatalf("expected event %s, got %s", evt.ID, got.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected published event delivery")
	}
}
