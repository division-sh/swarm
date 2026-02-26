package runtime_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
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
	am := rt.NewAgentManager(bus, nil, pg)
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
	bus := rt.NewEventBus(rt.InMemoryEventStore{})
	am := rt.NewAgentManager(bus, nil, nil)
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

func TestRuntimeLogger_IntegrationAndMissingTableTolerance(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS runtime_log (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
			detail JSONB NOT NULL DEFAULT '{}'::jsonb,
			error TEXT,
			duration_us INT,
			created_at TIMESTAMPTZ DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create runtime_log table: %v", err)
	}

	logger := rt.NewRuntimeLogger(db)
	bus := rt.NewEventBus(rt.InMemoryEventStore{})
	bus.SetRuntimeLogger(logger)
	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSONB(t, map[string]any{"directive_text": "SaaS in Chile"}),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish with runtime logger: %v", err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM runtime_log`).Scan(&count); err != nil {
		t.Fatalf("count runtime_log: %v", err)
	}
	if count == 0 {
		t.Fatal("expected runtime_log rows to be written")
	}

	logger.Log(ctx, rt.RuntimeLogEntry{
		Level:      "info",
		Component:  "test",
		Action:     "manual",
		EventID:    "not-a-uuid",
		VerticalID: "not-a-uuid",
		Detail:     map[string]any{"ok": true},
	})

	if _, err := db.ExecContext(ctx, `DROP TABLE runtime_log`); err != nil {
		t.Fatalf("drop runtime_log: %v", err)
	}
	// Should be best-effort no-op when diagnostics table is missing.
	logger.Log(ctx, rt.RuntimeLogEntry{
		Level:     "warn",
		Component: "test",
		Action:    "missing-table-path",
		Detail:    map[string]any{"expected": "no panic"},
	})
}
