package llm

import (
	"context"
	"encoding/json"
	"testing"

	"swarm/internal/config"
	"swarm/internal/events"
	runtimeactors "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/sessions"
)

type eventPublisherStub struct {
	events []events.Event
}

func (s *eventPublisherStub) Publish(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}

func TestAnthropicAPIRuntime_StartSessionPublishesAgentStarted(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewAnthropicAPIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, publisher)
	ctx := runtimeactors.WithActor(sessions.WithScope(context.Background(), sessions.RuntimeModeTask, "task-1"), runtimeactors.AgentConfig{
		ID:       "agent-1",
		Type:     "sonnet",
		EntityID: "entity-1",
	})

	s, err := runtime.StartSession(ctx, "agent-1", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s == nil {
		t.Fatal("expected session")
	}
	if len(publisher.events) != 1 {
		t.Fatalf("expected 1 platform.agent_started event, got %d", len(publisher.events))
	}
	evt := publisher.events[0]
	if evt.Type != events.EventType("platform.agent_started") {
		t.Fatalf("event type = %s, want platform.agent_started", evt.Type)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("Unmarshal payload: %v", err)
	}
	if got := payload["agent_id"]; got != "agent-1" {
		t.Fatalf("agent_id = %#v, want agent-1", got)
	}
	if got := payload["conversation_mode"]; got != sessions.RuntimeModeTask {
		t.Fatalf("conversation_mode = %#v, want task", got)
	}
	if got := payload["model_tier"]; got != "sonnet" {
		t.Fatalf("model_tier = %#v, want sonnet", got)
	}
	if evt.EntityID() != "entity-1" {
		t.Fatalf("entity_id = %q, want entity-1", evt.EntityID())
	}
}

func TestClaudeCLIRuntime_StartSessionPublishesAgentStarted(t *testing.T) {
	publisher := &eventPublisherStub{}
	runtime := NewClaudeCLIRuntime(&config.Config{}, sessions.NewInMemoryRegistry(0), "worker-1", nil, nil, nil, nil, publisher)
	ctx := runtimeactors.WithActor(sessions.WithScope(context.Background(), sessions.RuntimeModeTask, "task-1"), runtimeactors.AgentConfig{
		ID:   "agent-2",
		Type: "haiku",
	})

	s, err := runtime.StartSession(ctx, "agent-2", "system", nil)
	if err != nil {
		t.Fatalf("StartSession: %v", err)
	}
	if s == nil {
		t.Fatal("expected session")
	}
	if len(publisher.events) != 1 {
		t.Fatalf("expected 1 platform.agent_started event, got %d", len(publisher.events))
	}
	evt := publisher.events[0]
	if evt.Type != events.EventType("platform.agent_started") {
		t.Fatalf("event type = %s, want platform.agent_started", evt.Type)
	}
	var payload map[string]any
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("Unmarshal payload: %v", err)
	}
	if got := payload["agent_id"]; got != "agent-2" {
		t.Fatalf("agent_id = %#v, want agent-2", got)
	}
	if got := payload["model_tier"]; got != "haiku" {
		t.Fatalf("model_tier = %#v, want haiku", got)
	}
}
