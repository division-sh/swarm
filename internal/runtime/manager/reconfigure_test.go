package manager

import (
	"context"
	"testing"

	"swarm/internal/events"
	models "swarm/internal/runtime/core/actors"
	"swarm/internal/runtime/sessions"
)

type reconfigureTestAgent struct{ id string }

func (a reconfigureTestAgent) ID() string { return a.id }
func (reconfigureTestAgent) Type() string { return "generic" }
func (reconfigureTestAgent) Subscriptions() []events.EventType {
	return nil
}
func (reconfigureTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestReconfigureAgent_RotatesFlowScopedSession(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})

	cfg := models.AgentConfig{
		ID:               "flow-agent",
		ConversationMode: sessions.RuntimeModeSession,
		SessionScope:     sessions.SessionScopeFlow,
		FlowPath:         "review/inst-1",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	seedCtx := models.WithActor(context.Background(), cfg)
	lease, err := registry.Acquire(seedCtx, cfg.ID, cfg.ConversationMode, cfg.SessionScope, "reconfigure", cfg.CanonicalFlowPath())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{Tools: []string{"agent_message"}}); err != nil {
		t.Fatalf("ReconfigureAgent(flow): %v", err)
	}

	rec, ok := registry.Snapshot(cfg.ID)
	if !ok {
		t.Fatal("expected flow-scoped session record")
	}
	if rec.SessionID == lease.SessionID {
		t.Fatalf("expected rotated session id, got unchanged %q", rec.SessionID)
	}
	if rec.ScopeKey != cfg.CanonicalFlowPath() {
		t.Fatalf("scope_key = %q, want %q", rec.ScopeKey, cfg.CanonicalFlowPath())
	}
}

func TestReconfigureAgent_RotatesEntityScopedSession(t *testing.T) {
	registry := sessions.NewInMemoryRegistry(0)
	am := NewAgentManagerWithOptions(nil, func(cfg models.AgentConfig) (Agent, error) {
		return reconfigureTestAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{Sessions: registry})

	cfg := models.AgentConfig{
		ID:               "entity-agent",
		ConversationMode: sessions.RuntimeModeSessionPerEntity,
		SessionScope:     sessions.SessionScopeEntity,
		FlowPath:         "review/inst-1",
		EntityID:         "entity-1",
	}
	if err := am.SpawnAgent(cfg); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	seedCtx := models.WithActor(context.Background(), cfg)
	lease, err := registry.Acquire(seedCtx, cfg.ID, cfg.ConversationMode, cfg.SessionScope, "reconfigure", cfg.EffectiveEntityID())
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	if err := am.ReconfigureAgent(cfg.ID, models.AgentConfig{Tools: []string{"agent_message"}}); err != nil {
		t.Fatalf("ReconfigureAgent(entity): %v", err)
	}

	rec, ok := registry.Snapshot(cfg.ID)
	if !ok {
		t.Fatal("expected entity-scoped session record")
	}
	if rec.SessionID == lease.SessionID {
		t.Fatalf("expected rotated session id, got unchanged %q", rec.SessionID)
	}
	if rec.ScopeKey != cfg.EffectiveEntityID() {
		t.Fatalf("scope_key = %q, want %q", rec.ScopeKey, cfg.EffectiveEntityID())
	}
}
