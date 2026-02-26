package runtime

import (
	"context"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
)

func TestDirectiveRequiresCoordinator(t *testing.T) {
	complex := events.Event{
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "Focus on compliance-driven opportunities in LATAM countries with over 80 percent internet penetration",
		}),
	}
	if !directiveRequiresCoordinator(complex) {
		t.Fatal("expected complex directive to require coordinator")
	}

	simple := events.Event{
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "SaaS in Uruguay",
		}),
	}
	if directiveRequiresCoordinator(simple) {
		t.Fatal("expected simple directive to be runtime-handled")
	}

	forwarded := events.Event{
		Type:        events.EventType("system.directive"),
		SourceAgent: "scan-campaign-manager",
		Payload:     mustJSON(map[string]any{"directive_text": "anything"}),
	}
	if !directiveRequiresCoordinator(forwarded) {
		t.Fatal("expected scan-campaign-manager forwarded directive to require coordinator")
	}
}

type directiveProbeAgent struct {
	id    string
	calls int
}

func (a *directiveProbeAgent) ID() string { return a.id }

func (a *directiveProbeAgent) Type() string { return "worker" }

func (a *directiveProbeAgent) Subscriptions() []events.EventType {
	return []events.EventType{events.EventType("system.directive")}
}

func (a *directiveProbeAgent) OnEvent(_ context.Context, _ events.Event) ([]events.Event, error) {
	a.calls++
	return nil, nil
}

func TestAgentManager_ProcessEvent_InterceptsSimpleDirective(t *testing.T) {
	am := NewAgentManager(NewEventBus(InMemoryEventStore{}), nil, nil)
	agent := &directiveProbeAgent{id: "empire-coordinator"}
	am.agents[agent.id] = agent
	am.agentCfg[agent.id] = models.AgentConfig{
		ID:   agent.id,
		Role: "empire-coordinator",
		Mode: "holding",
	}

	err := am.processEvent(context.Background(), agent, events.Event{
		ID:          "evt-simple-directive",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "SaaS in Uruguay",
		}),
	})
	if err != nil {
		t.Fatalf("processEvent returned error: %v", err)
	}
	if agent.calls != 0 {
		t.Fatalf("expected simple directive to be intercepted by runtime, calls=%d", agent.calls)
	}
}

func TestAgentManager_ProcessEvent_ForwardsComplexDirective(t *testing.T) {
	am := NewAgentManager(NewEventBus(InMemoryEventStore{}), nil, nil)
	agent := &directiveProbeAgent{id: "empire-coordinator"}
	am.agents[agent.id] = agent
	am.agentCfg[agent.id] = models.AgentConfig{
		ID:   agent.id,
		Role: "empire-coordinator",
		Mode: "holding",
	}

	err := am.processEvent(context.Background(), agent, events.Event{
		ID:          "evt-complex-directive",
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "Focus on compliance-driven opportunities in LATAM countries with over 80 percent internet penetration",
		}),
	})
	if err != nil {
		t.Fatalf("processEvent returned error: %v", err)
	}
	if agent.calls != 1 {
		t.Fatalf("expected complex directive to reach coordinator agent, calls=%d", agent.calls)
	}
}
