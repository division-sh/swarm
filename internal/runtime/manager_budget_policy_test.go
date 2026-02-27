package runtime

import (
	"context"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
)

type budgetPolicyAgent struct {
	id    string
	calls int
}

func (a *budgetPolicyAgent) ID() string { return a.id }

func (a *budgetPolicyAgent) Type() string { return "llm" }

func (a *budgetPolicyAgent) Subscriptions() []events.EventType { return nil }

func (a *budgetPolicyAgent) OnEvent(_ context.Context, _ events.Event) ([]events.Event, error) {
	a.calls++
	return nil, nil
}

func TestAgentManager_BudgetSuppressionPolicy(t *testing.T) {
	makeMgr := func(stateKey, stateValue string, role string) (*AgentManager, *budgetPolicyAgent) {
		am := NewAgentManager(NewEventBus(InMemoryEventStore{}), nil, nil)
		agent := &budgetPolicyAgent{id: "a1"}
		am.agents[agent.id] = agent
		am.agentCfg[agent.id] = models.AgentConfig{
			ID:         agent.id,
			Role:       role,
			Mode:       "operating",
			VerticalID: "v1",
		}
		am.SetBudgetTracker(&BudgetTracker{
			lastState: map[string]string{
				stateKey: stateValue,
			},
		})
		return am, agent
	}

	t.Run("emergency suppresses non-critical flows", func(t *testing.T) {
		am, agent := makeMgr("vertical|v1", "emergency", "marketing-agent")
		err := am.processEvent(context.Background(), agent, events.Event{
			ID:   "e1",
			Type: events.EventType("market_signals"),
		})
		if err != nil {
			t.Fatalf("processEvent: %v", err)
		}
		if agent.calls != 0 {
			t.Fatalf("expected suppression during emergency, calls=%d", agent.calls)
		}
	})

	t.Run("emergency allows support", func(t *testing.T) {
		am, agent := makeMgr("vertical|v1", "emergency", "support-agent")
		err := am.processEvent(context.Background(), agent, events.Event{
			ID:   "e2",
			Type: events.EventType("customer_message"),
		})
		if err != nil {
			t.Fatalf("processEvent: %v", err)
		}
		if agent.calls != 1 {
			t.Fatalf("expected support flow allowed, calls=%d", agent.calls)
		}
	})

	t.Run("throttle pauses growth", func(t *testing.T) {
		am, agent := makeMgr("vertical|v1", "throttle", "vp-growth")
		err := am.processEvent(context.Background(), agent, events.Event{
			ID:   "e3",
			Type: events.EventType("outreach_digest"),
		})
		if err != nil {
			t.Fatalf("processEvent: %v", err)
		}
		if agent.calls != 0 {
			t.Fatalf("expected growth pause on throttle, calls=%d", agent.calls)
		}
	})

	t.Run("throttle suppresses management heartbeats", func(t *testing.T) {
		am, agent := makeMgr("vertical|v1", "throttle", "opco-ceo")
		err := am.processEvent(context.Background(), agent, events.Event{
			ID:   "e4",
			Type: events.EventType("heartbeat.opco_ceo"),
		})
		if err != nil {
			t.Fatalf("processEvent: %v", err)
		}
		if agent.calls != 0 {
			t.Fatalf("expected heartbeat suppression on throttle, calls=%d", agent.calls)
		}
	})
}
