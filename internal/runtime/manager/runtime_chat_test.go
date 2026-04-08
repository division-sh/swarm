package manager

import (
	"context"
	"testing"
	"time"

	"swarm/internal/events"
	runtimedelivery "swarm/internal/runtime/deliverylifecycle"
)

type chatTestAgent struct {
	id        string
	directive string
	calls     int
}

func (a *chatTestAgent) ID() string                        { return a.id }
func (a *chatTestAgent) Type() string                      { return "stub" }
func (a *chatTestAgent) Subscriptions() []events.EventType { return nil }
func (a *chatTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (a *chatTestAgent) BoardStep(_ context.Context, directive string) (string, error) {
	a.calls++
	a.directive = directive
	return "ok", nil
}

type chatTestStore struct {
	cancelCalls int
	cancelFor   string
	transitions []runtimedelivery.Transition
}

func (s *chatTestStore) UpsertAgent(context.Context, PersistedAgent) error { return nil }
func (s *chatTestStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}
func (s *chatTestStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (s *chatTestStore) EnsureEntitySchema(context.Context, string) error  { return nil }
func (s *chatTestStore) UpsertEventReceipt(context.Context, string, string, ReceiptStatus, string) error {
	return nil
}
func (s *chatTestStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *chatTestStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (s *chatTestStore) CancelActiveRunWorkByProducer(_ context.Context, producerID string) ([]runtimedelivery.Transition, error) {
	s.cancelCalls++
	s.cancelFor = producerID
	return append([]runtimedelivery.Transition(nil), s.transitions...), nil
}

func TestAgentManager_ChatWithAgent_KillPreviousCancelsBeforeBoardStep(t *testing.T) {
	store := &chatTestStore{}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(nil, nil, store)
	am.agents[agent.id] = agent

	got, err := am.ChatWithAgent(context.Background(), agent.id, "run corpus", true)
	if err != nil {
		t.Fatalf("ChatWithAgent: %v", err)
	}
	if got != "ok" {
		t.Fatalf("ChatWithAgent result = %q, want ok", got)
	}
	if store.cancelCalls != 1 || store.cancelFor != agent.id {
		t.Fatalf("cancel previous calls = %d for %q, want 1 for %q", store.cancelCalls, store.cancelFor, agent.id)
	}
	if agent.calls != 1 || agent.directive != "run corpus" {
		t.Fatalf("board step calls=%d directive=%q", agent.calls, agent.directive)
	}
}

func TestAgentManager_ChatWithAgent_WithoutKillPreviousSkipsCancellation(t *testing.T) {
	store := &chatTestStore{}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(nil, nil, store)
	am.agents[agent.id] = agent

	if _, err := am.ChatWithAgent(context.Background(), agent.id, "run corpus", false); err != nil {
		t.Fatalf("ChatWithAgent: %v", err)
	}
	if store.cancelCalls != 0 {
		t.Fatalf("cancel previous calls = %d, want 0", store.cancelCalls)
	}
}

func TestAgentManager_ChatWithAgent_KillPreviousLogsForcedTerminalLifecycle(t *testing.T) {
	bus := &recordingReceiptBus{}
	store := &chatTestStore{
		transitions: []runtimedelivery.Transition{{
			EventID:         "evt-1",
			AgentID:         "market-research-agent",
			EntityID:        "entity-1",
			State:           runtimedelivery.StateExhausted,
			PreviousState:   runtimedelivery.StateActive,
			Reason:          "cancelled_by_kill_previous",
			TerminalOutcome: "cancelled_by_kill_previous",
			Error:           "cancelled by --kill-previous",
		}},
	}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent
	am.agents["market-research-agent"] = &chatTestAgent{id: "market-research-agent"}

	if _, err := am.ChatWithAgent(context.Background(), agent.id, "run corpus", true); err != nil {
		t.Fatalf("ChatWithAgent: %v", err)
	}
	if len(bus.runtimeLogs) != 1 {
		t.Fatalf("runtime log count = %d, want 1", len(bus.runtimeLogs))
	}
	entry := bus.runtimeLogs[0]
	if entry.Action != "delivery_lifecycle_transition" {
		t.Fatalf("action = %q, want delivery_lifecycle_transition", entry.Action)
	}
	detail, ok := entry.Detail.(map[string]any)
	if !ok {
		t.Fatalf("detail = %#v", entry.Detail)
	}
	if detail["delivery_state"] != "exhausted" || detail["delivery_previous_state"] != "active" {
		t.Fatalf("delivery lifecycle detail = %#v", detail)
	}
	if detail["delivery_reason"] != "cancelled_by_kill_previous" || detail["delivery_terminal_outcome"] != "cancelled_by_kill_previous" {
		t.Fatalf("terminal detail = %#v", detail)
	}
}

func TestAgentManager_ChatWithAgent_DeniesWhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
	})
	am.agents[agent.id] = agent

	if _, err := am.ChatWithAgent(context.Background(), agent.id, "run corpus", false); err == nil || err.Error() != "runtime shutting down" {
		t.Fatalf("ChatWithAgent err = %v, want runtime shutting down", err)
	}
	if agent.calls != 0 {
		t.Fatalf("board step calls = %d, want 0", agent.calls)
	}
}
