package manager

import (
	"context"
	"testing"
	"time"

	"swarm/internal/events"
)

type chatTestAgent struct {
	id        string
	directive string
	calls     int
}

func (a *chatTestAgent) ID() string                              { return a.id }
func (a *chatTestAgent) Type() string                            { return "stub" }
func (a *chatTestAgent) Subscriptions() []events.EventType       { return nil }
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
	agents      []string
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
func (s *chatTestStore) CancelActiveRunWorkByProducer(_ context.Context, producerID string) ([]string, error) {
	s.cancelCalls++
	s.cancelFor = producerID
	return append([]string(nil), s.agents...), nil
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
