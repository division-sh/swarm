package manager

import (
	"context"
	"testing"
	"time"

	"swarm/internal/events"
	runtimeagentcontrol "swarm/internal/runtime/agentcontrol"
	runtimebus "swarm/internal/runtime/bus"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
)

type chatTestAgent struct {
	id              string
	directive       string
	runID           string
	directiveEvent  string
	directiveSource string
	calls           int
}

func (a *chatTestAgent) ID() string                        { return a.id }
func (a *chatTestAgent) Type() string                      { return "stub" }
func (a *chatTestAgent) Subscriptions() []events.EventType { return nil }
func (a *chatTestAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (a *chatTestAgent) BoardStep(_ context.Context, directive runtimeagentcontrol.BoardDirective) (string, error) {
	a.calls++
	a.directive = directive.Directive
	a.runID = directive.Event.RunID
	a.directiveEvent = directive.Event.ID
	a.directiveSource = string(directive.Event.Type)
	return "ok", nil
}

type chatTestStore struct{}

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

type directiveTargetStore struct {
	chatTestStore
	target runtimeagentcontrol.RunTargetResolution
	err    error
	calls  int
}

func (s *directiveTargetStore) ResolveAgentDirectiveRunTarget(context.Context, string, string) (runtimeagentcontrol.RunTargetResolution, error) {
	s.calls++
	if s.err != nil {
		return runtimeagentcontrol.RunTargetResolution{}, s.err
	}
	return s.target, nil
}

type directiveTestBus struct {
	direct []events.Event
	store  *directiveEventStore
}

func (b *directiveTestBus) Publish(_ context.Context, evt events.Event) error {
	return nil
}
func (b *directiveTestBus) PublishDirect(_ context.Context, evt events.Event, _ []string) error {
	b.direct = append(b.direct, evt)
	return nil
}
func (b *directiveTestBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (b *directiveTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (b *directiveTestBus) Unsubscribe(string) {}
func (b *directiveTestBus) Store() runtimebus.EventStore {
	if b.store == nil {
		b.store = &directiveEventStore{}
	}
	return b.store
}
func (b *directiveTestBus) ResetInMemoryState() error { return nil }
func (b *directiveTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}

type directiveEventStore struct {
	events []events.Event
}

func (s *directiveEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.events = append(s.events, evt)
	return nil
}
func (*directiveEventStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (*directiveEventStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return nil, runtimereplayclaim.ErrAuthoritativeRecipientManifestUnavailable
}
func (*directiveEventStore) SupportsPersistedReplay() bool { return false }

func TestAgentManager_ChatWithAgentPersistsDirectiveEventBeforeBoardStep(t *testing.T) {
	bus := &directiveTestBus{}
	store := &chatTestStore{}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent

	got, err := am.ChatWithAgent(context.Background(), agent.id, "run corpus")
	if err != nil {
		t.Fatalf("ChatWithAgent: %v", err)
	}
	if got != "ok" {
		t.Fatalf("ChatWithAgent result = %q, want ok", got)
	}
	if agent.calls != 1 || agent.directive != "run corpus" {
		t.Fatalf("board step calls=%d directive=%q", agent.calls, agent.directive)
	}
	if agent.runID == "" || agent.directiveEvent == "" || agent.directiveSource != runtimeagentcontrol.DirectiveEventType {
		t.Fatalf("board directive event = run:%q event:%q type:%q", agent.runID, agent.directiveEvent, agent.directiveSource)
	}
	eventCount := 0
	if bus.store != nil {
		eventCount = len(bus.store.events)
	}
	if eventCount != 1 {
		t.Fatalf("persisted directive events = %d, want 1", eventCount)
	}
	if bus.store.events[0].ID != agent.directiveEvent || bus.store.events[0].RunID != agent.runID {
		t.Fatalf("persisted directive event = %#v, board saw event=%q run=%q", bus.store.events[0], agent.directiveEvent, agent.runID)
	}
}

func TestAgentManager_SendDirectivePersistsCanonicalDirectiveEventBeforeBoardStep(t *testing.T) {
	runID := "00000000-0000-0000-0000-000000000701"
	bus := &directiveTestBus{}
	store := &directiveTargetStore{
		target: runtimeagentcontrol.RunTargetResolution{
			RunID: runID,
			Mode:  runtimeagentcontrol.RunResolutionActiveSession,
			ActiveSessions: []runtimeagentcontrol.ActiveSessionTarget{{
				SessionID: "00000000-0000-0000-0000-000000000801",
				RunID:     runID,
			}},
		},
	}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent

	result, err := am.SendDirective(context.Background(), runtimeagentcontrol.SendDirectiveRequest{
		AgentID:    agent.id,
		Directive:  "run corpus",
		Source:     runtimeagentcontrol.DirectiveSourceV1RPC,
		OperatorID: "operator-token",
	})
	if err != nil {
		t.Fatalf("SendDirective: %v", err)
	}
	if result.RunID != runID || result.RunIDResolution != runtimeagentcontrol.RunResolutionActiveSession || result.DirectiveEventID == "" {
		t.Fatalf("directive result = %#v", result)
	}
	if store.calls != 1 {
		t.Fatalf("target resolver calls = %d, want 1", store.calls)
	}
	eventCount := 0
	if bus.store != nil {
		eventCount = len(bus.store.events)
	}
	if eventCount != 1 {
		t.Fatalf("persisted directive events = %d, want 1", eventCount)
	}
	evt := bus.store.events[0]
	if string(evt.Type) != runtimeagentcontrol.DirectiveEventType || evt.RunID != runID || evt.ID == "" {
		t.Fatalf("directive event = %#v", evt)
	}
	if agent.calls != 1 || agent.runID != runID || agent.directiveEvent != evt.ID {
		t.Fatalf("board step saw calls=%d run=%q event=%q, want event %q", agent.calls, agent.runID, agent.directiveEvent, evt.ID)
	}
}

func TestAgentManager_SendDirectiveTargetErrorFailsBeforeBoardStep(t *testing.T) {
	bus := &directiveTestBus{}
	store := &directiveTargetStore{
		err: &runtimeagentcontrol.StateError{
			Err:     runtimeagentcontrol.ErrRunNotFound,
			AgentID: "campaign-coordinator",
			RunID:   "00000000-0000-0000-0000-000000000404",
		},
	}
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManager(bus, nil, store)
	am.agents[agent.id] = agent

	_, err := am.SendDirective(context.Background(), runtimeagentcontrol.SendDirectiveRequest{
		AgentID:   agent.id,
		Directive: "run corpus",
		RunID:     "00000000-0000-0000-0000-000000000404",
	})
	if err == nil {
		t.Fatal("SendDirective error = nil")
	}
	eventCount := 0
	if bus.store != nil {
		eventCount = len(bus.store.events)
	}
	if agent.calls != 0 || eventCount != 0 {
		t.Fatalf("side effects after target error: board=%d events=%d", agent.calls, eventCount)
	}
}

func TestAgentManager_ChatWithAgent_DeniesWhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	agent := &chatTestAgent{id: "campaign-coordinator"}
	am := NewAgentManagerWithOptions(nil, nil, AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
	})
	am.agents[agent.id] = agent

	if _, err := am.ChatWithAgent(context.Background(), agent.id, "run corpus"); err == nil || err.Error() != "runtime shutting down" {
		t.Fatalf("ChatWithAgent err = %v, want runtime shutting down", err)
	}
	if agent.calls != 0 {
		t.Fatalf("board step calls = %d, want 0", agent.calls)
	}
}
