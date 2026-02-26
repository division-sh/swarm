package runtime

import (
	"context"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
)

type panicProbeAgent struct{ id string }

func (a *panicProbeAgent) ID() string                        { return a.id }
func (a *panicProbeAgent) Type() string                      { return "stub" }
func (a *panicProbeAgent) Subscriptions() []events.EventType { return nil }
func (a *panicProbeAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func TestPanicBackoff(t *testing.T) {
	cases := []struct {
		panics int
		want   time.Duration
	}{
		{panics: 0, want: 1 * time.Second},
		{panics: 1, want: 1 * time.Second},
		{panics: 2, want: 5 * time.Second},
		{panics: 3, want: 30 * time.Second},
		{panics: 4, want: 2 * time.Minute},
		{panics: 5, want: 10 * time.Minute},
		{panics: 9, want: 10 * time.Minute},
	}
	for _, tc := range cases {
		if got := panicBackoff(tc.panics); got != tc.want {
			t.Fatalf("panicBackoff(%d)=%s want=%s", tc.panics, got, tc.want)
		}
	}
}

func TestAgentManager_HandleAgentLoopPanic_EmitsAndEscalates(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	am := NewAgentManager(bus, nil)

	am.mu.Lock()
	am.agentCfg["worker-1"] = models.AgentConfig{
		ID:          "worker-1",
		Type:        "stub",
		Role:        "backend-agent",
		Mode:        "holding",
		ParentAgent: "empire-coordinator",
		Config:      mustJSON(map[string]any{"system_prompt": "x"}),
	}
	am.mu.Unlock()

	panicCh := bus.Subscribe("watch-panic", events.EventType("ops.agent_panic"))
	failedCh := bus.Subscribe("empire-coordinator", events.EventType("ops.agent_failed"))
	agent := &panicProbeAgent{id: "worker-1"}

	am.handleAgentLoopPanic(context.Background(), agent, 1, "boom once")

	select {
	case evt := <-panicCh:
		if evt.Type != events.EventType("ops.agent_panic") {
			t.Fatalf("unexpected panic event type: %s", evt.Type)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected ops.agent_panic event for first panic")
	}

	select {
	case evt := <-failedCh:
		t.Fatalf("did not expect ops.agent_failed on first panic, got %s", evt.ID)
	case <-time.After(200 * time.Millisecond):
		// expected no escalation yet
	}

	am.handleAgentLoopPanic(context.Background(), agent, 5, "boom terminal")

	// terminal panic still emits panic telemetry
	select {
	case <-panicCh:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected ops.agent_panic on terminal panic")
	}
	select {
	case evt := <-failedCh:
		if evt.Type != events.EventType("ops.agent_failed") {
			t.Fatalf("unexpected failure escalation type: %s", evt.Type)
		}
	case <-time.After(800 * time.Millisecond):
		t.Fatal("expected ops.agent_failed escalation event after terminal panic")
	}
}
