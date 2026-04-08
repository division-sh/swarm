package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
)

type shutdownAdmissionManagerStore struct {
	listPendingCalled atomic.Bool
}

func (*shutdownAdmissionManagerStore) UpsertAgent(context.Context, PersistedAgent) error {
	return nil
}

func (*shutdownAdmissionManagerStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}

func (*shutdownAdmissionManagerStore) MarkAgentTerminated(context.Context, string) error {
	return nil
}

func (*shutdownAdmissionManagerStore) EnsureEntitySchema(context.Context, string) error {
	return nil
}

func (*shutdownAdmissionManagerStore) UpsertEventReceipt(context.Context, string, string, ReceiptStatus, string) error {
	return nil
}

func (s *shutdownAdmissionManagerStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	s.listPendingCalled.Store(true)
	return nil, nil
}

func (s *shutdownAdmissionManagerStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	s.listPendingCalled.Store(true)
	return nil, nil
}

func TestRun_UsesRuntimeShutdownAdmissionOwner(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	var closed atomic.Bool
	closed.Store(true)

	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: closed.Load,
	})

	am.Run(context.Background())

	if am.IsRunning() {
		t.Fatal("Run started manager even though runtime shutdown admission was already closed")
	}
}

func TestReplayAgentBacklog_UsesRuntimeShutdownAdmissionOwnerBeforeStoreAccess(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	var closed atomic.Bool
	closed.Store(true)
	store := &shutdownAdmissionManagerStore{}

	am := NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: closed.Load,
	}, store)

	if err := am.ReplayAgentBacklog(context.Background(), "agent-1"); err != nil {
		t.Fatalf("ReplayAgentBacklog: %v", err)
	}
	if store.listPendingCalled.Load() {
		t.Fatal("ReplayAgentBacklog touched the store even though runtime shutdown admission was already closed")
	}
}
