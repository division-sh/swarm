package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
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

	if err := am.ReplayAgentBacklog(context.Background(), "agent-1"); err == nil || err.Error() != "runtime shutting down" {
		t.Fatalf("ReplayAgentBacklog err = %v, want runtime shutting down", err)
	}
	if store.listPendingCalled.Load() {
		t.Fatal("ReplayAgentBacklog touched the store even though runtime shutdown admission was already closed")
	}
}

func TestRestartAgent_DeniesWhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	agent := shutdownTestAgent{id: "agent-1"}
	am := NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	}, AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
	})
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: agent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	if err := am.RestartAgent(agent.id); err == nil || err.Error() != "runtime shutting down" {
		t.Fatalf("RestartAgent err = %v, want runtime shutting down", err)
	}
}

func TestResetRuntimeState_KeepsManagerAdmissionClosedDuringManagerLocalShutdown(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &shutdownAdmissionManagerStore{}
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	agent := shutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, nil
		},
	}

	am := NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	}, AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return false },
	}, store)
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: agent.id},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	am.Run(context.Background())
	if err := bus.Publish(context.Background(), events.NewProjectionEvent("evt-in-1",
		events.EventType("test.in"),
		"tester", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reset-path in-flight work to start")
	}

	resetErrCh := make(chan error, 1)
	go func() {
		resetErrCh <- am.ResetRuntimeState()
	}()

	waitForManagerShuttingDown(t, am)

	if err := am.ReplayAgentBacklog(context.Background(), "agent-1"); err == nil || err.Error() != "runtime shutting down" {
		t.Fatalf("ReplayAgentBacklog during reset shutdown err = %v, want runtime shutting down", err)
	}
	if store.listPendingCalled.Load() {
		t.Fatal("ReplayAgentBacklog touched the store while reset-driven manager shutdown was in progress")
	}

	close(release)

	select {
	case err := <-resetErrCh:
		if err != nil {
			t.Fatalf("ResetRuntimeState: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for ResetRuntimeState to finish")
	}
}

func TestAuthBreakerShutdown_KeepsManagerAdmissionClosedDuringManagerLocalShutdown(t *testing.T) {
	runtimebus.ResumeRuntimeIngress()
	defer runtimebus.ResumeRuntimeIngress()

	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	store := &shutdownAdmissionManagerStore{}
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	agent := shutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, nil
		},
	}

	am := NewAgentManagerWithOptions(bus, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	}, AgentManagerOptions{
		RuntimeShutdownAdmissionClosed: func() bool { return false },
	}, store)
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: agent.id},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	am.Run(context.Background())
	if err := bus.Publish(context.Background(), events.NewProjectionEvent("evt-in-1",
		events.EventType("test.in"),
		"tester", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for auth-breaker in-flight work to start")
	}

	breakerDone := make(chan struct{})
	go func() {
		am.maybeTripAuthCircuitBreaker(context.Background(), agent.id, "evt-in-1", errors.New("claude auth required"))
		close(breakerDone)
	}()

	waitForManagerShuttingDown(t, am)

	if err := am.ReplayAgentBacklog(context.Background(), "agent-1"); err == nil || err.Error() != "runtime shutting down" {
		t.Fatalf("ReplayAgentBacklog during auth-breaker shutdown err = %v, want runtime shutting down", err)
	}
	if store.listPendingCalled.Load() {
		t.Fatal("ReplayAgentBacklog touched the store while auth-breaker shutdown was in progress")
	}

	close(release)

	select {
	case <-breakerDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for auth-breaker shutdown to finish")
	}
}

func waitForManagerShuttingDown(t *testing.T, am *AgentManager) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		am.runMu.Lock()
		shuttingDown := am.shuttingDown
		am.runMu.Unlock()
		if shuttingDown {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for manager shutdown to start")
}
