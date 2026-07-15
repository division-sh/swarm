package manager

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

type shutdownTestAgent struct {
	id            string
	subscriptions []events.EventType
	onEvent       func(context.Context, events.Event) ([]events.Event, error)
}

func (a shutdownTestAgent) ID() string { return a.id }
func (shutdownTestAgent) Type() string { return "test" }
func (a shutdownTestAgent) Subscriptions() []events.EventType {
	return append([]events.EventType(nil), a.subscriptions...)
}
func (a shutdownTestAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	return a.onEvent(ctx, evt)
}

func TestShutdown_DrainsInFlightWorkBeforeCancellingLoopContext(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	outputs := bus.Subscribe("observer", events.EventType("test.out"))

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	ctxErrCh := make(chan error, 1)

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
				ctxErrCh <- ctx.Err()
				return nil, ctx.Err()
			}
			ctxErrCh <- ctx.Err()
			return []events.Event{
				eventtest.RootIngress("evt-out-1", events.EventType("test.out"), "agent-1", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC()),
			}, nil
		},
	}

	am := NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		if cfg.ID != agent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return agent, nil
	})
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	am.Run(managedExecutionTestContext(t, context.Background()))
	if err := bus.Publish(context.Background(), eventtest.RootIngress("evt-in-1",
		events.EventType("test.in"),
		"tester", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight work to start")
	}

	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- am.Shutdown()
	}()

	select {
	case err := <-shutdownErrCh:
		t.Fatalf("Shutdown returned before in-flight work drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-ctxErrCh:
		if err != nil {
			t.Fatalf("OnEvent context canceled during shutdown drain: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OnEvent completion")
	}

	select {
	case evt := <-outputs:
		if got := string(evt.Type()); got != "test.out" {
			t.Fatalf("output event type = %q, want test.out", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for output publish during shutdown")
	}

	select {
	case err := <-shutdownErrCh:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown to finish")
	}
}

func TestShutdownWithOptions_TimesOutAfterConfiguredGraceAndCancelsLoopContext(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	started := make(chan struct{}, 1)
	ctxErrCh := make(chan error, 1)

	agent := shutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-ctx.Done()
			ctxErrCh <- ctx.Err()
			return nil, ctx.Err()
		},
	}

	am := NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	})
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	am.Run(managedExecutionTestContext(t, context.Background()))
	if err := bus.Publish(context.Background(), eventtest.RootIngress("evt-in-1",
		events.EventType("test.in"),
		"tester", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight work to start")
	}

	grace := 25 * time.Millisecond
	err = am.ShutdownWithOptions(ShutdownOptions{Grace: grace})
	if err == nil || !strings.Contains(err.Error(), "agent manager shutdown drain timed out after 25ms") {
		t.Fatalf("ShutdownWithOptions err = %v, want configured grace timeout", err)
	}

	select {
	case err := <-ctxErrCh:
		if err == nil {
			t.Fatal("OnEvent context error = nil, want cancellation after timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight context cancellation")
	}
}

func TestShutdownWithOptions_RejectsNegativeGrace(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	am := NewAgentManager(bus, nil)

	err = am.ShutdownWithOptions(ShutdownOptions{Grace: -time.Second})
	if err == nil || !strings.Contains(err.Error(), "shutdown grace must be positive") {
		t.Fatalf("ShutdownWithOptions err = %v, want positive grace validation", err)
	}
}

func TestShutdown_DoesNotStartQueuedWorkAfterDrainBegins(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})
	var processed atomic.Int32

	agent := shutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			n := processed.Add(1)
			if n == 1 {
				select {
				case firstStarted <- struct{}{}:
				default:
				}
				select {
				case <-releaseFirst:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return nil, nil
		},
	}

	am := NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	})
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	am.Run(managedExecutionTestContext(t, context.Background()))
	for _, eventID := range []string{"evt-in-1", "evt-in-2"} {
		if err := bus.Publish(context.Background(), eventtest.RootIngress(eventID,
			events.EventType("test.in"),
			"tester", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
			t.Fatalf("Publish(%s): %v", eventID, err)
		}
	}

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first event to start")
	}

	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- am.Shutdown()
	}()

	select {
	case err := <-shutdownErrCh:
		t.Fatalf("Shutdown returned before in-flight work drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirst)

	select {
	case err := <-shutdownErrCh:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown to finish")
	}

	if got := processed.Load(); got != 1 {
		t.Fatalf("processed events = %d, want 1 after shutdown drain", got)
	}
}

func TestShutdown_DoesNotAllowRunToReplaceActiveRunContextDuringDrain(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	firstStarted := make(chan struct{}, 1)
	releaseFirst := make(chan struct{})

	agent := shutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, evt events.Event) ([]events.Event, error) {
			select {
			case firstStarted <- struct{}{}:
			default:
			}
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return nil, nil
		},
	}

	am := NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	})
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: agent.id},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	am.Run(managedExecutionTestContext(t, context.Background()))
	initialRunCtx, _, _ := am.lifecycle.runSnapshot()
	if initialRunCtx == nil {
		t.Fatal("expected initial run context")
	}

	if err := bus.Publish(context.Background(), eventtest.RootIngress("evt-in-1",
		events.EventType("test.in"),
		"tester", "", nil, 0, "", "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first event to start")
	}

	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- am.Shutdown()
	}()

	deadline := time.Now().Add(time.Second)
	for !am.isShuttingDown() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for shutdown drain to begin")
		}
		time.Sleep(5 * time.Millisecond)
	}

	am.Run(managedExecutionTestContext(t, context.Background()))

	currentRunCtx, _, running := am.lifecycle.runSnapshot()
	shuttingDown := am.lifecycle.phaseSnapshot() == runtimeLifecycleShuttingDown
	if currentRunCtx != initialRunCtx {
		t.Fatal("Run replaced the active run context during shutdown drain")
	}
	if running {
		t.Fatal("Run reopened manager during shutdown drain")
	}
	if !shuttingDown {
		t.Fatal("shutdown drain state was cleared by concurrent Run")
	}

	close(releaseFirst)

	select {
	case err := <-shutdownErrCh:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown to finish")
	}
}
