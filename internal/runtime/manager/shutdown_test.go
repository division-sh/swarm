package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeactors "swarm/internal/runtime/core/actors"
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
			return []events.Event{{
				ID:          "evt-out-1",
				Type:        events.EventType("test.out"),
				SourceAgent: "agent-1",
				CreatedAt:   time.Now().UTC(),
			}}, nil
		},
	}

	am := NewAgentManager(bus, func(cfg runtimeactors.AgentConfig) (Agent, error) {
		if cfg.ID != agent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return agent, nil
	})
	if err := am.spawnAgentInternal(context.Background(), PersistedAgent{
		Config: runtimeactors.AgentConfig{ID: agent.id},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	am.Run(context.Background())
	if err := bus.Publish(context.Background(), events.Event{
		ID:          "evt-in-1",
		Type:        events.EventType("test.in"),
		SourceAgent: "tester",
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
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
		if got := string(evt.Type); got != "test.out" {
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
		Config: runtimeactors.AgentConfig{ID: agent.id},
	}, false); err != nil {
		t.Fatalf("spawnAgentInternal: %v", err)
	}

	am.Run(context.Background())
	for _, eventID := range []string{"evt-in-1", "evt-in-2"} {
		if err := bus.Publish(context.Background(), events.Event{
			ID:          eventID,
			Type:        events.EventType("test.in"),
			SourceAgent: "tester",
			CreatedAt:   time.Now().UTC(),
		}); err != nil {
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
