package manager

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestAgentManagerRestartReplacesRunningLoopGeneration(t *testing.T) {
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	oldLoopStarted := make(chan struct{}, 1)
	oldLoopCanceled := make(chan struct{}, 1)
	newLoopHandled := make(chan struct{}, 1)
	var calls atomic.Int32
	agent := shutdownTestAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"test.in"},
		onEvent: func(ctx context.Context, _ events.Event) ([]events.Event, error) {
			if calls.Add(1) == 1 {
				oldLoopStarted <- struct{}{}
				<-ctx.Done()
				oldLoopCanceled <- struct{}{}
				return nil, ctx.Err()
			}
			newLoopHandled <- struct{}{}
			return nil, nil
		},
	}
	am := NewAgentManager(bus, func(runtimeactors.AgentConfig) (Agent, error) {
		return agent, nil
	})
	if err := am.SpawnAgent(runtimeactors.AgentConfig{ID: agent.id}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	am.Run(context.Background())
	t.Cleanup(func() {
		if err := am.Shutdown(); err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	})

	if err := bus.Publish(context.Background(), eventtest.RootIngress(
		"restart-old-generation",
		events.EventType("test.in"),
		"tester",
		"",
		nil,
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("publish old-generation event: %v", err)
	}
	select {
	case <-oldLoopStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for old loop generation")
	}

	am.runMu.Lock()
	previousGeneration := am.loopGeneration[agent.id]
	am.runMu.Unlock()
	if previousGeneration == 0 {
		t.Fatal("old loop generation = 0, want active generation")
	}

	restartDone := make(chan error, 1)
	go func() {
		_, err := am.Restart(context.Background(), runtimeagentcontrol.RestartRequest{AgentID: agent.id})
		restartDone <- err
	}()
	select {
	case <-oldLoopCanceled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for old loop cancellation")
	}
	select {
	case err := <-restartDone:
		if err != nil {
			t.Fatalf("Restart: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Restart returned before replacing the old loop generation")
	}

	am.runMu.Lock()
	currentGeneration := am.loopGeneration[agent.id]
	am.runMu.Unlock()
	if currentGeneration != previousGeneration+1 {
		t.Fatalf("loop generation = %d, want %d", currentGeneration, previousGeneration+1)
	}

	if err := bus.Publish(context.Background(), eventtest.RootIngress(
		"restart-new-generation",
		events.EventType("test.in"),
		"tester",
		"",
		nil,
		0,
		"",
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("publish new-generation event: %v", err)
	}
	select {
	case <-newLoopHandled:
	case <-time.After(time.Second):
		t.Fatal("new loop generation did not handle post-restart event")
	}
}
