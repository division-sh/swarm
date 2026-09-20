package conformance

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
)

// Pause exactly the first real task handler before its engine mutation. The
// pipeline's existing lifecycle hook owns this call; no subscriber is added.
type nestedChildHandlerGate struct {
	eventName string
	mu        sync.Mutex
	chosen    bool
	started   chan lifecycleprobe.Signal
	release   chan struct{}
	once      sync.Once
}

func newNestedChildHandlerGate(t *testing.T, eventName string) *nestedChildHandlerGate {
	t.Helper()
	if eventName == "" {
		t.Fatal("nested child gate requires a canonical event name")
	}
	g := &nestedChildHandlerGate{eventName: eventName, started: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *nestedChildHandlerGate) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.HandlerStarted || eventidentity.LeafName(signal.EventType) != g.eventName {
		return
	}
	g.mu.Lock()
	if g.chosen {
		g.mu.Unlock()
		return
	}
	g.chosen = true
	g.mu.Unlock()
	g.started <- signal
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

func (g *nestedChildHandlerGate) open() { g.once.Do(func() { close(g.release) }) }

func (g *nestedChildHandlerGate) wait(t *testing.T) lifecycleprobe.Signal {
	t.Helper()
	return g.waitUntil(t, time.Now().Add(5*time.Second))
}

func (g *nestedChildHandlerGate) waitUntil(t *testing.T, deadline time.Time) lifecycleprobe.Signal {
	t.Helper()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case signal := <-g.started:
		if !time.Now().Before(deadline) {
			t.Fatal("real nested handler reached execution gate after deadline")
		}
		if signal.EventID == "" || signal.SubscriberID == "" || signal.SubscriberType != "node" {
			t.Fatalf("held child lacks actual event/handler identity: %+v", signal)
		}
		return signal
	case <-timer.C:
		t.Fatalf("real nested handler %s never reached execution gate", g.eventName)
	}
	return lifecycleprobe.Signal{}
}
