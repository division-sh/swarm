package decisioncard

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestCausalExecutionModeUsesExecutorModeOverLiveSourceEvent(t *testing.T) {
	event := eventtest.RootIngress("source-event", "message.received", "external", "", []byte(`{"text":"hello"}`), 0, "run-1", "", events.EventEnvelope{}, time.Now().UTC())
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), event)
	ctx = runtimeeffects.WithExecutionMode(ctx, executionmode.Mock)
	mode, err := CausalExecutionMode(ctx)
	if err != nil {
		t.Fatalf("CausalExecutionMode: %v", err)
	}
	if mode != executionmode.Mock {
		t.Fatalf("execution mode = %q, want mock executor authority", mode)
	}
}

func TestCausalExecutionModeUsesEventForEventOnlyWork(t *testing.T) {
	event := eventtest.RootIngress("source-event", "message.received", "external", "", []byte(`{"text":"hello"}`), 0, "run-1", "", events.EventEnvelope{}, time.Now().UTC())
	mode, err := CausalExecutionMode(runtimecorrelation.WithInboundEvent(context.Background(), event))
	if err != nil {
		t.Fatalf("CausalExecutionMode: %v", err)
	}
	if mode != executionmode.Live {
		t.Fatalf("execution mode = %q, want live source event", mode)
	}
}
