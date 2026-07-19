package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

type contextualLineagePipelineBus struct {
	noopPipelineBus
	published []events.Event
	direct    []events.Event
}

func (b *contextualLineagePipelineBus) Publish(_ context.Context, event events.Event) error {
	b.published = append(b.published, event)
	return nil
}

func (b *contextualLineagePipelineBus) PublishDirect(_ context.Context, event events.Event, _ []string) error {
	b.direct = append(b.direct, event)
	return nil
}

func TestPipelineRuntimeDiagnosticsPreserveExactContextualLineage(t *testing.T) {
	runID, parentID := uuid.NewString(), uuid.NewString()
	parent := eventtest.RunCreatingRootIngressWithMode(
		parentID, "work.received", "gateway", "task-1", []byte(`{}`), 0, runID, "",
		events.EventEnvelope{}, time.Now().UTC(),
		executionmode.Mock,
	)
	ctx := runtimecorrelation.WithInboundEvent(context.Background(), parent)
	bus := &contextualLineagePipelineBus{}
	coordinator := &PipelineCoordinator{bus: bus}

	if err := coordinator.publish(ctx, "platform.dead_letter", "", map[string]any{}); err != nil {
		t.Fatalf("publish contextual diagnostic: %v", err)
	}
	if err := coordinator.publishDirect(ctx, "platform.dead_letter", "", map[string]any{}, []string{"agent-a"}); err != nil {
		t.Fatalf("publish direct contextual diagnostic: %v", err)
	}
	collector := []events.Event{}
	collectorCtx := context.WithValue(ctx, pipelineEmitCollectorKey{}, &collector)
	coordinator.recordInterceptedEmitDeadLetters(collectorCtx, parent, "node-a", &handlerExecutionOutcome{InterceptedEmits: []runtimeengine.EmitIntent{{
		Event:      eventtest.Child(uuid.NewString(), "work.emitted", "agent-a", "", []byte(`{}`), 1, parent, events.EventEnvelope{}, time.Now().UTC()),
		ChainDepth: 2, DeadLetterHint: "chain_depth_exceeded",
	}}})

	manifestations := []struct {
		name  string
		event events.Event
	}{
		{name: "normal", event: bus.published[0]},
		{name: "direct", event: bus.direct[0]},
		{name: "intercepted", event: collector[0]},
	}
	for _, manifestation := range manifestations {
		t.Run(manifestation.name, func(t *testing.T) {
			got := manifestation.event
			if got.RunID() != runID || got.ParentEventID() != parentID || got.TaskID() != "task-1" || got.ExecutionMode() != executionmode.Mock {
				t.Fatalf("lineage = run:%q parent:%q task:%q mode:%q", got.RunID(), got.ParentEventID(), got.TaskID(), got.ExecutionMode())
			}
		})
	}
}
