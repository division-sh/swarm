package pipeline

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/google/uuid"
)

type deadLetterAckPipelineBus struct {
	pipelineTestBus
	logs []RuntimeLogEntry
}

func (b *deadLetterAckPipelineBus) LogRuntime(_ context.Context, entry RuntimeLogEntry) error {
	b.logs = append(b.logs, entry)
	return nil
}

type deadLetterAckPipelineRecorder struct {
	outcome runtimedeadletters.RecordOutcome
	err     error
	calls   int
}

func (r *deadLetterAckPipelineRecorder) RecordDeadLetterOutcome(context.Context, runtimedeadletters.Record) (runtimedeadletters.RecordOutcome, error) {
	r.calls++
	return r.outcome, r.err
}

func TestInterceptedEmitDeadLetterAcknowledgementLogging(t *testing.T) {
	fault := errors.New("injected post-commit cleanup failure")
	for _, tc := range []struct {
		name       string
		outcome    runtimedeadletters.RecordOutcome
		err        error
		wantAction string
	}{
		{name: "acknowledged_cleanup", outcome: runtimedeadletters.RecordOutcome{Acknowledged: true}, err: fault, wantAction: "intercepted_emit_dead_letter_post_commit_failed"},
		{name: "unacknowledged_error", err: fault, wantAction: "intercepted_emit_dead_letter_persist_failed"},
		{name: "unacknowledged_without_error", wantAction: "intercepted_emit_dead_letter_persist_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bus := &deadLetterAckPipelineBus{}
			recorder := &deadLetterAckPipelineRecorder{outcome: tc.outcome, err: tc.err}
			coordinator := &PipelineCoordinator{bus: bus, deadLetters: recorder}
			trigger := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.received", "gateway", "task-1", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			intercepted := eventtest.Child(uuid.NewString(), "work.emitted", "agent-a", "", []byte(`{}`), 1, trigger, events.EventEnvelope{}, time.Now().UTC())
			emissions := &pipelineEmissionPlan{}
			coordinator.recordInterceptedEmitDeadLetters(context.Background(), trigger, "node-a", &handlerExecutionOutcome{InterceptedEmits: []runtimeengine.EmitIntent{{
				Event: intercepted, ChainDepth: 2, DeadLetterHint: "chain_depth_exceeded",
			}}}, emissions)
			if recorder.calls != 1 || len(emissions.immutableEvents()) != 1 {
				t.Fatalf("record calls=%d emitted diagnostics=%d, want 1/1", recorder.calls, len(emissions.immutableEvents()))
			}
			if len(bus.logs) != 1 || bus.logs[0].Action != tc.wantAction {
				t.Fatalf("logging = %+v, want only %s", bus.logs, tc.wantAction)
			}
		})
	}
}
