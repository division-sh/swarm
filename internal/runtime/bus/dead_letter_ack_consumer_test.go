package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedeadletters "github.com/division-sh/swarm/internal/runtime/deadletters"
	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

type deadLetterAckBusLogger struct {
	actions []string
}

func (*deadLetterAckBusLogger) ProjectLifecycleDiagnostic(context.Context, diaglog.LifecycleDiagnostic) error {
	return nil
}

func (l *deadLetterAckBusLogger) Log(_ context.Context, _ diaglog.Level, _, _, action, _, _, _, _, _ string, _ map[string]string, _ any, _ *runtimefailures.Envelope, _ int) error {
	l.actions = append(l.actions, action)
	return nil
}

type deadLetterAckBusRecorder struct {
	outcome runtimedeadletters.RecordOutcome
	err     error
	calls   int
}

func (r *deadLetterAckBusRecorder) RecordDeadLetterOutcome(context.Context, runtimedeadletters.Record) (runtimedeadletters.RecordOutcome, error) {
	r.calls++
	return r.outcome, r.err
}

func TestTargetFailureDeadLetterAcknowledgementLogging(t *testing.T) {
	fault := errors.New("injected post-commit cleanup failure")
	for _, tc := range []struct {
		name       string
		outcome    runtimedeadletters.RecordOutcome
		err        error
		wantAction string
	}{
		{name: "acknowledged_cleanup", outcome: runtimedeadletters.RecordOutcome{Acknowledged: true}, err: fault, wantAction: "target_resolution_failed_dead_letter_post_commit_failed"},
		{name: "unacknowledged_error", err: fault, wantAction: "target_resolution_failed_dead_letter_failed"},
		{name: "unacknowledged_without_error", wantAction: "target_resolution_failed_dead_letter_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := &deadLetterAckBusLogger{}
			recorder := &deadLetterAckBusRecorder{outcome: tc.outcome, err: tc.err}
			bus := &EventBus{logger: logger, ephemeral: true, durable: DurableDependencies{TargetFailureRecorder: recorder}}
			event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.requested", "gateway", "task-1", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
			bus.recordTargetDeliveryFailure(context.Background(), event, RoutePlan{TargetFailure: runtimepinrouting.FailureTargetRequiredMissing})
			if recorder.calls != 1 {
				t.Fatalf("record calls=%d, want 1", recorder.calls)
			}
			if len(logger.actions) != 2 || logger.actions[0] != "target_resolution_failed" || logger.actions[1] != tc.wantAction {
				t.Fatalf("log actions=%v, want target_resolution_failed/%s", logger.actions, tc.wantAction)
			}
		})
	}
}
