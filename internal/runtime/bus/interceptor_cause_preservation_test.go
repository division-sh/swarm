package bus

import (
	"context"
	"errors"
	"fmt"
	"testing"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
)

func TestInterceptorSetPreservesTypedFailureCause(t *testing.T) {
	conflict := runtimefailures.New(runtimefailures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict", "pipeline-engine", "commit_state", nil)
	for _, scenario := range []string{"typed_error", "wrapped_error", "retry_release", "committed_cleanup", "untyped_error"} {
		t.Run(scenario, func(t *testing.T) {
			cause := error(conflict)
			outcome := runtimepipelineobligation.Continue()
			wantClass, wantCode := runtimefailures.ClassLifecycleConflict, "workflow_engine_state_revision_conflict"
			switch scenario {
			case "wrapped_error":
				cause = fmt.Errorf("transition rejected: %w", conflict)
			case "retry_release":
				failure := runtimefailures.Normalize(conflict, "pipeline", "timer")
				outcome = runtimepipelineobligation.ReleaseForRetry(wantCode, &failure)
			case "committed_cleanup":
				outcome.Committed = true
			case "untyped_error":
				cause = errors.New("interceptor failed")
				wantClass, wantCode = runtimefailures.ClassInternalFailure, "event_interceptor_failed"
			}
			interceptor := &outcomeTestInterceptor{outcome: outcome, err: cause}
			_, _, got, err := (&EventBus{}).runInterceptorSet(context.Background(), receiverProjectionEvent("timer"), []EventInterceptor{interceptor})
			failure := runtimefailures.Normalize(err, "eventbus", "test")
			if !errors.Is(err, cause) || failure.Class != wantClass || failure.Detail.Code != wantCode || got.Committed != outcome.Committed {
				t.Fatalf("outcome=%+v failure=%+v error=%v", got, failure, err)
			}
			if retry, ok := got.RetryRelease(); scenario == "retry_release" && (!ok || retry.ReasonCode() != wantCode || retry.Failure().Detail.Code != wantCode) {
				t.Fatalf("typed retry lost: %+v", got)
			}
		})
	}
}

func TestInterceptorSetPreservesUnclassifiedContextInterruption(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, fmt.Errorf("authorization read: %w", context.DeadlineExceeded)} {
		interceptor := &outcomeTestInterceptor{outcome: runtimepipelineobligation.Continue(), err: cause}
		_, _, outcome, err := (&EventBus{}).runInterceptorSet(context.Background(), receiverProjectionEvent("timer"), []EventInterceptor{interceptor})
		if !errors.Is(err, cause) || !runtimefailures.IsContextInterruption(err) || outcome.Committed || !outcome.ContinueDispatch() {
			t.Fatalf("interruption became refusal or acknowledgement: %+v %v", outcome, err)
		}
	}
}
