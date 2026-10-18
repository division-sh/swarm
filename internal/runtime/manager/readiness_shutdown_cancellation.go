package manager

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// This outcome is minted only after the exact attachment's native cancellation
// and durable abandonment have joined. It remains cancellation to its caller.
type readinessShutdownCancellation struct {
	cause    error
	key      dynamicFlowRuntimeReadinessKey
	ordinal  uint64
	planHash string
}

func (e *readinessShutdownCancellation) Error() string { return e.cause.Error() }
func (e *readinessShutdownCancellation) Unwrap() error { return e.cause }

func (am *AgentManager) cleanReadinessShutdownCancellation(ctx context.Context, result runtimepipeline.FlowAttachmentAdvanceResult, err error) bool {
	_, clean := result.CleanCancellation(err)
	return clean && ctx.Err() == context.Canceled &&
		context.Cause(ctx) == worklifetime.ErrRetired &&
		am.lifecycle.phaseSnapshot() == runtimeLifecycleShuttingDown
}

func isReadinessShutdownCompletion(err error, key dynamicFlowRuntimeReadinessKey, attempt *dynamicFlowRuntimeReadinessAttempt) bool {
	if err == nil {
		return false
	}
	if cancelled, ok := err.(*readinessShutdownCancellation); ok {
		return cancelled != nil && cancelled.cause != nil && cancelled.ordinal != 0 && cancelled.planHash != "" &&
			attempt != nil && cancelled.key == key &&
			cancelled.ordinal == attempt.attemptOrdinal && cancelled.planHash == attempt.planHash
	}
	if isReadinessShutdownRefusal(err) {
		return true
	}
	if _, classified := err.(*runtimefailures.Error); classified {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		branches := joined.Unwrap()
		if len(branches) == 0 {
			return false
		}
		for _, branch := range branches {
			if !isReadinessShutdownCompletion(branch, key, attempt) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return isReadinessShutdownCompletion(wrapped.Unwrap(), key, attempt)
	}
	return false
}
