package manager

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// This outcome is minted only after exact attachment settlement joins, or its
// owner refuses forward admission. It remains cancellation to its caller.
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
	return clean && am.ownsReadinessShutdownCancellation(ctx)
}

func (am *AgentManager) ownsReadinessShutdownCancellation(ctx context.Context) bool {
	if ctx.Err() != context.Canceled {
		return false
	}
	am.lifecycle.mu.Lock()
	defer am.lifecycle.mu.Unlock()
	am.lifecycle.workMu.Lock()
	defer am.lifecycle.workMu.Unlock()
	return am.lifecycle.phase == runtimeLifecycleShuttingDown && am.lifecycle.runOwner.CancelledByShutdown(ctx)
}

func (am *AgentManager) cleanReadinessShutdownReadCancellation(ctx context.Context, err error) bool {
	if _, classified := runtimefailures.As(err); classified {
		return false
	}
	return runtimefailures.OnlyBranches(err, func(branch error) bool { return branch == context.Canceled }) &&
		am.ownsReadinessShutdownCancellation(ctx)
}

// The forward owner refused admission. This is not a cleanup result: the
// readiness owner must still join its exact native attempt before completing.
func (am *AgentManager) cleanReadinessShutdownAdmissionRefusal(ctx context.Context, err error) bool {
	if _, classified := runtimefailures.As(err); classified {
		return false
	}
	return runtimefailures.OnlyBranches(err, func(branch error) bool {
		return branch == worklifetime.ErrRetired || branch == worklifetime.ErrAdmissionFenced
	}) && am.ownsReadinessShutdownCancellation(ctx)
}

func (am *AgentManager) cleanReadinessShutdownForwardCancellation(ctx context.Context, err error) bool {
	if _, classified := runtimefailures.As(err); classified {
		return false
	}
	return runtimefailures.OnlyBranches(err, func(branch error) bool {
		return branch == context.Canceled || branch == worklifetime.ErrRetired || branch == worklifetime.ErrAdmissionFenced
	}) && am.ownsReadinessShutdownCancellation(ctx)
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
	if _, classified := err.(*runtimefailures.Error); classified {
		return canonicalReadinessShutdownRefusal(err) && isReadinessShutdownRefusal(err)
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
