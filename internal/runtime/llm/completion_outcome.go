package llm

import (
	"errors"

	"github.com/division-sh/swarm/internal/runtime/diaglog"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

// Completion cleanup is a diagnostic only when every error branch is a
// committed phase of the exact response attempt. A joined independent failure
// cannot borrow acknowledgement from its sibling.
func committedCompletionCleanup(response *Response, err error) bool {
	if err == nil {
		return true
	}
	if response == nil || response.completionAttempt == nil {
		return false
	}
	return completionCleanupOnly(err, *response.completionAttempt)
}

func completionCleanupOnly(err error, attempt runtimeeffects.Attempt) bool {
	if err == nil {
		return false
	}
	if committed, ok := err.(*runtimeeffects.PostCommitMutationError); ok {
		if committed.OperationID != attempt.OperationID || committed.AttemptID != attempt.AttemptID || committed.Cause == nil ||
			attempt.OperationID == "" || attempt.AttemptID == "" {
			return false
		}
		switch committed.Phase {
		case runtimeeffects.MutationLaunch, runtimeeffects.MutationObservation, runtimeeffects.MutationHeartbeat,
			runtimeeffects.MutationSettlement, runtimeeffects.MutationProjection:
			return true
		default:
			return false
		}
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		causes := joined.Unwrap()
		if len(causes) == 0 {
			return false
		}
		for _, cause := range causes {
			if !completionCleanupOnly(cause, attempt) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return completionCleanupOnly(wrapped.Unwrap(), attempt)
	}
	return false
}

func recordCompletionCleanup(response *Response, err error) {
	if response == nil || err == nil {
		return
	}
	response.completionCleanupDiagnostics = errors.Join(response.completionCleanupDiagnostics, err)
	if response.completionAttempt != nil {
		attempt := response.completionAttempt
		diaglog.ProcessLog(diaglog.LevelWarn, "llm-runtime", "committed completion cleanup failed",
			"operation_id", attempt.OperationID, "attempt_id", attempt.AttemptID, "error", err.Error())
	}
}
