package runforkpersistence

import (
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// These errors carry only values whose native transaction acknowledged commit.
// The existing store signatures retain the cleanup diagnostic without exposing
// an uncommitted value to their callers.
type selectedForkMaterializationPostCommitError struct {
	value runfork.RunForkMaterialization
	cause error
}

func (e *selectedForkMaterializationPostCommitError) Error() string { return e.cause.Error() }
func (e *selectedForkMaterializationPostCommitError) Unwrap() error { return e.cause }
func (e *selectedForkMaterializationPostCommitError) SelectedForkMaterializationCommit() runfork.RunForkMaterialization {
	return e.value
}

type selectedForkSourceEventsPostCommitError struct {
	sourceRunID string
	forkRunID   string
	value       []runfork.RunForkSelectedContractSourceEvent
	cause       error
}

func (e *selectedForkSourceEventsPostCommitError) Error() string { return e.cause.Error() }
func (e *selectedForkSourceEventsPostCommitError) Unwrap() error { return e.cause }
func (e *selectedForkSourceEventsPostCommitError) SelectedForkSourceEventsCommit() (string, string, []runfork.RunForkSelectedContractSourceEvent) {
	return e.sourceRunID, e.forkRunID, append([]runfork.RunForkSelectedContractSourceEvent(nil), e.value...)
}
