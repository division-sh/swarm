package pipelinepersistence

import (
	"testing"

	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestWorkflowEngineStateRevisionConflictIsRetryable(t *testing.T) {
	err := workflowEngineStateRevisionConflict(runtimepipeline.WorkflowEngineStateRecord{
		ExpectedRevision: 3,
		ExpectedState:    "collecting",
	})
	failure, ok := runtimefailures.As(err)
	if !ok {
		t.Fatalf("workflow revision conflict is untyped: %v", err)
	}
	if failure.Failure.Class != runtimefailures.ClassLifecycleConflict ||
		failure.Failure.Detail.Code != "workflow_engine_state_revision_conflict" ||
		!failure.Failure.Retryable {
		t.Fatalf("workflow revision conflict = %#v, want retryable lifecycle conflict", failure.Failure)
	}
	if got := runtimeengine.FailureDispositionFor(err); got != runtimeengine.FailureDispositionRetry {
		t.Fatalf("workflow revision conflict disposition = %s, want retry", got)
	}
}
