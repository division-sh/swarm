package store

import "github.com/division-sh/swarm/internal/runtime/core/timeridentity"

func aggregateWorkflowTimerTaskID(timerID string) string {
	return timeridentity.WorkflowTimerActivationRef{
		ActivationID:        timerID,
		Declaration:         "aggregate.cleanup.proof",
		DeclarationRevision: "sha256:aggregate-cleanup-proof",
		Cause:               timeridentity.WorkflowTimerActivationCauseInitial,
	}.TaskID()
}
