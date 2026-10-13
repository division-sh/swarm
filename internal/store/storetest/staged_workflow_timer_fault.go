package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type StagedWorkflowTimerFault = private.StagedWorkflowTimerFault

const (
	StagedWorkflowTimerMissing       = private.StagedWorkflowTimerMissing
	StagedWorkflowTimerProgressed    = private.StagedWorkflowTimerProgressed
	StagedWorkflowTimerForeignOrigin = private.StagedWorkflowTimerForeignOrigin
)

func CorruptStagedWorkflowTimer(ctx context.Context, selected any, runID, activationID string, fault StagedWorkflowTimerFault) error {
	return private.CorruptStagedWorkflowTimerForTest(ctx, selected, runID, activationID, fault)
}
