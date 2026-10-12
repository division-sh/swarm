package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func AdvanceSelectionRevisionPair(ctx context.Context, selected any, state pipeline.WorkflowEngineStateRecord) error {
	return private.AdvanceSelectionRevisionPairForTest(ctx, selected, state)
}

func AdvanceProspectiveRevisionPair(ctx context.Context, selected any, state pipeline.WorkflowEngineStateRecord) error {
	return private.AdvanceProspectiveRevisionPairForTest(ctx, selected, state)
}
