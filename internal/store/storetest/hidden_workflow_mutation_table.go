package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func HideWorkflowMutationTable(ctx context.Context, selected any, hidden bool) error {
	return private.HideWorkflowMutationTableForTest(ctx, selected, hidden)
}
