package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SetSelectedBranchDivergenceInsertFault(ctx context.Context, selected any, enabled bool) error {
	return private.SetSelectedBranchDivergenceInsertFaultForTest(ctx, selected, enabled)
}
