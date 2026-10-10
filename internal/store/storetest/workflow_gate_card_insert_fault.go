package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SetWorkflowGateCardInsertFault(ctx context.Context, selected any, run string, enabled bool) error {
	return private.SetWorkflowGateCardInsertFaultForTest(ctx, selected, run, enabled)
}
