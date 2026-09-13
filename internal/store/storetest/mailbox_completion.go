package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SetMailboxCompletionInsertFault(ctx context.Context, selected any, key, witness string, enabled bool) error {
	return private.SetMailboxCompletionInsertFaultForTest(ctx, selected, key, witness, enabled)
}
