package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func HoldUnstampedPipelineAdmissionTransaction(ctx context.Context, selected any, run string) (func() error, error) {
	return private.HoldUnstampedPipelineAdmissionTransactionForTest(ctx, selected, run)
}
