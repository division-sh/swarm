package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ProbeWorkflowMutationMissingDomainPath(ctx context.Context, selected any, run, entity string) error {
	return private.ProbeWorkflowMutationMissingDomainPathForTest(ctx, selected, run, entity)
}
