package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SetBarrierCompletionPublicationFault(ctx context.Context, selected any, run, entity string, revision, mutations int, enabled bool) error {
	return private.SetBarrierCompletionPublicationFaultForTest(ctx, selected, run, entity, revision, mutations, enabled)
}
