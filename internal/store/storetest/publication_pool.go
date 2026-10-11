package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/store"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func LimitPostgresPublicationFixturePool(ctx context.Context, selected *store.PostgresStore) error {
	return private.LimitPostgresPublicationFixturePoolForTest(ctx, selected)
}
