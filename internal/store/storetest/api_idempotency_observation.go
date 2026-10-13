package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadAPIIdempotencyCardinality(ctx context.Context, selected any, method, key string) (int, error) {
	return private.ReadAPIIdempotencyCardinalityForTest(ctx, selected, method, key)
}
