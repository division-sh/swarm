package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func runSelectedFixtureMutation(ctx context.Context, selected any, label string, write func(context.Context, *mutationprotocol.Attempt) error) error {
	apply := func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(txctx, attempt)
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, apply).Err()
	case *SQLiteRuntimeStore:
		return mutationprotocol.RunSQLite(ctx, store.backend, label, mutationprotocol.Story, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, apply).Err()
	default:
		return fmt.Errorf("selected fixture store %T is unsupported", selected)
	}
}
