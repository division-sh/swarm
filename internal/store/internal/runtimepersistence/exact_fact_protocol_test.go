package runtimepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func runExactFactProtocol[T any](ctx context.Context, s exactFactStore, write func(context.Context, *mutationprotocol.Attempt) (T, error)) mutationprotocol.Result[T] {
	switch selected := s.selected.(type) {
	case *PostgresStore:
		return mutationprotocol.RunPostgres(ctx, selected.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	case *SQLiteRuntimeStore:
		return mutationprotocol.RunSQLite(ctx, selected.backend, "exact fact protocol proof", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, write)
	default:
		return mutationprotocol.Reject[T](fmt.Errorf("unsupported exact fact store %T", s.selected))
	}
}
