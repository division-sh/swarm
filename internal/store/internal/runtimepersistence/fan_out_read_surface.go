package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func (s *PostgresStore) ListFanOutIntents(ctx context.Context, query fanoutobligation.ListQuery) (fanoutobligation.ListPage, error) {
	return s.operatorRunPostgres.ListFanOutIntents(ctx, query)
}

func (s *SQLiteRuntimeStore) ListFanOutIntents(ctx context.Context, query fanoutobligation.ListQuery) (fanoutobligation.ListPage, error) {
	return s.operatorRunSQLite.ListFanOutIntents(ctx, query)
}
