package operatorsurface

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
)

func (s *RunPostgres) ListFanOutIntents(ctx context.Context, query fanoutobligation.ListQuery) (fanoutobligation.ListPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return fanoutobligation.ListPage{}, err
	}
	return s.pipeline.ListFanOutIntents(ctx, query)
}

func (s *RunSQLite) ListFanOutIntents(ctx context.Context, query fanoutobligation.ListQuery) (fanoutobligation.ListPage, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return fanoutobligation.ListPage{}, err
	}
	return s.pipeline.ListFanOutIntents(ctx, query)
}
