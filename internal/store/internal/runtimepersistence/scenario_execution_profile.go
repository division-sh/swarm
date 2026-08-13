package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/scenarioexecution"
	scenariopersistence "github.com/division-sh/swarm/internal/store/internal/backend/scenarioexecutionpersistence"
)

func (s *PostgresStore) LoadScenarioExecutionProfile(ctx context.Context, runID string) (scenarioexecution.Profile, bool, error) {
	return scenariopersistence.LoadPostgres(ctx, s.backend.ConstructionHandle(), runID)
}

func (s *SQLiteRuntimeStore) LoadScenarioExecutionProfile(ctx context.Context, runID string) (scenarioexecution.Profile, bool, error) {
	return scenariopersistence.LoadSQLite(ctx, s.backend.ConstructionHandle(), runID)
}
