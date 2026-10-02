package runtimepersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func (s *PostgresStore) CommitScenarioSetup(ctx context.Context, command bus.ScenarioSetupCommand) (pipeline.ScenarioSetupResult, error) {
	return s.pipelinePostgresOwner.CommitScenarioSetup(ctx, command)
}

func (s *SQLiteRuntimeStore) CommitScenarioSetup(ctx context.Context, command bus.ScenarioSetupCommand) (pipeline.ScenarioSetupResult, error) {
	return s.pipelineSQLiteOwner.CommitScenarioSetup(ctx, command)
}
