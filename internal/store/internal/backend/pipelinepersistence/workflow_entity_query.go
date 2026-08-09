package pipelinepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/entityquery"
)

func (s *PipelinePostgresOwner) CountWorkflowEntities(ctx context.Context, request entityquery.Request) (int, error) {
	if s == nil || s.workflowEntityQueries == nil {
		return 0, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return 0, err
	}
	return s.workflowEntityQueries.Count(ctx, request)
}

func (s *PipelineSQLiteOwner) CountWorkflowEntities(ctx context.Context, request entityquery.Request) (int, error) {
	if s == nil || s.workflowEntityQueries == nil {
		return 0, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return 0, err
	}
	return s.workflowEntityQueries.Count(ctx, request)
}
