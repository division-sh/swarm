package pipelinepersistence

import (
	"context"
	"fmt"

	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
)

func (s *PipelinePostgresOwner) LoadActiveWorkflowRoute(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	if s == nil || s.workflowRoutes == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return s.workflowRoutes.LoadActive(ctx, instancePath)
}

func (s *PipelineSQLiteOwner) LoadActiveWorkflowRoute(ctx context.Context, instancePath string) (runtimeworkflowroute.RecoveryRecord, error) {
	if s == nil || s.workflowRoutes == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return s.workflowRoutes.LoadActive(ctx, instancePath)
}
