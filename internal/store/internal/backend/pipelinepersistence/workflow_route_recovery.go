package pipelinepersistence

import (
	"context"
	"fmt"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeworkflowroute "github.com/division-sh/swarm/internal/runtime/workflowroute"
)

func (s *PipelinePostgresOwner) LoadActiveWorkflowRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (runtimeworkflowroute.RecoveryRecord, error) {
	if s == nil || s.workflowRoutes == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return s.workflowRoutes.LoadActive(ctx, identity)
}

func (s *PipelineSQLiteOwner) LoadActiveWorkflowRoute(ctx context.Context, identity runtimeflowidentity.RunScopedFlowInstance) (runtimeworkflowroute.RecoveryRecord, error) {
	if s == nil || s.workflowRoutes == nil {
		return runtimeworkflowroute.RecoveryRecord{}, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimeworkflowroute.RecoveryRecord{}, err
	}
	return s.workflowRoutes.LoadActive(ctx, identity)
}
