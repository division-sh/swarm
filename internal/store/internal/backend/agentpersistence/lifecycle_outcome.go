package agentpersistence

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/manager"
)

func (s *AgentPostgresOwner) ReadAgentLifecycleOutcome(ctx context.Context, req manager.AgentLifecycleTransition) (manager.AgentLifecycleTransitionResult, bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return manager.AgentLifecycleTransitionResult{}, false, err
	}
	return loadPostgresLifecycleOperationResult(ctx, s.backend, req)
}

func (s *AgentSQLiteOwner) ReadAgentLifecycleOutcome(ctx context.Context, req manager.AgentLifecycleTransition) (manager.AgentLifecycleTransitionResult, bool, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return manager.AgentLifecycleTransitionResult{}, false, err
	}
	return loadSQLiteLifecycleOperationResult(ctx, s.backend, req)
}
