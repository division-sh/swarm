package pipelinepersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type WorkflowTurnTerminationTxOwner interface {
	RequestWorkflowTurnTerminationTx(context.Context, *mutationprotocol.Attempt, workflowlifecycle.TurnTermination) (effects.WorkflowTurnTerminationResult, error)
}

func (s *PipelinePostgresOwner) BindWorkflowTurnTermination(owner WorkflowTurnTerminationTxOwner) error {
	if owner == nil || s.turnTerminations != nil {
		return fmt.Errorf("workflow turn termination owner must be bound exactly once")
	}
	s.turnTerminations = owner
	return nil
}

func (s *PipelineSQLiteOwner) BindWorkflowTurnTermination(owner WorkflowTurnTerminationTxOwner) error {
	if owner == nil || s.turnTerminations != nil {
		return fmt.Errorf("workflow turn termination owner must be bound exactly once")
	}
	s.turnTerminations = owner
	return nil
}

func (s *PipelinePostgresOwner) workflowTurnTerminationOwner() WorkflowTurnTerminationTxOwner {
	return s.turnTerminations
}
func (s *PipelineSQLiteOwner) workflowTurnTerminationOwner() WorkflowTurnTerminationTxOwner {
	return s.turnTerminations
}
