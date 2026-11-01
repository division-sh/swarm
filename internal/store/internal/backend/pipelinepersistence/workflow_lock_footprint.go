package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func workflowActivationLockOwners(owners []flowidentity.RunScopedFlowInstance, plans []pipeline.FlowInstanceActivationPlan) ([]flowidentity.RunScopedFlowInstance, error) {
	for _, plan := range plans {
		readiness, err := plan.Readiness.Normalized()
		if err != nil {
			return nil, err
		}
		owners = append(owners, flowidentity.RunScopedFlowInstance{RunID: readiness.RunID, Route: readiness.Identity.Route()})
		owners, err = workflowActivationLockOwners(owners, plan.Children)
		if err != nil {
			return nil, err
		}
	}
	return owners, nil
}

func workflowPublicationLockOwners(owners []flowidentity.RunScopedFlowInstance, command runtimebus.PublicationCommand) ([]flowidentity.RunScopedFlowInstance, error) {
	if command.StageFeedback != nil {
		owners = append(owners, command.StageFeedback.Instance)
	}
	if creation := command.DynamicFlowCreation; creation != nil {
		readiness, err := creation.Plan.Normalized()
		if err != nil {
			return nil, err
		}
		owners = append(owners, flowidentity.RunScopedFlowInstance{RunID: readiness.RunID, Route: readiness.Identity.Route()})
	}
	owners, err := workflowActivationLockOwners(owners, command.Activations)
	if err != nil {
		return nil, err
	}
	for _, fence := range command.Commit.JoinAdmissionFences {
		owners = append(owners, fence.Owner)
	}
	return owners, nil
}

// Callers retain run/source admission. These are the existing instance locks,
// not prospective entry validation or permission to write any declared target.
func lockWorkflowInstancesTx(ctx context.Context, tx *sql.Tx, postgres bool, owners []flowidentity.RunScopedFlowInstance) error {
	if !postgres {
		return nil
	}
	unique := make(map[string]bool, len(owners))
	for _, owner := range owners {
		if err := owner.Validate(); err != nil {
			return fmt.Errorf("workflow lock footprint requires an exact instance: %w", err)
		}
		unique[fmt.Sprintf("%d:%s%s", len(owner.RunID), owner.RunID, owner.Route.InstancePath)] = true
	}
	keys := make([]string, 0, len(unique))
	for key := range unique {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, key); err != nil {
			return err
		}
	}
	return nil
}

func (s *PipelinePostgresOwner) LockWorkflowPublicationTx(ctx context.Context, tx *sql.Tx, command runtimebus.PublicationCommand) error {
	owners, err := workflowPublicationLockOwners(nil, command)
	if err != nil {
		return err
	}
	return lockWorkflowInstancesTx(ctx, tx, true, owners)
}

func (s *PipelineSQLiteOwner) LockWorkflowPublicationTx(ctx context.Context, tx *sql.Tx, command runtimebus.PublicationCommand) error {
	return nil
}
