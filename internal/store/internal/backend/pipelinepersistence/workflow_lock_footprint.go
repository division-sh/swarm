package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
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
		unique[workflowInstanceLockKey(owner)] = true
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
	return lockWorkflowPublicationFootprintTx(ctx, tx, true, nil, []runtimebus.PublicationCommand{command})
}

func (s *PipelineSQLiteOwner) LockWorkflowPublicationTx(ctx context.Context, tx *sql.Tx, command runtimebus.PublicationCommand) error {
	return nil
}

// The prepared plans name locks, not accepted state. Semantic checks and receipt
// writes stay at their commit sites after this complete native acquisition prefix.
func lockWorkflowPublicationFootprintTx(ctx context.Context, tx *sql.Tx, postgres bool, owners []flowidentity.RunScopedFlowInstance, publications []runtimebus.PublicationCommand) error {
	if !postgres {
		return nil
	}
	strong := make(map[flowidentity.RunScopedFlowInstance]bool)
	ready := make(map[flowidentity.RunScopedFlowInstance]bool)
	creating := make(map[string]bool)
	for _, owner := range owners {
		strong[owner] = true
	}
	for _, publication := range publications {
		if publication.Commit.Event.RunDisposition() == events.AdmittedRunCreateAuthorized {
			creating[publication.Commit.Event.Event().RunID()] = true
		}
		if publication.StageFeedback != nil {
			owner := publication.StageFeedback.Instance
			if _, exists := strong[owner]; !exists {
				strong[owner] = false
			}
		}
		publication.StageFeedback = nil
		writeOwners, err := workflowPublicationLockOwners(nil, publication)
		if err != nil {
			return err
		}
		for _, owner := range writeOwners {
			strong[owner] = true
		}
		activationOwners, err := workflowActivationLockOwners(nil, publication.Activations)
		if err != nil {
			return err
		}
		if creation := publication.DynamicFlowCreation; creation != nil {
			plan, err := creation.Plan.Normalized()
			if err != nil {
				return err
			}
			activationOwners = append(activationOwners, flowidentity.RunScopedFlowInstance{RunID: plan.RunID, Route: plan.Identity.Route()})
		}
		for _, owner := range activationOwners {
			ready[owner] = true
		}
	}
	ordered := make([]flowidentity.RunScopedFlowInstance, 0, len(strong))
	for owner := range strong {
		ordered = append(ordered, owner)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return workflowInstanceLockKey(ordered[i]) < workflowInstanceLockKey(ordered[j])
	})
	runIDs := make([]string, 0, len(ordered))
	seen := make(map[string]bool)
	for _, owner := range ordered {
		if !seen[owner.RunID] && !creating[owner.RunID] {
			runIDs = append(runIDs, owner.RunID)
			seen[owner.RunID] = true
		}
	}
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		if _, err := sourceadmission.LoadPostgresTx(ctx, tx, runID, false, false); err != nil {
			return err
		}
	}
	if err := lockWorkflowInstancesTx(ctx, tx, true, ordered); err != nil {
		return err
	}
	for _, owner := range ordered {
		if strong[owner] {
			if _, _, err := workflowheader.LoadForMutation(ctx, tx, true, owner.RunID, "", owner.Route.InstancePath); err != nil {
				return err
			}
		} else {
			var id string
			err := tx.QueryRowContext(ctx, `SELECT entity_id FROM flow_instances WHERE run_id=$1::uuid AND instance_path=$2 FOR SHARE`, owner.RunID, owner.Route.InstancePath).Scan(&id)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
	}
	for _, owner := range ordered {
		if ready[owner] {
			if _, _, err := loadDynamicFlowRuntimeReadiness(ctx, tx, true, owner.RunID, owner.Route, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func workflowInstanceLockKey(owner flowidentity.RunScopedFlowInstance) string {
	return fmt.Sprintf("%d:%s%s", len(owner.RunID), owner.RunID, owner.Route.InstancePath)
}
