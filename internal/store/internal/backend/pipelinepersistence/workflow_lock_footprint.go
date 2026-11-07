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
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
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
	return lockWorkflowPublicationFootprintTx(ctx, tx, false, nil, []runtimebus.PublicationCommand{command})
}

func prepareWorkflowTerminationOwnershipTx(ctx context.Context, tx *sql.Tx, owner WorkflowTurnTerminationTxOwner, command *workflowlifecycle.TurnTermination, publications []runtimebus.PublicationCommand) (context.Context, error) {
	if command == nil {
		return ctx, nil
	}
	if owner == nil {
		return nil, fmt.Errorf("authored termination requires its native turn owner")
	}
	var lives []events.InstanceLife
	var binding processbinding.Binding
	for _, publication := range publications {
		for _, plan := range publication.Activations {
			if err := collectRestartActorScopes(plan, &lives, &binding); err != nil {
				return nil, err
			}
		}
	}
	actors, err := agentpersistence.InstanceLifeActorIdentitiesTx(ctx, tx, lives)
	if err != nil {
		return nil, err
	}
	// Union the restart census with retained turn actors BEFORE either owner
	// takes source/run/header locks. Each later owner retains its own checks.
	return owner.PrepareWorkflowTurnTerminationTx(ctx, tx, *command, actors)
}

// The prepared plans name locks, not accepted state. Semantic checks and receipt
// writes stay at their commit sites after this complete native acquisition prefix.
func lockWorkflowPublicationFootprintTx(ctx context.Context, tx *sql.Tx, postgres bool, owners []flowidentity.RunScopedFlowInstance, publications []runtimebus.PublicationCommand) error {
	var lives []events.InstanceLife
	var binding processbinding.Binding
	for _, publication := range publications {
		for _, plan := range publication.Activations {
			if err := collectRestartActorScopes(plan, &lives, &binding); err != nil {
				return err
			}
		}
	}
	if len(lives) != 0 {
		if err := agentpersistence.LockInstanceLifeActorsTx(ctx, tx, lives, binding, !postgres); err != nil {
			return err
		}
	}
	if !postgres {
		return nil
	}
	footprint, err := collectWorkflowPublicationFootprint(owners, publications)
	if err != nil {
		return err
	}
	return acquireWorkflowPublicationFootprintTx(ctx, tx, footprint)
}

func collectRestartActorScopes(plan pipeline.FlowInstanceActivationPlan, lives *[]events.InstanceLife, binding *processbinding.Binding) error {
	if plan.Restart != nil {
		if len(*lives) != 0 && !binding.Equal(plan.Restart.ProcessBinding) {
			return fmt.Errorf("instance restarts cross their exact publication process binding")
		}
		*binding = plan.Restart.ProcessBinding
		*lives = append(*lives, events.InstanceLife{
			RunID: plan.Readiness.RunID, FlowInstance: plan.Identity.InstancePath, Life: plan.Restart.Life,
		})
	}
	for _, child := range plan.Children {
		if err := collectRestartActorScopes(child, lives, binding); err != nil {
			return err
		}
	}
	return nil
}

type preparedWorkflowPublicationFootprint struct {
	ordered  []flowidentity.RunScopedFlowInstance
	strong   map[flowidentity.RunScopedFlowInstance]bool
	ready    map[flowidentity.RunScopedFlowInstance]bool
	creating map[string]bool
}

func collectWorkflowPublicationFootprint(owners []flowidentity.RunScopedFlowInstance, publications []runtimebus.PublicationCommand) (preparedWorkflowPublicationFootprint, error) {
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
			return preparedWorkflowPublicationFootprint{}, err
		}
		for _, owner := range writeOwners {
			strong[owner] = true
		}
		activationOwners, err := workflowActivationLockOwners(nil, publication.Activations)
		if err != nil {
			return preparedWorkflowPublicationFootprint{}, err
		}
		if creation := publication.DynamicFlowCreation; creation != nil {
			plan, err := creation.Plan.Normalized()
			if err != nil {
				return preparedWorkflowPublicationFootprint{}, err
			}
			activationOwners = append(activationOwners, flowidentity.RunScopedFlowInstance{RunID: plan.RunID, Route: plan.Identity.Route()})
		}
		for _, owner := range activationOwners {
			ready[owner] = true
		}
	}
	ordered := make([]flowidentity.RunScopedFlowInstance, 0, len(strong))
	for owner := range strong {
		if err := owner.Validate(); err != nil {
			return preparedWorkflowPublicationFootprint{}, fmt.Errorf("prepared workflow footprint %v: %w", owner, err)
		}
		ordered = append(ordered, owner)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return workflowInstanceLockKey(ordered[i]) < workflowInstanceLockKey(ordered[j])
	})
	return preparedWorkflowPublicationFootprint{ordered: ordered, strong: strong, ready: ready, creating: creating}, nil
}

func workflowFootprintRunIDs(ordered []flowidentity.RunScopedFlowInstance, creating map[string]bool) []string {
	runIDs := make([]string, 0, len(ordered))
	seen := make(map[string]bool)
	for _, owner := range ordered {
		if !seen[owner.RunID] && !creating[owner.RunID] {
			runIDs = append(runIDs, owner.RunID)
			seen[owner.RunID] = true
		}
	}
	sort.Strings(runIDs)
	return runIDs
}

func workflowFootprintReadFailed(err error) bool {
	return err != nil && !errors.Is(err, sql.ErrNoRows)
}

func acquireWorkflowPublicationFootprintTx(ctx context.Context, tx *sql.Tx, footprint preparedWorkflowPublicationFootprint) error {
	ordered, strong, ready := footprint.ordered, footprint.strong, footprint.ready
	runIDs := workflowFootprintRunIDs(ordered, footprint.creating)
	for _, runID := range runIDs {
		if _, err := sourceadmission.LoadPostgresTx(ctx, tx, runID, false, false); err != nil {
			return fmt.Errorf("admit prepared workflow run %s: %w", runID, err)
		}
	}
	if err := lockWorkflowInstancesTx(ctx, tx, true, ordered); err != nil {
		return err
	}
	for _, owner := range ordered {
		if strong[owner] {
			var id string
			var entityType sql.NullString
			err := tx.QueryRowContext(ctx, `SELECT entity_id, entity_type FROM flow_instances WHERE run_id=$1::uuid AND instance_path=$2 FOR UPDATE`, owner.RunID, owner.Route.InstancePath).Scan(&id, &entityType)
			if workflowFootprintReadFailed(err) {
				return fmt.Errorf("lock prepared workflow header %v: %w", owner, err)
			}
			if err == nil && entityType.Valid {
				err := tx.QueryRowContext(ctx, `SELECT entity_id FROM entity_state WHERE run_id=$1 AND entity_id=$2 AND flow_instance=$3 AND entity_type=$4 FOR UPDATE`, owner.RunID, id, owner.Route.InstancePath, entityType.String).Scan(&id)
				if workflowFootprintReadFailed(err) {
					return fmt.Errorf("lock prepared workflow fields %v: %w", owner, err)
				}
			}
		} else {
			var id string
			err := tx.QueryRowContext(ctx, `SELECT entity_id FROM flow_instances WHERE run_id=$1::uuid AND instance_path=$2 FOR SHARE`, owner.RunID, owner.Route.InstancePath).Scan(&id)
			if workflowFootprintReadFailed(err) {
				return fmt.Errorf("lock prepared publication stage %v: %w", owner, err)
			}
		}
	}
	for _, owner := range ordered {
		if ready[owner] {
			if _, _, err := loadDynamicFlowRuntimeReadiness(ctx, tx, true, owner.RunID, owner.Route, true); err != nil {
				return fmt.Errorf("lock prepared workflow readiness %v: %w", owner, err)
			}
		}
	}
	return nil
}

func workflowInstanceLockKey(owner flowidentity.RunScopedFlowInstance) string {
	return fmt.Sprintf("%d:%s%s", len(owner.RunID), owner.RunID, owner.Route.InstancePath)
}
