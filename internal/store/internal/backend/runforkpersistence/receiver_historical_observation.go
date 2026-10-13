package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

func (s *RunForkPostgresOwner) ReadReceiverConstructionHistoryTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (bool, error) {
	return readReceiverConstructionHistoryTx(ctx, tx, run, header)
}

func (s *RunForkSQLiteOwner) ReadReceiverConstructionHistoryTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (bool, error) {
	return readReceiverConstructionHistoryTx(ctx, tx, run, header)
}

// Fixed history proves the constructed coordinates, not a creating event or
// its outcome. Native constructions outside this cut retain native admission.
func readReceiverConstructionHistoryTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (bool, error) {
	_, found, err := readProjectedReceiverConstructionReceiptTx(ctx, tx, run, header)
	return found, err
}

func (s *RunForkPostgresOwner) ReadProjectedReceiverConstructionReceiptTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (pipeline.FlowConstructionReceipt, bool, error) {
	return readProjectedReceiverConstructionReceiptTx(ctx, tx, run, header)
}

func (s *RunForkSQLiteOwner) ReadProjectedReceiverConstructionReceiptTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (pipeline.FlowConstructionReceipt, bool, error) {
	return readProjectedReceiverConstructionReceiptTx(ctx, tx, run, header)
}

// Positive fixed-cut admission precedes physical receipt inspection. A projected
// receipt is historical evidence, never a new creating turn or native fallback.
func readProjectedReceiverConstructionReceiptTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (pipeline.FlowConstructionReceipt, bool, error) {
	if tx == nil {
		return pipeline.FlowConstructionReceipt{}, false, fmt.Errorf("receiver history requires the original delivery transaction")
	}
	if err := run.Validate(); err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	if run.Origin.Kind() != runlifecycle.OriginForkMaterialization {
		return pipeline.FlowConstructionReceipt{}, false, nil
	}
	sourceRunID, revision := run.Origin.SourceRunID(), run.Origin.ForkRevision()
	var recorded int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, sourceRunID, revision).Scan(&recorded); err != nil {
		return pipeline.FlowConstructionReceipt{}, false, fmt.Errorf("read receiver's exact fork cut: %w", err)
	}
	snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, sourceRunID, recorded)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	captured, err := capturedReceiverMetadata(snapshot, sourceRunID, run.RunID, header)
	if err != nil || captured == nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	entities, err := loadRunForkEntityStates(snapshot)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	entities, _, err = attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	plan := runfork.RunForkPlan{SourceRunID: sourceRunID, ForkPoint: runfork.RunForkPoint{Revision: recorded}, Entities: entities}
	for _, reply := range snapshot.ReplyContexts {
		plan.ReplyContexts = append(plan.ReplyContexts, reply.Record)
	}
	var selected *runfork.RunForkEntityState
	for index := range plan.Entities {
		if plan.Entities[index].EntityID == captured.EntityID {
			selected = &plan.Entities[index]
		}
	}
	if selected == nil || selected.MaterializationMetadata == nil {
		return pipeline.FlowConstructionReceipt{}, false, fmt.Errorf("historical receiver lacks its exact constructed state")
	}
	if _, err := matchesCapturedReceiver(sourceRunID, run.RunID, header, *selected); err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	born, err := readRunForkReceiverBirthTx(ctx, tx, run.RunID, header)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	expected, err := ProjectRunForkConstructionReceipt(plan, run.RunID, *selected, born)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_instance_initial_materializations
		WHERE run_id=$1 AND instance_path=$2)`, run.RunID, header.StorageRef).Scan(&present); err != nil {
		return pipeline.FlowConstructionReceipt{}, false, fmt.Errorf("inspect projected construction receipt: %w", err)
	}
	if present {
		if err := pipelinepersistence.RequireSelectedHistoricalWorkflowReceipt(ctx, tx, expected); err != nil {
			return pipeline.FlowConstructionReceipt{}, false, err
		}
	}
	return expected, true, nil
}

func capturedReceiverMetadata(snapshot *runForkRevisionSnapshot, sourceRunID, forkRunID string, header pipeline.WorkflowInstance) (*runForkRevisionEntityMetadata, error) {
	var captured *runForkRevisionEntityMetadata
	for index := range snapshot.EntityMetadata {
		metadata := &snapshot.EntityMetadata[index]
		if metadata.ConstructionKind != "constructed" {
			continue
		}
		projection, err := runfork.ProjectEntityOwnership(sourceRunID, forkRunID, metadata.EntityID, metadata.FlowInstance)
		if err != nil {
			return nil, err
		}
		if projection.Fork.FlowInstance != header.StorageRef {
			continue
		}
		if captured != nil || projection.Fork.EntityID != header.EntityID {
			return nil, fmt.Errorf("historical receiver contradicts exact captured entity ownership")
		}
		captured = metadata
	}
	return captured, nil
}

func matchesCapturedReceiver(sourceRunID, forkRunID string, header pipeline.WorkflowInstance, state runfork.RunForkEntityState) (bool, error) {
	captured := state.MaterializationMetadata
	expected, _, err := selectedContractWorkflowIdentity(selectedContractWorkflowState{
		SourceRunID: sourceRunID, RunID: forkRunID, EntityID: header.EntityID,
		WorkflowName: captured.FlowTemplate, Route: header.StorageRef, History: state,
	})
	if err != nil {
		return false, err
	}
	if header.InstanceID != expected.InstanceID || header.StorageRef != expected.InstancePath ||
		header.WorkflowName != expected.TemplateID || header.EntityID != expected.EntityID ||
		header.ParentFlowID != expected.ParentRoute.FlowID || header.ParentFlowInstance != expected.ParentRoute.FlowInstance ||
		header.ParentEntityID != expected.ParentRoute.EntityID || header.Mode != captured.Mode || header.EntityType != captured.EntityType {
		return false, fmt.Errorf("historical receiver disagrees with captured parent, key, mode or declaration")
	}
	return true, nil
}

// ReadConstructedEntityAtRevisionTx delegates fact selection, tombstones and
// reconstruction to the same fixed-snapshot owner as fork planning and replay.
func ReadConstructedEntityAtRevisionTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance, entityID string, revision int64) (runfork.RunForkEntityState, error) {
	if tx == nil {
		return runfork.RunForkEntityState{}, fmt.Errorf("historical evidence requires the selected read transaction")
	}
	if err := owner.Validate(); err != nil {
		return runfork.RunForkEntityState{}, err
	}
	id, err := uuid.Parse(entityID)
	if err != nil || id == uuid.Nil || id.String() != entityID || revision <= 0 {
		return runfork.RunForkEntityState{}, fmt.Errorf("historical evidence requires an exact entity and positive revision")
	}
	var recorded int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, owner.RunID, revision).Scan(&recorded); err != nil {
		return runfork.RunForkEntityState{}, fmt.Errorf("read exact historical cut: %w", err)
	}
	snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, owner.RunID, recorded)
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	state, err := loadRunForkConstructedEntityState(snapshot, entityID)
	if err != nil {
		return runfork.RunForkEntityState{}, err
	}
	metadata := state.MaterializationMetadata
	if metadata.FlowInstance != owner.Route.InstancePath || metadata.FlowTemplate != owner.Route.ScopeKey {
		return runfork.RunForkEntityState{}, fmt.Errorf("historical evidence contradicts exact receiver route/template")
	}
	return state, nil
}
