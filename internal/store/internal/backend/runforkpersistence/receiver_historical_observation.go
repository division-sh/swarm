package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func (s *RunForkPostgresOwner) ReadHistoricalFlowInstanceConstructionTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	return readHistoricalFlowInstanceConstructionTx(ctx, tx, source, run, instance)
}

func (s *RunForkSQLiteOwner) ReadHistoricalFlowInstanceConstructionTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	return readHistoricalFlowInstanceConstructionTx(ctx, tx, source, run, instance)
}

func readHistoricalFlowInstanceConstructionTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	if tx == nil || source == nil || run.Validate() != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("historical instance observation requires its admitted snapshot")
	}
	if run.Origin.Kind() != runlifecycle.OriginForkMaterialization {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, nil
	}
	sourceRunID, revision := run.Origin.SourceRunID(), run.Origin.ForkRevision()
	var recorded int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, sourceRunID, revision).Scan(&recorded); err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("read admitted historical instance cut: %w", err)
	}
	snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, sourceRunID, recorded)
	if err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, err
	}
	metadata, found, err := findProjectedFlowInstanceMetadata(snapshot, run.RunID, instance.StorageRef)
	if err != nil || !found {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, err
	}
	state, err := loadRunForkConstructedEntityState(snapshot, metadata.EntityID)
	if err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, err
	}
	route := flowidentity.StoredRoute(flowidentity.ScopeKey(source, metadata.FlowTemplate), flowidentity.LogicalInstanceID(metadata.FlowInstance), metadata.FlowInstance)
	fixed, _, found, err := runforkadmission.FixedConstructionForRoute(source, runfork.RunForkPlan{SourceRunID: sourceRunID, Entities: []runfork.RunForkEntityState{state}}, route)
	if err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("admit historical construction: %w", err)
	}
	if !found {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("admit historical construction: fixed header disappeared")
	}
	projected, err := projectHistoricalFlowInstanceIdentity(sourceRunID, run.RunID, fixed)
	if err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, err
	}
	return pipeline.HistoricalFlowInstanceConstruction{
		Identity: projected, InstanceKey: metadata.InstanceKey, SourceRevision: recorded,
		SourceOwner: flowidentity.RunScopedFlowInstance{RunID: sourceRunID, Route: fixed.Route()},
	}, true, nil
}

// Match recorded constructed headers, not receipt absence or today's source run.
// A fork may also contain later native constructions, which still need receipts.
func findProjectedFlowInstanceMetadata(snapshot *runForkRevisionSnapshot, forkRunID, path string) (runForkRevisionEntityMetadata, bool, error) {
	var selected runForkRevisionEntityMetadata
	found := false
	for _, metadata := range snapshot.EntityMetadata {
		if metadata.ConstructionKind != "constructed" {
			continue
		}
		projection, err := runfork.ProjectEntityOwnership(snapshot.RunID, forkRunID, metadata.EntityID, metadata.FlowInstance)
		if err != nil {
			return selected, false, err
		}
		if projection.Fork.FlowInstance != path {
			continue
		}
		if found {
			return selected, false, fmt.Errorf("historical instance has ambiguous fixed construction")
		}
		selected, found = metadata, true
	}
	return selected, found, nil
}

func projectHistoricalFlowInstanceIdentity(sourceRunID, forkRunID string, fixed flowidentity.Instance) (flowidentity.Instance, error) {
	ownership, err := runfork.ProjectEntityOwnership(sourceRunID, forkRunID, fixed.EntityID, fixed.InstancePath)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	route, err := runfork.ProjectExecutionRoute(sourceRunID, forkRunID, fixed.ScopeKey, fixed.Route())
	if err != nil {
		return flowidentity.Instance{}, err
	}
	parent, err := runfork.ProjectParentRoute(sourceRunID, forkRunID, fixed.ParentRoute)
	if err != nil {
		return flowidentity.Instance{}, err
	}
	fixed.EntityID, fixed.InstancePath, fixed.InstanceID = ownership.Fork.EntityID, route.InstancePath, route.InstanceID
	fixed.ParentRoute, fixed.ParentEntityID = parent, parent.EntityID
	return fixed, nil
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
