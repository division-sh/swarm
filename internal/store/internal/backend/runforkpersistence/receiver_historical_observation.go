package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/google/uuid"
)

func (s *RunForkPostgresOwner) ReadHistoricalFlowInstanceConstructionTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	return readHistoricalFlowInstanceConstructionTx(ctx, tx, source, run, instance)
}

func (s *RunForkSQLiteOwner) ReadHistoricalFlowInstanceConstructionTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	return readHistoricalFlowInstanceConstructionTx(ctx, tx, source, run, instance)
}

func (s *RunForkPostgresOwner) ReadHistoricalFlowInstanceConstructionEvidenceTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	return readHistoricalFlowInstanceConstructionEvidenceTx(ctx, tx, run, instance)
}

func (s *RunForkSQLiteOwner) ReadHistoricalFlowInstanceConstructionEvidenceTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	return readHistoricalFlowInstanceConstructionEvidenceTx(ctx, tx, run, instance)
}

func (s *RunForkPostgresOwner) ReadHistoricalFlowInstanceConstructionEvidenceBatchTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, instances []pipeline.WorkflowInstance) ([]pipelinepersistence.HistoricalFlowInstanceConstructionResult, error) {
	return readHistoricalFlowInstanceConstructionEvidenceBatchTx(ctx, tx, run, instances)
}

func (s *RunForkSQLiteOwner) ReadHistoricalFlowInstanceConstructionEvidenceBatchTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, instances []pipeline.WorkflowInstance) ([]pipelinepersistence.HistoricalFlowInstanceConstructionResult, error) {
	return readHistoricalFlowInstanceConstructionEvidenceBatchTx(ctx, tx, run, instances)
}

func readHistoricalFlowInstanceConstructionTx(ctx context.Context, tx *sql.Tx, source semanticview.Source, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	if source == nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("historical instance observation requires its admitted snapshot")
	}
	cut, found, err := loadHistoricalFlowInstanceConstructionCutTx(ctx, tx, run, instance)
	if err != nil || !found {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, err
	}
	fixed, _, found, err := runforkadmission.FixedConstructionForRoute(source, cut.plan, cut.route)
	if err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("admit historical construction: fixed header refusal: %w", err)
	}
	if !found {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("admit historical construction: fixed header disappeared")
	}
	return cut.project(run, instance, fixed)
}

func readHistoricalFlowInstanceConstructionEvidenceTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	cut, found, err := loadHistoricalFlowInstanceConstructionCutTx(ctx, tx, run, instance)
	if err != nil || !found {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, err
	}
	return cut.evidence(run, instance)
}

func (cut historicalFlowInstanceConstructionCut) evidence(run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	fixed, _, found, err := runforkadmission.FixedConstructionEvidenceForRoute(cut.plan, cut.route)
	if err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("inspect historical construction: fixed header refusal: %w", err)
	}
	if !found {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, fmt.Errorf("inspect historical construction: fixed header disappeared")
	}
	return cut.project(run, instance, fixed)
}

type historicalFlowInstanceConstructionCut struct {
	plan  runfork.RunForkPlan
	route flowidentity.Route
	key   string
	birth runForkReceiverBirth
}

func loadHistoricalFlowInstanceConstructionCutTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance) (historicalFlowInstanceConstructionCut, bool, error) {
	if err := ctx.Err(); err != nil {
		return historicalFlowInstanceConstructionCut{}, false, err
	}
	if tx == nil || run.Validate() != nil {
		return historicalFlowInstanceConstructionCut{}, false, fmt.Errorf("historical construction evidence requires its admitted read snapshot")
	}
	if run.Origin.Kind() != runlifecycle.OriginForkMaterialization {
		return historicalFlowInstanceConstructionCut{}, false, nil
	}
	snapshot, err := loadHistoricalFlowInstanceSourceSnapshotTx(ctx, tx, run)
	if err != nil {
		return historicalFlowInstanceConstructionCut{}, false, err
	}
	return historicalFlowInstanceConstructionCutForHeader(ctx, run, instance, snapshot, func(ctx context.Context, header pipeline.WorkflowInstance) (runForkReceiverBirth, error) {
		return readRunForkReceiverBirthTx(ctx, tx, run.RunID, header)
	})
}

func loadHistoricalFlowInstanceSourceSnapshotTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot) (*runForkRevisionSnapshot, error) {
	sourceRunID, revision := run.Origin.SourceRunID(), run.Origin.ForkRevision()
	var recorded int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, sourceRunID, revision).Scan(&recorded); err != nil {
		return nil, fmt.Errorf("read admitted historical instance cut: %w", err)
	}
	return loadRunForkRevisionSnapshot(ctx, tx, sourceRunID, recorded)
}

func historicalFlowInstanceConstructionCutForHeader(ctx context.Context, run runlifecycle.Snapshot, instance pipeline.WorkflowInstance, snapshot *runForkRevisionSnapshot, readBirth func(context.Context, pipeline.WorkflowInstance) (runForkReceiverBirth, error)) (historicalFlowInstanceConstructionCut, bool, error) {
	if err := ctx.Err(); err != nil {
		return historicalFlowInstanceConstructionCut{}, false, err
	}
	metadata, found, err := findProjectedFlowInstanceMetadata(snapshot, run.RunID, instance.StorageRef)
	if err != nil || !found {
		return historicalFlowInstanceConstructionCut{}, false, err
	}
	born, err := readBirth(ctx, instance)
	if err != nil {
		return historicalFlowInstanceConstructionCut{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return historicalFlowInstanceConstructionCut{}, false, err
	}
	if !born.createdAt.Equal(instance.CreatedAt) {
		return historicalFlowInstanceConstructionCut{}, false, &pipeline.FlowInstanceConstructionCorruption{RunID: run.RunID, FlowID: instance.WorkflowName, InstancePath: instance.StorageRef, Cause: fmt.Errorf("historical header contradicts its original materialization birth")}
	}
	state, err := loadRunForkConstructedEntityState(snapshot, metadata.EntityID)
	if err != nil {
		return historicalFlowInstanceConstructionCut{}, false, err
	}
	route := flowidentity.StoredRoute(metadata.FlowTemplate, flowidentity.LogicalInstanceID(metadata.FlowInstance), metadata.FlowInstance)
	plan := runfork.RunForkPlan{SourceRunID: snapshot.RunID, ForkPoint: runfork.RunForkPoint{Revision: snapshot.Revision}, Entities: []runfork.RunForkEntityState{state}}
	fixed, _, found, err := runforkadmission.FixedConstructionEvidenceForRoute(plan, route)
	if err != nil || !found {
		return historicalFlowInstanceConstructionCut{}, false, fmt.Errorf("read captured historical construction: %w", err)
	}
	return historicalFlowInstanceConstructionCut{
		plan: plan, route: fixed.Route(), key: metadata.InstanceKey, birth: born,
	}, true, nil
}

func readHistoricalFlowInstanceConstructionEvidenceBatchTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, instances []pipeline.WorkflowInstance) ([]pipelinepersistence.HistoricalFlowInstanceConstructionResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tx == nil || run.Validate() != nil {
		return nil, fmt.Errorf("historical construction evidence requires its admitted read snapshot")
	}
	results := make([]pipelinepersistence.HistoricalFlowInstanceConstructionResult, len(instances))
	if len(instances) == 0 || run.Origin.Kind() != runlifecycle.OriginForkMaterialization {
		return results, nil
	}
	snapshot, err := loadHistoricalFlowInstanceSourceSnapshotTx(ctx, tx, run)
	if err != nil {
		return nil, err
	}
	// These complete immutable cuts belong only to this synchronous call and tx.
	var birth *runForkRevisionSnapshot
	readBirth := func(ctx context.Context, header pipeline.WorkflowInstance) (runForkReceiverBirth, error) {
		owner, err := flowidentity.NewRunScopedFlowInstance(run.RunID, flowidentity.StoredRoute(header.WorkflowName, header.InstanceID, header.StorageRef))
		if err != nil {
			return runForkReceiverBirth{}, err
		}
		if birth == nil {
			var recorded int64
			if err := tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, run.RunID, 1).Scan(&recorded); err != nil {
				return runForkReceiverBirth{}, fmt.Errorf("read exact historical cut: %w", err)
			}
			birth, err = loadRunForkRevisionSnapshot(ctx, tx, run.RunID, recorded)
			if err != nil {
				return runForkReceiverBirth{}, err
			}
		}
		initial, err := constructedEntityAtRevisionSnapshot(birth, owner, header.EntityID, 1)
		if err != nil {
			return runForkReceiverBirth{}, fmt.Errorf("read original fork receiver birth: %w", err)
		}
		return runForkReceiverBirthFromConstructed(owner, initial)
	}
	for index, instance := range instances {
		cut, found, err := historicalFlowInstanceConstructionCutForHeader(ctx, run, instance, snapshot, readBirth)
		if err != nil {
			return nil, err
		}
		if found {
			construction, admitted, err := cut.evidence(run, instance)
			if err != nil {
				return nil, err
			}
			results[index] = pipelinepersistence.HistoricalFlowInstanceConstructionResult{Construction: construction, Found: admitted}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (cut historicalFlowInstanceConstructionCut) project(run runlifecycle.Snapshot, instance pipeline.WorkflowInstance, fixed flowidentity.Instance) (pipeline.HistoricalFlowInstanceConstruction, bool, error) {
	projected, err := runfork.ProjectCapturedConstructionIdentity(cut.plan.SourceRunID, run.RunID, fixed)
	if err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, err
	}
	header, err := instance.ConstructionIdentity(flowidentity.RunScopedFlowInstance{RunID: run.RunID, Route: projected.Route()})
	metadata := cut.plan.Entities[0].MaterializationMetadata
	if err != nil {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, &pipeline.FlowInstanceConstructionCorruption{RunID: run.RunID, FlowID: instance.WorkflowName, InstancePath: instance.StorageRef, Cause: err}
	}
	recorded, err := pipeline.DecodeWorkflowInstanceRecordedHeader(fixed.Route(), metadata.FlowConfig)
	// The source cut, child birth and physical header retain one captured
	// construction version; selected executable readiness is independent.
	if err != nil || recorded.WorkflowVersion() == "" || header != projected || instance.InstanceKey != cut.key || instance.Mode != metadata.Mode || instance.EntityType != metadata.EntityType ||
		cut.birth.owner.RunID != run.RunID || cut.birth.owner.Route != projected.Route() || cut.birth.header.ParentRoute() != projected.ParentRoute ||
		cut.birth.createdAt.IsZero() || !cut.birth.createdAt.Equal(instance.CreatedAt) || cut.birth.header.WorkflowVersion() != recorded.WorkflowVersion() || instance.WorkflowVersion != recorded.WorkflowVersion() {
		return pipeline.HistoricalFlowInstanceConstruction{}, false, &pipeline.FlowInstanceConstructionCorruption{RunID: run.RunID, FlowID: instance.WorkflowName, InstancePath: instance.StorageRef, Cause: errors.Join(fmt.Errorf("header contradicts its exact projected historical construction"), err)}
	}
	return pipeline.HistoricalFlowInstanceConstruction{
		Identity: projected, InstanceKey: cut.key, SourceRevision: cut.plan.ForkPoint.Revision, WorkflowVersion: recorded.WorkflowVersion(),
		SourceOwner: flowidentity.RunScopedFlowInstance{RunID: cut.plan.SourceRunID, Route: fixed.Route()},
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

func (s *RunForkPostgresOwner) ReadReceiverConstructionHistoryTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (bool, error) {
	return readReceiverConstructionHistoryTx(ctx, tx, run, header)
}

func (s *RunForkSQLiteOwner) ReadReceiverConstructionHistoryTx(ctx context.Context, tx *sql.Tx, run runlifecycle.Snapshot, header pipeline.WorkflowInstance) (bool, error) {
	return readReceiverConstructionHistoryTx(ctx, tx, run, header)
}

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
	cut, found, err := loadHistoricalFlowInstanceConstructionCutTx(ctx, tx, run, header)
	if err != nil || !found {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	fixed, _, found, err := runforkadmission.FixedConstructionEvidenceForRoute(cut.plan, cut.route)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	if !found {
		return pipeline.FlowConstructionReceipt{}, false, fmt.Errorf("historical receiver lacks its exact constructed state")
	}
	if _, _, err := cut.project(run, header, fixed); err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	sourceRunID, recorded := cut.plan.SourceRunID, cut.plan.ForkPoint.Revision
	snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, sourceRunID, recorded)
	if err != nil {
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
		if plan.Entities[index].EntityID == cut.plan.Entities[0].EntityID {
			selected = &plan.Entities[index]
		}
	}
	if selected == nil || selected.MaterializationMetadata == nil {
		return pipeline.FlowConstructionReceipt{}, false, fmt.Errorf("historical receiver lacks its exact constructed state")
	}
	born, err := readRunForkReceiverBirthTx(ctx, tx, run.RunID, header)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	expected, err := projectRunForkConstructionReceiptTx(ctx, tx, plan, run.RunID, *selected, born.createdAt)
	if err != nil {
		return pipeline.FlowConstructionReceipt{}, false, err
	}
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_instance_initial_materializations
		WHERE run_id=$1 AND instance_path=$2)`, run.RunID, header.StorageRef).Scan(&present); err != nil {
		return pipeline.FlowConstructionReceipt{}, false, fmt.Errorf("inspect projected construction receipt: %w", err)
	}
	if present {
		return pipeline.FlowConstructionReceipt{}, false, fmt.Errorf("receiver has conflicting native and historical construction evidence")
	}
	return expected, true, nil
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
	return constructedEntityAtRevisionSnapshot(snapshot, owner, entityID, revision)
}

func constructedEntityAtRevisionSnapshot(snapshot *runForkRevisionSnapshot, owner flowidentity.RunScopedFlowInstance, entityID string, revision int64) (runfork.RunForkEntityState, error) {
	if err := owner.Validate(); err != nil {
		return runfork.RunForkEntityState{}, err
	}
	id, err := uuid.Parse(entityID)
	if err != nil || id == uuid.Nil || id.String() != entityID || revision <= 0 {
		return runfork.RunForkEntityState{}, fmt.Errorf("historical evidence requires an exact entity and positive revision")
	}
	if snapshot == nil || snapshot.RunID != owner.RunID || snapshot.Revision != revision {
		return runfork.RunForkEntityState{}, fmt.Errorf("historical evidence contradicts its exact run and revision")
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
