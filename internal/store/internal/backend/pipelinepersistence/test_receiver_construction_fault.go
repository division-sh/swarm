package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// Removes one validated native receipt to exercise fail-closed admission; it
// never manufactures historical authority or changes the receiver header.
func RemoveReceiverConstructionReceiptForTest(ctx context.Context, attempt *mutationprotocol.Attempt, owner flowidentity.RunScopedFlowInstance, entityID string) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if _, err := ReadFlowConstructionPublicationTx(ctx, tx, owner, entityID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, owner.RunID, owner.Route.InstancePath, entityID)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("receipt fault changed %d rows, want exactly one", count)
		}
		return nil
	})
}

func ReadReceiverConstructionHeaderForTest(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance, entityID string, sqlite bool) (pipeline.WorkflowInstance, error) {
	if tx == nil || entityID == "" {
		return pipeline.WorkflowInstance{}, fmt.Errorf("receiver fault requires its original transaction and exact entity")
	}
	if err := owner.Validate(); err != nil {
		return pipeline.WorkflowInstance{}, err
	}
	return constructedCreatingReceiverHeader(ctx, tx, owner.RunID, events.RouteIdentity{FlowID: owner.Route.ScopeKey, FlowInstance: owner.Route.InstancePath, EntityID: entityID}, sqlite)
}

// This hostile receipt deliberately contradicts an existing historical header.
// It copies real source bytes, not executable construction or readiness authority.
func (s *PipelinePostgresOwner) CopyReceiverConstructionReceiptForTest(ctx context.Context, tx *sql.Tx, source, destination flowidentity.RunScopedFlowInstance, sourceEntity, destinationEntity string) error {
	run, err := s.LoadSnapshotTx(ctx, tx, destination.RunID, false)
	if err != nil {
		return err
	}
	return copyReceiverConstructionReceiptForTest(ctx, tx, source, destination, sourceEntity, destinationEntity, run, s.historicalInstances, false)
}

func (s *PipelineSQLiteOwner) CopyReceiverConstructionReceiptForTest(ctx context.Context, tx *sql.Tx, source, destination flowidentity.RunScopedFlowInstance, sourceEntity, destinationEntity string) error {
	run, err := s.LoadSnapshotTx(ctx, tx, destination.RunID)
	if err != nil {
		return err
	}
	return copyReceiverConstructionReceiptForTest(ctx, tx, source, destination, sourceEntity, destinationEntity, run, s.historicalInstances, true)
}

func copyReceiverConstructionReceiptForTest(ctx context.Context, tx *sql.Tx, source, destination flowidentity.RunScopedFlowInstance, sourceEntity, destinationEntity string, run runlifecycle.Snapshot, history HistoricalFlowInstanceReader, sqlite bool) error {
	if _, err := ReadFlowConstructionPublicationTx(ctx, tx, source, sourceEntity); err != nil {
		return err
	}
	if err := destination.Validate(); err != nil {
		return err
	}
	if err := validateReceiverReceiptFaultHistory(ctx, tx, source, destination, sourceEntity, destinationEntity, run, history, sqlite); err != nil {
		return err
	}
	var present bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id=$1 AND instance_path=$2)`, destination.RunID, destination.Route.InstancePath).Scan(&present); err != nil {
		return err
	}
	if present {
		return fmt.Errorf("contradictory receipt fault requires zero destination receipts")
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO workflow_instance_initial_materializations
		(run_id, entity_id, instance_path, projection_version, projection, occurred_at)
		SELECT $1,$2,$3,projection_version,projection,occurred_at FROM workflow_instance_initial_materializations
		WHERE run_id=$4 AND instance_path=$5 AND entity_id=$6`, destination.RunID, destinationEntity, destination.Route.InstancePath, source.RunID, source.Route.InstancePath, sourceEntity)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("contradictory receipt fault requires one source: rows=%d err=%v", count, err)
	}
	return nil
}

func validateReceiverReceiptFaultHistory(ctx context.Context, tx *sql.Tx, source, destination flowidentity.RunScopedFlowInstance, sourceEntity, destinationEntity string, run runlifecycle.Snapshot, history HistoricalFlowInstanceReader, sqlite bool) error {
	if history == nil || run.RunID != destination.RunID || run.Origin.SourceRunID() != source.RunID || source.Route.ScopeKey != destination.Route.ScopeKey {
		return fmt.Errorf("contradictory receipt fault requires its bound historical destination and native ancestor")
	}
	projected, err := runfork.ProjectEntityOwnership(source.RunID, destination.RunID, sourceEntity, source.Route.InstancePath)
	if err != nil || projected.Fork.EntityID != destinationEntity || projected.Fork.FlowInstance != destination.Route.InstancePath {
		return fmt.Errorf("contradictory receipt fault source is not the captured receiver ancestor: %v", err)
	}
	header, err := constructedCreatingReceiverHeader(ctx, tx, destination.RunID, events.RouteIdentity{FlowID: destination.Route.ScopeKey, FlowInstance: destination.Route.InstancePath, EntityID: destinationEntity}, sqlite)
	if err != nil {
		return err
	}
	_, historical, err := history.ReadHistoricalFlowInstanceConstructionEvidenceTx(ctx, tx, run, header)
	if err != nil {
		return err
	}
	if !historical {
		return fmt.Errorf("contradictory receipt fault destination is not captured history")
	}
	return nil
}
