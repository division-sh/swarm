package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

func prepareReceiverNullableConstructionObservationTx(ctx context.Context, tx *sql.Tx, owner flowidentity.RunScopedFlowInstance, entityID string) error {
	result, err := tx.ExecContext(ctx, `UPDATE flow_instances SET entity_type=NULL WHERE run_id=$1 AND instance_path=$2 AND entity_id=$3`, owner.RunID, owner.Route.InstancePath, entityID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("nullable receiver observation requires its exact existing header: rows=%d err=%v", count, err)
	}
	result, err = tx.ExecContext(ctx, `DELETE FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`, owner.RunID, owner.Route.InstancePath)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		return fmt.Errorf("nullable receiver observation requires its exact attachment: rows=%d err=%v", count, err)
	}
	return nil
}

func setReceiverConstructionObservationUnavailableTx(ctx context.Context, tx *sql.Tx, unavailable bool) error {
	statement := `ALTER TABLE flow_instances RENAME TO unavailable_construction_readback`
	if !unavailable {
		statement = `ALTER TABLE unavailable_construction_readback RENAME TO flow_instances`
	}
	_, err := tx.ExecContext(ctx, statement)
	return err
}

func (s *PipelinePostgresOwner) PrepareReceiverNullableConstructionObservationForTest(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return prepareReceiverNullableConstructionObservationTx(ctx, tx, owner, entityID)
	})
}

func (s *PipelineSQLiteOwner) PrepareReceiverNullableConstructionObservationForTest(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entityID string) error {
	return s.backend.RunTransaction(ctx, "receiver physical observation fault", func(ctx context.Context, tx *sql.Tx) error {
		return prepareReceiverNullableConstructionObservationTx(ctx, tx, owner, entityID)
	})
}

func (s *PipelinePostgresOwner) SetReceiverConstructionObservationUnavailableForTest(ctx context.Context, unavailable bool) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return setReceiverConstructionObservationUnavailableTx(ctx, tx, unavailable)
	})
}

func (s *PipelineSQLiteOwner) SetReceiverConstructionObservationUnavailableForTest(ctx context.Context, unavailable bool) error {
	return s.backend.RunTransaction(ctx, "receiver physical observation fault", func(ctx context.Context, tx *sql.Tx) error {
		return setReceiverConstructionObservationUnavailableTx(ctx, tx, unavailable)
	})
}
