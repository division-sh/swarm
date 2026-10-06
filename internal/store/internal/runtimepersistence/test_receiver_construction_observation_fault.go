package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/google/uuid"
)

// This deliberately incomplete physical cut starts with lawful construction.
// It never seeds or admits a retired configuration envelope.
func PrepareReceiverNullableConstructionObservationForTest(ctx context.Context, selected any, owner flowidentity.RunScopedFlowInstance, entityID string) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	id, err := uuid.Parse(entityID)
	if err != nil || id == uuid.Nil || id.String() != entityID {
		return fmt.Errorf("nullable receiver observation requires an exact entity")
	}
	return runReceiverConstructionObservationFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
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
	})
}

func SetReceiverConstructionObservationUnavailableForTest(ctx context.Context, selected any, unavailable bool) error {
	return runReceiverConstructionObservationFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		statement := `ALTER TABLE flow_instances RENAME TO unavailable_construction_readback`
		if !unavailable {
			statement = `ALTER TABLE unavailable_construction_readback RENAME TO flow_instances`
		}
		_, err := tx.ExecContext(ctx, statement)
		return err
	})
}

func runReceiverConstructionObservationFault(ctx context.Context, selected any, write func(context.Context, *sql.Tx) error) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.backend.RunTransaction(ctx, write)
	case *SQLiteRuntimeStore:
		return owner.backend.RunTransaction(ctx, "receiver physical observation fault", write)
	default:
		return fmt.Errorf("receiver observation fault requires the original native owner")
	}
}
