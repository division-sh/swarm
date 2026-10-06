package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

type FiniteRunStartStorageCounts struct {
	Runs, Events, FlowInstances, EntityState                                 int
	ResourceVersions, ResourceHeads, ResourceSourceInvocations, ResourcePins int
	FanOutIntents, CreationOperations, ChildEvaluations, ChildReservations   int
	APIIdempotency                                                           int
}

// Fixed readback of every durable family that finite-start refusal must leave
// unchanged. No table selector, connection or SQL callback crosses this owner.
func ReadFiniteRunStartStorageCountsForTest(ctx context.Context, selected any) (FiniteRunStartStorageCounts, error) {
	var observed FiniteRunStartStorageCounts
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return observed, err
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM runs),
			(SELECT COUNT(*) FROM events),
			(SELECT COUNT(*) FROM flow_instances),
			(SELECT COUNT(*) FROM entity_state),
			(SELECT COUNT(*) FROM resource_versions),
			(SELECT COUNT(*) FROM resource_heads),
			(SELECT COUNT(*) FROM resource_source_invocations),
			(SELECT COUNT(*) FROM resource_version_pins),
			(SELECT COUNT(*) FROM fan_out_intents),
			(SELECT COUNT(*) FROM resource_run_creation_operations),
			(SELECT COUNT(*) FROM resource_run_creation_child_evaluations),
			(SELECT COUNT(*) FROM resource_run_creation_child_reservations),
			(SELECT COUNT(*) FROM api_idempotency)`).Scan(
			&observed.Runs, &observed.Events, &observed.FlowInstances, &observed.EntityState,
			&observed.ResourceVersions, &observed.ResourceHeads, &observed.ResourceSourceInvocations, &observed.ResourcePins,
			&observed.FanOutIntents, &observed.CreationOperations, &observed.ChildEvaluations, &observed.ChildReservations, &observed.APIIdempotency)
	})
	if err != nil {
		return FiniteRunStartStorageCounts{}, err
	}
	return observed, nil
}

// This private adapter is extracted from the existing served observation owner.
// Its callback never crosses a public fixture boundary.
func readServedDeliveryObservation(ctx context.Context, selected any, read func(context.Context, *sql.Tx) error) error {
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		return owner.backend.RunReadTransaction(ctx, read)
	default:
		return fmt.Errorf("storage observation requires the original selected owner, got %T", selected)
	}
}

func validateChannelObservationOwner(selected any) error {
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return fmt.Errorf("channel observation requires an initialized postgres read owner")
		}
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil || !owner.backend.Valid() {
			return fmt.Errorf("channel observation requires an initialized sqlite read owner")
		}
	default:
		return fmt.Errorf("channel observation requires an original native read owner, got %T", selected)
	}
	return nil
}
