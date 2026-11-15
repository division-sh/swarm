package runtimepersistence

import (
	"context"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
)

func InsertConstraintEventRowForTest(ctx context.Context, selected any, row eventpersistence.ConstraintEventRow) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.InsertConstraintEventRowForTest(ctx, row)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.InsertConstraintEventRowForTest(ctx, row)
	}
	return nil
}

func CorruptTargetRouteScalarForTest(ctx context.Context, selected any, eventID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.CorruptTargetRouteScalarForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.CorruptTargetRouteScalarForTest(ctx, eventID)
	}
	return nil
}

func CorruptInboundDuplicatePayloadForTest(ctx context.Context, selected any, eventID string) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.CorruptInboundDuplicatePayloadForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.CorruptInboundDuplicatePayloadForTest(ctx, eventID)
	}
	return nil
}

func CountExactEventForTest(ctx context.Context, selected any, eventID string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.eventPostgresOwner.CountExactEventForTest(ctx, eventID)
	case *SQLiteRuntimeStore:
		return owner.eventSQLiteOwner.CountExactEventForTest(ctx, eventID)
	}
	return 0, nil
}

func InstallEventDeliveryFailureAfterFlowMaterializationForTest(ctx context.Context, selected any, invariant, reason, flowTemplate string) (func(), error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.deliveryPostgresOwner.InstallEventDeliveryFailureAfterFlowMaterializationForTest(ctx, invariant, reason, flowTemplate)
	case *SQLiteRuntimeStore:
		return owner.deliverySQLiteOwner.InstallEventDeliveryFailureAfterFlowMaterializationForTest(ctx, invariant, reason, flowTemplate)
	}
	return nil, nil
}
