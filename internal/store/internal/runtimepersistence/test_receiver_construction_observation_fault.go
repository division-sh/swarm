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
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.PrepareReceiverNullableConstructionObservationForTest(ctx, owner, entityID)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.PrepareReceiverNullableConstructionObservationForTest(ctx, owner, entityID)
	default:
		return fmt.Errorf("receiver observation fault requires the original native owner")
	}
}

func SetReceiverConstructionObservationUnavailableForTest(ctx context.Context, selected any, unavailable bool) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.SetReceiverConstructionObservationUnavailableForTest(ctx, unavailable)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.SetReceiverConstructionObservationUnavailableForTest(ctx, unavailable)
	default:
		return fmt.Errorf("receiver observation fault requires the original native owner")
	}
}

// Retained for the distinct fork-predecessor and receipt-correspondence cuts.
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
