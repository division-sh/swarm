package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
)

type PostgresCompletionCandidateClockStorage = runlifecycle.PostgresCompletionCandidateClockStorage
type PostgresSystemNodeDeliveryStorage = delivery.PostgresSystemNodeDeliveryStorage
type PostgresSystemNodeOutcomeStorage = delivery.PostgresSystemNodeOutcomeStorage
type PostgresFlowEntityTerminalStorage = entitystore.PostgresFlowEntityTerminalStorage

func ReadPostgresCompletionCandidateClockStorageForTest(ctx context.Context, selected any, runID string) (PostgresCompletionCandidateClockStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return PostgresCompletionCandidateClockStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresCompletionCandidateClockStorage, error) {
		return runlifecycle.ReadPostgresCompletionCandidateClockStorageTx(ctx, tx, runID)
	})
}

func ReadPostgresRunTriggerEventIdentityStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return runlifecycle.ReadPostgresRunTriggerEventIdentityStorageTx(ctx, tx, runID)
	})
}

func ReadPostgresSystemNodeDeliveryStorageForTest(ctx context.Context, selected any, eventID, nodeID string) (PostgresSystemNodeDeliveryStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, eventID); err != nil {
		return PostgresSystemNodeDeliveryStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresSystemNodeDeliveryStorage, error) {
		return delivery.ReadPostgresSystemNodeDeliveryStorageTx(ctx, tx, eventID, nodeID)
	})
}

func ReadPostgresSystemNodeOutcomeStorageForTest(ctx context.Context, selected any, eventID, nodeID string) (PostgresSystemNodeOutcomeStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, eventID); err != nil {
		return PostgresSystemNodeOutcomeStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresSystemNodeOutcomeStorage, error) {
		return delivery.ReadPostgresSystemNodeOutcomeStorageTx(ctx, tx, eventID, nodeID)
	})
}

func ReadPostgresExactPipelineReceiptOutcomeStorageForTest(ctx context.Context, selected any, eventID string) (string, error) {
	if err := validatePostgresPublicationIdentities(selected, eventID); err != nil {
		return "", err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadPostgresExactPipelineReceiptOutcomeStorageTx(ctx, tx, eventID)
	})
}

func ReadPostgresFlowEntityTerminalStorageForTest(ctx context.Context, selected any, runID, entityID, flowInstance string) (PostgresFlowEntityTerminalStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, runID, entityID); err != nil {
		return PostgresFlowEntityTerminalStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresFlowEntityTerminalStorage, error) {
		return entitystore.ReadPostgresFlowEntityTerminalStorageTx(ctx, tx, runID, entityID, flowInstance)
	})
}
