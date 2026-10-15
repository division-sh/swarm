package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/llmpersistence"
)

type PostgresAgentOneDeliverySessionStorage = delivery.PostgresAgentOneDeliverySessionStorage
type PostgresSessionFlowStorage = llmpersistence.PostgresSessionFlowStorage

func ReadPostgresAgentOneDeliverySessionStorageForTest(ctx context.Context, selected any, eventID string) (PostgresAgentOneDeliverySessionStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, eventID); err != nil {
		return PostgresAgentOneDeliverySessionStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresAgentOneDeliverySessionStorage, error) {
		return delivery.ReadPostgresAgentOneDeliverySessionStorageTx(ctx, tx, eventID)
	})
}

func ReadPostgresSessionFlowStorageForTest(ctx context.Context, selected any, sessionID string) (PostgresSessionFlowStorage, error) {
	if err := validatePostgresPublicationIdentities(selected, sessionID); err != nil {
		return PostgresSessionFlowStorage{}, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (PostgresSessionFlowStorage, error) {
		return llmpersistence.ReadPostgresSessionFlowStorageTx(ctx, tx, sessionID)
	})
}

func CountPostgresAgentOneSupportLiveSessionsForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validatePostgresPublicationIdentities(selected, runID); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return llmpersistence.CountPostgresAgentOneSupportLiveSessionsTx(ctx, tx, runID)
	})
}

func CountPostgresAgentOneSessionsForTest(ctx context.Context, selected any) (int, error) {
	if err := validatePostgresPublicationIdentities(selected); err != nil {
		return 0, err
	}
	return readFanOutSemanticStorage(ctx, selected, nil, llmpersistence.CountPostgresAgentOneSessionsTx)
}
