package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
)

type ClosedReceiverReadinessStorage = delivery.ClosedReceiverReadinessStorage
type ClosedReceiverAuthorityStorage = effectpersistence.ClosedReceiverAuthorityStorage
type ClosedReceiverDeliveryStorage = delivery.ClosedReceiverDeliveryStorage
type ClosedReceiverAgentRouteStorage = delivery.ClosedReceiverAgentRouteStorage
type ClosedReceiverReceiptStorage = pipelinepersistence.ClosedReceiverReceiptStorage
type ClosedReceiverAgentStorage = agentpersistence.ClosedReceiverAgentStorage

func ReadClosedReceiverReadinessStorageForTest(ctx context.Context, selected any, runID, bridgeNodeID string) (ClosedReceiverReadinessStorage, error) {
	if bridgeNodeID == "" {
		return ClosedReceiverReadinessStorage{}, fmt.Errorf("closed receiver observation requires its exact bridge node")
	}
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (ClosedReceiverReadinessStorage, error) {
		return delivery.ReadClosedReceiverReadinessStorageTx(ctx, tx, postgres, runID, bridgeNodeID)
	})
}

func ReadClosedReceiverAuthorityStorageForTest(ctx context.Context, selected any) ([]ClosedReceiverAuthorityStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, effectpersistence.ReadClosedReceiverAuthorityStorageTx)
}

func CountClosedReceiverPipelineSuccessStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountClosedReceiverPipelineSuccessStorageTx(ctx, tx, postgres, runID)
	})
}

func ReadClosedReceiverDeliveryStorageForTest(ctx context.Context, selected any, runID string) ([]ClosedReceiverDeliveryStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([]ClosedReceiverDeliveryStorage, error) {
		return delivery.ReadClosedReceiverDeliveryStorageTx(ctx, tx, postgres, runID)
	})
}

func ReadClosedReceiverReceiptStorageForTest(ctx context.Context, selected any, runID string) ([]ClosedReceiverReceiptStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([]ClosedReceiverReceiptStorage, error) {
		return pipelinepersistence.ReadClosedReceiverReceiptStorageTx(ctx, tx, postgres, runID)
	})
}

func ReadClosedReceiverAgentStorageForTest(ctx context.Context, selected any) ([]ClosedReceiverAgentStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, agentpersistence.ReadClosedReceiverAgentStorageTx)
}

func ReadClosedReceiverAgentRouteStorageForTest(ctx context.Context, selected any, runID string) ([]ClosedReceiverAgentRouteStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) ([]ClosedReceiverAgentRouteStorage, error) {
		return delivery.ReadClosedReceiverAgentRouteStorageTx(ctx, tx, postgres, runID)
	})
}
