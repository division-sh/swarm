package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/agentpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	entitystore "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

func CountSelectedExecutionRootFlowHeadersStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, pipelinepersistence.CountSelectedExecutionRootFlowHeadersStorageTx)
}

func CountSelectedExecutionRootEntityRowsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, entitystore.CountSelectedExecutionRootEntityRowsStorageTx)
}

func CountEventReceiptStorageForTest(ctx context.Context, selected any, eventID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, eventID, pipelinepersistence.CountEventReceiptStorageTx)
}

func CountSelectedExecutionEventMappingStorageForTest(ctx context.Context, selected any, forkRunID, sourceRunID, sourceEventID, forkEventID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{forkRunID, sourceRunID, sourceEventID, forkEventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return runforkpersistence.CountSelectedExecutionEventMappingStorageTx(ctx, tx, forkRunID, sourceRunID, sourceEventID, forkEventID)
	})
}

type SelectedNodeDeliveryFailureStorage = delivery.SelectedNodeDeliveryFailureStorage

func ReadSelectedNodeDeliveryFailureStorageForTest(ctx context.Context, selected any, runID, eventID string) (SelectedNodeDeliveryFailureStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID, eventID}, func(ctx context.Context, tx *sql.Tx) (SelectedNodeDeliveryFailureStorage, error) {
		return delivery.ReadSelectedNodeDeliveryFailureStorageTx(ctx, tx, runID, eventID)
	})
}

func CountRunEventSubscriberDeliveriesStorageForTest(ctx context.Context, selected any, runID, eventID, subscriberID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID, eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountRunEventSubscriberDeliveriesStorageTx(ctx, tx, runID, eventID, subscriberID)
	})
}

func CountForkSourceSubscriberActiveDeliveriesStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, runID, delivery.CountForkSourceSubscriberActiveDeliveriesStorageTx)
}

func CountRunNamedAgentStorageForTest(ctx context.Context, selected any, runID, agentID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return agentpersistence.CountRunNamedAgentStorageTx(ctx, tx, runID, agentID)
	})
}

func CountPostgresSelectedForkCancelledBranchEvidenceStorageForTest(ctx context.Context, selected any, forkRunID, sourceRunID, forkEventID, policy string) (int, error) {
	if owner, ok := selected.(*PostgresStore); !ok || owner == nil {
		return 0, fmt.Errorf("selected branch evidence requires its original PostgreSQL owner")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{forkRunID, sourceRunID, forkEventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return runforkpersistence.CountPostgresSelectedForkCancelledBranchEvidenceStorageTx(ctx, tx, forkRunID, sourceRunID, forkEventID, policy)
	})
}

func CountRunEventAgentDeliveriesStorageForTest(ctx context.Context, selected any, runID, eventID, agentID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID, eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountRunEventAgentDeliveriesStorageTx(ctx, tx, runID, eventID, agentID)
	})
}

func CountDeliveredAgentAttemptsStorageForTest(ctx context.Context, selected any, eventID, agentID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountDeliveredAgentAttemptsStorageTx(ctx, tx, eventID, agentID)
	})
}

func ReadLatestDeadLetterFailureStorageForTest(ctx context.Context, selected any) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadLatestDeadLetterFailureStorageTx(ctx, tx)
	})
}

func ReadLatestReceiptFailureStorageForTest(ctx context.Context, selected any) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadLatestReceiptFailureStorageTx(ctx, tx)
	})
}

func ReadPostgresSelectedAgentDeliveryFailureStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	if owner, ok := selected.(*PostgresStore); !ok || owner == nil {
		return "", fmt.Errorf("selected execution diagnostic requires its original PostgreSQL owner")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadPostgresSelectedAgentDeliveryFailureStorageTx(ctx, tx, runID)
	})
}

func ReadPostgresRunDeliveryAttemptArrayStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	if owner, ok := selected.(*PostgresStore); !ok || owner == nil {
		return "", fmt.Errorf("selected execution diagnostic requires its original PostgreSQL owner")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadPostgresRunDeliveryAttemptArrayStorageTx(ctx, tx, runID)
	})
}

func ReadPostgresSelectedForkDeliveryTimelineStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	if owner, ok := selected.(*PostgresStore); !ok || owner == nil {
		return "", fmt.Errorf("selected execution diagnostic requires its original PostgreSQL owner")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadPostgresSelectedForkDeliveryTimelineStorageTx(ctx, tx, runID)
	})
}

func ReadPostgresSelectedForkRouteTimelineStorageForTest(ctx context.Context, selected any, runID string) (string, error) {
	if owner, ok := selected.(*PostgresStore); !ok || owner == nil {
		return "", fmt.Errorf("selected execution diagnostic requires its original PostgreSQL owner")
	}
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return delivery.ReadPostgresSelectedForkRouteTimelineStorageTx(ctx, tx, runID)
	})
}
