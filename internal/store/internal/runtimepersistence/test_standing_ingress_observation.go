package runtimepersistence

import (
	"context"
	"database/sql"
	decisioncard "github.com/division-sh/swarm/internal/store/internal/backend/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	entityruntime "github.com/division-sh/swarm/internal/store/internal/backend/entityruntime"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/standingdisposition"
)

type StandingCurrentStorage = standingdisposition.StandingCurrentStorage
type StandingServiceRunStorage = standingdisposition.StandingServiceRunStorage
type ServedRunControlStorage = runlifecycle.ServedRunControlStorage
type ServedIngressStorage = runlifecycle.ServedIngressStorage
type ServedDeliveryStatusReasonStorage = delivery.ServedDeliveryStatusReasonStorage

func ReadFirstStandingServiceStorageForTest(ctx context.Context, selected any) (StandingFlowStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (StandingFlowStorage, error) {
		return standingdisposition.ReadFirstStandingServiceStorageTx(ctx, tx, postgres)
	})
}

func ReadUnfilteredStandingCurrentStorageForTest(ctx context.Context, selected any) (StandingCurrentStorage, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, standingdisposition.ReadUnfilteredStandingCurrentStorageTx)
}

func ReadUnfilteredStandingServiceIDStorageForTest(ctx context.Context, selected any) (string, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, standingdisposition.ReadUnfilteredStandingServiceIDStorageTx)
}

func ReadStandingServiceRunStorageForTest(ctx context.Context, selected any, serviceID string) (StandingServiceRunStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{serviceID}, func(ctx context.Context, tx *sql.Tx) (StandingServiceRunStorage, error) {
		return standingdisposition.ReadStandingServiceRunStorageTx(ctx, tx, postgres, serviceID)
	})
}

func ReadActiveDeclaredStandingFlowRunStorageForTest(ctx context.Context, selected any, flow string) (string, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return standingdisposition.ReadActiveDeclaredStandingFlowRunStorageTx(ctx, tx, postgres, flow)
	})
}

func CountStandingFlowAuthoritiesStorageForTest(ctx context.Context, selected any, flow string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return standingdisposition.CountStandingFlowAuthoritiesStorageTx(ctx, tx, flow)
	})
}

func CountStandingServicesStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, standingdisposition.CountStandingServicesStorageTx)
}

func CountStandingGenerationsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, standingdisposition.CountStandingGenerationsStorageTx)
}

func CountStandingJournalStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, standingdisposition.CountStandingJournalStorageTx)
}

func CountStandingOriginRunsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, runlifecycle.CountStandingOriginRunsStorageTx)
}

func ReadServedRunControlStorageForTest(ctx context.Context, selected any, runID string) (ServedRunControlStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (ServedRunControlStorage, error) {
		return runlifecycle.ReadServedRunControlStorageTx(ctx, tx, postgres, runID)
	})
}

func ReadServedIngressStorageForTest(ctx context.Context, selected any) (ServedIngressStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, nil, func(ctx context.Context, tx *sql.Tx) (ServedIngressStorage, error) {
		return runlifecycle.ReadServedIngressStorageTx(ctx, tx, postgres)
	})
}

func ReadServedDeliveryStatusReasonStorageForTest(ctx context.Context, selected any, eventID, recipientType, recipientID string) (ServedDeliveryStatusReasonStorage, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{eventID}, func(ctx context.Context, tx *sql.Tx) (ServedDeliveryStatusReasonStorage, error) {
		return delivery.ReadServedDeliveryStatusReasonStorageTx(ctx, tx, postgres, eventID, recipientType, recipientID)
	})
}

func CountUnsettledStandingDeliveriesStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, delivery.CountUnsettledStandingDeliveriesStorageTx)
}

func CountPendingStandingRetirementCardsStorageForTest(ctx context.Context, selected any, entityID string) (int, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{entityID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return decisioncard.CountPendingStandingRetirementCardsStorageTx(ctx, tx, postgres, entityID)
	})
}

func CountStandingEntityStorageForTest(ctx context.Context, selected any, entityID string) (int, error) {
	_, postgres := selected.(*PostgresStore)
	return readFanOutSemanticStorage(ctx, selected, []string{entityID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return entityruntime.CountStandingEntityStorageTx(ctx, tx, postgres, entityID)
	})
}

func CountInboundPublicationsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, eventpersistence.CountInboundPublicationsStorageTx)
}

func CountExternalEffectOperationsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, nil, effectpersistence.CountExternalEffectOperationsStorageTx)
}
