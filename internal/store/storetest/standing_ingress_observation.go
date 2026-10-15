package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type StandingCurrentStorage = private.StandingCurrentStorage
type StandingServiceRunStorage = private.StandingServiceRunStorage
type ServedRunControlStorage = private.ServedRunControlStorage
type ServedIngressStorage = private.ServedIngressStorage
type ServedDeliveryStatusReasonStorage = private.ServedDeliveryStatusReasonStorage
type StandingTelegramLineageStorage = private.StandingTelegramLineageStorage

func CountPhysicalSourceArtifactsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountPhysicalSourceArtifactsStorageForTest(ctx, selected)
}

func ReadFirstStandingServiceStorage(ctx context.Context, selected any) (StandingFlowStorage, error) {
	return private.ReadFirstStandingServiceStorageForTest(ctx, selected)
}

func ReadUnfilteredStandingCurrentStorage(ctx context.Context, selected any) (StandingCurrentStorage, error) {
	return private.ReadUnfilteredStandingCurrentStorageForTest(ctx, selected)
}

func ReadUnfilteredStandingServiceIDStorage(ctx context.Context, selected any) (string, error) {
	return private.ReadUnfilteredStandingServiceIDStorageForTest(ctx, selected)
}

func ReadStandingServiceRunStorage(ctx context.Context, selected any, serviceID string) (StandingServiceRunStorage, error) {
	return private.ReadStandingServiceRunStorageForTest(ctx, selected, serviceID)
}

func ReadActiveDeclaredStandingFlowRunStorage(ctx context.Context, selected any, flow string) (string, error) {
	return private.ReadActiveDeclaredStandingFlowRunStorageForTest(ctx, selected, flow)
}

func CountStandingFlowAuthoritiesStorage(ctx context.Context, selected any, flow string) (int, error) {
	return private.CountStandingFlowAuthoritiesStorageForTest(ctx, selected, flow)
}

func CountStandingServicesStorage(ctx context.Context, selected any) (int, error) {
	return private.CountStandingServicesStorageForTest(ctx, selected)
}

func CountStandingGenerationsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountStandingGenerationsStorageForTest(ctx, selected)
}

func CountStandingJournalStorage(ctx context.Context, selected any) (int, error) {
	return private.CountStandingJournalStorageForTest(ctx, selected)
}

func CountStandingOriginRunsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountStandingOriginRunsStorageForTest(ctx, selected)
}

func ReadServedRunControlStorage(ctx context.Context, selected any, runID string) (ServedRunControlStorage, error) {
	return private.ReadServedRunControlStorageForTest(ctx, selected, runID)
}

func ReadServedIngressStorage(ctx context.Context, selected any) (ServedIngressStorage, error) {
	return private.ReadServedIngressStorageForTest(ctx, selected)
}

func ReadServedDeliveryStatusReasonStorage(ctx context.Context, selected any, eventID, recipientType, recipientID string) (ServedDeliveryStatusReasonStorage, error) {
	return private.ReadServedDeliveryStatusReasonStorageForTest(ctx, selected, eventID, recipientType, recipientID)
}

func CountUnsettledStandingDeliveriesStorage(ctx context.Context, selected any) (int, error) {
	return private.CountUnsettledStandingDeliveriesStorageForTest(ctx, selected)
}

func CountPendingStandingRetirementCardsStorage(ctx context.Context, selected any, entityID string) (int, error) {
	return private.CountPendingStandingRetirementCardsStorageForTest(ctx, selected, entityID)
}

func CountStandingEntityStorage(ctx context.Context, selected any, entityID string) (int, error) {
	return private.CountStandingEntityStorageForTest(ctx, selected, entityID)
}

func CountInboundPublicationsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountInboundPublicationsStorageForTest(ctx, selected)
}

func CountExternalEffectOperationsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountExternalEffectOperationsStorageForTest(ctx, selected)
}

func ReadEventNameStorage(ctx context.Context, selected any, eventID string) (string, error) {
	return private.ReadEventNameStorageForTest(ctx, selected, eventID)
}

func CountInboundNamedEventsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountInboundNamedEventsStorageForTest(ctx, selected)
}

func ReadStandingTelegramLineageStorage(ctx context.Context, selected any, runID string) (StandingTelegramLineageStorage, error) {
	return private.ReadStandingTelegramLineageStorageForTest(ctx, selected, runID)
}
