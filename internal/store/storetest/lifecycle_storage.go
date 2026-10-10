package storetest

import (
	"context"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func ReadLifecycleEventCardinality(ctx context.Context, selected any, runID, eventName string) (int, error) {
	return private.ReadLifecycleEventCardinalityForTest(ctx, selected, runID, eventName)
}

type SelectedForkLifecycleDiagnosticReceipt = private.SelectedForkLifecycleDiagnosticReceipt
type SelectedForkLifecycleDiagnosticLog = private.SelectedForkLifecycleDiagnosticLog
type SelectedForkLifecycleDiagnosticStorage = private.SelectedForkLifecycleDiagnosticStorage

func ReadSelectedForkLifecycleDiagnosticStorage(ctx context.Context, selected any, runID string) (SelectedForkLifecycleDiagnosticStorage, error) {
	return private.ReadSelectedForkLifecycleDiagnosticStorageForTest(ctx, selected, runID)
}

type SelectedCausalDiagnosticStorage = private.SelectedCausalDiagnosticStorage
type SelectedCausalDiagnosticConservation = private.SelectedCausalDiagnosticConservation

func ReadSelectedCausalDiagnosticStorage(ctx context.Context, selected any, outboxID string) (SelectedCausalDiagnosticStorage, error) {
	return private.ReadSelectedCausalDiagnosticStorageForTest(ctx, selected, outboxID)
}

func ReadSelectedCausalDiagnosticConservation(ctx context.Context, selected any, outboxID string) (SelectedCausalDiagnosticConservation, error) {
	return private.ReadSelectedCausalDiagnosticConservationForTest(ctx, selected, outboxID)
}
