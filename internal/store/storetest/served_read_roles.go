package storetest

import (
	"context"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ServedDeliveryRouteEvidenceStorage = private.ServedDeliveryRouteEvidenceStorage
type DeliveryRecoveryIdentityStorage = private.DeliveryRecoveryIdentityStorage
type DeliveryRecoveryAttemptStorage = private.DeliveryRecoveryAttemptStorage
type OrderedFanOutOutcomeStorage = private.OrderedFanOutOutcomeStorage
type CommittedFanOutOrdinalStorage = private.CommittedFanOutOrdinalStorage
type ServedForkWorkflowDiagnosticsStorage = private.ServedForkWorkflowDiagnosticsStorage
type ServedForkRuntimeDiagnosticsStorage = private.ServedForkRuntimeDiagnosticsStorage
type ServedTemplateCompletionDiagnosticStorage = private.ServedTemplateCompletionDiagnosticStorage

func ReadServedForkWorkflowDiagnosticsStorage(ctx context.Context, selected any, runID string) (ServedForkWorkflowDiagnosticsStorage, error) {
	return private.ReadServedForkWorkflowDiagnosticsStorageForTest(ctx, selected, runID)
}
func ReadServedForkRuntimeDiagnosticsStorage(ctx context.Context, selected any) (ServedForkRuntimeDiagnosticsStorage, error) {
	return private.ReadServedForkRuntimeDiagnosticsStorageForTest(ctx, selected)
}
func ReadTemplateCompletionDeliveryDiagnosticsStorage(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return private.ReadTemplateCompletionDeliveryDiagnosticsStorageForTest(ctx, selected, runID)
}
func ReadTemplateCompletionReceiptDiagnosticsStorage(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return private.ReadTemplateCompletionReceiptDiagnosticsStorageForTest(ctx, selected, runID)
}
func ReadTemplateCompletionEventDiagnosticsStorage(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return private.ReadTemplateCompletionEventDiagnosticsStorageForTest(ctx, selected, runID)
}
func ReadReporterIntentSnapshotStorage(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return private.ReadReporterIntentSnapshotStorageForTest(ctx, selected, runID)
}
func ReadReporterOutcomeSnapshotStorage(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return private.ReadReporterOutcomeSnapshotStorageForTest(ctx, selected, runID)
}
func ReadReporterEventSnapshotStorage(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return private.ReadReporterEventSnapshotStorageForTest(ctx, selected, runID)
}
func CountPhysicalAPIReceiptsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountPhysicalAPIReceiptsStorageForTest(ctx, selected)
}
func CountPhysicalEventsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountPhysicalEventsStorageForTest(ctx, selected)
}

func ReadServedDeliveryRouteEvidenceStorage(ctx context.Context, selected any, runID string) ([]ServedDeliveryRouteEvidenceStorage, error) {
	return private.ReadServedDeliveryRouteEvidenceStorageForTest(ctx, selected, runID)
}
func ReadDeliveryRecoveryIdentityStorage(ctx context.Context, selected any, deliveryID string) (DeliveryRecoveryIdentityStorage, error) {
	return private.ReadDeliveryRecoveryIdentityStorageForTest(ctx, selected, deliveryID)
}
func ReadDeliveryRecoveryAttemptsStorage(ctx context.Context, selected any, deliveryID string) ([]DeliveryRecoveryAttemptStorage, error) {
	return private.ReadDeliveryRecoveryAttemptsStorageForTest(ctx, selected, deliveryID)
}
func CountSourceRunDeliveryStorage(ctx context.Context, selected any, runID, sourceEventID string) (int, error) {
	return private.CountSourceRunDeliveryStorageForTest(ctx, selected, runID, sourceEventID)
}
func CountSourceNoticeStorage(ctx context.Context, selected any, sourceEventID string) (int, error) {
	return private.CountSourceNoticeStorageForTest(ctx, selected, sourceEventID)
}
func CountSourceNonDiagnosticEventsStorage(ctx context.Context, selected any, runID, sourceEventID, diagnosticClass, diagnosticName string) (int, error) {
	return private.CountSourceNonDiagnosticEventsStorageForTest(ctx, selected, runID, sourceEventID, diagnosticClass, diagnosticName)
}
func CountGlobalBusinessEventsStorage(ctx context.Context, selected any) (int, error) {
	return private.CountGlobalBusinessEventsStorageForTest(ctx, selected)
}
func CountRunUnselectedTemplateActivationsStorage(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountRunUnselectedTemplateActivationsStorageForTest(ctx, selected, runID)
}
func ReadOrderedRunFanOutOutcomesStorage(ctx context.Context, selected any, runID string) ([]OrderedFanOutOutcomeStorage, error) {
	return private.ReadOrderedRunFanOutOutcomesStorageForTest(ctx, selected, runID)
}
func ReadCommittedRunFanOutOrdinalsStorage(ctx context.Context, selected any, runID string) ([]CommittedFanOutOrdinalStorage, error) {
	return private.ReadCommittedRunFanOutOrdinalsStorageForTest(ctx, selected, runID)
}

func ReadEntityAccumulatorTextStorage(ctx context.Context, selected any, runID, entityID string) (string, error) {
	return private.ReadEntityAccumulatorTextStorageForTest(ctx, selected, runID, entityID)
}

func ReadTemplateEntityAccumulatorTextStorage(ctx context.Context, selected any, runID, entityID, template string) (string, error) {
	return private.ReadTemplateEntityAccumulatorTextStorageForTest(ctx, selected, runID, entityID, template)
}

func ReadInstanceTemplateStorage(ctx context.Context, selected any, runID, instance string) (string, error) {
	return private.ReadInstanceTemplateStorageForTest(ctx, selected, runID, instance)
}

func ReadWorkflowTimerDueValueStorage(ctx context.Context, selected any, runID, entityID string) (any, error) {
	return private.ReadWorkflowTimerDueValueStorageForTest(ctx, selected, runID, entityID)
}

func ReadInstanceClockAdvancedStorage(ctx context.Context, selected any, runID, instance string, previous any) (bool, error) {
	return private.ReadInstanceClockAdvancedStorageForTest(ctx, selected, runID, instance, previous)
}

func ReadRunEventMutationOwnersStorage(ctx context.Context, selected any, runID, eventID string) ([]string, error) {
	return private.ReadRunEventMutationOwnersStorageForTest(ctx, selected, runID, eventID)
}
