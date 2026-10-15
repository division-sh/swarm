package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/mailboxpersistence"
)

// A missing row is a completed physical observation. Retain sql.ErrNoRows only
// after read-transaction cleanup; never hide cancellation or cleanup failures.
func readServedReadRole[T any](ctx context.Context, selected any, read func(context.Context, *sql.Tx) (T, error)) (T, error) {
	var out T
	if err := validateChannelObservationOwner(selected); err != nil {
		return out, err
	}
	missing := false
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = read(ctx, tx)
		if err == sql.ErrNoRows {
			missing = true
			return nil
		}
		return err
	})
	if err == nil && missing {
		err = sql.ErrNoRows
	}
	if err != nil {
		var empty T
		return empty, err
	}
	return out, err
}

type ServedDeliveryRouteEvidenceStorage = delivery.ServedDeliveryRouteEvidenceStorage
type DeliveryRecoveryIdentityStorage = delivery.DeliveryRecoveryIdentityStorage
type DeliveryRecoveryAttemptStorage = delivery.DeliveryRecoveryAttemptStorage
type OrderedFanOutOutcomeStorage = delivery.OrderedFanOutOutcomeStorage
type CommittedFanOutOrdinalStorage = delivery.CommittedFanOutOrdinalStorage
type ServedForkWorkflowDiagnosticsStorage = pipelinepersistence.ServedForkWorkflowDiagnosticsStorage
type ServedTemplateCompletionDiagnosticStorage = delivery.ServedTemplateCompletionDiagnosticStorage

func ReadServedForkWorkflowDiagnosticsStorageForTest(ctx context.Context, selected any, runID string) (ServedForkWorkflowDiagnosticsStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedForkWorkflowDiagnosticsStorage, error) {
		return pipelinepersistence.ReadServedForkWorkflowDiagnosticsStorageTx(ctx, tx, runID)
	})
}
func ReadTemplateCompletionDeliveryDiagnosticsStorageForTest(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedTemplateCompletionDiagnosticStorage, error) {
		return delivery.ReadTemplateCompletionDeliveryDiagnosticsStorageTx(ctx, tx, runID)
	})
}
func ReadTemplateCompletionReceiptDiagnosticsStorageForTest(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedTemplateCompletionDiagnosticStorage, error) {
		out, err := pipelinepersistence.ReadTemplateCompletionReceiptDiagnosticsStorageTx(ctx, tx, runID)
		return ServedTemplateCompletionDiagnosticStorage(out), err
	})
}
func ReadReporterIntentSnapshotStorageForTest(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedTemplateCompletionDiagnosticStorage, error) {
		out, err := pipelinepersistence.ReadReporterIntentSnapshotStorageTx(ctx, tx, runID)
		return ServedTemplateCompletionDiagnosticStorage(out), err
	})
}
func ReadReporterOutcomeSnapshotStorageForTest(ctx context.Context, selected any, runID string) (ServedTemplateCompletionDiagnosticStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (ServedTemplateCompletionDiagnosticStorage, error) {
		out, err := pipelinepersistence.ReadReporterOutcomeSnapshotStorageTx(ctx, tx, runID)
		return ServedTemplateCompletionDiagnosticStorage(out), err
	})
}
func CountPhysicalAPIReceiptsStorageForTest(ctx context.Context, selected any) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return apiidempotency.CountPhysicalAPIReceiptsStorageTx(ctx, tx)
	})
}

func ReadServedDeliveryRouteEvidenceStorageForTest(ctx context.Context, selected any, runID string) ([]ServedDeliveryRouteEvidenceStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]ServedDeliveryRouteEvidenceStorage, error) {
		return delivery.ReadServedDeliveryRouteEvidenceStorageTx(ctx, tx, runID)
	})
}
func ReadDeliveryRecoveryIdentityStorageForTest(ctx context.Context, selected any, deliveryID string) (DeliveryRecoveryIdentityStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (DeliveryRecoveryIdentityStorage, error) {
		return delivery.ReadDeliveryRecoveryIdentityStorageTx(ctx, tx, deliveryID)
	})
}
func ReadDeliveryRecoveryAttemptsStorageForTest(ctx context.Context, selected any, deliveryID string) ([]DeliveryRecoveryAttemptStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]DeliveryRecoveryAttemptStorage, error) {
		return delivery.ReadDeliveryRecoveryAttemptsStorageTx(ctx, tx, deliveryID)
	})
}
func CountSourceRunDeliveryStorageForTest(ctx context.Context, selected any, runID, sourceEventID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return delivery.CountSourceRunDeliveryStorageTx(ctx, tx, runID, sourceEventID)
	})
}
func CountSourceNoticeStorageForTest(ctx context.Context, selected any, sourceEventID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return mailboxpersistence.CountSourceNoticeStorageTx(ctx, tx, sourceEventID)
	})
}
func CountRunUnselectedTemplateActivationsStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountRunUnselectedTemplateActivationsStorageTx(ctx, tx, runID)
	})
}
func ReadOrderedRunFanOutOutcomesStorageForTest(ctx context.Context, selected any, runID string) ([]OrderedFanOutOutcomeStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]OrderedFanOutOutcomeStorage, error) {
		return delivery.ReadOrderedRunFanOutOutcomesStorageTx(ctx, tx, runID)
	})
}
func ReadCommittedRunFanOutOrdinalsStorageForTest(ctx context.Context, selected any, runID string) ([]CommittedFanOutOrdinalStorage, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]CommittedFanOutOrdinalStorage, error) {
		return delivery.ReadCommittedRunFanOutOrdinalsStorageTx(ctx, tx, runID)
	})
}

func ReadEntityAccumulatorTextStorageForTest(ctx context.Context, selected any, runID, entityID string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadEntityAccumulatorTextStorageTx(ctx, tx, runID, entityID)
	})
}

func ReadTemplateEntityAccumulatorTextStorageForTest(ctx context.Context, selected any, runID, entityID, template string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadTemplateEntityAccumulatorTextStorageTx(ctx, tx, runID, entityID, template)
	})
}

func ReadInstanceTemplateStorageForTest(ctx context.Context, selected any, runID, instance string) (string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (string, error) {
		return pipelinepersistence.ReadInstanceTemplateStorageTx(ctx, tx, runID, instance)
	})
}

func ReadWorkflowTimerDueValueStorageForTest(ctx context.Context, selected any, runID, entityID string) (any, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (any, error) {
		return pipelinepersistence.ReadWorkflowTimerDueValueStorageTx(ctx, tx, runID, entityID)
	})
}

func ReadInstanceClockAdvancedStorageForTest(ctx context.Context, selected any, runID, instance string, previous any) (bool, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) (bool, error) {
		return pipelinepersistence.ReadInstanceClockAdvancedStorageTx(ctx, tx, runID, instance, previous)
	})
}

func ReadRunEventMutationOwnersStorageForTest(ctx context.Context, selected any, runID, eventID string) ([]string, error) {
	return readServedReadRole(ctx, selected, func(ctx context.Context, tx *sql.Tx) ([]string, error) {
		return pipelinepersistence.ReadRunEventMutationOwnersStorageTx(ctx, tx, runID, eventID)
	})
}
