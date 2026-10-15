package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/effectpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

func CountRunFlowRuntimeReadinessStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	return readFanOutSemanticStorage(ctx, selected, []string{runID}, func(ctx context.Context, tx *sql.Tx) (int, error) {
		return pipelinepersistence.CountRunFlowRuntimeReadinessStorageTx(ctx, tx, runID)
	})
}

func CountSelectedForkSourceEventLineageStorageForTest(ctx context.Context, selected any, forkRunID, sourceEventID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, forkRunID); err != nil {
		return 0, err
	}
	if err := validateStandaloneStorageEvent(selected, sourceEventID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.CountSelectedForkSourceEventLineageStorageTx(ctx, tx, forkRunID, sourceEventID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountSelectedExecutionAttemptsStorageForTest(ctx context.Context, selected any, executionID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, executionID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.CountSelectedExecutionAttemptsStorageTx(ctx, tx, executionID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountSelectedAuthoredHTTPAttemptsStorageForTest(ctx context.Context, selected any, executionID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, executionID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.CountSelectedAuthoredHTTPAttemptsStorageTx(ctx, tx, executionID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorageForTest(ctx context.Context, selected any, executionID, surfaceID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, executionID); err != nil {
		return 0, err
	}
	if err := validateStandaloneStorageEvent(selected, surfaceID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorageTx(ctx, tx, executionID, surfaceID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountClosedFirstSelectedRuntimeExecutionStorageForTest(ctx context.Context, selected any, forkRunID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, forkRunID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.CountClosedFirstSelectedRuntimeExecutionStorageTx(ctx, tx, forkRunID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountSelectedExecutionSettledAttemptsStorageForTest(ctx context.Context, selected any, executionID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, executionID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.CountSelectedExecutionSettledAttemptsStorageTx(ctx, tx, executionID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountCompletionAttemptBudgetReservationsStorageForTest(ctx context.Context, selected any, attemptID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, attemptID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = effectpersistence.CountCompletionAttemptBudgetReservationsStorageTx(ctx, tx, attemptID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountSelectedForkRunsForSourceForTest(ctx context.Context, selected any, sourceRunID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, sourceRunID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.CountSelectedForkRunsForSourceTx(ctx, tx, sourceRunID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountSelectedContractBindingStorageForTest(ctx context.Context, selected any) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.CountSelectedContractBindingStorageTx(ctx, tx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func ReadSelectedRuntimeExecutionStateStorageForTest(ctx context.Context, selected any, forkRunID string) (string, error) {
	if err := validateStandaloneStorageEvent(selected, forkRunID); err != nil {
		return "", err
	}
	var out string
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.ReadSelectedRuntimeExecutionStateStorageTx(ctx, tx, forkRunID)
		return err
	})
	if err != nil {
		return "", err
	}
	return out, nil
}

func CountSelectedRuntimeExecutionStorageForTest(ctx context.Context, selected any, forkRunID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, forkRunID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.CountSelectedRuntimeExecutionStorageTx(ctx, tx, forkRunID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func CountSelectedSourceRuntimeExecutionsStorageForTest(ctx context.Context, selected any, sourceRunID string) (int, error) {
	return readDeploymentStorageForTest(ctx, selected, sourceRunID, runforkpersistence.CountSelectedSourceRuntimeExecutionsStorageTx)
}

func CountRunFlowInstanceStorageForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = pipelinepersistence.CountRunFlowInstanceStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}
