package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

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
