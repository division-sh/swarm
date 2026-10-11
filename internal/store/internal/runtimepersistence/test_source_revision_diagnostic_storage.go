package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
)

type SourceRevisionDiagnosticStorage = eventpersistence.SourceRevisionDiagnosticStorage

func ReadSourceRevisionDiagnosticStorageForTest(ctx context.Context, selected any, runID string) ([]SourceRevisionDiagnosticStorage, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var out []SourceRevisionDiagnosticStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventpersistence.ReadSourceRevisionDiagnosticStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadRunLifecycleProjectionLogPayloadsForTest(ctx context.Context, selected any, runID string) ([][]byte, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return nil, err
	}
	var out [][]byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadRunLifecycleProjectionLogPayloadsTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
