package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
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
