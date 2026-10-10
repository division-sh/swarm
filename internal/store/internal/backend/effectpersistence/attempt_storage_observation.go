package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
)

type ExternalAttemptStorage struct {
	OperationID string
	State       string
}

func (s *EffectPostgresOwner) ReadExternalAttemptStorageForTest(ctx context.Context) ([]ExternalAttemptStorage, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() || s.requireCurrent == nil {
		return nil, fmt.Errorf("external attempt observation requires the original postgres owner")
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	return readExternalAttemptStorageForTest(ctx, s.backend.RunReadTransaction)
}

func (s *EffectSQLiteOwner) ReadExternalAttemptStorageForTest(ctx context.Context) ([]ExternalAttemptStorage, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() || s.requireCurrent == nil {
		return nil, fmt.Errorf("external attempt observation requires the original sqlite owner")
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	return readExternalAttemptStorageForTest(ctx, s.backend.RunReadTransaction)
}

func readExternalAttemptStorageForTest(ctx context.Context, read func(context.Context, func(context.Context, *sql.Tx) error) error) ([]ExternalAttemptStorage, error) {
	var evidence []ExternalAttemptStorage
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT CAST(operation_id AS TEXT),state FROM runtime_external_effect_attempts ORDER BY attempt_id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row ExternalAttemptStorage
			if err := rows.Scan(&row.OperationID, &row.State); err != nil {
				return err
			}
			evidence = append(evidence, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return evidence, nil
}
