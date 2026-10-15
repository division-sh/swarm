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

type ProviderTurnAttemptLineageStorage struct {
	OperationID, AttemptID, State string
	Lineage                       []byte
}

func (s *EffectPostgresOwner) ReadProviderTurnAttemptLineageStorageForTest(ctx context.Context) ([]ProviderTurnAttemptLineageStorage, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() || s.requireCurrent == nil {
		return nil, fmt.Errorf("provider attempt observation requires the original postgres owner")
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	return readProviderTurnAttemptLineageStorage(ctx, s.backend.RunReadTransaction)
}

func (s *EffectSQLiteOwner) ReadProviderTurnAttemptLineageStorageForTest(ctx context.Context) ([]ProviderTurnAttemptLineageStorage, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() || s.requireCurrent == nil {
		return nil, fmt.Errorf("provider attempt observation requires the original sqlite owner")
	}
	if err := s.requireCurrent(); err != nil {
		return nil, err
	}
	return readProviderTurnAttemptLineageStorage(ctx, s.backend.RunReadTransaction)
}

func readProviderTurnAttemptLineageStorage(ctx context.Context, read func(context.Context, func(context.Context, *sql.Tx) error) error) ([]ProviderTurnAttemptLineageStorage, error) {
	var out []ProviderTurnAttemptLineageStorage
	err := read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT CAST(o.operation_id AS TEXT),CAST(a.attempt_id AS TEXT),a.state,o.lineage FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.effect_kind='provider_turn' ORDER BY a.attempt_ordinal`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row ProviderTurnAttemptLineageStorage
			if err := rows.Scan(&row.OperationID, &row.AttemptID, &row.State, &row.Lineage); err != nil {
				return err
			}
			out = append(out, row)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func CountSelectedExecutionSettledAttemptsStorageTx(ctx context.Context, tx *sql.Tx, executionID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1 AND a.state='settled'`, executionID).Scan(&count)
	return count, err
}

func CountCompletionAttemptBudgetReservationsStorageTx(ctx context.Context, tx *sql.Tx, attemptID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=$1`, attemptID).Scan(&count)
	return count, err
}

func CountSelectedExecutionAttemptsStorageTx(ctx context.Context, tx *sql.Tx, executionID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1`, executionID).Scan(&count)
	return count, err
}

func CountSelectedAuthoredHTTPAttemptsStorageTx(ctx context.Context, tx *sql.Tx, executionID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1 AND o.effect_kind='http_tool_target' AND a.adapter='authored_http_tool'`, executionID).Scan(&count)
	return count, err
}

func CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorageTx(ctx context.Context, tx *sql.Tx, executionID, surfaceID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1 AND o.authority_kind='selected_contract_fork' AND o.effect_kind='http_tool_target' AND a.adapter='authored_http_tool' AND a.capability_surface_id=$2 AND a.state='settled'`, executionID, surfaceID).Scan(&count)
	return count, err
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
