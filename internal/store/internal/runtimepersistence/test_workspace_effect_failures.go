package runtimepersistence

import (
	"context"
	"database/sql"
)

type WorkspaceEffectFailureStorage struct {
	State, Failure string
}

// Failure-only fixture diagnostics retain all failed attempts, including those
// outside the fork, without granting a query or selected connection capability.
func ReadWorkspaceEffectFailuresForTest(ctx context.Context, selected any) ([]WorkspaceEffectFailureStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []WorkspaceEffectFailureStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT state, CAST(failure AS TEXT) FROM runtime_external_effect_attempts WHERE failure IS NOT NULL`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row WorkspaceEffectFailureStorage
			if err := rows.Scan(&row.State, &row.Failure); err != nil {
				return err
			}
			out = append(out, row)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}
