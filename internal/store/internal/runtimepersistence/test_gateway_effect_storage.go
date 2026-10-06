package runtimepersistence

import (
	"context"
	"database/sql"
)

type AuthoredHTTPToolEffectStorage struct{ BundleHash, OperationMode, AttemptMode string }

func ReadAuthoredHTTPToolEffectStorageForTest(ctx context.Context, selected any) ([]AuthoredHTTPToolEffectStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []AuthoredHTTPToolEffectStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT o.bundle_hash,o.execution_mode,a.execution_mode FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE a.adapter='authored_http_tool' AND a.state='settled'`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row AuthoredHTTPToolEffectStorage
			if err := rows.Scan(&row.BundleHash, &row.OperationMode, &row.AttemptMode); err != nil {
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
