package mutationlog

import (
	"context"
	"database/sql"
	"errors"
)

type GateDuplicateMutationDiagnosticStorage struct{ Domain, Path, OldValue, NewValue string }

func ReadGateDuplicateMutationDiagnosticsTx(ctx context.Context, tx *sql.Tx, eventID string) ([]GateDuplicateMutationDiagnosticStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT domain,path,CAST(old_value AS TEXT),CAST(new_value AS TEXT) FROM entity_mutations WHERE caused_by_event=$1`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GateDuplicateMutationDiagnosticStorage
	for rows.Next() {
		var row GateDuplicateMutationDiagnosticStorage
		if err := rows.Scan(&row.Domain, &row.Path, &row.OldValue, &row.NewValue); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}

func CountRunMutationStorageTx(ctx context.Context, tx *sql.Tx, run string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1`, run).Scan(&count)
	return count, err
}

func CountRunEventMutationStorageTx(ctx context.Context, tx *sql.Tx, runID, eventID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1 AND caused_by_event=$2`, runID, eventID).Scan(&count)
	return count, err
}
