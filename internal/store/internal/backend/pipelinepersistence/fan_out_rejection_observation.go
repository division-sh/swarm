package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
)

type FanOutSemanticRejectionStorage struct {
	Ordinal int
	Failure string
}

func ReadFanOutSemanticRejectionStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]FanOutSemanticRejectionStorage, error) {
	rows, err := tx.QueryContext(ctx, `SELECT ordinal,CAST(failure AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 AND outcome_kind='semantic_rejected' ORDER BY ordinal`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []FanOutSemanticRejectionStorage
	for rows.Next() {
		var row FanOutSemanticRejectionStorage
		if err := rows.Scan(&row.Ordinal, &row.Failure); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	return out, nil
}
