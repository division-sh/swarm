package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
)

type ActivityAttemptStorageEvidence struct {
	RequestEventID string
	Tool           string
	SourceEventID  string
	Status         string
}

// ReadActivityAttemptStorageForTest retains the journal's physical cardinality
// and original started-at ordering, even without a surviving request event.
func ReadActivityAttemptStorageForTest(ctx context.Context, selected any, runID string) ([]ActivityAttemptStorageEvidence, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID {
		return nil, fmt.Errorf("activity storage evidence requires an exact canonical run identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var evidence []ActivityAttemptStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT request_event_id, tool,
			COALESCE(CAST(source_event_id AS TEXT), ''), status
			FROM activity_attempts WHERE run_id=$1 ORDER BY started_at ASC`, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row ActivityAttemptStorageEvidence
			if err := rows.Scan(&row.RequestEventID, &row.Tool, &row.SourceEventID, &row.Status); err != nil {
				return err
			}
			evidence = append(evidence, row)
		}
		return rows.Err()
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return nil, err
	}
	return evidence, nil
}
