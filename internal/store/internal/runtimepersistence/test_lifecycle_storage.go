package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

func ReadLifecycleEventCardinalityForTest(ctx context.Context, selected any, runID, eventName string) (int, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return 0, err
	}
	if eventName == "" {
		return 0, fmt.Errorf("lifecycle event cardinality requires an exact event name")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return 0, err
	}
	var count int
	read := func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1 AND event_name = $2`, runID, eventName).Scan(&count)
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return 0, err
	}
	return count, nil
}

// LifecycleStorageRow is physical evidence; transition and loop interpretation
// remain with the existing workflow and loop owners.
