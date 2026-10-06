package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
)

func validateSelectedForkStorageIdentity(value string) error {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil || id.String() != value {
		return fmt.Errorf("selected-fork storage evidence requires an exact canonical identity")
	}
	return nil
}

func ReadSelectedForkRunBundleHashForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return "", err
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return "", err
	}
	var hash string
	read := func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, runID).Scan(&hash)
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return "", err
	}
	return hash, nil
}
