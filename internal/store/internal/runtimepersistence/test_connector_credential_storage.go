package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

type ConnectorCredentialLeakStorageEvidence struct {
	EventPayloads             int
	ActivityResultsOrFailures int
}

// The counts preserve the serialized-storage LIKE witnesses used by the
// connector journeys; no token, query, or persisted value is returned.
func ReadConnectorCredentialLeakStorageForTest(ctx context.Context, selected any, runID, secret string) (ConnectorCredentialLeakStorageEvidence, error) {
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID || secret == "" {
		return ConnectorCredentialLeakStorageEvidence{}, fmt.Errorf("connector credential evidence requires an exact run identity and nonempty secret")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return ConnectorCredentialLeakStorageEvidence{}, err
	}
	var evidence ConnectorCredentialLeakStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events
			WHERE run_id=$1 AND CAST(payload AS TEXT) LIKE '%' || $2 || '%'`, runID, secret).Scan(&evidence.EventPayloads); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM activity_attempts
			WHERE run_id=$1 AND (COALESCE(CAST(result_payload AS TEXT), '') LIKE '%' || $2 || '%'
			OR COALESCE(CAST(failure AS TEXT), '') LIKE '%' || $2 || '%')`, runID, secret).Scan(&evidence.ActivityResultsOrFailures)
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return ConnectorCredentialLeakStorageEvidence{}, err
	}
	return evidence, nil
}
