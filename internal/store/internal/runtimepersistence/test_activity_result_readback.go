package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
)

// ActivityResultPublicationStorage retains the original fixture-wide
// cardinality witnesses. It grants no query or transaction capability.
type ActivityResultPublicationStorage struct {
	Runs, Events, Deliveries, Entities, Receipts, ConversationForks int
	ResetOperations, ActivityAttempts, SuccessfulActivityAttempts   int
}

func ObserveActivityResultPublicationStorageForTest(ctx context.Context, selected any) (ActivityResultPublicationStorage, error) {
	var observed ActivityResultPublicationStorage
	read := func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM runs),
			(SELECT COUNT(*) FROM events),
			(SELECT COUNT(*) FROM event_deliveries),
			(SELECT COUNT(*) FROM entity_state),
			(SELECT COUNT(*) FROM api_idempotency),
			(SELECT COUNT(*) FROM conversation_forks),
			(SELECT COUNT(*) FROM runtime_reset_operations),
			(SELECT COUNT(*) FROM activity_attempts),
			(SELECT COUNT(*) FROM activity_attempts WHERE status='succeeded')`).
			Scan(&observed.Runs, &observed.Events, &observed.Deliveries, &observed.Entities, &observed.Receipts,
				&observed.ConversationForks, &observed.ResetOperations, &observed.ActivityAttempts, &observed.SuccessfulActivityAttempts)
	}
	var err error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil {
			return observed, fmt.Errorf("activity result evidence requires the original selected store")
		}
		if err = owner.requireCurrentSchema(); err == nil {
			err = owner.backend.RunReadTransaction(ctx, read)
		}
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil {
			return observed, fmt.Errorf("activity result evidence requires the original selected store")
		}
		if err = owner.requireCurrentSchema(); err == nil {
			err = owner.backend.RunReadTransaction(ctx, read)
		}
	default:
		return observed, fmt.Errorf("activity result evidence requires the original selected store")
	}
	if err != nil {
		return ActivityResultPublicationStorage{}, err
	}
	return observed, nil
}
