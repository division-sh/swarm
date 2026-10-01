package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
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
		if err := tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM runs),
			(SELECT COUNT(*) FROM events),
			(SELECT COUNT(*) FROM entity_state),
			(SELECT COUNT(*) FROM api_idempotency),
			(SELECT COUNT(*) FROM conversation_forks),
			(SELECT COUNT(*) FROM runtime_reset_operations),
			(SELECT COUNT(*) FROM activity_attempts),
			(SELECT COUNT(*) FROM activity_attempts WHERE status='succeeded')`).
			Scan(&observed.Runs, &observed.Events, &observed.Entities, &observed.Receipts,
				&observed.ConversationForks, &observed.ResetOperations, &observed.ActivityAttempts, &observed.SuccessfulActivityAttempts); err != nil {
			return err
		}
		var err error
		observed.Deliveries, err = storedelivery.FixtureDeliveryCardinalityTx(ctx, tx)
		return err
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
