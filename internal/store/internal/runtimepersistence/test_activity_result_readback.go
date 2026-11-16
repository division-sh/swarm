package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

// ActivityResultPublicationStorage retains the original fixture-wide
// cardinality witnesses. It grants no query or transaction capability.
type ActivityResultPublicationStorage struct {
	Runs, Events, Deliveries, Entities, Receipts, ConversationForks int
	ResetOperations, ActivityAttempts, SuccessfulActivityAttempts   int
}

type ProposedEffectRunExecutionStorage = activityjournal.ProposedEffectRunExecutionStorage

func ReadProposedEffectRunExecutionStorageForTest(ctx context.Context, selected any, runID string) (ProposedEffectRunExecutionStorage, error) {
	var observed ProposedEffectRunExecutionStorage
	if err := validateChannelObservationOwner(selected); err != nil {
		return observed, err
	}
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return observed, err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.activityPostgresOwner.ReadProposedEffectRunExecutionStorageForTest(ctx, runID)
	case *SQLiteRuntimeStore:
		return owner.activitySQLiteOwner.ReadProposedEffectRunExecutionStorageForTest(ctx, runID)
	}
	return observed, nil
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
