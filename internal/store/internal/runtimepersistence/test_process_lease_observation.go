package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

// Witness the original constructed backend, even if its filesystem path changed.
func ReadProcessGenerationCardinalityForTest(ctx context.Context, selected any) (rows, generation int, err error) {
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM runtime_process_generation), generation FROM runtime_process_generation WHERE singleton_id=1`).Scan(&rows, &generation)
	})
	return
}

// Only this exact retained-activation coordinate can be replaced. The expected
// value and row count fence both installation and restoration in the same owner.
func ReplaceRetainedChannelRuntimeForTest(ctx context.Context, selected channelonboarding.Store, activation, from, to string) error {
	if err := validateSelectedForkStorageIdentity(activation); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return owner.channelOnboardingPostgresOwner.ReplaceRetainedChannelRuntimeForTest(ctx, activation, from, to)
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return owner.channelOnboardingSQLiteOwner.ReplaceRetainedChannelRuntimeForTest(ctx, activation, from, to)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
