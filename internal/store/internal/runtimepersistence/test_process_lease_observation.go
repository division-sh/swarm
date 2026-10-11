package runtimepersistence

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

// Witness the original constructed backend, even if its filesystem path changed.
func ReadProcessGenerationCardinalityForTest(ctx context.Context, selected any) (rows, generation int, err error) {
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*), generation FROM runtime_process_generation GROUP BY generation`).Scan(&rows, &generation)
	})
	return
}

// Only this exact retained-activation coordinate can be replaced. The expected
// value and row count fence both installation and restoration in the same owner.
func ReplaceRetainedChannelRuntimeForTest(ctx context.Context, selected channelonboarding.Store, activation, from, to string) error {
	if err := validateSelectedForkStorageIdentity(activation); err != nil {
		return err
	}
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE connected_channel_activations SET runtime_instance_id=$1 WHERE activation_id=$2 AND runtime_instance_id=$3`, to, activation, from)
		if err != nil {
			return err
		}
		return requireMailboxFixtureRow(result)
	})
}
