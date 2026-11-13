package channelonboarding

import (
	"context"
	"database/sql"
)

func readChannelRegistrationCountsForTestTx(ctx context.Context, tx *sql.Tx) (int, int, int, error) {
	var total, current, retired int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),SUM(CASE WHEN account_confirmed AND phase<>'retired' THEN 1 ELSE 0 END),
		SUM(CASE WHEN phase='retired' AND logout_completed THEN 1 ELSE 0 END)
		FROM deployment_channel_operations WHERE provider='telegram'`).Scan(&total, &current, &retired)
	return total, current, retired, err
}

func (s *PostgresOwner) ReadChannelRegistrationCountsForTest(ctx context.Context) (int, int, int, error) {
	var total, current, retired int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		total, current, retired, err = readChannelRegistrationCountsForTestTx(ctx, tx)
		return err
	})
	return total, current, retired, err
}

func (s *SQLiteOwner) ReadChannelRegistrationCountsForTest(ctx context.Context) (int, int, int, error) {
	var total, current, retired int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		total, current, retired, err = readChannelRegistrationCountsForTestTx(ctx, tx)
		return err
	})
	return total, current, retired, err
}
