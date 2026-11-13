package decisionpersistence

import (
	"context"
	"database/sql"
)

func countChannelSourceCardsForTestTx(ctx context.Context, tx *sql.Tx, run, source string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_cards WHERE run_id=$1 AND bundle_hash=$2`, run, source).Scan(&count)
	return count, err
}

func (s *DecisionPostgresOwner) CountChannelSourceCardsForTest(ctx context.Context, run, source string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = countChannelSourceCardsForTestTx(ctx, tx, run, source)
		return err
	})
	return count, err
}

func (s *DecisionSQLiteOwner) CountChannelSourceCardsForTest(ctx context.Context, run, source string) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = countChannelSourceCardsForTestTx(ctx, tx, run, source)
		return err
	})
	return count, err
}
