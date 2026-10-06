package llmpersistence

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
)

func (s *LLMPostgresOwner) ObserveLiveWriterCountForTest(ctx context.Context, runID string) (int, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return 0, err
	}
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeLiveWriterCount(ctx, tx, runID, &count) })
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *LLMSQLiteOwner) ObserveLiveWriterCountForTest(ctx context.Context, runID string) (int, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return 0, err
	}
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error { return observeLiveWriterCount(ctx, tx, runID, &count) })
	if err != nil {
		return 0, err
	}
	return count, nil
}

func observeLiveWriterCount(ctx context.Context, tx *sql.Tx, runID string, count *int) error {
	if _, err := uuid.Parse(runID); err != nil {
		return err
	}
	return tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT agent_id) FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE`, runID).Scan(count)
}
