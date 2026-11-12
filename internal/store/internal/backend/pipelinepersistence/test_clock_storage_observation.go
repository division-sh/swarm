package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type ClockStorageObservation struct {
	Rows                  int
	Status, ImmutableHash string
}

func (s *PipelinePostgresOwner) ReadPostgresObservationTimeForTest(ctx context.Context) (time.Time, error) {
	var out time.Time
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT NOW()`).Scan(&out)
	})
	if err != nil {
		return time.Time{}, err
	}
	return out.UTC(), nil
}

func (s *PipelinePostgresOwner) CountInstanceClockActivationsForTest(ctx context.Context) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE owner_kind='instance'`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelineSQLiteOwner) CountInstanceClockActivationsForTest(ctx context.Context) (int, error) {
	var count int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE owner_kind='instance'`).Scan(&count)
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *PipelinePostgresOwner) CorruptClockImmutableHashForTest(ctx context.Context, runID, activationID string) error {
	return s.backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return corruptClockImmutableHashForTest(ctx, tx, runID, activationID)
	})
}

func (s *PipelineSQLiteOwner) CorruptClockImmutableHashForTest(ctx context.Context, runID, activationID string) error {
	return s.backend.RunTransaction(ctx, "clock immutable hash fault", func(ctx context.Context, tx *sql.Tx) error {
		return corruptClockImmutableHashForTest(ctx, tx, runID, activationID)
	})
}

func corruptClockImmutableHashForTest(ctx context.Context, tx *sql.Tx, runID, activationID string) error {
	result, err := tx.ExecContext(ctx, `UPDATE timers SET immutable_hash='corrupt' WHERE run_id=$1 AND timer_id=$2 AND owner_kind='instance'`, runID, activationID)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return fmt.Errorf("clock fault requires one exact existing instance clock: rows=%d error=%v", count, err)
	}
	return nil
}

func (s *PipelinePostgresOwner) ReadClockStorageForTest(ctx context.Context, runID, activationID string) (ClockStorageObservation, error) {
	var out ClockStorageObservation
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return readClockStorageForTest(ctx, tx, runID, activationID, &out)
	})
	if err != nil {
		return ClockStorageObservation{}, err
	}
	return out, nil
}

func (s *PipelineSQLiteOwner) ReadClockStorageForTest(ctx context.Context, runID, activationID string) (ClockStorageObservation, error) {
	var out ClockStorageObservation
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return readClockStorageForTest(ctx, tx, runID, activationID, &out)
	})
	if err != nil {
		return ClockStorageObservation{}, err
	}
	return out, nil
}

func readClockStorageForTest(ctx context.Context, tx *sql.Tx, runID, activationID string, out *ClockStorageObservation) error {
	if err := tx.QueryRowContext(ctx, `SELECT status,immutable_hash FROM timers WHERE run_id=$1 AND timer_id=$2`, runID, activationID).Scan(&out.Status, &out.ImmutableHash); err != nil {
		return err
	}
	return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers`).Scan(&out.Rows)
}
