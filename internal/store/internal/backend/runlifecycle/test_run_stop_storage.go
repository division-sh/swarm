package runlifecycle

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type RunStopStorage struct {
	Status, Control string
	Pending         int
	Revisions       int64
}

func (s *RunLifecyclePostgresOwner) ReadRunStopStorageForTest(ctx context.Context, runID string) (RunStopStorage, error) {
	var out RunStopStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readRunStopStorageTxForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return RunStopStorage{}, err
	}
	return out, nil
}

func (s *RunLifecycleSQLiteOwner) ReadRunStopStorageForTest(ctx context.Context, runID string) (RunStopStorage, error) {
	var out RunStopStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readRunStopStorageTxForTest(ctx, tx, runID)
		return err
	})
	if err != nil {
		return RunStopStorage{}, err
	}
	return out, nil
}

func readRunStopStorageTxForTest(ctx context.Context, tx *sql.Tx, runID string) (RunStopStorage, error) {
	var out RunStopStorage
	state, err := ReadStopRunControlStorageTx(ctx, tx, runID)
	if err != nil {
		return RunStopStorage{}, err
	}
	out.Status, out.Control = state.Status, state.Control
	out.Pending, err = readStopPendingDeliveryCountTx(ctx, tx, runID)
	if err != nil {
		return RunStopStorage{}, err
	}
	out.Revisions, err = runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
	if err != nil {
		return RunStopStorage{}, err
	}
	return out, nil
}

func readStopPendingDeliveryCountTx(ctx context.Context, tx *sql.Tx, runID string) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','processing','retrying')`, runID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
