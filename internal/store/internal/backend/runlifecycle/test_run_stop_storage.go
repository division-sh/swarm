package runlifecycle

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
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
		out, err = readRunStopStorageTx(ctx, tx, runID)
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
		out, err = readRunStopStorageTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return RunStopStorage{}, err
	}
	return out, nil
}

func readRunStopStorageTx(ctx context.Context, tx *sql.Tx, runID string) (RunStopStorage, error) {
	var out RunStopStorage
	state, err := ReadStopRunControlStorageTx(ctx, tx, runID)
	if err != nil {
		return RunStopStorage{}, err
	}
	out.Status, out.Control = state.Status, state.Control
	out.Pending, err = delivery.ReadStopPendingDeliveryCountTx(ctx, tx, runID)
	if err != nil {
		return RunStopStorage{}, err
	}
	out.Revisions, err = runforkrevision.CountActivityJournalRevisionsForTest(ctx, tx, runID)
	if err != nil {
		return RunStopStorage{}, err
	}
	return out, nil
}
