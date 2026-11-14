package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *RunLifecyclePostgresOwner) ReadRuntimeLogRunSnapshotForTest(ctx context.Context, runID string) (runtimerunlifecycle.Snapshot, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return runtimerunlifecycle.Snapshot{}, fmt.Errorf("runtime log snapshot requires the original postgres owner")
	}
	var snapshot runtimerunlifecycle.Snapshot
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		snapshot, err = s.LoadSnapshotTx(ctx, tx, runID, false)
		return err
	})
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	return snapshot, nil
}

func (s *RunLifecycleSQLiteOwner) ReadRuntimeLogRunSnapshotForTest(ctx context.Context, runID string) (runtimerunlifecycle.Snapshot, error) {
	if s == nil || s.backend == nil || !s.backend.Valid() {
		return runtimerunlifecycle.Snapshot{}, fmt.Errorf("runtime log snapshot requires the original sqlite owner")
	}
	var snapshot runtimerunlifecycle.Snapshot
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		snapshot, err = s.LoadSnapshotTx(ctx, tx, runID)
		return err
	})
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	return snapshot, nil
}
