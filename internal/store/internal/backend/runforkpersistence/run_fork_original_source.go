package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *RunForkPostgresOwner) LoadRunForkSourceRunID(ctx context.Context, forkRunID string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres fork owner is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	snapshot, err := s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, forkRunID, false)
	if err != nil {
		return "", err
	}
	return originalRunFromForkSnapshot(snapshot)
}

func (s *RunForkSQLiteOwner) LoadRunForkSourceRunID(ctx context.Context, forkRunID string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite fork owner is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return "", err
	}
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	snapshot, err := s.RunLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, forkRunID)
	if err != nil {
		return "", err
	}
	return originalRunFromForkSnapshot(snapshot)
}

func originalRunFromForkSnapshot(snapshot runlifecycle.Snapshot) (string, error) {
	if err := snapshot.Validate(); err != nil {
		return "", err
	}
	if snapshot.Origin.Kind() != runlifecycle.OriginForkMaterialization {
		return "", fmt.Errorf("original fork source requires fork-materialization origin")
	}
	return snapshot.Origin.SourceRunID(), nil
}
