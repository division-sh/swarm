package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

func (s *RunForkPostgresOwner) InspectSelectedForkRecovery(ctx context.Context, entry runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error) {
	var inspection runfork.SelectedForkRecoveryInspection
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := s.LoadSnapshotTx(ctx, tx, entry.Binding.ForkRunID, false)
		if err != nil {
			return err
		}
		inspection, err = inspectSelectedRecoveryTx(ctx, tx, snapshot, entry, false)
		return err
	})
	return inspection, err
}

func (s *RunForkSQLiteOwner) InspectSelectedForkRecovery(ctx context.Context, entry runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error) {
	var inspection runfork.SelectedForkRecoveryInspection
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := s.LoadSnapshotTx(ctx, tx, entry.Binding.ForkRunID)
		if err != nil {
			return err
		}
		inspection, err = inspectSelectedRecoveryTx(ctx, tx, snapshot, entry, true)
		return err
	})
	return inspection, err
}

func inspectSelectedRecoveryTx(ctx context.Context, tx *sql.Tx, snapshot runlifecycle.Snapshot, entry runfork.SelectedForkRecoveryEntry, sqlite bool) (runfork.SelectedForkRecoveryInspection, error) {
	inspection := runfork.SelectedForkRecoveryInspection{RunState: snapshot.State}
	record, err := loadSelectedRecoveryRecordTx(ctx, tx, snapshot, entry, sqlite, false)
	if err != nil {
		return inspection, err
	}
	inspection.Plan, inspection.ExecutionState = record.SelectedForkRecoveryResult, record.state
	if !record.hasExecution {
		return inspection, nil
	}
	settled, complete, err := settledSelectedRecoveryTx(ctx, tx, snapshot, record, sqlite, false)
	if err != nil {
		return inspection, err
	}
	if complete {
		inspection.Plan = settled
		return inspection, nil
	}
	recorded, err := storestartup.PreparedProcessRecorded(ctx, tx, record.preparation.Coordinates, record.preparation.ProcessGeneration, sqlite)
	if err != nil {
		return inspection, err
	}
	if !recorded {
		return inspection, fmt.Errorf("selected recovery preparation has no matching recorded process lineage")
	}
	plan, err := planSelectedRecoveryTx(ctx, tx, snapshot, record, sqlite, false)
	if err != nil {
		return inspection, err
	}
	inspection.Plan = plan.SelectedForkRecoveryResult
	return inspection, ctx.Err()
}
