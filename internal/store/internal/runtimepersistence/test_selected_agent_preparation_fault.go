package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
)

type SelectedAgentPreparationFault = runforkpersistence.SelectedAgentPreparationFault

const (
	SelectedAgentPreparationMissing = runforkpersistence.SelectedAgentPreparationMissing
	SelectedAgentPreparationCorrupt = runforkpersistence.SelectedAgentPreparationCorrupt
	SelectedAgentPreparationForeign = runforkpersistence.SelectedAgentPreparationForeign
)

type SelectedAgentPreparationFaultReceipt struct {
	owner   any
	receipt *runforkpersistence.SelectedAgentPreparationFaultReceipt
}

func ApplySelectedAgentPreparationFaultForTest(ctx context.Context, selected any, expected deliverylifecycle.Snapshot, fault SelectedAgentPreparationFault) (*SelectedAgentPreparationFaultReceipt, error) {
	var receipt *runforkpersistence.SelectedAgentPreparationFaultReceipt
	err := runReceiverConstructionObservationFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var run runlifecycle.Snapshot
		var err error
		owner, postgres := selected.(*PostgresStore)
		if postgres {
			run, err = owner.runLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, expected.RunID, false)
		} else {
			run, err = selected.(*SQLiteRuntimeStore).runLifecycleSQLiteOwner.LoadSnapshotTx(ctx, tx, expected.RunID)
		}
		if err != nil {
			return err
		}
		receipt, err = runforkpersistence.ApplySelectedAgentPreparationFaultTxForTest(ctx, tx, expected, run, fault, !postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &SelectedAgentPreparationFaultReceipt{owner: selected, receipt: receipt}, nil
}

func RestoreSelectedAgentPreparationFaultForTest(ctx context.Context, selected any, receipt *SelectedAgentPreparationFaultReceipt) error {
	if receipt == nil || receipt.owner != selected || receipt.receipt == nil {
		return fmt.Errorf("selected preparation restoration requires the original native owner")
	}
	return runReceiverConstructionObservationFault(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		return runforkpersistence.RestoreSelectedAgentPreparationFaultTxForTest(ctx, tx, receipt.receipt, !postgres)
	})
}
