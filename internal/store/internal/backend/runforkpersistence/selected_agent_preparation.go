package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

// ReadSelectedAgentPreparation observes the exact immutable preparation; it
// neither acquires a process nor proves a selected execution current.
func (s *RunForkPostgresOwner) ReadSelectedAgentPreparation(ctx context.Context, expected deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error) {
	var actor runfork.SelectedForkPreparedActor
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		actual, err := delivery.ReadExactClaimedAgentSnapshotTx(ctx, tx, delivery.DialectPostgres, expected)
		if err != nil {
			return err
		}
		snapshot, err := s.LoadSnapshotTx(ctx, tx, actual.RunID, false)
		if err != nil {
			return err
		}
		actor, err = readSelectedAgentPreparationTx(ctx, tx, actual, snapshot, false)
		return err
	})
	if err != nil {
		return runfork.SelectedForkPreparedActor{}, err
	}
	return actor, nil
}

func (s *RunForkSQLiteOwner) ReadSelectedAgentPreparation(ctx context.Context, expected deliverylifecycle.Snapshot) (runfork.SelectedForkPreparedActor, error) {
	var actor runfork.SelectedForkPreparedActor
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		actual, err := delivery.ReadExactClaimedAgentSnapshotTx(ctx, tx, delivery.DialectSQLite, expected)
		if err != nil {
			return err
		}
		snapshot, err := s.LoadSnapshotTx(ctx, tx, actual.RunID)
		if err != nil {
			return err
		}
		actor, err = readSelectedAgentPreparationTx(ctx, tx, actual, snapshot, true)
		return err
	})
	if err != nil {
		return runfork.SelectedForkPreparedActor{}, err
	}
	return actor, nil
}

func readSelectedAgentPreparationTx(ctx context.Context, tx *sql.Tx, deliverySnapshot deliverylifecycle.Snapshot, run runlifecycle.Snapshot, sqlite bool) (runfork.SelectedForkPreparedActor, error) {
	var absent runfork.SelectedForkPreparedActor
	authority := deliverySnapshot.Authority
	if authority.Kind() != deliverylifecycle.ExecutionAuthoritySelectedContractFork || authority.ForkRunID() != run.RunID ||
		deliverySnapshot.RunID != run.RunID || authority.SourceArtifact().BundleHash() != run.BundleHash {
		return absent, fmt.Errorf("selected agent preparation requires exact fork/source authority")
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, run.RunID)
	if err != nil {
		return absent, err
	}
	record, err := loadSelectedRecoveryExecutionTx(ctx, tx, run, runfork.SelectedForkRecoveryEntry{Binding: binding, BundleHash: run.BundleHash}, authority.ExecutionID(), sqlite, false)
	if err != nil {
		return absent, err
	}
	if !record.hasExecution || record.ExecutionID != authority.ExecutionID() || record.executionGeneration != authority.Generation() {
		return absent, fmt.Errorf("selected agent preparation differs from exact execution generation")
	}
	plan, err := deliverySnapshot.Route.AgentIdentity.Plan()
	if err != nil {
		return absent, err
	}
	var prepared runfork.SelectedForkPreparedActor
	for _, actor := range record.preparation.Actors {
		if actor.Plan == plan {
			prepared = actor
			break
		}
	}
	if prepared.Plan != plan {
		return absent, fmt.Errorf("selected agent is absent from immutable preparation")
	}
	for _, declared := range record.declarations.Agents {
		if declared.Identity == plan && declared.ConfigRevision == prepared.ConfigurationRevision && declared.Source.BundleHash == run.BundleHash {
			return prepared, nil
		}
	}
	return absent, fmt.Errorf("selected prepared actor differs from immutable declaration")
}
