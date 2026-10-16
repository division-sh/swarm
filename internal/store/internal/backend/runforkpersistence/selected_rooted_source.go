package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	artifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
)

// The child responsibility selects the root. Reading its lineage, binding,
// permanent operation and source in one snapshot precedes compiler admission.
func (s *RunForkPostgresOwner) LoadSelectedForkRootedSource(ctx context.Context, forkRunID string) (sourceartifact.RootedSource, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	var source sourceartifact.RootedSource
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := s.LoadSnapshotTx(ctx, tx, forkRunID, false)
		if err != nil {
			return err
		}
		source, err = loadSelectedForkRootedSourceTx(ctx, tx, snapshot, true)
		return err
	})
	if err = errors.Join(err, ctx.Err()); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return source, nil
}

func (s *RunForkSQLiteOwner) LoadSelectedForkRootedSource(ctx context.Context, forkRunID string) (sourceartifact.RootedSource, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	var source sourceartifact.RootedSource
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		snapshot, err := s.LoadSnapshotTx(ctx, tx, forkRunID)
		if err != nil {
			return err
		}
		source, err = loadSelectedForkRootedSourceTx(ctx, tx, snapshot, false)
		return err
	})
	if err = errors.Join(err, ctx.Err()); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return source, nil
}

func loadSelectedForkRootedSourceTx(ctx context.Context, tx *sql.Tx, snapshot runlifecycle.Snapshot, postgres bool) (sourceartifact.RootedSource, error) {
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, snapshot.RunID)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	origin, err := binding.ForkPoint.MaterializationRunOrigin(binding.SourceRunID)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	if !snapshot.Origin.Equal(origin) || snapshot.RootReference != binding.TargetRootReference {
		return sourceartifact.RootedSource{}, fmt.Errorf("selected source child differs from its exact binding")
	}
	// This is a read-only snapshot; recovery's mutation-side row lock is not
	// needed or permitted here. The permanent request still has to agree.
	if _, err := selectedForkOperationForRecoveryTx(ctx, tx, binding, snapshot.BundleHash, false); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return artifactstore.LoadRootedSourceTx(ctx, tx, snapshot.RootReference, postgres)
}
