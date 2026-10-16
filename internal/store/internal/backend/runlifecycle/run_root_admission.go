package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	sourceartifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
)

func loadRunRootReference(ctx context.Context, queryer rowQueryer, runID string, postgres bool) (sourceartifact.RootAdmissionReference, error) {
	query := `SELECT bundle_hash, source_root_directory FROM runs WHERE run_id = ?`
	if postgres {
		query = `SELECT bundle_hash, source_root_directory FROM runs WHERE run_id = $1::uuid`
	}
	var reference sourceartifact.RootAdmissionReference
	err := queryer.QueryRowContext(ctx, query, runID).Scan(&reference.BundleHash, &reference.RootDirectory)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceartifact.RootAdmissionReference{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return sourceartifact.RootAdmissionReference{}, fmt.Errorf("load exact run root reference: %w", err)
	}
	if err := reference.Validate(); err != nil {
		return sourceartifact.RootAdmissionReference{}, fmt.Errorf("invalid run root reference: %w", err)
	}
	return reference, nil
}

func loadRunRootedSourceTx(ctx context.Context, tx *sql.Tx, runID string, postgres bool) (sourceartifact.RootedSource, error) {
	reference, err := loadRunRootReference(ctx, tx, runID, postgres)
	if err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return sourceartifactstore.LoadRootedSourceTx(ctx, tx, reference, postgres)
}

// Responsibility and content are read in the same native snapshot. No unbound
// discovery or ambient filesystem path participates in run reconstruction.
func (s *RunLifecyclePostgresOwner) LoadRunRootedSource(ctx context.Context, runID string) (sourceartifact.RootedSource, error) {
	var source sourceartifact.RootedSource
	err := s.runRead(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		source, err = loadRunRootedSourceTx(ctx, tx, runID, true)
		return err
	})
	if err = errors.Join(err, ctx.Err()); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return source, nil
}

func (s *RunLifecycleSQLiteOwner) LoadRunRootedSource(ctx context.Context, runID string) (sourceartifact.RootedSource, error) {
	var source sourceartifact.RootedSource
	err := s.runRead(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		source, err = loadRunRootedSourceTx(ctx, tx, runID, false)
		return err
	})
	if err = errors.Join(err, ctx.Err()); err != nil {
		return sourceartifact.RootedSource{}, err
	}
	return source, nil
}
