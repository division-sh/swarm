package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	sourceartifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
)

func (s *postgresSession) LoadStartupRunSources(ctx context.Context, loaded []agenttopology.SourceCoordinate) (map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource, error) {
	var sources map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource
	err := s.lease.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		sources, err = loadStartupRunSourcesTx(ctx, tx, loaded, true)
		return err
	})
	return sources, errors.Join(err, ctx.Err())
}

func (s *sqliteSession) LoadStartupRunSources(ctx context.Context, loaded []agenttopology.SourceCoordinate) (map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource, error) {
	var sources map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource
	err := s.owner.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		sources, err = loadStartupRunSourcesTx(ctx, tx, loaded, false)
		return err
	})
	return sources, errors.Join(err, ctx.Err())
}

func loadStartupRunSourcesTx(ctx context.Context, tx *sql.Tx, loaded []agenttopology.SourceCoordinate, postgres bool) (map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource, error) {
	rows, err := tx.QueryContext(ctx, `SELECT run_id, bundle_hash, source_root_directory FROM runs
		WHERE status IN ('`+string(runlifecycle.StateRunning)+`','`+string(runlifecycle.StatePaused)+`','`+string(runlifecycle.StateLive)+`') ORDER BY run_id`)
	if err != nil {
		return nil, err
	}
	type retainedRun struct {
		runID     string
		reference sourceartifact.RootAdmissionReference
	}
	var runs []retainedRun
	for _, source := range loaded {
		runs = append(runs, retainedRun{reference: source.RootReference()})
	}
	for rows.Next() {
		var runID string
		var reference sourceartifact.RootAdmissionReference
		if err := rows.Scan(&runID, &reference.BundleHash, &reference.RootDirectory); err != nil {
			return nil, errors.Join(err, rows.Close())
		}
		runs = append(runs, retainedRun{runID: runID, reference: reference})
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	admitted := map[sourceartifact.RootAdmissionReference]sourceartifact.RootedSource{}
	for _, run := range runs {
		runID, reference := run.runID, run.reference
		if _, found := admitted[reference]; found {
			continue
		}
		if err := reference.Validate(); err != nil {
			return nil, fmt.Errorf("retained run %s source: %w", runID, err)
		}
		fact, err := correlation.DecodeSourceArtifactFact(reference.BundleHash)
		if err != nil {
			return nil, err
		}
		dialect := authoractivity.DialectSQLite
		if postgres {
			dialect = authoractivity.DialectPostgres
		}
		if err := sourceadmission.RequireArtifact(ctx, tx, dialect, fact); err != nil {
			return nil, fmt.Errorf("retained run %s source: %w", runID, err)
		}
		source, err := sourceartifactstore.LoadRootedSourceTx(ctx, tx, reference, postgres)
		if err != nil {
			return nil, fmt.Errorf("admit retained run %s source: %w", runID, err)
		}
		admitted[reference] = source
	}
	return admitted, nil
}
