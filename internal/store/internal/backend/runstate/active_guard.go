package runstate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

const ActiveStateSQLValues = "'" +
	string(runtimerunlifecycle.StateRunning) + "', '" +
	string(runtimerunlifecycle.StatePaused) + "'"

func RequirePostgresActiveTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return errors.New("PostgreSQL run lifecycle transaction is required")
	}
	_, err := requireActiveSource(ctx, tx.QueryRowContext, runID, true, true)
	return err
}

func RequirePostgresActiveSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	if tx == nil {
		return runtimecorrelation.SourceArtifactFact{}, errors.New("PostgreSQL run lifecycle transaction is required")
	}
	return requireActiveSource(ctx, tx.QueryRowContext, runID, true, true)
}

type RowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func RequirePostgresActiveQuery(ctx context.Context, queryer RowQueryer, runID string) error {
	if queryer == nil {
		return errors.New("PostgreSQL run lifecycle query authority is required")
	}
	_, err := requireActiveSource(ctx, queryer.QueryRowContext, runID, true, false)
	return err
}

func RequireSQLiteActiveTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return errors.New("SQLite run lifecycle transaction is required")
	}
	_, err := requireActiveSource(ctx, tx.QueryRowContext, runID, false, false)
	return err
}

func RequireSQLiteActiveSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	if tx == nil {
		return runtimecorrelation.SourceArtifactFact{}, errors.New("SQLite run lifecycle transaction is required")
	}
	return requireActiveSource(ctx, tx.QueryRowContext, runID, false, false)
}

func RequireSQLiteActiveQuery(ctx context.Context, queryer RowQueryer, runID string) error {
	if queryer == nil {
		return errors.New("SQLite run lifecycle query authority is required")
	}
	_, err := requireActiveSource(ctx, queryer.QueryRowContext, runID, false, false)
	return err
}

func requireActiveSource(ctx context.Context, queryRow func(context.Context, string, ...any) *sql.Row, runID string, postgres, lock bool) (runtimecorrelation.SourceArtifactFact, error) {
	runID = strings.TrimSpace(runID)
	query := `SELECT status, bundle_hash FROM runs WHERE run_id = ?`
	if postgres {
		query = `SELECT status, bundle_hash FROM runs WHERE run_id = $1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var state, bundleHash string
	if err := queryRow(ctx, query, runID).Scan(&state, &bundleHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("read active run lifecycle: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	if !parsed.Active() {
		return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	source, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("decode active run lifecycle source: %w", err)
	}
	existsQuery := `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = ?)`
	if postgres {
		existsQuery = `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)`
	}
	var exists bool
	if err := queryRow(ctx, existsQuery, source.BundleHash()).Scan(&exists); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("validate active run lifecycle source: %w", err)
	}
	if !exists {
		return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.SourceArtifactUnavailableError{
			BundleHash: source.BundleHash(),
			Cause:      "missing_source_artifact",
		}
	}
	return source, nil
}
