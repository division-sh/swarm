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

func RequirePostgresActiveSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.BundleSourceFact, error) {
	if tx == nil {
		return runtimecorrelation.BundleSourceFact{}, errors.New("PostgreSQL run lifecycle transaction is required")
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

func RequireSQLiteActiveSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.BundleSourceFact, error) {
	if tx == nil {
		return runtimecorrelation.BundleSourceFact{}, errors.New("SQLite run lifecycle transaction is required")
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

func requireActiveSource(ctx context.Context, queryRow func(context.Context, string, ...any) *sql.Row, runID string, postgres, lock bool) (runtimecorrelation.BundleSourceFact, error) {
	runID = strings.TrimSpace(runID)
	query := `SELECT status, bundle_hash, bundle_source FROM runs WHERE run_id = ?`
	if postgres {
		query = `SELECT status, bundle_hash, bundle_source FROM runs WHERE run_id = $1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var state, bundleHash, bundleSource string
	if err := queryRow(ctx, query, runID).Scan(&state, &bundleHash, &bundleSource); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("read active run lifecycle: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	if !parsed.Active() {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	source, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("decode active run lifecycle source: %w", err)
	}
	if !source.IsPersisted() {
		return source, nil
	}
	existsQuery := `SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = ?)`
	if postgres {
		existsQuery = `SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)`
	}
	var exists bool
	if err := queryRow(ctx, existsQuery, source.BundleHash()).Scan(&exists); err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("validate active run lifecycle source: %w", err)
	}
	if !exists {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.PersistedBundleUnavailableError{
			BundleHash: source.BundleHash(), BundleSource: runtimerunlifecycle.BundleSourcePersisted,
			Cause: "persisted_missing_bundle_row",
		}
	}
	return source, nil
}
