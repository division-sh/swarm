// Package sourceadmission owns physical run-state/source admission. It is a
// RunLifecycle leaf so nested writers need not import the lifecycle orchestrator.
package sourceadmission

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type RowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func LoadPostgresTx(ctx context.Context, tx *sql.Tx, runID string, active, readOnly bool) (runtimecorrelation.SourceArtifactFact, error) {
	return loadTx(ctx, tx, runID, privateactivity.DialectPostgres, active, readOnly)
}

func LoadSQLiteTx(ctx context.Context, tx *sql.Tx, runID string, active, readOnly bool) (runtimecorrelation.SourceArtifactFact, error) {
	return loadTx(ctx, tx, runID, privateactivity.DialectSQLite, active, readOnly)
}

func RequirePostgresActiveNonlockingTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return requireActiveNonlockingTx(ctx, tx, runID, privateactivity.DialectPostgres)
}

func RequireSQLiteActiveNonlockingTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return requireActiveNonlockingTx(ctx, tx, runID, privateactivity.DialectSQLite)
}

// A mutable card consumer may use an already locked admission. On a miss,
// preserve its original nonlocking query; that result cannot mint admission.
func requireActiveNonlockingTx(ctx context.Context, tx *sql.Tx, runID string, dialect privateactivity.Dialect) error {
	if tx == nil {
		return errors.New("run source admission requires transaction")
	}
	fact, cached, err := mutationprotocol.CachedActiveRunSource(ctx, tx, runID)
	if err != nil {
		return err
	}
	if cached {
		return RequireArtifact(ctx, tx, dialect, fact)
	}
	_, err = load(ctx, tx, runID, dialect, true, false)
	return err
}

func loadTx(ctx context.Context, tx *sql.Tx, runID string, dialect privateactivity.Dialect, active, readOnly bool) (runtimecorrelation.SourceArtifactFact, error) {
	if tx == nil {
		return runtimecorrelation.SourceArtifactFact{}, errors.New("run source admission requires transaction")
	}
	if active && !readOnly {
		fact, cached, err := mutationprotocol.CachedActiveRunSource(ctx, tx, runID)
		if err != nil {
			return runtimecorrelation.SourceArtifactFact{}, err
		}
		if cached {
			// runs has no artifact FK: availability is never cached.
			if err := RequireArtifact(ctx, tx, dialect, fact); err != nil {
				return runtimecorrelation.SourceArtifactFact{}, err
			}
			return fact, nil
		}
	}
	fact, err := load(ctx, tx, runID, dialect, active, !readOnly)
	if err == nil && active && !readOnly {
		err = mutationprotocol.CacheActiveRunSource(ctx, tx, runID, fact)
	}
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	return fact, nil
}

// Query admission is always fresh and nonminting, even if q contains a sql.Tx.
func RequirePostgresActiveQuery(ctx context.Context, q RowQueryer, runID string) error {
	_, err := load(ctx, q, runID, privateactivity.DialectPostgres, true, false)
	return err
}

func RequireSQLiteActiveQuery(ctx context.Context, q RowQueryer, runID string) error {
	_, err := load(ctx, q, runID, privateactivity.DialectSQLite, true, false)
	return err
}

func load(ctx context.Context, q RowQueryer, runID string, dialect privateactivity.Dialect, active, lock bool) (runtimecorrelation.SourceArtifactFact, error) {
	if ctx == nil || q == nil {
		return runtimecorrelation.SourceArtifactFact{}, errors.New("run source admission requires context and query authority")
	}
	if err := ctx.Err(); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	runID = strings.TrimSpace(runID)
	query := `SELECT status, bundle_hash FROM runs WHERE run_id = ?`
	if dialect == privateactivity.DialectPostgres {
		query = `SELECT status, bundle_hash FROM runs WHERE run_id = $1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var status, bundleHash string
	if err := q.QueryRowContext(ctx, query, runID).Scan(&status, &bundleHash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("read run lifecycle source: %w", err)
	}
	state, err := runtimerunlifecycle.ParseState(status)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	if active && !state.Active() {
		return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: state}
	}
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("decode run lifecycle source: %w", err)
	}
	if err := RequireArtifact(ctx, q, dialect, fact); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	return fact, nil
}

func RequireArtifact(ctx context.Context, q RowQueryer, dialect privateactivity.Dialect, fact runtimecorrelation.SourceArtifactFact) error {
	query := `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = ?)`
	if dialect == privateactivity.DialectPostgres {
		query = `SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)`
	}
	var exists bool
	if err := q.QueryRowContext(ctx, query, fact.BundleHash()).Scan(&exists); err != nil {
		return fmt.Errorf("validate run source artifact: %w", err)
	}
	if !exists {
		return &runtimerunlifecycle.SourceArtifactUnavailableError{BundleHash: fact.BundleHash(), Cause: "missing_source_artifact"}
	}
	return nil
}
