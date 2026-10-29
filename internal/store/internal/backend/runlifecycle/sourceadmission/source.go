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
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/sourceartifact"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	sourceartifactstore "github.com/division-sh/swarm/internal/store/internal/sourceartifact"
)

type RowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type ExecutionQuery = interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
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
	fact, root, cached, err := mutationprotocol.CachedActiveRunSource(ctx, tx, runID)
	if err != nil {
		return err
	}
	if cached {
		return requireRetainedPair(ctx, tx, dialect, fact, root)
	}
	_, _, err = load(ctx, tx, runID, dialect, true, false)
	return err
}

func loadTx(ctx context.Context, tx *sql.Tx, runID string, dialect privateactivity.Dialect, active, readOnly bool) (runtimecorrelation.SourceArtifactFact, error) {
	if tx == nil {
		return runtimecorrelation.SourceArtifactFact{}, errors.New("run source admission requires transaction")
	}
	if active && !readOnly {
		fact, root, cached, err := mutationprotocol.CachedActiveRunSource(ctx, tx, runID)
		if err != nil {
			return runtimecorrelation.SourceArtifactFact{}, err
		}
		if cached {
			// runs has no artifact FK: availability is never cached.
			if err := requireRetainedPair(ctx, tx, dialect, fact, root); err != nil {
				return runtimecorrelation.SourceArtifactFact{}, err
			}
			return fact, nil
		}
	}
	fact, root, err := load(ctx, tx, runID, dialect, active, !readOnly)
	if err == nil && active && !readOnly {
		err = mutationprotocol.CacheActiveRunSource(ctx, tx, runID, fact, root)
	}
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	return fact, nil
}

// Query admission is always fresh and nonminting, even if q contains a sql.Tx.
func RequirePostgresActiveQuery(ctx context.Context, q RowQueryer, runID string) error {
	_, err := ReadPostgresActiveSourceQuery(ctx, q, runID)
	return err
}

func RequireSQLiteActiveQuery(ctx context.Context, q RowQueryer, runID string) error {
	_, err := ReadSQLiteActiveSourceQuery(ctx, q, runID)
	return err
}

func ReadPostgresActiveSourceQuery(ctx context.Context, q RowQueryer, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	fact, _, err := load(ctx, q, runID, privateactivity.DialectPostgres, true, false)
	return fact, err
}

func ReadSQLiteActiveSourceQuery(ctx context.Context, q RowQueryer, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	fact, _, err := load(ctx, q, runID, privateactivity.DialectSQLite, true, false)
	return fact, err
}

func ReadActiveObservationSourceQuery(ctx context.Context, q RowQueryer, runID string, postgres bool) (runtimecorrelation.SourceArtifactFact, error) {
	dialect := privateactivity.DialectSQLite
	if postgres {
		dialect = privateactivity.DialectPostgres
	}
	fact, _, err := loadMode(ctx, q, runID, dialect, true, false, true)
	return fact, err
}

func load(ctx context.Context, q RowQueryer, runID string, dialect privateactivity.Dialect, active, lock bool) (runtimecorrelation.SourceArtifactFact, sourceartifact.RootAdmissionReference, error) {
	return loadMode(ctx, q, runID, dialect, active, lock, false)
}

func loadMode(ctx context.Context, q RowQueryer, runID string, dialect privateactivity.Dialect, active, lock, observation bool) (runtimecorrelation.SourceArtifactFact, sourceartifact.RootAdmissionReference, error) {
	if ctx == nil || q == nil {
		return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, errors.New("run source admission requires context and query authority")
	}
	if err := ctx.Err(); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, err
	}
	runID = strings.TrimSpace(runID)
	query := `SELECT status, bundle_hash, source_root_directory FROM runs WHERE run_id = ?`
	exclusive := false
	if lock {
		tx, ok := q.(*sql.Tx)
		if !ok {
			return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, errors.New("mutable run admission requires transaction")
		}
		var err error
		exclusive, err = mutationprotocol.RunRowLock(ctx, tx, runID, false)
		if err != nil {
			return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, err
		}
	}
	if dialect == privateactivity.DialectPostgres {
		query = `SELECT status, bundle_hash, source_root_directory FROM runs WHERE run_id = $1::uuid`
		if lock {
			if exclusive {
				query += ` FOR UPDATE`
			} else {
				query += ` FOR SHARE`
			}
		}
	}
	var status string
	var root sourceartifact.RootAdmissionReference
	if err := q.QueryRowContext(ctx, query, runID).Scan(&status, &root.BundleHash, &root.RootDirectory); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, fmt.Errorf("read run lifecycle source: %w", err)
	}
	state, err := runtimerunlifecycle.ParseState(status)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, err
	}
	if active && !state.Active() {
		return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: state}
	}
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(root.BundleHash)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, fmt.Errorf("decode run lifecycle source: %w", err)
	}
	if observation && runtimecorrelation.CheckRootAdmissionContext(ctx, root) != nil {
		plan, present, planErr := generationauthority.ReadSourceSetQuery(ctx, q, dialect == privateactivity.DialectSQLite)
		if planErr != nil || !present {
			return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, errors.Join(planErr, errors.New("retained observation requires the current source set"))
		}
		ctx, err = startupownership.RunSourceObservationContext(ctx, runID, root, plan)
		if err != nil {
			return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, err
		}
	}
	if err := runtimecorrelation.CheckRootAdmissionContext(ctx, root); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, err
	}
	if err := requireRetainedPair(ctx, q, dialect, fact, root); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, sourceartifact.RootAdmissionReference{}, err
	}
	return fact, root, nil
}

func requireRetainedPair(ctx context.Context, q RowQueryer, dialect privateactivity.Dialect, fact runtimecorrelation.SourceArtifactFact, root sourceartifact.RootAdmissionReference) error {
	if err := RequireArtifact(ctx, q, dialect, fact); err != nil {
		return err
	}
	return sourceartifactstore.RequireRootReference(ctx, q, root, dialect == privateactivity.DialectPostgres)
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
