package runstate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/standingdisposition"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
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

// DispatchParked consumes lifecycle/control and the exact-current standing owner.
// The caller takes the existing run fence before using this to admit a new claim.
// Active remains broader: paused topology and already-owned settlement survive.
func DispatchParked(ctx context.Context, q RowQueryer, postgres bool, runID string) (bool, error) {
	ctx, finishDiagnostic := transactiontest.BeginGuardDiagnostic(ctx)
	defer finishDiagnostic()
	if strings.TrimSpace(runID) == "" {
		return false, nil
	}
	if q == nil {
		return false, errors.New("dispatch lifecycle query authority is required")
	}
	query := `SELECT r.status, COALESCE(c.control_status, ''), r.bundle_hash,
		EXISTS (SELECT 1 FROM source_artifacts a WHERE a.bundle_hash = r.bundle_hash)
		FROM runs r LEFT JOIN run_control_state c ON c.run_id = r.run_id WHERE r.run_id = ?`
	if postgres {
		query = strings.Replace(query, "r.run_id = ?", "r.run_id = $1::uuid", 1)
	}
	var status, control, bundleHash string
	var sourcePresent bool
	sqlStarted := time.Now()
	sqlErr := q.QueryRowContext(ctx, query, runID).Scan(&status, &control, &bundleHash, &sourcePresent)
	transactiontest.RecordGuardDiagnosticSQL(ctx, false, time.Since(sqlStarted))
	if err := sqlErr; err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return false, err
	}
	state, err := runtimerunlifecycle.ParseState(status)
	if err != nil {
		return false, err
	}
	if !state.Active() {
		// Retirement is fenced by the existing dispatch owner, not an
		// operator pause or a corrupt active-run disposition.
		return false, nil
	}
	source, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		return false, err
	}
	if !sourcePresent {
		return false, &runtimerunlifecycle.SourceArtifactUnavailableError{BundleHash: source.BundleHash(), Cause: "missing_source_artifact"}
	}
	standing, err := standingdisposition.ReadByRun(ctx, q, postgres, runID)
	if err != nil {
		return false, err
	}
	if standing.Kind == runtimerunlifecycle.StandingRestartInvalidCurrent {
		return false, fmt.Errorf("run %s has invalid standing authority: %s", runID, standing.RunControlGuidance())
	}
	switch status {
	case string(runtimerunlifecycle.StateRunning):
		if control != "" && control != string(runtimerunlifecycle.StateRunning) {
			return false, fmt.Errorf("run %s has contradictory running/control state %q", runID, control)
		}
		if standing.ExactCurrent() && !standing.Executable() {
			return false, fmt.Errorf("run %s has non-executable standing authority: %s", runID, standing.RunControlGuidance())
		}
		return false, nil
	case string(runtimerunlifecycle.StatePaused):
		if control != string(runtimerunlifecycle.StatePaused) {
			return false, fmt.Errorf("run %s has no admitted paused control", runID)
		}
		return true, nil
	default:
		return false, fmt.Errorf("run %s has invalid dispatch lifecycle %q", runID, status)
	}
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
