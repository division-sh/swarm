package runstate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	"github.com/division-sh/swarm/internal/store/internal/backend/standingdisposition"
)

const ActiveStateSQLValues = "'" +
	string(runtimerunlifecycle.StateRunning) + "', '" +
	string(runtimerunlifecycle.StatePaused) + "'"

func RequirePostgresActiveTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return errors.New("PostgreSQL run lifecycle transaction is required")
	}
	_, err := sourceadmission.LoadPostgresTx(ctx, tx, runID, true, false)
	return err
}

type RowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func RequirePostgresActiveNonlockingTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return sourceadmission.RequirePostgresActiveNonlockingTx(ctx, tx, runID)
}

func RequireSQLiteActiveNonlockingTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return sourceadmission.RequireSQLiteActiveNonlockingTx(ctx, tx, runID)
}

// DispatchParked consumes lifecycle/control and the exact-current standing owner.
// The caller takes the existing run fence before using this to admit a new claim.
// Active remains broader: paused topology and already-owned settlement survive.
func DispatchParked(ctx context.Context, q RowQueryer, postgres bool, runID string) (bool, error) {
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
	if err := q.QueryRowContext(ctx, query, runID).Scan(&status, &control, &bundleHash, &sourcePresent); err != nil {
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
	return sourceadmission.RequirePostgresActiveQuery(ctx, queryer, runID)
}

func RequireSQLiteActiveTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil {
		return errors.New("SQLite run lifecycle transaction is required")
	}
	_, err := sourceadmission.LoadSQLiteTx(ctx, tx, runID, true, false)
	return err
}

func RequireSQLiteActiveQuery(ctx context.Context, queryer RowQueryer, runID string) error {
	if queryer == nil {
		return errors.New("SQLite run lifecycle query authority is required")
	}
	return sourceadmission.RequireSQLiteActiveQuery(ctx, queryer, runID)
}
