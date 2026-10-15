// Package counterprojection is the run-lifecycle owner's physical event-count
// projection. This leaf lets attempt bookkeeping consume it without importing
// the lifecycle orchestrator back into the mutation protocol.
package counterprojection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

type Delta struct {
	RunID  string
	Amount int64
}

type StorageConsistency struct {
	Stored, Actual int
}

// One physical statement retains the original projection-versus-event-row cut.
// This is evidence only: it neither repairs a counter nor treats zero as unknown.
func ReadStorageConsistencyForTestTx(ctx context.Context, tx *sql.Tx, runID string) (StorageConsistency, error) {
	var out StorageConsistency
	if err := tx.QueryRowContext(ctx, `SELECT r.event_count, (SELECT COUNT(*) FROM events e WHERE e.run_id=r.run_id) FROM runs r WHERE r.run_id=$1`, runID).Scan(&out.Stored, &out.Actual); err != nil {
		return StorageConsistency{}, err
	}
	return out, nil
}

func ApplyAll(ctx context.Context, tx *sql.Tx, dialect authoractivity.Dialect, deltas []Delta) error {
	for _, delta := range deltas {
		if err := Apply(ctx, tx, dialect, delta.RunID, delta.Amount); err != nil {
			return err
		}
	}
	return nil
}

func Apply(ctx context.Context, tx *sql.Tx, dialect authoractivity.Dialect, runID string, delta int64) error {
	if tx == nil || strings.TrimSpace(runID) == "" {
		return errors.New("run event-count projection requires transaction and run_id")
	}
	if delta == 0 {
		return nil
	}
	query := `UPDATE runs SET event_count = event_count + ? WHERE run_id = ? AND event_count + ? >= 0`
	args := []any{delta, runID, delta}
	switch dialect {
	case authoractivity.DialectPostgres:
		query = `UPDATE runs SET event_count = event_count + $1 WHERE run_id = $2::uuid AND event_count + $1 >= 0`
		args = args[:2]
	case authoractivity.DialectSQLite:
	default:
		return errors.New("run event-count projection requires a supported dialect")
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("apply run event-count delta: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		return errors.Join(fmt.Errorf("run %s event-count delta affected %d rows", runID, rows), err)
	}
	return nil
}
