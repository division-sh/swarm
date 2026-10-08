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
