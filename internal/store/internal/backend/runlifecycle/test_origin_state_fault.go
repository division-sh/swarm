package runlifecycle

import (
	"context"
	"database/sql"
	"strings"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func CorruptPostgresOriginForTest(ctx context.Context, db *sql.DB, runID string, origin runtimerunlifecycle.RunOrigin) error {
	return corruptOriginForTest(ctx, db, true, runID, origin)
}

func CorruptSQLiteOriginForTest(ctx context.Context, db *sql.DB, runID string, origin runtimerunlifecycle.RunOrigin) error {
	return corruptOriginForTest(ctx, db, false, runID, origin)
}

func corruptOriginForTest(
	ctx context.Context,
	db *sql.DB,
	postgres bool,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) error {
	if err := origin.Validate(); err != nil {
		return err
	}
	query := `
		UPDATE runs
		SET origin_kind = ?,
		    trigger_event_id = NULLIF(?, ''),
		    trigger_event_type = NULLIF(?, ''),
		    forked_from_run_id = NULLIF(?, ''),
		    forked_from_point_kind = NULLIF(?, ''),
		    forked_from_revision = NULLIF(?, 0),
		    forked_from_event_id = NULLIF(?, '')
		WHERE run_id = ?
	`
	args := []any{
		origin.Kind(), origin.EventID(), origin.EventType(),
		origin.SourceRunID(), origin.ForkPointKind(), origin.ForkRevision(), origin.SourceEventID(), strings.TrimSpace(runID),
	}
	if postgres {
		query = `
			UPDATE runs
			SET origin_kind = $2,
			    trigger_event_id = NULLIF($3, '')::uuid,
			    trigger_event_type = NULLIF($4, ''),
			    forked_from_run_id = NULLIF($5, '')::uuid,
			    forked_from_point_kind = NULLIF($6, ''),
			    forked_from_revision = NULLIF($7, 0),
			    forked_from_event_id = NULLIF($8, '')::uuid
			WHERE run_id = $1::uuid
		`
		args = []any{
			strings.TrimSpace(runID), origin.Kind(), origin.EventID(), origin.EventType(),
			origin.SourceRunID(), origin.ForkPointKind(), origin.ForkRevision(), origin.SourceEventID(),
		}
	}
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

func CorruptSQLiteStateForTest(ctx context.Context, db *sql.DB, runID, state string, endedAt time.Time) error {
	var end any
	if !endedAt.IsZero() {
		end = endedAt.UTC()
	}
	_, err := db.ExecContext(ctx, `
  UPDATE runs
  SET status = ?, ended_at = ?
  WHERE run_id = ?
 `, strings.TrimSpace(state), end, strings.TrimSpace(runID))
	return err
}
