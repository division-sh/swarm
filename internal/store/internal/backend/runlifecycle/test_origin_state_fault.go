package runlifecycle

import (
	"context"
	"strings"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *RunLifecyclePostgresOwner) CorruptOriginForTest(ctx context.Context, runID string, origin runtimerunlifecycle.RunOrigin) error {
	query, args, err := corruptOriginStatementForTest(true, runID, origin)
	if err != nil {
		return err
	}
	_, err = s.backend.ExecContext(ctx, query, args...)
	return err
}

func (s *RunLifecycleSQLiteOwner) CorruptOriginForTest(ctx context.Context, runID string, origin runtimerunlifecycle.RunOrigin) error {
	query, args, err := corruptOriginStatementForTest(false, runID, origin)
	if err != nil {
		return err
	}
	_, err = s.backend.ExecContext(ctx, query, args...)
	return err
}

func corruptOriginStatementForTest(
	postgres bool,
	runID string,
	origin runtimerunlifecycle.RunOrigin,
) (string, []any, error) {
	if err := origin.Validate(); err != nil {
		return "", nil, err
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
	return query, args, nil
}

func (s *RunLifecycleSQLiteOwner) CorruptStateForTest(ctx context.Context, runID, state string, endedAt time.Time) error {
	var end any
	if !endedAt.IsZero() {
		end = endedAt.UTC()
	}
	_, err := s.backend.ExecContext(ctx, `
  UPDATE runs
  SET status = ?, ended_at = ?
  WHERE run_id = ?
 `, strings.TrimSpace(state), end, strings.TrimSpace(runID))
	return err
}
