package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/tests/support/testutil/runlifecyclefixture"
)

// AttemptCorruptPostgresSnapshotForTest materializes hostile persisted state
// without valid lifecycle construction or source admission. Database constraint
// errors are returned unchanged for schema rejection tests.
func AttemptCorruptPostgresSnapshotForTest(
	ctx context.Context,
	db *sql.DB,
	snapshot runlifecyclefixture.CorruptSnapshot,
) error {
	snapshot, failure, endedAt, err := runlifecyclefixture.NormalizeCorruptSnapshot(snapshot)
	if err != nil {
		return err
	}
	return snapshotFaultTransaction(ctx, db, func(tx *sql.Tx) error {
		_, err = tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, origin_kind,
			trigger_event_id, trigger_event_type,
			forked_from_run_id, forked_from_point_kind, forked_from_revision, forked_from_event_id, continued_as_run_id,
			failure, started_at, ended_at, source_root_directory
		)
		VALUES (
			$1::uuid, $2, $3, $4,
			NULLIF($5, '')::uuid, NULLIF($6, ''),
			NULLIF($7, '')::uuid, NULLIF($8, ''), NULLIF($9, 0), NULLIF($10, '')::uuid, NULLIF($11, '')::uuid,
			NULLIF($12, '')::jsonb, $13, $14, $15
		)
	`, strings.TrimSpace(snapshot.RunID), strings.TrimSpace(snapshot.State),
			strings.TrimSpace(snapshot.BundleHash), strings.TrimSpace(snapshot.OriginKind),
			strings.TrimSpace(snapshot.TriggerEventID), strings.TrimSpace(snapshot.TriggerEventType),
			strings.TrimSpace(snapshot.ForkedFromRunID), strings.TrimSpace(snapshot.ForkedFromPointKind), snapshot.ForkedFromRevision, strings.TrimSpace(snapshot.ForkedFromEventID),
			strings.TrimSpace(snapshot.ContinuedAsRunID),
			failure, snapshot.StartedAt.UTC(), endedAt, snapshot.RootDirectory)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO run_event_counts (run_id, event_count) VALUES ($1::uuid, $2)`, snapshot.RunID, snapshot.EventCount)
		return err
	})
}

// AttemptCorruptSQLiteSnapshotForTest is the SQLite form of the same exact
// hostile snapshot fault, including attempts before valid source admission.
func AttemptCorruptSQLiteSnapshotForTest(
	ctx context.Context,
	db *sql.DB,
	snapshot runlifecyclefixture.CorruptSnapshot,
) error {
	snapshot, failure, endedAt, err := runlifecyclefixture.NormalizeCorruptSnapshot(snapshot)
	if err != nil {
		return err
	}
	return snapshotFaultTransaction(ctx, db, func(tx *sql.Tx) error {
		_, err = tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, origin_kind,
			trigger_event_id, trigger_event_type,
			forked_from_run_id, forked_from_point_kind, forked_from_revision, forked_from_event_id, continued_as_run_id,
			failure, started_at, ended_at, source_root_directory
		)
		VALUES (
			?, ?, ?, ?,
			NULLIF(?, ''), NULLIF(?, ''),
			NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0), NULLIF(?, ''), NULLIF(?, ''),
			NULLIF(?, ''), ?, ?, ?
		)
	`, strings.TrimSpace(snapshot.RunID), strings.TrimSpace(snapshot.State),
			strings.TrimSpace(snapshot.BundleHash), strings.TrimSpace(snapshot.OriginKind),
			strings.TrimSpace(snapshot.TriggerEventID), strings.TrimSpace(snapshot.TriggerEventType),
			strings.TrimSpace(snapshot.ForkedFromRunID), strings.TrimSpace(snapshot.ForkedFromPointKind), snapshot.ForkedFromRevision, strings.TrimSpace(snapshot.ForkedFromEventID),
			strings.TrimSpace(snapshot.ContinuedAsRunID),
			failure, snapshot.StartedAt.UTC(), endedAt, snapshot.RootDirectory)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO run_event_counts (run_id, event_count) VALUES (?, ?)`, snapshot.RunID, snapshot.EventCount)
		return err
	})
}

func snapshotFaultTransaction(ctx context.Context, db *sql.DB, write func(*sql.Tx) error) (err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if rollbackErr := tx.Rollback(); !errors.Is(rollbackErr, sql.ErrTxDone) {
			err = errors.Join(err, rollbackErr)
		}
	}()
	if err := write(tx); err != nil {
		return err
	}
	return tx.Commit()
}

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
