package runlifecycle

import (
	"context"
	"database/sql"
	"strings"

	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
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
	_, err = db.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, origin_kind,
			trigger_event_id, trigger_event_type, origin_service_id, origin_generation,
			forked_from_run_id, forked_from_point_kind, forked_from_revision, forked_from_event_id, continued_as_run_id,
			event_count, failure, started_at, ended_at
		)
		VALUES (
			$1::uuid, $2, $3, $4,
			NULLIF($5, '')::uuid, NULLIF($6, ''), NULLIF($7, '')::uuid, NULLIF($8, 0),
			NULLIF($9, '')::uuid, NULLIF($10, ''), NULLIF($11, 0), NULLIF($12, '')::uuid, NULLIF($13, '')::uuid,
			$14, NULLIF($15, '')::jsonb, $16, $17
		)
	`, strings.TrimSpace(snapshot.RunID), strings.TrimSpace(snapshot.State),
		strings.TrimSpace(snapshot.BundleHash), strings.TrimSpace(snapshot.OriginKind),
		strings.TrimSpace(snapshot.TriggerEventID), strings.TrimSpace(snapshot.TriggerEventType),
		strings.TrimSpace(snapshot.OriginServiceID), snapshot.OriginGeneration,
		strings.TrimSpace(snapshot.ForkedFromRunID), strings.TrimSpace(snapshot.ForkedFromPointKind), snapshot.ForkedFromRevision, strings.TrimSpace(snapshot.ForkedFromEventID),
		strings.TrimSpace(snapshot.ContinuedAsRunID),
		snapshot.EventCount, failure, snapshot.StartedAt.UTC(), endedAt)
	return err
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
	_, err = db.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, origin_kind,
			trigger_event_id, trigger_event_type, origin_service_id, origin_generation,
			forked_from_run_id, forked_from_point_kind, forked_from_revision, forked_from_event_id, continued_as_run_id,
			event_count, failure, started_at, ended_at
		)
		VALUES (
			?, ?, ?, ?,
			NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0),
			NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0), NULLIF(?, ''), NULLIF(?, ''),
			?, NULLIF(?, ''), ?, ?
		)
	`, strings.TrimSpace(snapshot.RunID), strings.TrimSpace(snapshot.State),
		strings.TrimSpace(snapshot.BundleHash), strings.TrimSpace(snapshot.OriginKind),
		strings.TrimSpace(snapshot.TriggerEventID), strings.TrimSpace(snapshot.TriggerEventType),
		strings.TrimSpace(snapshot.OriginServiceID), snapshot.OriginGeneration,
		strings.TrimSpace(snapshot.ForkedFromRunID), strings.TrimSpace(snapshot.ForkedFromPointKind), snapshot.ForkedFromRevision, strings.TrimSpace(snapshot.ForkedFromEventID),
		strings.TrimSpace(snapshot.ContinuedAsRunID),
		snapshot.EventCount, failure, snapshot.StartedAt.UTC(), endedAt)
	return err
}
