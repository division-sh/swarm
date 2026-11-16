package runlifecycle

import (
	"context"
	"database/sql"
	"fmt"

	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *RunLifecyclePostgresOwner) ReadStandaloneRunStorageForTest(ctx context.Context, eventID string) (StandaloneRunStorage, error) {
	var out StandaloneRunStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COALESCE(CAST(r.run_id AS TEXT),''), COALESCE(r.status,''), COALESCE(r.trigger_event_type,'')
FROM events e INNER JOIN runs r ON r.run_id=e.run_id WHERE e.event_id=$1`, eventID).Scan(&out.RunID, &out.Status, &out.TriggerEventType)
	})
	if err != nil {
		return StandaloneRunStorage{}, err
	}
	return out, nil
}

func (s *RunLifecycleSQLiteOwner) ReadStandaloneRunStorageForTest(ctx context.Context, eventID string) (StandaloneRunStorage, error) {
	var out StandaloneRunStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COALESCE(CAST(r.run_id AS TEXT),''), COALESCE(r.status,''), COALESCE(r.trigger_event_type,'')
FROM events e INNER JOIN runs r ON r.run_id=e.run_id WHERE e.event_id=$1`, eventID).Scan(&out.RunID, &out.Status, &out.TriggerEventType)
	})
	if err != nil {
		return StandaloneRunStorage{}, err
	}
	return out, nil
}

func (s *RunLifecyclePostgresOwner) ReadStandaloneCompletionCandidateForTest(ctx context.Context, eventID string) (runtimerunlifecycle.Candidate, error) {
	var out runtimerunlifecycle.Candidate
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT CAST(r.run_id AS TEXT),r.bundle_hash,COALESCE(c.revision,0),`+completionEvidenceDue+`
FROM runs r `+completionEvidenceJoin+` JOIN events e ON e.run_id=r.run_id WHERE e.event_id=$1`, eventID).
			Scan(&out.RunID, &out.BundleHash, &out.Revision, &out.DueAt)
	})
	if err != nil {
		return runtimerunlifecycle.Candidate{}, err
	}
	return out, nil
}

func (s *RunLifecycleSQLiteOwner) ReadStandaloneCompletionCandidateForTest(ctx context.Context, eventID string) (runtimerunlifecycle.Candidate, error) {
	var out runtimerunlifecycle.Candidate
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var due any
		if err := tx.QueryRowContext(ctx, `SELECT CAST(r.run_id AS TEXT),r.bundle_hash,COALESCE(c.revision,0),`+completionEvidenceDue+`
FROM runs r `+completionEvidenceJoin+` JOIN events e ON e.run_id=r.run_id WHERE e.event_id=$1`, eventID).
			Scan(&out.RunID, &out.BundleHash, &out.Revision, &due); err != nil {
			return err
		}
		parsed, found, err := sqliteTimeValue(due)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("standalone candidate has no stored due time")
		}
		out.DueAt = parsed
		return nil
	})
	if err != nil {
		return runtimerunlifecycle.Candidate{}, err
	}
	return out, nil
}
