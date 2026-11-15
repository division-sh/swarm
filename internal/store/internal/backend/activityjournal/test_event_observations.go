package activityjournal

import (
	"context"
	"database/sql"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
)

type ProposedEffectRunExecutionStorage struct{ Requests, SuccessfulAttempts int }

func (s *ActivityPostgresOwner) ReadProposedEffectRunExecutionStorageForTest(ctx context.Context, runID string) (ProposedEffectRunExecutionStorage, error) {
	var out ProposedEffectRunExecutionStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='platform.activity_requested'),
			(SELECT COUNT(*) FROM activity_attempts WHERE CAST(run_id AS TEXT)=$1 AND status='succeeded')`, runID).
			Scan(&out.Requests, &out.SuccessfulAttempts)
	})
	if err != nil {
		return ProposedEffectRunExecutionStorage{}, err
	}
	return out, nil
}

func (s *ActivitySQLiteOwner) ReadProposedEffectRunExecutionStorageForTest(ctx context.Context, runID string) (ProposedEffectRunExecutionStorage, error) {
	var out ProposedEffectRunExecutionStorage
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT
			(SELECT COUNT(*) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='platform.activity_requested'),
			(SELECT COUNT(*) FROM activity_attempts WHERE CAST(run_id AS TEXT)=$1 AND status='succeeded')`, runID).
			Scan(&out.Requests, &out.SuccessfulAttempts)
	})
	if err != nil {
		return ProposedEffectRunExecutionStorage{}, err
	}
	return out, nil
}

func (s *ActivityPostgresOwner) CountRecordedDeadLetterStorageForTest(ctx context.Context, runID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM author_activity_occurrences aa
JOIN dead_letters dl ON aa.source_identity=dl.dead_letter_id::text
WHERE aa.run_id=$1::uuid AND aa.kind='dead_letter.recorded' AND aa.version=2
AND aa.transition='recorded' AND aa.source_owner='dead_letters'
AND aa.projection->>'subject_id'=dl.original_event_id::text`, runID).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *ActivitySQLiteOwner) CountRecordedDeadLetterStorageForTest(ctx context.Context, runID string) (int, error) {
	var out int
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM author_activity_occurrences aa
JOIN dead_letters dl ON aa.source_identity=CAST(dl.dead_letter_id AS TEXT)
WHERE aa.run_id=? AND aa.kind='dead_letter.recorded' AND aa.version=2
AND aa.transition='recorded' AND aa.source_owner='dead_letters'
AND json_extract(aa.projection,'$.subject_id')=CAST(dl.original_event_id AS TEXT)`, runID).Scan(&out)
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

func (s *ActivityPostgresOwner) ObserveChannelAuthorActivityForTest(ctx context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	var out runtimeauthoractivity.ListResult
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = authoractivityadapter.List(ctx, tx, authoractivityadapter.DialectPostgres, opts)
		return err
	})
	return out, err
}

func (s *ActivitySQLiteOwner) ObserveChannelAuthorActivityForTest(ctx context.Context, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	var out runtimeauthoractivity.ListResult
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = authoractivityadapter.List(ctx, tx, authoractivityadapter.DialectSQLite, opts)
		return err
	})
	return out, err
}
