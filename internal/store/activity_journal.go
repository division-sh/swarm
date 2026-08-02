package store

import (
	"context"
	"database/sql"
	"fmt"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	activityjournaladapter "github.com/division-sh/swarm/internal/store/internal/activityjournal"
)

func (s *PostgresStore) StartActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, inserted bool, err error) {
	err = s.runAuthorActivityMutation(ctx, "start activity attempt", func(txctx context.Context, tx *sql.Tx) error {
		out, inserted, err = activityjournaladapter.Start(txctx, tx, activityjournaladapter.DialectPostgres, s, record)
		return err
	})
	return
}

func (s *SQLiteRuntimeStore) StartActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, inserted bool, err error) {
	err = s.runAuthorActivityMutation(ctx, "start activity attempt", func(txctx context.Context, tx *sql.Tx) error {
		out, inserted, err = activityjournaladapter.Start(txctx, tx, activityjournaladapter.DialectSQLite, s, record)
		return err
	})
	return
}

func (s *PostgresStore) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, inserted bool, err error) {
	err = s.runAuthorActivityMutation(ctx, "claim activity attempt", func(txctx context.Context, tx *sql.Tx) error {
		out, inserted, err = activityjournaladapter.Claim(txctx, tx, activityjournaladapter.DialectPostgres, s, record)
		return err
	})
	return
}

func (s *SQLiteRuntimeStore) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, inserted bool, err error) {
	err = s.runAuthorActivityMutation(ctx, "claim activity attempt", func(txctx context.Context, tx *sql.Tx) error {
		out, inserted, err = activityjournaladapter.Claim(txctx, tx, activityjournaladapter.DialectSQLite, s, record)
		return err
	})
	return
}

func (s *PostgresStore) CompleteActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, err error) {
	err = s.runAuthorActivityMutation(ctx, "complete activity attempt", func(txctx context.Context, tx *sql.Tx) error {
		out, err = activityjournaladapter.Complete(txctx, tx, activityjournaladapter.DialectPostgres, s, record)
		return err
	})
	return
}

func (s *SQLiteRuntimeStore) CompleteActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, err error) {
	err = s.runAuthorActivityMutation(ctx, "complete activity attempt", func(txctx context.Context, tx *sql.Tx) error {
		out, err = activityjournaladapter.Complete(txctx, tx, activityjournaladapter.DialectSQLite, s, record)
		return err
	})
	return
}

func (s *PostgresStore) MarkActivityAttemptUncertain(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, err error) {
	err = s.runAuthorActivityMutation(ctx, "mark activity attempt uncertain", func(txctx context.Context, tx *sql.Tx) error {
		out, err = activityjournaladapter.MarkUncertain(txctx, tx, activityjournaladapter.DialectPostgres, s, record)
		return err
	})
	return
}

func (s *SQLiteRuntimeStore) MarkActivityAttemptUncertain(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, err error) {
	err = s.runAuthorActivityMutation(ctx, "mark activity attempt uncertain", func(txctx context.Context, tx *sql.Tx) error {
		out, err = activityjournaladapter.MarkUncertain(txctx, tx, activityjournaladapter.DialectSQLite, s, record)
		return err
	})
	return
}

func (s *PostgresStore) LoadActivityAttempt(ctx context.Context, requestEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.backend == nil || s.backend.db == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("postgres store is required")
	}
	return activityjournaladapter.Load(ctx, s.backend.db, activityjournaladapter.DialectPostgres, requestEventID)
}

func (s *SQLiteRuntimeStore) LoadActivityAttempt(ctx context.Context, requestEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.backend == nil || s.backend.db == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("sqlite runtime store is required")
	}
	return activityjournaladapter.Load(ctx, s.backend.db, activityjournaladapter.DialectSQLite, requestEventID)
}
