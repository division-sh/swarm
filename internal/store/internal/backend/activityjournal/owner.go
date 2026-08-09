package activityjournal

import (
	"context"
	"database/sql"
	"fmt"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func postgresActivityRunOwner(tx *sql.Tx) RequireActiveRun {
	return func(ctx context.Context, runID string) error {
		return requirePostgresRunActive(ctx, tx, runID)
	}
}

func sqliteActivityRunOwner(tx *sql.Tx) RequireActiveRun {
	return func(ctx context.Context, runID string) error {
		return requireSQLiteRunActive(ctx, tx, runID)
	}
}

func (s *ActivityPostgresOwner) StartActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, inserted bool, err error) {
	err = s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		out, inserted, err = Start(txctx, tx, DialectPostgres, postgresActivityRunOwner(tx), story, record)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *ActivitySQLiteOwner) StartActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, inserted bool, err error) {
	err = s.runPrivateAuthorActivityMutation(ctx, "start activity attempt", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		out, inserted, err = Start(txctx, tx, DialectSQLite, sqliteActivityRunOwner(tx), story, record)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *ActivityPostgresOwner) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, inserted bool, err error) {
	err = s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		out, inserted, err = Claim(txctx, tx, DialectPostgres, postgresActivityRunOwner(tx), story, record)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *ActivitySQLiteOwner) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, inserted bool, err error) {
	err = s.runPrivateAuthorActivityMutation(ctx, "claim activity attempt", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		out, inserted, err = Claim(txctx, tx, DialectSQLite, sqliteActivityRunOwner(tx), story, record)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *ActivityPostgresOwner) CompleteActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, err error) {
	err = s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		out, err = Complete(txctx, tx, DialectPostgres, postgresActivityRunOwner(tx), story, record)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *ActivitySQLiteOwner) CompleteActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, err error) {
	err = s.runPrivateAuthorActivityMutation(ctx, "complete activity attempt", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		out, err = Complete(txctx, tx, DialectSQLite, sqliteActivityRunOwner(tx), story, record)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *ActivityPostgresOwner) MarkActivityAttemptUncertain(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, err error) {
	err = s.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		out, err = MarkUncertain(txctx, tx, DialectPostgres, postgresActivityRunOwner(tx), story, record)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *ActivitySQLiteOwner) MarkActivityAttemptUncertain(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (out runtimepipeline.ActivityAttemptRecord, err error) {
	err = s.runPrivateAuthorActivityMutation(ctx, "mark activity attempt uncertain", func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
		out, err = MarkUncertain(txctx, tx, DialectSQLite, sqliteActivityRunOwner(tx), story, record)
		if err != nil {
			return err
		}
		return nil
	})
	return
}

func (s *ActivityPostgresOwner) LoadActivityAttempt(ctx context.Context, requestEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("postgres store is required")
	}
	return Load(ctx, s.backend, DialectPostgres, requestEventID)
}

func (s *ActivitySQLiteOwner) LoadActivityAttempt(ctx context.Context, requestEventID string) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.backend == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("sqlite runtime store is required")
	}
	return Load(ctx, s.backend, DialectSQLite, requestEventID)
}
