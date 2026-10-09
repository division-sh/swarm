package activityjournal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type activityAttemptMutationResult struct {
	record      runtimepipeline.ActivityAttemptRecord
	changed     bool
	cardChanged bool
}

func linkedActivityCardChange(ctx context.Context, tx *sql.Tx, record runtimepipeline.ActivityAttemptRecord, changed, postgres bool) (bool, error) {
	if !changed {
		return false, nil
	}
	return decisionpersistence.ProposedEffectCardDependsOnAttemptInTx(ctx, tx, record.RequestEventID, record.RunID, record.ExecutionMode, postgres)
}

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

func (s *ActivityPostgresOwner) StartActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.schemaGuard == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity journal postgres owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (activityAttemptMutationResult, error) {
		var value activityAttemptMutationResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.changed, err = Start(txctx, tx, DialectPostgres, postgresActivityRunOwner(tx), attempt, record)
			if err != nil {
				return err
			}
			value.cardChanged, err = linkedActivityCardChange(txctx, tx, value.record, value.changed, true)
			return err
		})
		return value, err
	})
	value, ok := result.Value()
	if !ok {
		return runtimepipeline.ActivityAttemptRecord{}, false, result.Err()
	}
	return value.record, value.changed, errors.Join(result.Err(), s.channelChanges.PublishAcknowledged(value.cardChanged, render.ReconcileOrdinary))
}

func (s *ActivitySQLiteOwner) StartActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.schemaGuard == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity journal sqlite owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "start activity attempt", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (activityAttemptMutationResult, error) {
		var value activityAttemptMutationResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.changed, err = Start(txctx, tx, DialectSQLite, sqliteActivityRunOwner(tx), attempt, record)
			if err != nil {
				return err
			}
			value.cardChanged, err = linkedActivityCardChange(txctx, tx, value.record, value.changed, false)
			return err
		})
		return value, err
	})
	value, ok := result.Value()
	if !ok {
		return runtimepipeline.ActivityAttemptRecord{}, false, result.Err()
	}
	return value.record, value.changed, errors.Join(result.Err(), s.channelChanges.PublishAcknowledged(value.cardChanged, render.ReconcileOrdinary))
}

func (s *ActivityPostgresOwner) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.schemaGuard == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity journal postgres owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (activityAttemptMutationResult, error) {
		var value activityAttemptMutationResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.changed, err = Claim(txctx, tx, DialectPostgres, postgresActivityRunOwner(tx), attempt, record)
			if err != nil {
				return err
			}
			value.cardChanged, err = linkedActivityCardChange(txctx, tx, value.record, value.changed, true)
			return err
		})
		return value, err
	})
	value, ok := result.Value()
	if !ok {
		return runtimepipeline.ActivityAttemptRecord{}, false, result.Err()
	}
	return value.record, value.changed, errors.Join(result.Err(), s.channelChanges.PublishAcknowledged(value.cardChanged, render.ReconcileOrdinary))
}

func (s *ActivitySQLiteOwner) ClaimActivityAttemptForLoopGeneration(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.schemaGuard == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity journal sqlite owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "claim activity attempt", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (activityAttemptMutationResult, error) {
		var value activityAttemptMutationResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.changed, err = Claim(txctx, tx, DialectSQLite, sqliteActivityRunOwner(tx), attempt, record)
			if err != nil {
				return err
			}
			value.cardChanged, err = linkedActivityCardChange(txctx, tx, value.record, value.changed, false)
			return err
		})
		return value, err
	})
	value, ok := result.Value()
	if !ok {
		return runtimepipeline.ActivityAttemptRecord{}, false, result.Err()
	}
	return value.record, value.changed, errors.Join(result.Err(), s.channelChanges.PublishAcknowledged(value.cardChanged, render.ReconcileOrdinary))
}

func (s *ActivityPostgresOwner) CompleteActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.schemaGuard == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity journal postgres owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (activityAttemptMutationResult, error) {
		var value activityAttemptMutationResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.changed, err = Complete(txctx, tx, DialectPostgres, postgresActivityRunOwner(tx), attempt, record)
			if err != nil {
				return err
			}
			value.cardChanged, err = linkedActivityCardChange(txctx, tx, value.record, value.changed, true)
			return err
		})
		return value, err
	})
	value, ok := result.Value()
	if !ok {
		return runtimepipeline.ActivityAttemptRecord{}, false, result.Err()
	}
	return value.record, true, errors.Join(result.Err(), s.channelChanges.PublishAcknowledged(value.cardChanged, render.ReconcileOrdinary))
}

func (s *ActivitySQLiteOwner) CompleteActivityAttempt(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.schemaGuard == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity journal sqlite owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "complete activity attempt", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (activityAttemptMutationResult, error) {
		var value activityAttemptMutationResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.changed, err = Complete(txctx, tx, DialectSQLite, sqliteActivityRunOwner(tx), attempt, record)
			if err != nil {
				return err
			}
			value.cardChanged, err = linkedActivityCardChange(txctx, tx, value.record, value.changed, false)
			return err
		})
		return value, err
	})
	value, ok := result.Value()
	if !ok {
		return runtimepipeline.ActivityAttemptRecord{}, false, result.Err()
	}
	return value.record, true, errors.Join(result.Err(), s.channelChanges.PublishAcknowledged(value.cardChanged, render.ReconcileOrdinary))
}

func (s *ActivityPostgresOwner) MarkActivityAttemptUncertain(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.schemaGuard == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity journal postgres owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (activityAttemptMutationResult, error) {
		var value activityAttemptMutationResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.changed, err = MarkUncertain(txctx, tx, DialectPostgres, postgresActivityRunOwner(tx), attempt, record)
			if err != nil {
				return err
			}
			value.cardChanged, err = linkedActivityCardChange(txctx, tx, value.record, value.changed, true)
			return err
		})
		return value, err
	})
	value, ok := result.Value()
	if !ok {
		return runtimepipeline.ActivityAttemptRecord{}, false, result.Err()
	}
	return value.record, true, errors.Join(result.Err(), s.channelChanges.PublishAcknowledged(value.cardChanged, render.ReconcileOrdinary))
}

func (s *ActivitySQLiteOwner) MarkActivityAttemptUncertain(ctx context.Context, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	if s == nil || s.schemaGuard == nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, fmt.Errorf("activity journal sqlite owner is required")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, false, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "mark activity attempt uncertain", mutationprotocol.Story, mutationprotocol.Ordinary, nil, nil, func(txctx context.Context, attempt *mutationprotocol.Attempt) (activityAttemptMutationResult, error) {
		var value activityAttemptMutationResult
		err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			value.record, value.changed, err = MarkUncertain(txctx, tx, DialectSQLite, sqliteActivityRunOwner(tx), attempt, record)
			if err != nil {
				return err
			}
			value.cardChanged, err = linkedActivityCardChange(txctx, tx, value.record, value.changed, false)
			return err
		})
		return value, err
	})
	value, ok := result.Value()
	if !ok {
		return runtimepipeline.ActivityAttemptRecord{}, false, result.Err()
	}
	return value.record, true, errors.Join(result.Err(), s.channelChanges.PublishAcknowledged(value.cardChanged, render.ReconcileOrdinary))
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
