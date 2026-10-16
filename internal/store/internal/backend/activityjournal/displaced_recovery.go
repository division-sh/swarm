package activityjournal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	postgresevents "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	sqliteevents "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

func (s *ActivityPostgresOwner) ReconcileDisplacedHTTPActivitiesTx(ctx context.Context, attempt *mutationprotocol.Attempt, cut storestartup.DisplacedHTTPActivityCut) (runtimepipeline.ActivityRecoverySummary, error) {
	if s == nil || s.schemaGuard == nil || s.executionGrant != nil {
		return runtimepipeline.ActivityRecoverySummary{}, errors.New("activity recovery requires its original unbound PostgreSQL journal owner")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityRecoverySummary{}, err
	}
	return reconcileDisplacedHTTPActivities(ctx, attempt, cut, DialectPostgres)
}

func (s *ActivitySQLiteOwner) ReconcileDisplacedHTTPActivitiesTx(ctx context.Context, attempt *mutationprotocol.Attempt, cut storestartup.DisplacedHTTPActivityCut) (runtimepipeline.ActivityRecoverySummary, error) {
	if s == nil || s.schemaGuard == nil || s.executionGrant != nil {
		return runtimepipeline.ActivityRecoverySummary{}, errors.New("activity recovery requires its original unbound SQLite journal owner")
	}
	if err := s.schemaGuard(); err != nil {
		return runtimepipeline.ActivityRecoverySummary{}, err
	}
	return reconcileDisplacedHTTPActivities(ctx, attempt, cut, DialectSQLite)
}

func reconcileDisplacedHTTPActivities(ctx context.Context, attempt *mutationprotocol.Attempt, cut storestartup.DisplacedHTTPActivityCut, dialect Dialect) (runtimepipeline.ActivityRecoverySummary, error) {
	var result runtimepipeline.ActivityRecoverySummary
	if ctx == nil {
		return result, errors.New("activity recovery requires its original context")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := generationauthority.FenceMutation(ctx, tx, dialect == DialectSQLite); err != nil {
			return err
		}
		ids, err := activityRecoveryRequestIDs(ctx, tx)
		if err != nil {
			return err
		}
		for _, id := range ids {
			stored, err := recoverActivityEvidence(ctx, tx, attempt, cut, dialect, id)
			if err != nil {
				return err
			}
			if stored.RequestEventID != "" {
				result.Results = append(result.Results, stored)
			}
		}
		return ctx.Err()
	})
	if err != nil {
		return runtimepipeline.ActivityRecoverySummary{}, err
	}
	return result, nil
}

func recoverActivityEvidence(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, cut storestartup.DisplacedHTTPActivityCut, dialect Dialect, id string) (runtimepipeline.ActivityAttemptRecord, error) {
	var absent runtimepipeline.ActivityAttemptRecord
	record, found, err := Load(ctx, tx, dialect, id)
	if err != nil {
		return absent, err
	}
	if !found {
		return absent, fmt.Errorf("activity recovery request %s disappeared", id)
	}
	if record.Status != runtimepipeline.ActivityAttemptStatusStarted {
		// Copied/legacy terminal history is not a displaced live execution.
		if record.ExecutionBinding.IsZero() {
			return absent, nil
		}
		_, published, err := loadActivityRecoveryEvent(ctx, tx, dialect, record.ResultEventID)
		if err != nil || published {
			return absent, err
		}
	}
	grant, displaced, err := cut.ProveDisplacedGrantTx(ctx, attempt, record.ExecutionBinding)
	if err != nil || !displaced {
		return absent, err
	}
	return reconcileDisplacedHTTPActivity(ctx, tx, attempt, dialect, record, grant.BundleHash)
}

func activityRecoveryRequestIDs(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT request_event_id FROM activity_attempts WHERE execution_mode = 'live' ORDER BY request_event_id`)
	if err != nil {
		return nil, fmt.Errorf("list activity recovery evidence: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, errors.Join(rows.Err(), rows.Close())
}

func reconcileDisplacedHTTPActivity(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, dialect Dialect, record runtimepipeline.ActivityAttemptRecord, bundleHash string) (runtimepipeline.ActivityAttemptRecord, error) {
	if record.ExecutionMode != runtimeeffects.ExecutionModeLive {
		return runtimepipeline.ActivityAttemptRecord{}, errors.New("activity recovery requires exact live mode")
	}
	var bundle string
	var err error
	if dialect == DialectSQLite {
		source, sourceErr := sourceadmission.LoadSQLiteTx(ctx, tx, record.RunID, true, false)
		bundle, err = source.BundleHash(), sourceErr
	} else {
		source, sourceErr := sourceadmission.LoadPostgresTx(ctx, tx, record.RunID, true, false)
		bundle, err = source.BundleHash(), sourceErr
	}
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if bundle != bundleHash {
		return runtimepipeline.ActivityAttemptRecord{}, errors.New("activity recovery run differs from the original execution source")
	}
	request, found, err := loadActivityRecoveryEvent(ctx, tx, dialect, record.RequestEventID)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if !found {
		return runtimepipeline.ActivityAttemptRecord{}, errors.New("activity recovery original request is missing")
	}
	terminal, err := runtimepipeline.DisplacedHTTPActivityResult(request, record, bundleHash)
	if err != nil {
		return runtimepipeline.ActivityAttemptRecord{}, err
	}
	if record.Status != runtimepipeline.ActivityAttemptStatusStarted {
		return terminal, nil
	}
	runOwner := postgresActivityRunOwner(tx)
	if dialect == DialectSQLite {
		runOwner = sqliteActivityRunOwner(tx)
	}
	stored, _, err := MarkUncertain(ctx, tx, dialect, runOwner, attempt, terminal)
	return stored, err
}

func loadActivityRecoveryEvent(ctx context.Context, tx *sql.Tx, dialect Dialect, eventID string) (events.Event, bool, error) {
	var admitted events.AdmittedEvent
	var found bool
	var err error
	if dialect == DialectSQLite {
		admitted, _, found, err = sqliteevents.LoadAdmitted(ctx, tx, eventID)
	} else {
		admitted, _, found, err = postgresevents.LoadAdmitted(ctx, tx, eventID)
	}
	return admitted.Event(), found, err
}
