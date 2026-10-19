package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	postgresevents "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	sqliteevents "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

func reconcileDisplacedHTTPActivities(ctx context.Context, attempt *mutationprotocol.Attempt, cut displacedHTTPActivityCut, dialect activityjournal.Dialect) (runtimepipeline.ActivityRecoverySummary, error) {
	var result runtimepipeline.ActivityRecoverySummary
	if ctx == nil {
		return result, errors.New("activity recovery requires its original context")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		if err := generationauthority.FenceMutation(ctx, tx, dialect == activityjournal.DialectSQLite); err != nil {
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

func recoverActivityEvidence(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, cut displacedHTTPActivityCut, dialect activityjournal.Dialect, id string) (runtimepipeline.RecoveredHTTPActivity, error) {
	var absent runtimepipeline.RecoveredHTTPActivity
	record, found, err := activityjournal.Load(ctx, tx, dialect, id)
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
	grant, displaced, err := cut.proveDisplacedGrantTx(ctx, attempt, record.ExecutionBinding)
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

func reconcileDisplacedHTTPActivity(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, dialect activityjournal.Dialect, record runtimepipeline.ActivityAttemptRecord, bundleHash string) (runtimepipeline.RecoveredHTTPActivity, error) {
	var absent runtimepipeline.RecoveredHTTPActivity
	if record.ExecutionMode != runtimeeffects.ExecutionModeLive {
		return absent, errors.New("activity recovery requires exact live mode")
	}
	var bundle string
	var err error
	if dialect == activityjournal.DialectSQLite {
		source, sourceErr := sourceadmission.LoadSQLiteTx(ctx, tx, record.RunID, true, false)
		bundle, err = source.BundleHash(), sourceErr
	} else {
		source, sourceErr := sourceadmission.LoadPostgresTx(ctx, tx, record.RunID, true, false)
		bundle, err = source.BundleHash(), sourceErr
	}
	if err != nil {
		return absent, err
	}
	if bundle != bundleHash {
		return absent, errors.New("activity recovery run differs from the original execution source")
	}
	scope, err := runtimeauthoractivity.BundleScopeForSource(ctx, bundle)
	if err != nil {
		return absent, err
	}
	rowCtx := runtimeauthoractivity.WithScope(ctx, scope)
	request, found, err := loadActivityRecoveryEvent(rowCtx, tx, dialect, record.RequestEventID)
	if err != nil {
		return absent, err
	}
	if !found {
		return absent, errors.New("activity recovery original request is missing")
	}
	terminal, err := runtimepipeline.DisplacedHTTPActivityResult(request, record, bundleHash)
	if err != nil {
		return absent, err
	}
	if record.Status != runtimepipeline.ActivityAttemptStatusStarted {
		return runtimepipeline.RecoveredHTTPActivity{ActivityAttemptRecord: terminal, Request: request, BundleHash: bundleHash}, nil
	}
	runOwner := func(ctx context.Context, runID string) error {
		if dialect == activityjournal.DialectSQLite {
			return runstate.RequireSQLiteActiveTx(ctx, tx, runID)
		}
		return runstate.RequirePostgresActiveTx(ctx, tx, runID)
	}
	stored, _, err := activityjournal.MarkUncertain(rowCtx, tx, dialect, runOwner, attempt, terminal)
	if err != nil {
		return absent, err
	}
	return runtimepipeline.RecoveredHTTPActivity{ActivityAttemptRecord: stored, Request: request, BundleHash: bundleHash}, nil
}

func loadActivityRecoveryEvent(ctx context.Context, tx *sql.Tx, dialect activityjournal.Dialect, eventID string) (events.Event, bool, error) {
	var admitted events.AdmittedEvent
	var found bool
	var err error
	if dialect == activityjournal.DialectSQLite {
		admitted, _, found, err = sqliteevents.LoadAdmitted(ctx, tx, eventID)
	} else {
		admitted, _, found, err = postgresevents.LoadAdmitted(ctx, tx, eventID)
	}
	return admitted.Event(), found, err
}
