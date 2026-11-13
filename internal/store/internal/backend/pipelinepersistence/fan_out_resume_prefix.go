package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
)

func validateFanOutResumeInput(ctx context.Context, db *sql.DB, postgres bool, resources *storedurabledata.Owner, intent fanoutobligation.Intent) ([]fanoutobligation.Outcome, error) {
	// Source hydration stays outside the writer. Its original native source
	// owner and ordinal-event validators remain the only semantic decoders.
	if err := refuseInheritedFanOutResume(ctx, db, intent); err != nil {
		return nil, err
	}
	if _, err := loadFanOutEvaluation(ctx, db, postgres, resources, intent); err != nil {
		return nil, err
	}
	prefix, err := readFanOutResumePrefix(ctx, db, intent)
	if err != nil {
		return nil, err
	}
	for _, outcome := range prefix {
		if outcome.SourceEventID != "" || outcome.Kind == fanoutobligation.OutcomeContinued {
			return nil, errors.New("inherited fan-out prefix resume is not admitted")
		}
		if outcome.Kind != fanoutobligation.OutcomeCommitted {
			continue
		}
		var records []events.PersistedReplayEvent
		if postgres {
			records, err = hydratePostgresPersistedReplayEvents(ctx, db, []string{outcome.EventID})
		} else {
			records, err = hydrateSQLitePersistedReplayEvents(ctx, db, []string{outcome.EventID})
		}
		if err != nil {
			return nil, err
		}
		if len(records) != 1 || records[0].ReplayFailure != nil || records[0].Event.RunID() != intent.Request.Key.RunID {
			return nil, fmt.Errorf("fan-out resume requires its exact committed event at ordinal %d", outcome.Ordinal)
		}
		if intent.Request.Deployment != nil {
			err = fanoutobligation.ValidateCommittedDeploymentOrdinalEvent(intent, outcome.Ordinal, records[0].Event.ID())
		} else {
			err = fanoutobligation.ValidateCommittedOrdinalEvent(intent.Request.Key, intent.Request.PlanRef, intent.Request.Capsule, outcome.Ordinal, records[0].Event)
		}
		if err != nil {
			return nil, err
		}
	}
	return prefix, nil
}

func refuseInheritedFanOutResume(ctx context.Context, db rowQueryer, intent fanoutobligation.Intent) error {
	if intent.Request.Deployment == nil {
		if intent.Request.Capsule.Lineage.RunID != intent.Request.Key.RunID {
			return errors.New("inherited handler resume is not admitted")
		}
		return nil
	}
	var parent sql.NullString
	if err := db.QueryRowContext(ctx, `SELECT CAST(forked_from_run_id AS TEXT) FROM runs WHERE run_id=$1`, intent.Request.Key.RunID).Scan(&parent); err != nil {
		return err
	}
	if parent.Valid {
		return errors.New("forked deployment feed resume is not admitted")
	}
	return nil
}

func readFanOutResumePrefix(ctx context.Context, db eventReadQueryer, intent fanoutobligation.Intent) ([]fanoutobligation.Outcome, error) {
	predicate, args := fanOutKeyPredicate("", intent.Request.Key, 1)
	rows, err := db.QueryContext(ctx, `SELECT ordinal,outcome_kind,COALESCE(CAST(event_id AS TEXT),''),COALESCE(CAST(source_event_id AS TEXT),''),COALESCE(inherited_disposition,''),failure,created_at FROM fan_out_outcomes WHERE `+predicate+` ORDER BY ordinal`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]fanoutobligation.Outcome, 0)
	for rows.Next() {
		var row fanoutobligation.Outcome
		var failure []byte
		var at any
		if err := rows.Scan(&row.Ordinal, &row.Kind, &row.EventID, &row.SourceEventID, &row.InheritedDisposition, &failure, &at); err != nil {
			return nil, err
		}
		row.Failure = failure
		row.CreatedAt, _, err = sqliteTimeValue(at)
		if err != nil {
			return nil, err
		}
		if row.Ordinal != len(out) || row.Ordinal >= intent.Cursor {
			return nil, errors.New("fan-out resume requires the complete exact ordinal prefix")
		}
		if err := row.Validate(); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if len(out) != intent.Cursor {
		return nil, errors.New("fan-out resume cannot skip a missing committed ordinal")
	}
	return out, nil
}
