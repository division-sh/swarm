package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type runForkContinuedOutcomeRow struct {
	kind                   string
	eventID, sourceEventID sql.NullString
	disposition            sql.NullString
	failurePresent         bool
	createdAt              time.Time
}

func requireSelectedForkFanOutPublishedTx(ctx context.Context, tx *sql.Tx, binding runfork.RunForkSelectedContractBinding, sourceEventID string, postgres bool) error {
	snapshot, plan, err := loadSelectedForkFanOutSourceEvidenceTx(ctx, tx, binding.ForkRunID, binding.SourceRunID, postgres)
	if err != nil || snapshot == nil {
		return err
	}
	key, replay, found, err := selectedForkFanOutContinuationTx(ctx, tx, binding.ForkRunID, plan, sourceEventID)
	if err != nil || !found {
		return err
	}
	row, present, err := loadRunForkContinuedOutcomeTx(ctx, tx, key, replay.Ordinal)
	if err != nil {
		return err
	}
	return requireRunForkContinuedPublicationReadback(ctx, tx, postgres, binding.ForkRunID, plan, replay, row, present)
}

func loadRunForkContinuedOutcomeTx(ctx context.Context, tx *sql.Tx, key fanoutobligation.IntentKey, ordinal int) (runForkContinuedOutcomeRow, bool, error) {
	if err := key.Validate(); err != nil {
		return runForkContinuedOutcomeRow{}, false, err
	}
	if ordinal < 0 {
		return runForkContinuedOutcomeRow{}, false, fmt.Errorf("continued fan-out requires a nonnegative ordinal")
	}
	where := `run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5 AND ordinal=$6`
	args := []any{key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, ordinal}
	if key.DeploymentFeedID != "" {
		where = `run_id=$1 AND deployment_feed_id=$2 AND ordinal=$3`
		args = []any{key.RunID, key.DeploymentFeedID, ordinal}
	}
	rows, err := tx.QueryContext(ctx, `SELECT outcome_kind,CAST(event_id AS TEXT),CAST(source_event_id AS TEXT),inherited_disposition,failure,created_at FROM fan_out_outcomes WHERE `+where, args...)
	if err != nil {
		return runForkContinuedOutcomeRow{}, false, err
	}
	defer rows.Close()
	var outcome runForkContinuedOutcomeRow
	found := false
	for rows.Next() {
		if found {
			return outcome, false, fmt.Errorf("continued fan-out has duplicate ordinal evidence")
		}
		var failure []byte
		var rawTime any
		if err := rows.Scan(&outcome.kind, &outcome.eventID, &outcome.sourceEventID, &outcome.disposition, &failure, &rawTime); err != nil {
			return outcome, false, err
		}
		created, present, err := sqliteTimeValue(rawTime)
		if err != nil || !present || created.IsZero() {
			return outcome, false, fmt.Errorf("continued fan-out outcome lacks a valid creation time: %v", err)
		}
		outcome.failurePresent, outcome.createdAt, found = failure != nil, created, true
	}
	if err := rows.Err(); err != nil {
		return outcome, false, err
	}
	return outcome, found, rows.Close()
}

// An unpublished hole is legal only before its existing publication owner has
// committed. Readback never fills the hole or borrows current source progress.
type runForkContinuedLineageOwner struct {
	eventID, sourceRunID, kind string
}

func loadRunForkContinuedLineageOwnerTx(ctx context.Context, tx *sql.Tx, childRun, sourceRun, sourceEvent string) (runForkContinuedLineageOwner, bool, error) {
	rows, err := tx.QueryContext(ctx, `SELECT CAST(fork_event_id AS TEXT),CAST(source_run_id AS TEXT),'selected' FROM run_fork_selected_contract_executions
		WHERE fork_run_id=$1 AND COALESCE(source_obligation_event_id,source_event_id)=$2
		UNION ALL
		SELECT DISTINCT CAST(fork_event_id AS TEXT),CAST(source_run_id AS TEXT),'generic' FROM run_fork_delivery_event_replays
		WHERE fork_run_id=$1 AND source_event_id=$2`, childRun, sourceEvent)
	if err != nil {
		return runForkContinuedLineageOwner{}, false, err
	}
	var owner runForkContinuedLineageOwner
	found := false
	for rows.Next() {
		if found {
			_ = rows.Close()
			return owner, false, fmt.Errorf("continued fan-out publication has competing lineage owners")
		}
		if err := rows.Scan(&owner.eventID, &owner.sourceRunID, &owner.kind); err != nil {
			_ = rows.Close()
			return owner, false, err
		}
		if owner.sourceRunID != sourceRun {
			_ = rows.Close()
			return owner, false, fmt.Errorf("continued fan-out publication has foreign source ownership")
		}
		found = true
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return owner, false, err
	}
	if err := rows.Close(); err != nil {
		return owner, false, err
	}
	return owner, found, nil
}

func requireRunForkContinuedPublicationReadback(ctx context.Context, tx *sql.Tx, postgres bool, childRun string, plan runfork.RunForkPlan, replay runfork.RunForkFanOutPendingReplay, outcome runForkContinuedOutcomeRow, present bool) error {
	owner, found, err := loadRunForkContinuedLineageOwnerTx(ctx, tx, childRun, plan.SourceRunID, replay.SourceEventID)
	if err != nil {
		return err
	}
	if !found {
		var orphan bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM events WHERE run_id=$1 AND event_id IN ($2,$3))`,
			childRun, activityidentity.ForkLineageEventID(childRun, replay.SourceEventID), deterministicRunForkReplayEventID(childRun, replay.SourceEventID)).Scan(&orphan); err != nil {
			return err
		}
		if present || orphan {
			return fmt.Errorf("continued fan-out has partial outcome/publication/lineage evidence")
		}
		return nil
	}
	eventID := owner.eventID
	want := fanoutobligation.Outcome{Ordinal: replay.Ordinal, Kind: fanoutobligation.OutcomeContinued,
		EventID: eventID, SourceEventID: replay.SourceEventID, InheritedDisposition: replay.InheritedDisposition}
	if !present || !matchesRunForkFanOutContinuedOutcome(want, outcome.kind, outcome.eventID, outcome.sourceEventID, outcome.disposition, outcome.failurePresent) {
		return fmt.Errorf("continued fan-out publication has missing or contradictory ordinal evidence")
	}
	var publications []eventrecord.AdmittedRecord
	if postgres {
		publications, err = eventpostgres.LoadAdmittedMany(ctx, tx, []string{eventID})
	} else {
		publications, err = eventsqlite.LoadAdmittedMany(ctx, tx, []string{eventID})
	}
	if err != nil || len(publications) != 1 {
		return fmt.Errorf("continued fan-out lacks its canonical publication: %v", err)
	}
	publication := publications[0]
	if !outcome.createdAt.Equal(publication.Event.Event().CreatedAt()) {
		return fmt.Errorf("continued fan-out outcome changed its retained publication time")
	}
	lineage, selected := publication.Event.Event().SelectedForkLineage()
	if !selected || publication.Event.Event().RunID() != childRun || lineage.SourceRunID() != plan.SourceRunID || lineage.SourceEventID() != replay.SourceEventID {
		return fmt.Errorf("continued fan-out publication contradicts exact source/child lineage")
	}
	if owner.kind != "selected" && owner.kind != "generic" {
		return fmt.Errorf("continued fan-out publication has an unsupported owner")
	}
	return requireRunForkContinuedPublishedRecipientsTx(ctx, tx, postgres, childRun, plan, replay, owner, publication)
}

func requireRunForkContinuedPublishedRecipientsTx(ctx context.Context, tx *sql.Tx, postgres bool, childRun string, plan runfork.RunForkPlan, replay runfork.RunForkFanOutPendingReplay, owner runForkContinuedLineageOwner, publication eventrecord.AdmittedRecord) error {
	eventID := owner.eventID
	adapter := sqliteDeliveryAdapter
	validate, resolve := runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint
	if postgres {
		adapter = postgresDeliveryAdapter
		validate, resolve = runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint
	}
	deliveries, err := adapter.SnapshotsForEvents(ctx, tx, []string{eventID})
	if err != nil {
		return err
	}
	var routes []events.DeliveryRoute
	for _, delivery := range deliveries[eventID] {
		if delivery.RunID != childRun || delivery.EventID != eventID {
			return fmt.Errorf("continued fan-out contains foreign child delivery evidence")
		}
		routes = append(routes, delivery.Route)
	}
	if err := publication.Settlement.Validate(routes); err != nil {
		return err
	}
	if publication.Settlement.NoDelivery() || len(routes) == 0 {
		return fmt.Errorf("continued fan-out publication lost its unfinished recipient partition")
	}
	if owner.kind == "generic" {
		boundEvent, err := requireRunForkFanOutReplayMembershipTx(ctx, tx, plan.SourceRunID, childRun, replay)
		if err != nil {
			return err
		}
		if boundEvent != eventID {
			return fmt.Errorf("continued fan-out disagrees with generic replay publication ownership")
		}
		return nil
	}
	admission, err := loadRunForkSourceStateAdmission(ctx, tx, childRun, validate, resolve, postgres)
	if err != nil {
		return err
	}
	return requireSelectedFanOutRecipientPartition(admission.snapshot, replay, publication.Event, routes)
}
