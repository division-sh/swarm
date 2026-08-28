package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

const fanOutBarrierScheduleOwner = "workflow-runtime"

func foldFanOutIntentTerminalDispositions(
	ctx context.Context,
	db pipelineQueryer,
	postgres bool,
	key fanoutobligation.IntentKey,
) (fanoutbarrier.Fold, error) {
	if err := key.Validate(); err != nil {
		return fanoutbarrier.Fold{}, err
	}
	var cardinality, cursor int
	var rawStatus string
	err := db.QueryRowContext(ctx, `
		SELECT cardinality, cursor, status
		FROM fan_out_intents
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4
	`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.PackageKey, key.ElementRef.ElementID).Scan(&cardinality, &cursor, &rawStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s is missing", key.String())
	}
	if err != nil {
		return fanoutbarrier.Fold{}, err
	}
	status := fanoutobligation.Status(strings.TrimSpace(rawStatus))
	if cardinality < 0 || cursor < 0 || cursor > cardinality {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s has invalid progress", key.String())
	}
	fold := fanoutbarrier.Fold{
		Summary:           fanoutbarrier.Summary{Total: cardinality},
		EnumerationClosed: status == fanoutobligation.StatusClosed || status == fanoutobligation.StatusCanceled,
	}
	if status == fanoutobligation.StatusCanceled {
		fold.Summary.Canceled = cardinality - cursor
	} else if status != fanoutobligation.StatusOpen && status != fanoutobligation.StatusClosed && status != fanoutobligation.StatusBlocked {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s has invalid status %q", key.String(), status)
	}
	rows, err := db.QueryContext(ctx, `
		SELECT ordinal, outcome_kind, event_id, source_event_id, inherited_disposition
		FROM fan_out_outcomes
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4
		ORDER BY ordinal
	`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.PackageKey, key.ElementRef.ElementID)
	if err != nil {
		return fanoutbarrier.Fold{}, err
	}
	type outcomeFact struct {
		ordinal       int
		kind          string
		eventID       sql.NullString
		sourceEventID sql.NullString
		disposition   sql.NullString
	}
	facts := make([]outcomeFact, 0, cursor)
	for rows.Next() {
		var fact outcomeFact
		if err := rows.Scan(&fact.ordinal, &fact.kind, &fact.eventID, &fact.sourceEventID, &fact.disposition); err != nil {
			rows.Close()
			return fanoutbarrier.Fold{}, err
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fanoutbarrier.Fold{}, err
	}
	if err := rows.Close(); err != nil {
		return fanoutbarrier.Fold{}, err
	}
	if len(facts) != cursor {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s has %d ordinal outcomes, want contiguous cursor %d", key.String(), len(facts), cursor)
	}
	adapter := sqliteDeliveryAdapter
	if postgres {
		adapter = postgresDeliveryAdapter
	}
	for ordinal, fact := range facts {
		if fact.ordinal != ordinal {
			return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s outcome ordinal %d is not contiguous at %d", key.String(), fact.ordinal, ordinal)
		}
		switch fanoutobligation.OutcomeKind(strings.TrimSpace(fact.kind)) {
		case fanoutobligation.OutcomeSemanticRejected:
			if fact.eventID.Valid || fact.sourceEventID.Valid || fact.disposition.Valid {
				return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s rejected ordinal %d carries settlement identity", key.String(), ordinal)
			}
			fold.Summary.SemanticRejected++
		case fanoutobligation.OutcomeCommitted:
			if fact.sourceEventID.Valid {
				if fact.eventID.Valid || !fact.disposition.Valid {
					return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s inherited ordinal %d has contradictory settlement evidence", key.String(), ordinal)
				}
				switch fanoutobligation.InheritedTerminalDisposition(strings.TrimSpace(fact.disposition.String)) {
				case fanoutobligation.InheritedSucceeded:
					fold.Summary.Succeeded++
				case fanoutobligation.InheritedDeadLettered:
					fold.Summary.DeadLettered++
				case fanoutobligation.InheritedNoRoute:
					fold.Summary.NoRoute++
				default:
					return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s inherited ordinal %d has invalid terminal disposition %q", key.String(), ordinal, fact.disposition.String)
				}
				continue
			}
			if !fact.eventID.Valid || fact.disposition.Valid {
				return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s owned ordinal %d has contradictory settlement evidence", key.String(), ordinal)
			}
			_, settlement, err := loadFanOutSourceEvent(ctx, db, fact.eventID.String, postgres)
			if err != nil {
				return fanoutbarrier.Fold{}, fmt.Errorf("load fan-out ordinal %d settlement: %w", ordinal, err)
			}
			deliveries, err := adapter.SnapshotsForEvent(ctx, db, fact.eventID.String)
			if err != nil {
				return fanoutbarrier.Fold{}, fmt.Errorf("load fan-out ordinal %d deliveries: %w", ordinal, err)
			}
			switch {
			case settlement.NoDelivery() && len(deliveries) == 0:
				fold.Summary.NoRoute++
			case settlement.Delivered() && len(deliveries) > 0:
				allTerminal := true
				deadLettered := false
				for _, delivery := range deliveries {
					if !delivery.Terminal() {
						allTerminal = false
					}
					if delivery.Status == runtimedelivery.StatusDeadLetter {
						deadLettered = true
					}
				}
				if !allTerminal {
					fold.PendingCommitted++
				} else if deadLettered {
					fold.Summary.DeadLettered++
				} else {
					fold.Summary.Succeeded++
				}
			default:
				return fanoutbarrier.Fold{}, fmt.Errorf("fan-out ordinal %d route and delivery settlement are contradictory", ordinal)
			}
		default:
			return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s ordinal %d has invalid outcome kind %q", key.String(), ordinal, fact.kind)
		}
	}
	if err := fold.Validate(); err != nil {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s: %w", key.String(), err)
	}
	return fold, nil
}

func loadFanOutBarriersByStatus(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	runID string,
	status fanoutbarrier.Status,
) ([]fanoutbarrier.Registration, error) {
	if !status.Valid() {
		return nil, fmt.Errorf("fan-out barrier load requires valid status")
	}
	query := `
		SELECT triggering_delivery_id, package_key, element_id,
		       bundle_hash, semantic_digest, target_package_key, target_flow_id,
		       target_node_id, handler_event, join_id,
		       route_scope_key, route_instance_id, route_instance_path,
		       entity_id, routing_source, execution_mode, timer_handle, created_at
		FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND status=$2
		ORDER BY triggering_delivery_id, package_key, element_id`
	if postgres {
		query += " FOR UPDATE"
	}
	rows, err := tx.QueryContext(ctx, query, runID, string(status))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type persisted struct {
		registration fanoutbarrier.Registration
		bundleHash   string
		digest       string
		targetPkg    string
		targetFlow   string
		targetNode   string
		handlerEvent string
		joinID       string
		routingRaw   []byte
		handleRaw    []byte
		execution    string
	}
	persistedRows := make([]persisted, 0)
	for rows.Next() {
		var item persisted
		item.registration.IntentKey.RunID = runID
		var routingRaw, handleRaw, createdAtRaw any
		if err := rows.Scan(
			&item.registration.IntentKey.TriggeringDeliveryID,
			&item.registration.IntentKey.ElementRef.PackageKey,
			&item.registration.IntentKey.ElementRef.ElementID,
			&item.bundleHash, &item.digest, &item.targetPkg, &item.targetFlow,
			&item.targetNode, &item.handlerEvent, &item.joinID,
			&item.registration.Route.ScopeKey, &item.registration.Route.InstanceID, &item.registration.Route.InstancePath,
			&item.registration.EntityID, &routingRaw, &item.execution, &handleRaw, &createdAtRaw,
		); err != nil {
			return nil, err
		}
		createdAt, present, err := sqliteTimeValue(createdAtRaw)
		if err != nil || !present {
			return nil, fmt.Errorf("decode fan-out barrier created time: %w", err)
		}
		item.registration.CreatedAt = createdAt
		item.routingRaw = append([]byte(nil), jsonRawMessageValue(routingRaw)...)
		item.handleRaw = append([]byte(nil), jsonRawMessageValue(handleRaw)...)
		persistedRows = append(persistedRows, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]fanoutbarrier.Registration, 0, len(persistedRows))
	for _, item := range persistedRows {
		if err := json.Unmarshal(item.routingRaw, &item.registration.RoutingSource); err != nil {
			return nil, fmt.Errorf("decode fan-out barrier routing source: %w", err)
		}
		if err := json.Unmarshal(item.handleRaw, &item.registration.Handle); err != nil {
			return nil, fmt.Errorf("decode fan-out barrier timer handle: %w", err)
		}
		mode, ok := executionmode.Parse(item.execution)
		if !ok {
			return nil, fmt.Errorf("fan-out barrier execution mode %q is invalid", item.execution)
		}
		item.registration.ExecutionMode = mode
		if err := item.registration.Validate(); err != nil {
			return nil, err
		}
		ref, _ := item.registration.Handle.JoinRef()
		fanOut, _ := ref.FanOutDelivery()
		if fanOut.BundleHash() != strings.TrimSpace(item.bundleHash) || fanOut.SemanticDigest() != strings.TrimSpace(item.digest) ||
			ref.PackageKey() != strings.TrimSpace(item.targetPkg) || ref.FlowID() != strings.TrimSpace(item.targetFlow) ||
			ref.NodeID() != strings.TrimSpace(item.targetNode) || ref.HandlerEvent() != strings.TrimSpace(item.handlerEvent) ||
			ref.JoinID() != strings.TrimSpace(item.joinID) {
			return nil, fmt.Errorf("fan-out barrier persisted identity contradicts its typed handle")
		}
		out = append(out, item.registration)
	}
	return out, nil
}

func loadArmedFanOutBarriers(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]fanoutbarrier.Registration, error) {
	return loadFanOutBarriersByStatus(ctx, tx, postgres, runID, fanoutbarrier.StatusArmed)
}

func fanOutBarrierGenerationCurrent(ctx context.Context, tx *sql.Tx, registration fanoutbarrier.Registration) (bool, error) {
	ref, _ := registration.Handle.JoinRef()
	generation := ref.Generation()
	if !generation.Valid() {
		return true, nil
	}
	var fieldsRaw, stateBucketsRaw any
	err := tx.QueryRowContext(ctx, `
		SELECT fields, accumulator
		FROM entity_state
		WHERE run_id=$1 AND entity_id=$2
	`, registration.IntentKey.RunID, registration.EntityID).Scan(&fieldsRaw, &stateBucketsRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("fan-out barrier generation owner entity is missing")
	}
	if err != nil {
		return false, err
	}
	var fields, stateBuckets map[string]any
	if err := json.Unmarshal(jsonRawMessageValue(fieldsRaw), &fields); err != nil {
		return false, fmt.Errorf("decode fan-out barrier generation fields: %w", err)
	}
	if err := json.Unmarshal(jsonRawMessageValue(stateBucketsRaw), &stateBuckets); err != nil {
		return false, fmt.Errorf("decode fan-out barrier generation state: %w", err)
	}
	return runtimepipeline.WorkflowLoopGenerationCurrent(fields, stateBuckets, generation, "")
}

func fanOutBarrierSchedule(registration fanoutbarrier.Registration, summary fanoutbarrier.Summary, selectedNow time.Time) (runtimegenericschedule.AdmissionCommand, error) {
	payload := registration.Handle.PayloadMetadata()
	payload["join"] = summary.Context()
	semanticPayload, err := canonicaljson.FromGo(payload)
	if err != nil {
		return runtimegenericschedule.AdmissionCommand{}, err
	}
	flowInstance := ""
	if registration.RoutingSource.Kind() == events.RoutingSourceFlowOwnedControl {
		flowInstance = registration.RoutingSource.Route().Normalized().FlowInstance
	}
	return runtimegenericschedule.AdmissionCommand{
		ScheduleKey:   registration.Handle.TaskID(),
		RunID:         registration.IntentKey.RunID,
		EntityID:      registration.EntityID,
		FlowInstance:  flowInstance,
		OwnerKind:     runtimegenericschedule.OwnerSystem,
		OwnerID:       fanOutBarrierScheduleOwner,
		EventType:     registration.Handle.EventType(),
		Payload:       semanticPayload,
		RoutingSource: registration.RoutingSource,
		ExecutionMode: registration.ExecutionMode,
		Due:           runtimegenericschedule.AbsoluteDue(selectedNow),
		TaskID:        registration.Handle.TaskID(),
	}, nil
}

func advanceFanOutDeliveryBarriersTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *privaterunforkrevision.Effects,
	genericSchedules GenericScheduleTxOwner,
	runID string,
	selectedNow time.Time,
) error {
	if tx == nil || effects == nil || genericSchedules == nil || strings.TrimSpace(runID) == "" || selectedNow.IsZero() {
		return fmt.Errorf("fan-out barrier advancement requires transaction, owners, run, and selected-store time")
	}
	registrations, err := loadArmedFanOutBarriers(ctx, tx, postgres, runID)
	if err != nil {
		return err
	}
	for _, registration := range registrations {
		current, err := fanOutBarrierGenerationCurrent(ctx, tx, registration)
		if err != nil {
			return err
		}
		if !current {
			if err := suppressSupersededArmedFanOutBarrierTx(ctx, tx, postgres, effects, registration, selectedNow); err != nil {
				return err
			}
			continue
		}
		fold, err := foldFanOutIntentTerminalDispositions(ctx, tx, postgres, registration.IntentKey)
		if err != nil {
			return err
		}
		if !fold.Terminal() {
			continue
		}
		status := fanoutbarrier.StatusClosedPending
		scheduleKey := any(nil)
		command, err := fanOutBarrierSchedule(registration, fold.Summary, selectedNow)
		if err != nil {
			return err
		}
		if _, err := genericSchedules.AdmitTx(ctx, tx, effects, command); err != nil {
			return fmt.Errorf("admit fan-out barrier completion: %w", err)
		}
		scheduleKey = command.ScheduleKey
		summaryRaw, err := json.Marshal(fold.Summary)
		if err != nil {
			return err
		}
		query := `
			UPDATE fan_out_obligation_barriers
			SET status=$1, summary=$2, schedule_key=$3, updated_at=$4
			WHERE run_id=$5 AND triggering_delivery_id=$6 AND package_key=$7 AND element_id=$8 AND status='armed'`
		if postgres {
			query = strings.ReplaceAll(query, "$2", "$2::jsonb")
		} else {
			query = postgresPlaceholdersToSQLite(query, 8)
		}
		result, err := tx.ExecContext(ctx, query, string(status), string(summaryRaw), scheduleKey, selectedNow.UTC(),
			registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
			registration.IntentKey.ElementRef.PackageKey, registration.IntentKey.ElementRef.ElementID)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("fan-out barrier close lost exact armed owner")
		}
		if err := effects.Add(runID, privaterunforkrevision.FamilyFanOutObligations); err != nil {
			return err
		}
	}
	if err := suppressSupersededPendingFanOutBarriersTx(ctx, tx, postgres, effects, runID, selectedNow); err != nil {
		return err
	}
	return terminalizeDeadLetteredFanOutBarrierOutcomesTx(ctx, tx, postgres, effects, runID, selectedNow)
}

func suppressSupersededArmedFanOutBarrierTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *privaterunforkrevision.Effects,
	registration fanoutbarrier.Registration,
	at time.Time,
) error {
	query := `
		UPDATE fan_out_obligation_barriers
		SET status=$1, updated_at=$2
		WHERE run_id=$3 AND triggering_delivery_id=$4 AND package_key=$5 AND element_id=$6 AND status='armed'`
	if !postgres {
		query = postgresPlaceholdersToSQLite(query, 6)
	}
	result, err := tx.ExecContext(ctx, query, string(fanoutbarrier.StatusSuppressedGenerationSuperseded), at.UTC(),
		registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
		registration.IntentKey.ElementRef.PackageKey, registration.IntentKey.ElementRef.ElementID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("fan-out barrier generation suppression lost exact armed owner")
	}
	return effects.Add(registration.IntentKey.RunID, privaterunforkrevision.FamilyFanOutObligations)
}

func suppressSupersededPendingFanOutBarriersTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *privaterunforkrevision.Effects,
	runID string,
	at time.Time,
) error {
	registrations, err := loadFanOutBarriersByStatus(ctx, tx, postgres, runID, fanoutbarrier.StatusClosedPending)
	if err != nil {
		return err
	}
	for _, registration := range registrations {
		current, err := fanOutBarrierGenerationCurrent(ctx, tx, registration)
		if err != nil {
			return err
		}
		if current {
			continue
		}
		update := `
			UPDATE fan_out_obligation_barriers
			SET status=$1, updated_at=$2
			WHERE run_id=$3 AND triggering_delivery_id=$4 AND package_key=$5 AND element_id=$6 AND status='closed_pending'`
		if !postgres {
			update = postgresPlaceholdersToSQLite(update, 6)
		}
		result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusSuppressedGenerationSuperseded), at.UTC(),
			registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
			registration.IntentKey.ElementRef.PackageKey, registration.IntentKey.ElementRef.ElementID)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("fan-out barrier generation suppression lost exact pending owner")
		}
		if err := effects.Add(runID, privaterunforkrevision.FamilyFanOutObligations); err != nil {
			return err
		}
	}
	return nil
}

func terminalizeDeadLetteredFanOutBarrierOutcomesTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *privaterunforkrevision.Effects,
	runID string,
	selectedNow time.Time,
) error {
	query := `
		SELECT triggering_delivery_id, package_key, element_id, schedule_key
		FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND status='closed_pending'
		ORDER BY triggering_delivery_id, package_key, element_id`
	if postgres {
		query += " FOR UPDATE"
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return err
	}
	type pending struct {
		triggeringDeliveryID string
		packageKey           string
		elementID            string
		scheduleKey          string
	}
	pendingRows := make([]pending, 0)
	for rows.Next() {
		var item pending
		if err := rows.Scan(&item.triggeringDeliveryID, &item.packageKey, &item.elementID, &item.scheduleKey); err != nil {
			rows.Close()
			return err
		}
		pendingRows = append(pendingRows, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	adapter := sqliteDeliveryAdapter
	if postgres {
		adapter = postgresDeliveryAdapter
	}
	for _, item := range pendingRows {
		var occurrenceEventID sql.NullString
		var scheduleStatus string
		err := tx.QueryRowContext(ctx, `
			SELECT occurrence_event_id, status
			FROM timers
			WHERE run_id=$1 AND schedule_key=$2 AND task_type='timer'
		`, runID, item.scheduleKey).Scan(&occurrenceEventID, &scheduleStatus)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("closed fan-out barrier schedule %s is missing", item.scheduleKey)
		}
		if err != nil {
			return err
		}
		if !occurrenceEventID.Valid {
			if strings.TrimSpace(scheduleStatus) == string(runtimegenericschedule.StatusFailed) {
				return fmt.Errorf("fan-out barrier schedule %s failed before a canonical completion delivery", item.scheduleKey)
			}
			continue
		}
		_, settlement, err := loadFanOutSourceEvent(ctx, tx, occurrenceEventID.String, postgres)
		if err != nil {
			return fmt.Errorf("load fan-out barrier completion event: %w", err)
		}
		deliveries, err := adapter.SnapshotsForEvent(ctx, tx, occurrenceEventID.String)
		if err != nil {
			return err
		}
		deadLettered := settlement.NoDelivery() && len(deliveries) == 0
		if settlement.Delivered() {
			if len(deliveries) != 1 {
				return fmt.Errorf("fan-out barrier completion event %s has %d deliveries, want exactly one", occurrenceEventID.String, len(deliveries))
			}
			if !deliveries[0].Terminal() {
				continue
			}
			deadLettered = deliveries[0].Status == runtimedelivery.StatusDeadLetter
		} else if !deadLettered {
			return fmt.Errorf("fan-out barrier completion route settlement is contradictory")
		}
		if !deadLettered {
			return fmt.Errorf("delivered fan-out barrier completion %s remained closed_pending", occurrenceEventID.String)
		}
		update := `
			UPDATE fan_out_obligation_barriers
			SET status=$1, updated_at=$2
			WHERE run_id=$3 AND triggering_delivery_id=$4 AND package_key=$5 AND element_id=$6 AND status='closed_pending' AND schedule_key=$7`
		if !postgres {
			update = postgresPlaceholdersToSQLite(update, 7)
		}
		result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusOutcomeDeadLettered), selectedNow.UTC(), runID,
			item.triggeringDeliveryID, item.packageKey, item.elementID, item.scheduleKey)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("fan-out barrier dead-letter terminalization lost exact pending owner")
		}
		if err := effects.Add(runID, privaterunforkrevision.FamilyFanOutObligations); err != nil {
			return err
		}
	}
	return nil
}

func suppressRunTerminalFanOutBarriersTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *privaterunforkrevision.Effects,
	runID string,
	at time.Time,
) error {
	query := `
		SELECT triggering_delivery_id, package_key, element_id, status, summary
		FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND status IN ('armed','closed_pending')
		ORDER BY triggering_delivery_id, package_key, element_id`
	if postgres {
		query += " FOR UPDATE"
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return err
	}
	type candidate struct {
		key        fanoutobligation.IntentKey
		status     fanoutbarrier.Status
		summaryRaw any
	}
	candidates := make([]candidate, 0)
	for rows.Next() {
		var item candidate
		item.key.RunID = runID
		var status string
		if err := rows.Scan(&item.key.TriggeringDeliveryID, &item.key.ElementRef.PackageKey, &item.key.ElementRef.ElementID, &status, &item.summaryRaw); err != nil {
			rows.Close()
			return err
		}
		item.status = fanoutbarrier.Status(strings.TrimSpace(status))
		candidates = append(candidates, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, candidate := range candidates {
		var summary fanoutbarrier.Summary
		switch candidate.status {
		case fanoutbarrier.StatusArmed:
			fold, err := foldFanOutIntentTerminalDispositions(ctx, tx, postgres, candidate.key)
			if err != nil {
				return err
			}
			if !fold.Terminal() {
				return fmt.Errorf("run-terminal fan-out barrier %s did not reach a complete disposition fold", candidate.key.String())
			}
			summary = fold.Summary
		case fanoutbarrier.StatusClosedPending:
			if err := json.Unmarshal(jsonRawMessageValue(candidate.summaryRaw), &summary); err != nil {
				return fmt.Errorf("decode closed fan-out barrier summary: %w", err)
			}
			if err := summary.Validate(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("run-terminal fan-out barrier has invalid source status %q", candidate.status)
		}
		summaryRaw, err := json.Marshal(summary)
		if err != nil {
			return err
		}
		update := `
			UPDATE fan_out_obligation_barriers
			SET status=$1, summary=$2, updated_at=$3
			WHERE run_id=$4 AND triggering_delivery_id=$5 AND package_key=$6 AND element_id=$7 AND status=$8`
		if postgres {
			update = strings.ReplaceAll(update, "$2", "$2::jsonb")
		} else {
			update = postgresPlaceholdersToSQLite(update, 8)
		}
		result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusSuppressedRunTerminal), string(summaryRaw), at.UTC(),
			candidate.key.RunID, candidate.key.TriggeringDeliveryID, candidate.key.ElementRef.PackageKey, candidate.key.ElementRef.ElementID, string(candidate.status))
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("run-terminal fan-out barrier suppression lost exact owner")
		}
		if err := effects.Add(runID, privaterunforkrevision.FamilyFanOutObligations); err != nil {
			return err
		}
	}
	return nil
}

func summarizeFanOutDeliveryBarriersRun(ctx context.Context, queryer pipelineQueryer, runID string) (fanoutbarrier.RunSummary, error) {
	summary := fanoutbarrier.RunSummary{RunID: strings.TrimSpace(runID)}
	if summary.RunID == "" {
		return summary, fmt.Errorf("fan-out barrier summary requires run identity")
	}
	err := queryer.QueryRowContext(ctx, `
		SELECT COUNT(*),
		       COALESCE(SUM(CASE WHEN status='armed' THEN 1 ELSE 0 END),0),
		       COALESCE(SUM(CASE WHEN status='closed_pending' THEN 1 ELSE 0 END),0),
		       COALESCE(SUM(CASE WHEN status NOT IN ('armed','closed_pending') THEN 1 ELSE 0 END),0)
		FROM fan_out_obligation_barriers WHERE run_id=$1
	`, summary.RunID).Scan(&summary.Intents, &summary.Armed, &summary.ClosedPending, &summary.Terminal)
	if err != nil {
		return summary, err
	}
	return summary, summary.Validate()
}

func (s *PipelinePostgresOwner) AdvanceFanOutDeliveryBarriersTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, runID string, selectedNow time.Time) error {
	return advanceFanOutDeliveryBarriersTx(ctx, tx, true, effects, s.genericSchedules, runID, selectedNow)
}

func (s *PipelineSQLiteOwner) AdvanceFanOutDeliveryBarriersTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, runID string, selectedNow time.Time) error {
	return advanceFanOutDeliveryBarriersTx(ctx, tx, false, effects, s.genericSchedules, runID, selectedNow)
}

func (s *PipelinePostgresOwner) SummarizeFanOutDeliveryBarriersRunTx(ctx context.Context, tx *sql.Tx, runID string) (fanoutbarrier.RunSummary, error) {
	return summarizeFanOutDeliveryBarriersRun(ctx, tx, runID)
}

func (s *PipelineSQLiteOwner) SummarizeFanOutDeliveryBarriersRunTx(ctx context.Context, tx *sql.Tx, runID string) (fanoutbarrier.RunSummary, error) {
	return summarizeFanOutDeliveryBarriersRun(ctx, tx, runID)
}

func (s *PipelinePostgresOwner) MaterializeRunForkFanOutBarrierTx(
	ctx context.Context,
	tx *sql.Tx,
	effects *privaterunforkrevision.Effects,
	forkRunID string,
	source fanoutbarrier.Barrier,
	selectedRef runtimecontracts.FanOutPlanRef,
	at time.Time,
) error {
	return materializeRunForkFanOutBarrierTx(ctx, tx, true, effects, s.genericSchedules, forkRunID, source, selectedRef, at)
}

func (s *PipelineSQLiteOwner) MaterializeRunForkFanOutBarrierTx(
	ctx context.Context,
	tx *sql.Tx,
	effects *privaterunforkrevision.Effects,
	forkRunID string,
	source fanoutbarrier.Barrier,
	selectedRef runtimecontracts.FanOutPlanRef,
	at time.Time,
) error {
	return materializeRunForkFanOutBarrierTx(ctx, tx, false, effects, s.genericSchedules, forkRunID, source, selectedRef, at)
}

func materializeRunForkFanOutBarrierTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *privaterunforkrevision.Effects,
	genericSchedules GenericScheduleTxOwner,
	forkRunID string,
	source fanoutbarrier.Barrier,
	selectedRef runtimecontracts.FanOutPlanRef,
	at time.Time,
) error {
	if err := source.Validate(); err != nil {
		return fmt.Errorf("source fork fan-out barrier: %w", err)
	}
	if strings.TrimSpace(forkRunID) == "" || at.IsZero() || effects == nil {
		return fmt.Errorf("fork fan-out barrier materialization requires run, time, and revision owner")
	}
	sourceJoin, _ := source.Registration.Handle.JoinRef()
	selectedJoin, err := timeridentity.NewFanOutDeliveryJoinRef(
		sourceJoin.Node(), sourceJoin.HandlerEvent(), sourceJoin.JoinID(),
		selectedRef.ElementRef.PackageKey, selectedRef.ElementRef.ElementID,
		selectedRef.BundleHash, selectedRef.SemanticDigest,
	)
	if err != nil {
		return err
	}
	selectedJoin, err = selectedJoin.BindFanOutIntent(source.Registration.IntentKey.TriggeringDeliveryID, sourceJoin.Generation())
	if err != nil {
		return err
	}
	handle, err := timeridentity.JoinCompleteHandle(selectedJoin)
	if err != nil {
		return err
	}
	registration := source.Registration
	registration.IntentKey.RunID = strings.TrimSpace(forkRunID)
	registration.IntentKey.ElementRef = selectedRef.ElementRef
	registration.Handle = handle
	registration.CreatedAt = at.UTC()
	if err := commitFanOutBarrierRegistrationTx(ctx, tx, postgres, registration); err != nil {
		return err
	}
	if source.Status == fanoutbarrier.StatusArmed {
		return effects.Add(forkRunID, privaterunforkrevision.FamilyFanOutObligations)
	}
	var summaryRaw any
	if source.Summary != nil {
		raw, err := json.Marshal(source.Summary)
		if err != nil {
			return err
		}
		summaryRaw = string(raw)
	}
	var scheduleKey any
	if source.Status == fanoutbarrier.StatusClosedPending {
		if genericSchedules == nil || source.Summary == nil {
			return fmt.Errorf("closed fork fan-out barrier requires generic schedule owner and summary")
		}
		command, err := fanOutBarrierSchedule(registration, *source.Summary, at)
		if err != nil {
			return err
		}
		if _, err := genericSchedules.AdmitTx(ctx, tx, effects, command); err != nil {
			return fmt.Errorf("admit fork fan-out barrier completion: %w", err)
		}
		scheduleKey = command.ScheduleKey
	} else if source.Status == fanoutbarrier.StatusFired || source.Status == fanoutbarrier.StatusOutcomeDeadLettered {
		scheduleKey = handle.TaskID()
	}
	query := `
		UPDATE fan_out_obligation_barriers
		SET status=$1, summary=$2, schedule_key=$3, updated_at=$4
		WHERE run_id=$5 AND triggering_delivery_id=$6 AND package_key=$7 AND element_id=$8 AND status='armed'`
	if postgres {
		query = strings.ReplaceAll(query, "$2", "$2::jsonb")
	} else {
		query = postgresPlaceholdersToSQLite(query, 8)
	}
	result, err := tx.ExecContext(ctx, query, string(source.Status), summaryRaw, scheduleKey, at.UTC(),
		registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
		registration.IntentKey.ElementRef.PackageKey, registration.IntentKey.ElementRef.ElementID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("fork fan-out barrier materialization lost exact armed owner")
	}
	return effects.Add(forkRunID, privaterunforkrevision.FamilyFanOutObligations)
}

func commitFanOutBarrierRegistrationTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	registration fanoutbarrier.Registration,
) error {
	if tx == nil {
		return fmt.Errorf("fan-out delivery barrier requires private transaction")
	}
	if err := registration.Validate(); err != nil {
		return err
	}
	ref, _ := registration.Handle.JoinRef()
	fanOut, _ := ref.FanOutDelivery()
	handle, err := json.Marshal(registration.Handle)
	if err != nil {
		return fmt.Errorf("encode fan-out delivery barrier handle: %w", err)
	}
	routingSource, err := json.Marshal(registration.RoutingSource)
	if err != nil {
		return fmt.Errorf("encode fan-out delivery barrier routing source: %w", err)
	}
	query := `
		INSERT INTO fan_out_obligation_barriers (
			run_id, triggering_delivery_id, package_key, element_id,
			bundle_hash, semantic_digest, target_package_key, target_flow_id,
			target_node_id, handler_event, join_id,
			route_scope_key, route_instance_id, route_instance_path,
			entity_id, routing_source, execution_mode, timer_handle, status, created_at, updated_at
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$20
		)`
	if postgres {
		query = strings.ReplaceAll(query, "$16", "$16::jsonb")
		query = strings.ReplaceAll(query, "$18", "$18::jsonb")
	} else {
		query = postgresPlaceholdersToSQLite(query, 20)
	}
	_, err = tx.ExecContext(ctx, query,
		registration.IntentKey.RunID,
		registration.IntentKey.TriggeringDeliveryID,
		registration.IntentKey.ElementRef.PackageKey,
		registration.IntentKey.ElementRef.ElementID,
		fanOut.BundleHash(),
		fanOut.SemanticDigest(),
		ref.Node().PackageKey(),
		ref.Node().FlowID(),
		ref.Node().NodeID(),
		ref.HandlerEvent(),
		ref.JoinID(),
		registration.Route.ScopeKey,
		registration.Route.InstanceID,
		registration.Route.InstancePath,
		registration.EntityID,
		string(routingSource),
		string(registration.ExecutionMode),
		string(handle),
		string(fanoutbarrier.StatusArmed),
		registration.CreatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert fan-out delivery barrier: %w", err)
	}
	return nil
}

func commitFanOutBarrierCompletionTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects *privaterunforkrevision.Effects,
	runID string,
	completion fanoutbarrier.Completion,
	updatedAt time.Time,
) error {
	if tx == nil || effects == nil || updatedAt.IsZero() {
		return fmt.Errorf("fan-out barrier completion requires transaction, effects, and selected-store time")
	}
	key, err := completion.IntentKey(runID)
	if err != nil {
		return err
	}
	query := `
		SELECT status, summary, schedule_key
		FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND package_key=$3 AND element_id=$4`
	if postgres {
		query += " FOR UPDATE"
	}
	var status string
	var summaryRaw any
	var scheduleKey sql.NullString
	err = tx.QueryRowContext(ctx, query, key.RunID, key.TriggeringDeliveryID, key.ElementRef.PackageKey, key.ElementRef.ElementID).Scan(&status, &summaryRaw, &scheduleKey)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("fan-out barrier completion owner is missing")
	}
	if err != nil {
		return err
	}
	var persisted fanoutbarrier.Summary
	if err := json.Unmarshal(jsonRawMessageValue(summaryRaw), &persisted); err != nil {
		return fmt.Errorf("decode fan-out barrier completion summary: %w", err)
	}
	if persisted != completion.Summary || !scheduleKey.Valid || strings.TrimSpace(scheduleKey.String) != completion.Handle.TaskID() {
		return fmt.Errorf("fan-out barrier completion contradicts its closed schedule")
	}
	switch fanoutbarrier.Status(strings.TrimSpace(status)) {
	case fanoutbarrier.StatusFired:
		return nil
	case fanoutbarrier.StatusClosedPending:
	default:
		return fmt.Errorf("fan-out barrier completion requires closed_pending owner, got %q", status)
	}
	update := `
		UPDATE fan_out_obligation_barriers
		SET status=$1, updated_at=$2
		WHERE run_id=$3 AND triggering_delivery_id=$4 AND package_key=$5 AND element_id=$6 AND status='closed_pending' AND schedule_key=$7`
	if !postgres {
		update = postgresPlaceholdersToSQLite(update, 7)
	}
	result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusFired), updatedAt.UTC(), key.RunID, key.TriggeringDeliveryID, key.ElementRef.PackageKey, key.ElementRef.ElementID, completion.Handle.TaskID())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("fan-out barrier completion lost exact closed owner")
	}
	return effects.Add(runID, privaterunforkrevision.FamilyFanOutObligations)
}
