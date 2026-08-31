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
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5
	`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&cardinality, &cursor, &rawStatus)
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
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5
		ORDER BY ordinal
	`, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath)
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
) ([]fanoutbarrier.Barrier, error) {
	if !status.Valid() {
		return nil, fmt.Errorf("fan-out barrier load requires valid status")
	}
	query := `
		SELECT triggering_delivery_id, flow_path, declaration_family, semantic_path,
		       bundle_hash, semantic_digest, target_flow_path,
		       target_node_id, handler_event, join_id,
		       route_scope_key, route_instance_id, route_instance_path,
		       entity_id, routing_source, execution_mode, timer_handle,
		       summary, schedule_key, schedule_activation_id, created_at, updated_at
		FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND status=$2
		ORDER BY triggering_delivery_id, flow_path, declaration_family, semantic_path`
	if postgres {
		query += " FOR UPDATE"
	}
	rows, err := tx.QueryContext(ctx, query, runID, string(status))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	type persisted struct {
		barrier            fanoutbarrier.Barrier
		bundleHash         string
		digest             string
		targetFlowPath     string
		targetNode         string
		handlerEvent       string
		joinID             string
		routingRaw         []byte
		handleRaw          []byte
		summaryRaw         []byte
		execution          string
		scheduleKey        sql.NullString
		scheduleActivation sql.NullString
	}
	persistedRows := make([]persisted, 0)
	for rows.Next() {
		var item persisted
		item.barrier.Registration.IntentKey.RunID = runID
		item.barrier.Status = status
		var routingRaw, handleRaw, summaryRaw, createdAtRaw, updatedAtRaw any
		if err := rows.Scan(
			&item.barrier.Registration.IntentKey.TriggeringDeliveryID,
			&item.barrier.Registration.IntentKey.ElementRef.FlowPath,
			&item.barrier.Registration.IntentKey.ElementRef.Family,
			&item.barrier.Registration.IntentKey.ElementRef.SemanticPath,
			&item.bundleHash, &item.digest, &item.targetFlowPath,
			&item.targetNode, &item.handlerEvent, &item.joinID,
			&item.barrier.Registration.Route.ScopeKey, &item.barrier.Registration.Route.InstanceID, &item.barrier.Registration.Route.InstancePath,
			&item.barrier.Registration.EntityID, &routingRaw, &item.execution, &handleRaw,
			&summaryRaw, &item.scheduleKey, &item.scheduleActivation, &createdAtRaw, &updatedAtRaw,
		); err != nil {
			return nil, err
		}
		createdAt, present, err := sqliteTimeValue(createdAtRaw)
		if err != nil || !present {
			return nil, fmt.Errorf("decode fan-out barrier created time: %w", err)
		}
		updatedAt, updatedPresent, err := sqliteTimeValue(updatedAtRaw)
		if err != nil || !updatedPresent {
			return nil, fmt.Errorf("decode fan-out barrier updated time: %w", err)
		}
		item.barrier.Registration.CreatedAt = createdAt
		item.barrier.UpdatedAt = updatedAt
		item.routingRaw = append([]byte(nil), jsonRawMessageValue(routingRaw)...)
		item.handleRaw = append([]byte(nil), jsonRawMessageValue(handleRaw)...)
		item.summaryRaw = append([]byte(nil), jsonRawMessageValue(summaryRaw)...)
		persistedRows = append(persistedRows, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]fanoutbarrier.Barrier, 0, len(persistedRows))
	for _, item := range persistedRows {
		item.barrier.Registration.PlanRef = runtimecontracts.FanOutPlanRef{
			BundleHash: item.bundleHash, ElementRef: item.barrier.Registration.IntentKey.ElementRef, SemanticDigest: item.digest,
		}
		if err := json.Unmarshal(item.routingRaw, &item.barrier.Registration.RoutingSource); err != nil {
			return nil, fmt.Errorf("decode fan-out barrier routing source: %w", err)
		}
		if err := json.Unmarshal(item.handleRaw, &item.barrier.Registration.Handle); err != nil {
			return nil, fmt.Errorf("decode fan-out barrier timer handle: %w", err)
		}
		if len(item.summaryRaw) != 0 && string(item.summaryRaw) != "null" {
			var summary fanoutbarrier.Summary
			if err := json.Unmarshal(item.summaryRaw, &summary); err != nil {
				return nil, fmt.Errorf("decode fan-out barrier summary: %w", err)
			}
			item.barrier.Summary = &summary
		}
		item.barrier.ScheduleKey = strings.TrimSpace(item.scheduleKey.String)
		item.barrier.ScheduleActivationID = strings.TrimSpace(item.scheduleActivation.String)
		mode, ok := executionmode.Parse(item.execution)
		if !ok {
			return nil, fmt.Errorf("fan-out barrier execution mode %q is invalid", item.execution)
		}
		item.barrier.Registration.ExecutionMode = mode
		if err := item.barrier.Validate(); err != nil {
			return nil, err
		}
		ref, _ := item.barrier.Registration.Handle.JoinRef()
		fanOut, _ := ref.FanOutDelivery()
		intentDeclaration, identityErr := item.barrier.Registration.IntentKey.ElementRef.DeclarationIdentity()
		if identityErr != nil || !fanOut.DeclarationIdentity().Equal(intentDeclaration) ||
			fanOut.BundleHash() != strings.TrimSpace(item.bundleHash) || fanOut.SemanticDigest() != strings.TrimSpace(item.digest) ||
			ref.FlowPath() != strings.TrimSpace(item.targetFlowPath) ||
			ref.NodeID() != strings.TrimSpace(item.targetNode) || ref.HandlerEvent() != strings.TrimSpace(item.handlerEvent) ||
			ref.JoinID() != strings.TrimSpace(item.joinID) {
			return nil, fmt.Errorf("fan-out barrier persisted identity contradicts its typed handle")
		}
		out = append(out, item.barrier)
	}
	return out, nil
}

func loadArmedFanOutBarriers(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]fanoutbarrier.Registration, error) {
	barriers, err := loadFanOutBarriersByStatus(ctx, tx, postgres, runID, fanoutbarrier.StatusArmed)
	if err != nil {
		return nil, err
	}
	registrations := make([]fanoutbarrier.Registration, 0, len(barriers))
	for _, barrier := range barriers {
		registrations = append(registrations, barrier.Registration)
	}
	return registrations, nil
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
) ([]runtimerunlifecycle.CommittedGenericScheduleActivation, error) {
	if tx == nil || effects == nil || genericSchedules == nil || strings.TrimSpace(runID) == "" || selectedNow.IsZero() {
		return nil, fmt.Errorf("fan-out barrier advancement requires transaction, owners, run, and selected-store time")
	}
	registrations, err := loadArmedFanOutBarriers(ctx, tx, postgres, runID)
	if err != nil {
		return nil, err
	}
	activations := make([]runtimerunlifecycle.CommittedGenericScheduleActivation, 0, len(registrations))
	for _, registration := range registrations {
		current, err := fanOutBarrierGenerationCurrent(ctx, tx, registration)
		if err != nil {
			return nil, err
		}
		if !current {
			if err := suppressSupersededArmedFanOutBarrierTx(ctx, tx, postgres, effects, registration, selectedNow); err != nil {
				return nil, err
			}
			continue
		}
		fold, err := foldFanOutIntentTerminalDispositions(ctx, tx, postgres, registration.IntentKey)
		if err != nil {
			return nil, err
		}
		if !fold.Terminal() {
			continue
		}
		status := fanoutbarrier.StatusClosedPending
		scheduleKey := any(nil)
		scheduleActivationID := any(nil)
		command, err := fanOutBarrierSchedule(registration, fold.Summary, selectedNow)
		if err != nil {
			return nil, err
		}
		admitted, err := genericSchedules.AdmitTx(ctx, tx, effects, command)
		if err != nil {
			return nil, fmt.Errorf("admit fan-out barrier completion: %w", err)
		}
		if err := admitted.Validate(); err != nil {
			return nil, fmt.Errorf("validate admitted fan-out barrier completion: %w", err)
		}
		activation, err := runtimerunlifecycle.NewCommittedGenericScheduleActivation(admitted.Activation.ID)
		if err != nil {
			return nil, err
		}
		activations = append(activations, activation)
		scheduleKey = command.ScheduleKey
		scheduleActivationID = admitted.Activation.ID
		summaryRaw, err := json.Marshal(fold.Summary)
		if err != nil {
			return nil, err
		}
		query := `
			UPDATE fan_out_obligation_barriers
			SET status=$1, summary=$2, schedule_key=$3, schedule_activation_id=$4, updated_at=$5
			WHERE run_id=$6 AND triggering_delivery_id=$7 AND flow_path=$8 AND declaration_family=$9 AND semantic_path=$10 AND status='armed'`
		if postgres {
			query = strings.ReplaceAll(query, "$2", "$2::jsonb")
		} else {
			query = postgresPlaceholdersToSQLite(query, 10)
		}
		result, err := tx.ExecContext(ctx, query, string(status), string(summaryRaw), scheduleKey, scheduleActivationID, selectedNow.UTC(),
			registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
			registration.IntentKey.ElementRef.FlowPath, registration.IntentKey.ElementRef.Family, registration.IntentKey.ElementRef.SemanticPath)
		if err != nil {
			return nil, err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return nil, fmt.Errorf("fan-out barrier close lost exact armed owner")
		}
		if err := effects.Add(runID, privaterunforkrevision.FamilyFanOutObligations); err != nil {
			return nil, err
		}
	}
	if err := suppressSupersededPendingFanOutBarriersTx(ctx, tx, postgres, effects, genericSchedules, runID, selectedNow); err != nil {
		return nil, err
	}
	if err := terminalizeDeadLetteredFanOutBarrierOutcomesTx(ctx, tx, postgres, effects, runID, selectedNow); err != nil {
		return nil, err
	}
	return activations, nil
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
		WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND status='armed'`
	if !postgres {
		query = postgresPlaceholdersToSQLite(query, 7)
	}
	result, err := tx.ExecContext(ctx, query, string(fanoutbarrier.StatusSuppressedGenerationSuperseded), at.UTC(),
		registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
		registration.IntentKey.ElementRef.FlowPath, registration.IntentKey.ElementRef.Family, registration.IntentKey.ElementRef.SemanticPath)
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
	genericSchedules GenericScheduleTxOwner,
	runID string,
	at time.Time,
) error {
	barriers, err := loadFanOutBarriersByStatus(ctx, tx, postgres, runID, fanoutbarrier.StatusClosedPending)
	if err != nil {
		return err
	}
	for _, barrier := range barriers {
		registration := barrier.Registration
		current, err := fanOutBarrierGenerationCurrent(ctx, tx, registration)
		if err != nil {
			return err
		}
		if current {
			continue
		}
		if err := cancelSupersededFanOutBarrierScheduleTx(ctx, tx, effects, genericSchedules, barrier, at); err != nil {
			return err
		}
		update := `
			UPDATE fan_out_obligation_barriers
			SET status=$1, updated_at=$2
			WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND status='closed_pending' AND schedule_key=$8 AND schedule_activation_id=$9`
		if !postgres {
			update = postgresPlaceholdersToSQLite(update, 9)
		}
		result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusSuppressedGenerationSuperseded), at.UTC(),
			registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
			registration.IntentKey.ElementRef.FlowPath, registration.IntentKey.ElementRef.Family, registration.IntentKey.ElementRef.SemanticPath,
			barrier.ScheduleKey, barrier.ScheduleActivationID)
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

func cancelSupersededFanOutBarrierScheduleTx(
	ctx context.Context,
	tx *sql.Tx,
	effects *privaterunforkrevision.Effects,
	genericSchedules GenericScheduleTxOwner,
	barrier fanoutbarrier.Barrier,
	at time.Time,
) error {
	if genericSchedules == nil || barrier.Summary == nil {
		return fmt.Errorf("superseded pending fan-out barrier requires its generic schedule owner and summary")
	}
	activation, found, err := genericSchedules.LoadActivationTx(ctx, tx, barrier.ScheduleActivationID)
	if err != nil {
		return fmt.Errorf("load superseded fan-out barrier schedule: %w", err)
	}
	if !found {
		return fmt.Errorf("superseded fan-out barrier schedule activation %s is missing", barrier.ScheduleActivationID)
	}
	expected, err := fanOutBarrierSchedule(barrier.Registration, *barrier.Summary, activation.InitialDueAt)
	if err != nil {
		return err
	}
	expectedHash, err := expected.ImmutableHash()
	if err != nil {
		return err
	}
	if activation.ID != strings.TrimSpace(barrier.ScheduleActivationID) ||
		activation.Command.ScheduleKey != strings.TrimSpace(barrier.ScheduleKey) ||
		activation.Command.TaskID != barrier.Registration.Handle.TaskID() ||
		activation.ImmutableHash != expectedHash {
		return fmt.Errorf("superseded fan-out barrier schedule activation contradicts its exact barrier owner")
	}
	cancelled, err := genericSchedules.CancelActivationTx(ctx, tx, effects, runtimegenericschedule.CancelCommand{
		ActivationID: activation.ID,
		Cause:        "fan_out_generation_superseded",
		CancelledAt:  at,
	})
	if err != nil {
		return fmt.Errorf("cancel superseded fan-out barrier schedule: %w", err)
	}
	switch cancelled.Outcome {
	case runtimegenericschedule.CancelChanged:
		if cancelled.Activation.Status != runtimegenericschedule.StatusCancelled {
			return fmt.Errorf("superseded fan-out barrier schedule cancellation did not terminalize its activation")
		}
	case runtimegenericschedule.CancelTerminal:
		if cancelled.Activation.Status != runtimegenericschedule.StatusFired {
			return fmt.Errorf("superseded fan-out barrier schedule reached unexpected terminal status %q", cancelled.Activation.Status)
		}
	default:
		return fmt.Errorf("superseded fan-out barrier schedule cancellation outcome is %q", cancelled.Outcome)
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
		SELECT triggering_delivery_id, flow_path, declaration_family, semantic_path, schedule_key, schedule_activation_id
		FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND status='closed_pending'
		ORDER BY triggering_delivery_id, flow_path, declaration_family, semantic_path`
	if postgres {
		query += " FOR UPDATE"
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return err
	}
	type pending struct {
		triggeringDeliveryID string
		flowPath             string
		declarationFamily    string
		semanticPath         string
		scheduleKey          string
		scheduleActivationID string
	}
	pendingRows := make([]pending, 0)
	for rows.Next() {
		var item pending
		if err := rows.Scan(&item.triggeringDeliveryID, &item.flowPath, &item.declarationFamily, &item.semanticPath, &item.scheduleKey, &item.scheduleActivationID); err != nil {
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
			WHERE timer_id=$1 AND run_id=$2 AND schedule_key=$3 AND task_type='timer'
		`, item.scheduleActivationID, runID, item.scheduleKey).Scan(&occurrenceEventID, &scheduleStatus)
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
			WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND status='closed_pending' AND schedule_key=$8`
		if !postgres {
			update = postgresPlaceholdersToSQLite(update, 8)
		}
		result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusOutcomeDeadLettered), selectedNow.UTC(), runID,
			item.triggeringDeliveryID, item.flowPath, item.declarationFamily, item.semanticPath, item.scheduleKey)
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
		SELECT triggering_delivery_id, flow_path, declaration_family, semantic_path, status, summary
		FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND status IN ('armed','closed_pending')
		ORDER BY triggering_delivery_id, flow_path, declaration_family, semantic_path`
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
		if err := rows.Scan(&item.key.TriggeringDeliveryID, &item.key.ElementRef.FlowPath, &item.key.ElementRef.Family, &item.key.ElementRef.SemanticPath, &status, &item.summaryRaw); err != nil {
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
			WHERE run_id=$4 AND triggering_delivery_id=$5 AND flow_path=$6 AND declaration_family=$7 AND semantic_path=$8 AND status=$9`
		if postgres {
			update = strings.ReplaceAll(update, "$2", "$2::jsonb")
		} else {
			update = postgresPlaceholdersToSQLite(update, 9)
		}
		result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusSuppressedRunTerminal), string(summaryRaw), at.UTC(),
			candidate.key.RunID, candidate.key.TriggeringDeliveryID, candidate.key.ElementRef.FlowPath, candidate.key.ElementRef.Family, candidate.key.ElementRef.SemanticPath, string(candidate.status))
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

func (s *PipelinePostgresOwner) AdvanceFanOutDeliveryBarriersTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, runID string, selectedNow time.Time) ([]runtimerunlifecycle.CommittedGenericScheduleActivation, error) {
	return advanceFanOutDeliveryBarriersTx(ctx, tx, true, effects, s.genericSchedules, runID, selectedNow)
}

func (s *PipelineSQLiteOwner) AdvanceFanOutDeliveryBarriersTx(ctx context.Context, tx *sql.Tx, effects *privaterunforkrevision.Effects, runID string, selectedNow time.Time) ([]runtimerunlifecycle.CommittedGenericScheduleActivation, error) {
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
	fanOutDeclaration, identityErr := selectedRef.ElementRef.DeclarationIdentity()
	if identityErr != nil {
		return fmt.Errorf("selected fan-out declaration: %w", identityErr)
	}
	selectedJoin, err := timeridentity.NewFanOutDeliveryJoinRef(
		sourceJoin.Node(), sourceJoin.HandlerEvent(), sourceJoin.JoinID(),
		fanOutDeclaration,
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
	registration.PlanRef = selectedRef
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
	var scheduleActivationID any
	if source.Status == fanoutbarrier.StatusClosedPending {
		if genericSchedules == nil || source.Summary == nil {
			return fmt.Errorf("closed fork fan-out barrier requires generic schedule owner and summary")
		}
		command, err := fanOutBarrierSchedule(registration, *source.Summary, at)
		if err != nil {
			return err
		}
		admitted, err := genericSchedules.AdmitTx(ctx, tx, effects, command)
		if err != nil {
			return fmt.Errorf("admit fork fan-out barrier completion: %w", err)
		}
		scheduleKey = command.ScheduleKey
		scheduleActivationID = admitted.Activation.ID
	} else if source.Status == fanoutbarrier.StatusFired || source.Status == fanoutbarrier.StatusOutcomeDeadLettered {
		scheduleKey = handle.TaskID()
	}
	query := `
		UPDATE fan_out_obligation_barriers
		SET status=$1, summary=$2, schedule_key=$3, schedule_activation_id=$4, updated_at=$5
		WHERE run_id=$6 AND triggering_delivery_id=$7 AND flow_path=$8 AND declaration_family=$9 AND semantic_path=$10 AND status='armed'`
	if postgres {
		query = strings.ReplaceAll(query, "$2", "$2::jsonb")
	} else {
		query = postgresPlaceholdersToSQLite(query, 10)
	}
	result, err := tx.ExecContext(ctx, query, string(source.Status), summaryRaw, scheduleKey, scheduleActivationID, at.UTC(),
		registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
		registration.IntentKey.ElementRef.FlowPath, registration.IntentKey.ElementRef.Family, registration.IntentKey.ElementRef.SemanticPath)
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
			run_id, triggering_delivery_id, flow_path, declaration_family, semantic_path,
			bundle_hash, semantic_digest, target_flow_path,
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
		registration.IntentKey.ElementRef.FlowPath,
		registration.IntentKey.ElementRef.Family,
		registration.IntentKey.ElementRef.SemanticPath,
		registration.PlanRef.BundleHash,
		registration.PlanRef.SemanticDigest,
		ref.Node().FlowPath(),
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
		SELECT status, summary, schedule_key, schedule_activation_id
		FROM fan_out_obligation_barriers
		WHERE run_id=$1 AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5`
	if postgres {
		query += " FOR UPDATE"
	}
	var status string
	var summaryRaw any
	var scheduleKey sql.NullString
	var scheduleActivationID sql.NullString
	err = tx.QueryRowContext(ctx, query, key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath).Scan(&status, &summaryRaw, &scheduleKey, &scheduleActivationID)
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
	if persisted != completion.Summary || !scheduleKey.Valid || strings.TrimSpace(scheduleKey.String) != completion.Handle.TaskID() || !scheduleActivationID.Valid || strings.TrimSpace(scheduleActivationID.String) == "" {
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
		WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND status='closed_pending' AND schedule_key=$8`
	if !postgres {
		update = postgresPlaceholdersToSQLite(update, 8)
	}
	result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusFired), updatedAt.UTC(), key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath, completion.Handle.TaskID())
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("fan-out barrier completion lost exact closed owner")
	}
	return effects.Add(runID, privaterunforkrevision.FamilyFanOutObligations)
}
