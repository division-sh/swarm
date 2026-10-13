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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/store/internal/workflowheader"
)

const fanOutFoldJoinedQuery = `
		SELECT i.cardinality, i.cursor, i.status, o.run_id IS NOT NULL,
		       o.ordinal, o.outcome_kind, o.event_id, o.source_event_id, o.inherited_disposition
		FROM fan_out_intents i
		LEFT JOIN fan_out_outcomes o ON o.run_id=i.run_id
		 AND o.triggering_delivery_id=i.triggering_delivery_id AND o.flow_path=i.flow_path
		 AND o.declaration_family=i.declaration_family AND o.semantic_path=i.semantic_path
		WHERE i.run_id=$1 AND i.triggering_delivery_id=$2 AND i.flow_path=$3 AND i.declaration_family=$4 AND i.semantic_path=$5
		ORDER BY o.ordinal
	`

const fanOutFoldJoinedDeploymentQuery = `
		SELECT i.cardinality, i.cursor, i.status, o.run_id IS NOT NULL,
		       o.ordinal, o.outcome_kind, o.event_id, o.source_event_id, o.inherited_disposition
		FROM fan_out_intents i
		LEFT JOIN fan_out_outcomes o ON o.run_id=i.run_id AND o.deployment_feed_id=i.deployment_feed_id
		WHERE i.run_id=$1 AND i.origin_kind='deployment' AND i.deployment_feed_id=$2
		ORDER BY o.ordinal
	`

// fanOutFoldStatement retains only the fixed statement within one private
// transaction loop. Preparation stays at the canonical fold's first SQL read,
// after key and generation validation; every fold still reads fresh rows.
type fanOutFoldStatement struct {
	*sql.Tx
	joined *sql.Stmt
}

func (q *fanOutFoldStatement) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if query != fanOutFoldJoinedQuery {
		return q.Tx.QueryContext(ctx, query, args...)
	}
	if q.joined == nil {
		stmt, err := q.Tx.PrepareContext(ctx, fanOutFoldJoinedQuery)
		if err != nil {
			return nil, err
		}
		q.joined = stmt
	}
	return q.joined.QueryContext(ctx, args...)
}

func (q *fanOutFoldStatement) close() error {
	if q.joined == nil {
		return nil
	}
	return q.joined.Close()
}

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
	query := fanOutFoldJoinedQuery
	args := []any{key.RunID, key.TriggeringDeliveryID, key.ElementRef.FlowPath, key.ElementRef.Family, key.ElementRef.SemanticPath}
	if key.DeploymentFeedID != "" {
		query = fanOutFoldJoinedDeploymentQuery
		args = []any{key.RunID, key.DeploymentFeedID}
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fanoutbarrier.Fold{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return fanoutbarrier.Fold{}, err
		}
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s is missing", key.String())
	}
	// Validate the header before converting any outcome, as in the two-query
	// owner. Presence uses a joined key, never a nullable/corrupt ordinal.
	var present bool
	var rawOutcome [5]any
	if err := rows.Scan(&cardinality, &cursor, &rawStatus, &present,
		&rawOutcome[0], &rawOutcome[1], &rawOutcome[2], &rawOutcome[3], &rawOutcome[4]); err != nil {
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
	type outcomeFact struct {
		ordinal       int
		kind          string
		eventID       sql.NullString
		sourceEventID sql.NullString
		disposition   sql.NullString
	}
	facts := make([]outcomeFact, 0, cursor)
	for {
		if present {
			var fact outcomeFact
			if err := rows.Scan(&cardinality, &cursor, &rawStatus, &present,
				&fact.ordinal, &fact.kind, &fact.eventID, &fact.sourceEventID, &fact.disposition); err != nil {
				return fanoutbarrier.Fold{}, err
			}
			facts = append(facts, fact)
		}
		if !rows.Next() {
			break
		}
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
	// Bound retained decoded events as well as SQL parameters. The original
	// compact ordinal facts remain the complete membership proof for the fold.
	for start := 0; start < len(facts); start += 128 {
		batch := facts[start:min(start+128, len(facts))]
		var eventIDs []string
		for _, fact := range batch {
			kind := fanoutobligation.OutcomeKind(strings.TrimSpace(fact.kind))
			if fact.eventID.Valid && ((kind == fanoutobligation.OutcomeCommitted && !fact.sourceEventID.Valid) || kind == fanoutobligation.OutcomeContinued) {
				eventIDs = append(eventIDs, fact.eventID.String)
			}
		}
		var admitted []eventrecord.AdmittedRecord
		if postgres {
			admitted, err = eventrecordpostgres.LoadAdmittedMany(ctx, db, eventIDs)
		} else {
			admitted, err = eventrecordsqlite.LoadAdmittedMany(ctx, db, eventIDs)
		}
		if err != nil {
			var corrupt *eventrecord.CorruptError
			if errors.As(err, &corrupt) {
				for _, fact := range batch {
					if fact.eventID.String == corrupt.EventID {
						return fanoutbarrier.Fold{}, fmt.Errorf("load fan-out ordinal %d settlement: %w", fact.ordinal, err)
					}
				}
			}
			return fanoutbarrier.Fold{}, fmt.Errorf("load fan-out settlement batch: %w", err)
		}
		settlements := make(map[string]eventrecord.AdmittedRecord, len(admitted))
		for i, record := range admitted {
			if record.Event.Event().RunID() != key.RunID {
				return fanoutbarrier.Fold{}, fmt.Errorf("fan-out outcome event %s belongs to another run", eventIDs[i])
			}
			settlements[eventIDs[i]] = record
		}
		deliveriesByEvent, err := adapter.SnapshotsForEvents(ctx, db, eventIDs)
		if err != nil {
			return fanoutbarrier.Fold{}, fmt.Errorf("load fan-out delivery batch: %w", err)
		}
		for offset, fact := range batch {
			ordinal := start + offset
			if fact.ordinal != ordinal {
				return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s outcome ordinal %d is not contiguous at %d", key.String(), fact.ordinal, ordinal)
			}
			switch fanoutobligation.OutcomeKind(strings.TrimSpace(fact.kind)) {
			case fanoutobligation.OutcomeSemanticRejected:
				if fact.eventID.Valid || fact.sourceEventID.Valid || fact.disposition.Valid {
					return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s rejected ordinal %d carries settlement identity", key.String(), ordinal)
				}
				fold.Summary.SemanticRejected++
			case fanoutobligation.OutcomeCommitted, fanoutobligation.OutcomeContinued:
				continued := fanoutobligation.OutcomeKind(strings.TrimSpace(fact.kind)) == fanoutobligation.OutcomeContinued
				if continued && (!fact.eventID.Valid || !fact.sourceEventID.Valid || strings.TrimSpace(fact.eventID.String) == "" ||
					strings.TrimSpace(fact.sourceEventID.String) == "" || fact.eventID.String == fact.sourceEventID.String ||
					(fact.disposition.Valid && (!fanoutobligation.InheritedTerminalDisposition(fact.disposition.String).Valid() ||
						fanoutobligation.InheritedTerminalDisposition(fact.disposition.String) == fanoutobligation.InheritedNoRoute))) {
					return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s continued ordinal %d has contradictory settlement evidence", key.String(), ordinal)
				}
				if fact.sourceEventID.Valid && !continued {
					if fact.eventID.Valid || !fact.disposition.Valid {
						return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s inherited ordinal %d has contradictory settlement evidence", key.String(), ordinal)
					}
					switch fanoutobligation.InheritedTerminalDisposition(strings.TrimSpace(fact.disposition.String)) {
					case fanoutobligation.InheritedSucceeded:
						fold.Summary.Succeeded++
					case fanoutobligation.InheritedDeadLettered:
						fold.Summary.DeadLettered++
					case fanoutobligation.InheritedCanceled:
						fold.Summary.Canceled++
					case fanoutobligation.InheritedNoRoute:
						fold.Summary.NoRoute++
					default:
						return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s inherited ordinal %d has invalid terminal disposition %q", key.String(), ordinal, fact.disposition.String)
					}
					continue
				}
				if !fact.eventID.Valid || (fact.disposition.Valid && !continued) {
					return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s owned ordinal %d has contradictory settlement evidence", key.String(), ordinal)
				}
				admitted := settlements[fact.eventID.String]
				settlement := admitted.Settlement
				deliveries := deliveriesByEvent[admitted.Event.ID()]
				for _, delivery := range deliveries {
					if delivery.RunID != key.RunID || delivery.EventID != admitted.Event.ID() {
						return fanoutbarrier.Fold{}, fmt.Errorf("fan-out ordinal %d delivery belongs to another run or event", ordinal)
					}
				}
				switch {
				case settlement.NoDelivery() && len(deliveries) == 0 && !continued:
					fold.Summary.NoRoute++
				case settlement.Delivered() && len(deliveries) > 0:
					var disposition fanoutobligation.InheritedTerminalDisposition
					var terminal bool
					var err error
					if continued {
						disposition, terminal, err = continuedFanOutRecipientDisposition(key.RunID, fact.sourceEventID.String, admitted, deliveries, fanoutobligation.InheritedTerminalDisposition(fact.disposition.String))
					} else {
						disposition, terminal, err = fanoutbarrier.RecipientDisposition(deliveries)
					}
					if err != nil {
						return fanoutbarrier.Fold{}, fmt.Errorf("fan-out ordinal %d recipient disposition: %w", ordinal, err)
					}
					if !terminal {
						fold.PendingCommitted++
					} else if disposition == fanoutobligation.InheritedDeadLettered {
						fold.Summary.DeadLettered++
					} else if disposition == fanoutobligation.InheritedCanceled {
						fold.Summary.Canceled++
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
	}
	if err := fold.Validate(); err != nil {
		return fanoutbarrier.Fold{}, fmt.Errorf("fan-out barrier intent %s: %w", key.String(), err)
	}
	return fold, nil
}

func continuedFanOutRecipientDisposition(runID, sourceEventID string, publication eventrecord.AdmittedRecord, deliveries []runtimedelivery.Snapshot, retained fanoutobligation.InheritedTerminalDisposition) (fanoutobligation.InheritedTerminalDisposition, bool, error) {
	event := publication.Event.Event()
	lineage, found := event.SelectedForkLineage()
	if !found || event.RunID() != runID || lineage.DestinationRunID() != runID || lineage.SourceEventID() != sourceEventID {
		return "", false, fmt.Errorf("continued fan-out event contradicts its exact source publication")
	}
	routes := make([]events.DeliveryRoute, 0, len(deliveries))
	for _, delivery := range deliveries {
		if delivery.RunID != runID || delivery.EventID != event.ID() {
			return "", false, fmt.Errorf("continued fan-out delivery belongs to another run or event")
		}
		routes = append(routes, delivery.Route)
	}
	if err := publication.Settlement.Validate(routes); err != nil {
		return "", false, fmt.Errorf("continued fan-out recipient settlement: %w", err)
	}
	if !publication.Settlement.Delivered() {
		return "", false, fmt.Errorf("continued fan-out requires its child recipient partition")
	}
	if retained != "" {
		return fanoutbarrier.RecipientDispositionWithRetained(deliveries, retained)
	}
	return fanoutbarrier.RecipientDisposition(deliveries)
}

type persistedFanOutBarrier struct {
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
	transferredRaw     []byte
	execution          string
	scheduleKey        sql.NullString
	scheduleActivation sql.NullString
}

func loadFanOutBarriers(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	runID string,
	status fanoutbarrier.Status,
	exact *fanoutobligation.IntentKey,
	forUpdate bool,
) ([]fanoutbarrier.Barrier, error) {
	if exact != nil {
		if err := exact.Validate(); err != nil || exact.RunID != runID {
			return nil, fmt.Errorf("fan-out barrier load requires exact run/intent ownership")
		}
	} else if !status.Valid() {
		return nil, fmt.Errorf("fan-out barrier load requires valid status or exact intent")
	}
	query := `
		SELECT triggering_delivery_id, flow_path, declaration_family, semantic_path,
		       bundle_hash, semantic_digest, target_flow_path,
		       target_node_id, handler_event, join_id,
		       route_scope_key, route_instance_id, route_instance_path,
		       entity_id, routing_source, execution_mode, timer_handle,
		       summary, schedule_key, schedule_activation_id, created_at, updated_at, transferred_occurrence, status
		FROM fan_out_obligation_barriers
		WHERE run_id=$1`
	args := []any{runID}
	if exact != nil {
		query += " AND triggering_delivery_id=$2 AND flow_path=$3 AND declaration_family=$4 AND semantic_path=$5"
		args = append(args, exact.TriggeringDeliveryID, exact.ElementRef.FlowPath, exact.ElementRef.Family, exact.ElementRef.SemanticPath)
	} else {
		query += " AND status=$2"
		args = append(args, string(status))
	}
	query += " ORDER BY triggering_delivery_id, flow_path, declaration_family, semantic_path"
	if postgres && forUpdate {
		query += " FOR UPDATE"
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	persistedRows := make([]persistedFanOutBarrier, 0)
	for rows.Next() {
		var item persistedFanOutBarrier
		item.barrier.Registration.IntentKey.RunID = runID
		var persistedStatus string
		var routingRaw, handleRaw, summaryRaw, createdAtRaw, updatedAtRaw, transferredRaw any
		if err := rows.Scan(
			&item.barrier.Registration.IntentKey.TriggeringDeliveryID,
			&item.barrier.Registration.IntentKey.ElementRef.FlowPath,
			&item.barrier.Registration.IntentKey.ElementRef.Family,
			&item.barrier.Registration.IntentKey.ElementRef.SemanticPath,
			&item.bundleHash, &item.digest, &item.targetFlowPath,
			&item.targetNode, &item.handlerEvent, &item.joinID,
			&item.barrier.Registration.Route.ScopeKey, &item.barrier.Registration.Route.InstanceID, &item.barrier.Registration.Route.InstancePath,
			&item.barrier.Registration.EntityID, &routingRaw, &item.execution, &handleRaw,
			&summaryRaw, &item.scheduleKey, &item.scheduleActivation, &createdAtRaw, &updatedAtRaw, &transferredRaw, &persistedStatus,
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
		item.barrier.Status = fanoutbarrier.Status(persistedStatus)
		item.barrier.UpdatedAt = updatedAt
		item.routingRaw = append([]byte(nil), jsonRawMessageValue(routingRaw)...)
		item.handleRaw = append([]byte(nil), jsonRawMessageValue(handleRaw)...)
		item.summaryRaw = append([]byte(nil), jsonRawMessageValue(summaryRaw)...)
		item.transferredRaw = append([]byte(nil), jsonRawMessageValue(transferredRaw)...)
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
		barrier, err := decodePersistedFanOutBarrier(item)
		if err != nil {
			return nil, err
		}
		out = append(out, barrier)
	}
	return out, nil
}

func decodePersistedFanOutBarrier(item persistedFanOutBarrier) (fanoutbarrier.Barrier, error) {
	item.barrier.Registration.PlanRef = runtimecontracts.FanOutPlanRef{
		BundleHash: item.bundleHash, ElementRef: item.barrier.Registration.IntentKey.ElementRef, SemanticDigest: item.digest,
	}
	if err := json.Unmarshal(item.routingRaw, &item.barrier.Registration.RoutingSource); err != nil {
		return fanoutbarrier.Barrier{}, fmt.Errorf("decode fan-out barrier routing source: %w", err)
	}
	if err := json.Unmarshal(item.handleRaw, &item.barrier.Registration.Handle); err != nil {
		return fanoutbarrier.Barrier{}, fmt.Errorf("decode fan-out barrier timer handle: %w", err)
	}
	if len(item.summaryRaw) != 0 && string(item.summaryRaw) != "null" {
		var summary fanoutbarrier.Summary
		if err := json.Unmarshal(item.summaryRaw, &summary); err != nil {
			return fanoutbarrier.Barrier{}, fmt.Errorf("decode fan-out barrier summary: %w", err)
		}
		item.barrier.Summary = &summary
	}
	item.barrier.ScheduleKey = strings.TrimSpace(item.scheduleKey.String)
	item.barrier.ScheduleActivationID = strings.TrimSpace(item.scheduleActivation.String)
	mode, ok := executionmode.Parse(item.execution)
	if !ok {
		return fanoutbarrier.Barrier{}, fmt.Errorf("fan-out barrier execution mode %q is invalid", item.execution)
	}
	item.barrier.Registration.ExecutionMode = mode
	var err error
	item.barrier.TransferredOccurrence, err = fanoutbarrier.DecodeTransferredOccurrence(item.transferredRaw)
	if err != nil {
		return fanoutbarrier.Barrier{}, fmt.Errorf("decode transferred barrier occurrence: %w", err)
	}
	if err := item.barrier.Validate(); err != nil {
		return fanoutbarrier.Barrier{}, err
	}
	ref, _ := item.barrier.Registration.Handle.JoinRef()
	fanOut, _ := ref.FanOutDelivery()
	intentDeclaration, identityErr := item.barrier.Registration.IntentKey.ElementRef.DeclarationIdentity()
	if identityErr != nil || !fanOut.DeclarationIdentity().Equal(intentDeclaration) ||
		fanOut.BundleHash() != strings.TrimSpace(item.bundleHash) || fanOut.SemanticDigest() != strings.TrimSpace(item.digest) ||
		ref.FlowPath() != strings.TrimSpace(item.targetFlowPath) ||
		ref.NodeID() != strings.TrimSpace(item.targetNode) || ref.HandlerEvent() != strings.TrimSpace(item.handlerEvent) ||
		ref.JoinID() != strings.TrimSpace(item.joinID) {
		return fanoutbarrier.Barrier{}, fmt.Errorf("fan-out barrier persisted identity contradicts its typed handle")
	}
	return item.barrier, nil
}

func loadArmedFanOutBarriers(ctx context.Context, tx *sql.Tx, postgres bool, runID string) ([]fanoutbarrier.Registration, error) {
	barriers, err := loadFanOutBarriers(ctx, tx, postgres, runID, fanoutbarrier.StatusArmed, nil, true)
	if err != nil {
		return nil, err
	}
	registrations := make([]fanoutbarrier.Registration, 0, len(barriers))
	for _, barrier := range barriers {
		registrations = append(registrations, barrier.Registration)
	}
	return registrations, nil
}

func fanOutBarrierGenerationCurrent(ctx context.Context, tx *sql.Tx, postgres bool, registration fanoutbarrier.Registration) (bool, error) {
	ref, _ := registration.Handle.JoinRef()
	generation := ref.Generation()
	if !generation.Valid() {
		return true, nil
	}
	header, found, err := workflowheader.LoadForMutation(ctx, tx, postgres, registration.IntentKey.RunID, registration.EntityID, registration.Route.InstancePath)
	if err != nil {
		return false, err
	}
	if !found {
		return false, fmt.Errorf("fan-out barrier generation owner entity is missing")
	}
	if header.FlowTemplate != generation.FlowID || header.FlowTemplate != registration.Route.ScopeKey {
		return false, fmt.Errorf("fan-out generation disagrees with its constructed flow owner")
	}
	fields := map[string]any{}
	if header.EntityType != "" {
		if err := json.Unmarshal(jsonRawMessageValue(header.Fields), &fields); err != nil || fields == nil {
			return false, fmt.Errorf("decode fan-out barrier generation fields: %v", err)
		}
	}
	var stateBuckets map[string]any
	if err := json.Unmarshal(jsonRawMessageValue(header.Accumulator), &stateBuckets); err != nil || stateBuckets == nil {
		return false, fmt.Errorf("decode fan-out barrier generation state: %v", err)
	}
	return runtimepipeline.WorkflowLoopGenerationCurrent(fields, stateBuckets, generation, "")
}

func advanceFanOutDeliveryBarriersTx(
	ctx context.Context,
	tx *sql.Tx,
	attempt *mutationprotocol.Attempt,
	postgres bool,
	genericSchedules GenericScheduleTxOwner,
	runID string,
	selectedNow time.Time,
) (activations []runtimerunlifecycle.CommittedGenericScheduleActivation, err error) {
	if tx == nil || attempt == nil || genericSchedules == nil || strings.TrimSpace(runID) == "" || selectedNow.IsZero() {
		return nil, fmt.Errorf("fan-out barrier advancement requires transaction, owners, run, and selected-store time")
	}
	registrations, err := loadArmedFanOutBarriers(ctx, tx, postgres, runID)
	if err != nil {
		return nil, err
	}
	activations = make([]runtimerunlifecycle.CommittedGenericScheduleActivation, 0, len(registrations))
	var foldDB pipelineQueryer = tx
	if !postgres {
		queries := &fanOutFoldStatement{Tx: tx}
		defer func() {
			if closeErr := queries.close(); closeErr != nil && err == nil {
				activations = nil
				err = fmt.Errorf("close sqlite fan-out fold statement: %w", closeErr)
			}
		}()
		foldDB = queries
	}
	for _, registration := range registrations {
		current, err := fanOutBarrierGenerationCurrent(ctx, tx, postgres, registration)
		if err != nil {
			return nil, err
		}
		if !current {
			if err := suppressSupersededArmedFanOutBarrierTx(ctx, tx, postgres, attempt, registration, selectedNow); err != nil {
				return nil, err
			}
			continue
		}
		fold, err := foldFanOutIntentTerminalDispositions(ctx, foldDB, postgres, registration.IntentKey)
		if err != nil {
			return nil, err
		}
		if !fold.Terminal() {
			continue
		}
		status := fanoutbarrier.StatusClosedPending
		scheduleKey := any(nil)
		scheduleActivationID := any(nil)
		command, err := runtimegenericschedule.FanOutBarrierAdmission(registration, fold.Summary, selectedNow)
		if err != nil {
			return nil, err
		}
		admitted, err := genericSchedules.AdmitTx(ctx, attempt, command)
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
		if err := addFanOutBarrierRevisionEffect(attempt, registration.IntentKey); err != nil {
			return nil, err
		}
	}
	if err := suppressSupersededPendingFanOutBarriersTx(ctx, tx, attempt, postgres, genericSchedules, runID, selectedNow); err != nil {
		return nil, err
	}
	if err := terminalizeDeadLetteredFanOutBarrierOutcomesTx(ctx, tx, postgres, attempt, runID, selectedNow); err != nil {
		return nil, err
	}
	return activations, nil
}

func suppressSupersededArmedFanOutBarrierTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects interface {
		AddFacts(string, ...privaterunforkrevision.FactRef) error
	},
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
	return addFanOutBarrierRevisionEffect(effects, registration.IntentKey)
}

func suppressSupersededPendingFanOutBarriersTx(
	ctx context.Context,
	tx *sql.Tx,
	attempt *mutationprotocol.Attempt,
	postgres bool,
	genericSchedules GenericScheduleTxOwner,
	runID string,
	at time.Time,
) error {
	barriers, err := loadFanOutBarriers(ctx, tx, postgres, runID, fanoutbarrier.StatusClosedPending, nil, true)
	if err != nil {
		return err
	}
	for _, barrier := range barriers {
		registration := barrier.Registration
		current, err := fanOutBarrierGenerationCurrent(ctx, tx, postgres, registration)
		if err != nil {
			return err
		}
		if current {
			continue
		}
		if err := cancelSupersededFanOutBarrierScheduleTx(ctx, attempt, genericSchedules, barrier, at); err != nil {
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
		if err := addFanOutBarrierRevisionEffect(attempt, registration.IntentKey); err != nil {
			return err
		}
	}
	return nil
}

func cancelSupersededFanOutBarrierScheduleTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	genericSchedules GenericScheduleTxOwner,
	barrier fanoutbarrier.Barrier,
	at time.Time,
) error {
	if barrier.TransferredOccurrence != nil {
		return barrier.Validate()
	}
	if genericSchedules == nil || barrier.Summary == nil {
		return fmt.Errorf("superseded pending fan-out barrier requires its generic schedule owner and summary")
	}
	activation, found, err := genericSchedules.LoadActivationTx(ctx, attempt, barrier.ScheduleActivationID)
	if err != nil {
		return fmt.Errorf("load superseded fan-out barrier schedule: %w", err)
	}
	if !found {
		return fmt.Errorf("superseded fan-out barrier schedule activation %s is missing", barrier.ScheduleActivationID)
	}
	if err := runtimegenericschedule.ValidateFanOutBarrierScheduleRelation(barrier, activation); err != nil {
		return err
	}
	cancelled, err := genericSchedules.CancelActivationTx(ctx, attempt, runtimegenericschedule.CancelCommand{
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
	effects interface {
		AddFacts(string, ...privaterunforkrevision.FactRef) error
	},
	runID string,
	selectedNow time.Time,
) error {
	pendingRows, err := loadFanOutBarriers(ctx, tx, postgres, runID, fanoutbarrier.StatusClosedPending, nil, true)
	if err != nil {
		return err
	}
	adapter := sqliteDeliveryAdapter
	if postgres {
		adapter = postgresDeliveryAdapter
	}
	for _, item := range pendingRows {
		occurrenceEventID := sql.NullString{}
		if item.TransferredOccurrence != nil {
			occurrenceEventID = sql.NullString{String: item.TransferredOccurrence.Publication.EventID, Valid: true}
		} else {
			var scheduleStatus string
			err := tx.QueryRowContext(ctx, `SELECT occurrence_event_id, status FROM timers
				WHERE timer_id=$1 AND run_id=$2 AND schedule_key=$3 AND task_type='timer'`,
				item.ScheduleActivationID, runID, item.ScheduleKey).Scan(&occurrenceEventID, &scheduleStatus)
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("closed fan-out barrier schedule %s is missing", item.ScheduleKey)
			}
			if err != nil {
				return err
			}
			if !occurrenceEventID.Valid {
				if strings.TrimSpace(scheduleStatus) == string(runtimegenericschedule.StatusFailed) {
					return fmt.Errorf("fan-out barrier schedule %s failed before a canonical completion delivery", item.ScheduleKey)
				}
				continue
			}
		}
		original, settlement, err := loadFanOutSourceEvent(ctx, tx, occurrenceEventID.String, postgres)
		if item.TransferredOccurrence != nil && errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return fmt.Errorf("load fan-out barrier completion event: %w", err)
		}
		if item.TransferredOccurrence != nil {
			occurrence, err := runtimegenericschedule.NewTransferredFanOutBarrierOccurrence(item)
			if err != nil {
				return err
			}
			if err := occurrence.ValidateEvent(original); err != nil {
				return err
			}
		}
		deliveries, err := adapter.SnapshotsForEvent(ctx, tx, occurrenceEventID.String)
		if err != nil {
			return err
		}
		deadLettered, err := fanOutBarrierOutcomeDeadLettered(item, settlement, deliveries)
		if err != nil {
			return err
		}
		if !deadLettered {
			continue
		}
		update := `
			UPDATE fan_out_obligation_barriers
			SET status=$1, updated_at=$2
			WHERE run_id=$3 AND triggering_delivery_id=$4 AND flow_path=$5 AND declaration_family=$6 AND semantic_path=$7 AND status='closed_pending' AND schedule_key=$8`
		if !postgres {
			update = postgresPlaceholdersToSQLite(update, 8)
		}
		result, err := tx.ExecContext(ctx, update, string(fanoutbarrier.StatusOutcomeDeadLettered), selectedNow.UTC(), runID,
			item.Registration.IntentKey.TriggeringDeliveryID, item.Registration.IntentKey.ElementRef.FlowPath, item.Registration.IntentKey.ElementRef.Family, item.Registration.IntentKey.ElementRef.SemanticPath, item.ScheduleKey)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil || changed != 1 {
			return fmt.Errorf("fan-out barrier dead-letter terminalization lost exact pending owner")
		}
		key := item.Registration.IntentKey
		if err := addFanOutBarrierRevisionEffect(effects, key); err != nil {
			return err
		}
	}
	return nil
}

func fanOutBarrierOutcomeDeadLettered(barrier fanoutbarrier.Barrier, settlement events.RouteSettlement, deliveries []runtimedelivery.Snapshot) (bool, error) {
	if settlement.NoDelivery() && len(deliveries) == 0 {
		return true, nil
	}
	if !settlement.Delivered() {
		return false, fmt.Errorf("fan-out barrier completion route settlement is contradictory")
	}
	ref, _ := barrier.Registration.Handle.JoinRef()
	target := events.RouteIdentity{FlowID: ref.FlowPath(), FlowInstance: barrier.Registration.Route.InstancePath, EntityID: barrier.Registration.EntityID}
	var owner *runtimedelivery.Snapshot
	for index := range deliveries {
		row := &deliveries[index]
		if row.Route.Recipient != events.MustNodeDeliveryRecipient(ref.Node()) || !events.SameRouteIdentity(row.Route.Target.Route(), target) {
			continue
		}
		if owner != nil {
			return false, fmt.Errorf("fan-out barrier completion repeats its exact owner delivery")
		}
		owner = row
	}
	if owner == nil {
		return false, fmt.Errorf("fan-out barrier completion lacks its exact owner delivery")
	}
	if !owner.Terminal() {
		return false, nil
	}
	if owner.Status != runtimedelivery.StatusDeadLetter {
		return false, fmt.Errorf("delivered fan-out barrier completion %s remained closed_pending", owner.EventID)
	}
	return true, nil
}

func suppressRunTerminalFanOutBarriersTx(
	ctx context.Context,
	tx *sql.Tx,
	postgres bool,
	effects interface {
		AddFacts(string, ...privaterunforkrevision.FactRef) error
	},
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
		if err := addFanOutBarrierRevisionEffect(effects, candidate.key); err != nil {
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

func (s *PipelinePostgresOwner) AdvanceFanOutDeliveryBarriersTx(ctx context.Context, attempt *mutationprotocol.Attempt, runID string, selectedNow time.Time) ([]runtimerunlifecycle.CommittedGenericScheduleActivation, error) {
	var activations []runtimerunlifecycle.CommittedGenericScheduleActivation
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		activations, err = advanceFanOutDeliveryBarriersTx(ctx, tx, attempt, true, s.genericSchedules, runID, selectedNow)
		return err
	})
	return activations, err
}

func (s *PipelineSQLiteOwner) AdvanceFanOutDeliveryBarriersTx(ctx context.Context, attempt *mutationprotocol.Attempt, runID string, selectedNow time.Time) ([]runtimerunlifecycle.CommittedGenericScheduleActivation, error) {
	var activations []runtimerunlifecycle.CommittedGenericScheduleActivation
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		activations, err = advanceFanOutDeliveryBarriersTx(ctx, tx, attempt, false, s.genericSchedules, runID, selectedNow)
		return err
	})
	return activations, err
}

func (s *PipelinePostgresOwner) SummarizeFanOutDeliveryBarriersRunTx(ctx context.Context, tx *sql.Tx, runID string) (fanoutbarrier.RunSummary, error) {
	return summarizeFanOutDeliveryBarriersRun(ctx, tx, runID)
}

func (s *PipelineSQLiteOwner) SummarizeFanOutDeliveryBarriersRunTx(ctx context.Context, tx *sql.Tx, runID string) (fanoutbarrier.RunSummary, error) {
	return summarizeFanOutDeliveryBarriersRun(ctx, tx, runID)
}

func (s *PipelinePostgresOwner) MaterializeRunForkFanOutBarrierTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	forkRunID string,
	source fanoutbarrier.Barrier,
	sourceSchedule *runtimegenericschedule.Activation,
	published *runtimegenericschedule.JoinContinuation,
	selectedRef runtimecontracts.FanOutPlanRef,
	generation *loopruntime.ForkChildReference,
	at time.Time,
) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return materializeRunForkFanOutBarrierTx(ctx, tx, attempt, true, s.genericSchedules, forkRunID, source, sourceSchedule, published, selectedRef, generation, at)
	})
}

func (s *PipelineSQLiteOwner) MaterializeRunForkFanOutBarrierTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	forkRunID string,
	source fanoutbarrier.Barrier,
	sourceSchedule *runtimegenericschedule.Activation,
	published *runtimegenericschedule.JoinContinuation,
	selectedRef runtimecontracts.FanOutPlanRef,
	generation *loopruntime.ForkChildReference,
	at time.Time,
) error {
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return materializeRunForkFanOutBarrierTx(ctx, tx, attempt, false, s.genericSchedules, forkRunID, source, sourceSchedule, published, selectedRef, generation, at)
	})
}

func materializeRunForkFanOutBarrierTx(
	ctx context.Context,
	tx *sql.Tx,
	attempt *mutationprotocol.Attempt,
	postgres bool,
	genericSchedules GenericScheduleTxOwner,
	forkRunID string,
	source fanoutbarrier.Barrier,
	sourceSchedule *runtimegenericschedule.Activation,
	published *runtimegenericschedule.JoinContinuation,
	selectedRef runtimecontracts.FanOutPlanRef,
	generation *loopruntime.ForkChildReference,
	at time.Time,
) error {
	if err := source.Validate(); err != nil {
		return fmt.Errorf("source fork fan-out barrier: %w", err)
	}
	if strings.TrimSpace(forkRunID) == "" || at.IsZero() || attempt == nil {
		return fmt.Errorf("fork fan-out barrier materialization requires run, time, and revision owner")
	}
	registration, err := runfork.ProjectFanOutBarrierRegistration(source.Registration, forkRunID, selectedRef, generation, at)
	if err != nil {
		return err
	}
	var due time.Time
	var transferred *fanoutbarrier.TransferredOccurrence
	if published != nil {
		transferred, err = runForkFanOutBarrierTransferredOccurrence(source, sourceSchedule, *published, registration)
	} else {
		due, err = runForkFanOutBarrierDue(source, sourceSchedule)
	}
	if err != nil {
		return err
	}
	if err := commitFanOutBarrierRegistrationTx(ctx, attempt, postgres, registration); err != nil {
		return err
	}
	if source.Status == fanoutbarrier.StatusArmed {
		return nil
	}
	var summaryRaw any
	if source.Summary != nil {
		raw, err := json.Marshal(source.Summary)
		if err != nil {
			return err
		}
		summaryRaw = string(raw)
	}
	scheduleKey, scheduleActivationID, transferredRaw, err := materializeForkBarrierOccurrence(ctx, attempt, genericSchedules, registration, source, transferred, due)
	if err != nil {
		return err
	}
	query := `
		UPDATE fan_out_obligation_barriers
		SET status=$1, summary=$2, schedule_key=$3, schedule_activation_id=$4, updated_at=$5, transferred_occurrence=$11
		WHERE run_id=$6 AND triggering_delivery_id=$7 AND flow_path=$8 AND declaration_family=$9 AND semantic_path=$10 AND status='armed'`
	if postgres {
		query = strings.ReplaceAll(query, "$2", "$2::jsonb")
		query = strings.ReplaceAll(query, "$11", "$11::jsonb")
	} else {
		query = postgresPlaceholdersToSQLite(query, 11)
	}
	result, err := tx.ExecContext(ctx, query, string(source.Status), summaryRaw, scheduleKey, scheduleActivationID, at.UTC(),
		registration.IntentKey.RunID, registration.IntentKey.TriggeringDeliveryID,
		registration.IntentKey.ElementRef.FlowPath, registration.IntentKey.ElementRef.Family, registration.IntentKey.ElementRef.SemanticPath, transferredRaw)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil || changed != 1 {
		return fmt.Errorf("fork fan-out barrier materialization lost exact armed owner")
	}
	return addFanOutBarrierRevisionEffect(attempt, registration.IntentKey)
}

func materializeForkBarrierOccurrence(ctx context.Context, attempt *mutationprotocol.Attempt, schedules GenericScheduleTxOwner, registration fanoutbarrier.Registration, source fanoutbarrier.Barrier, transferred *fanoutbarrier.TransferredOccurrence, due time.Time) (any, any, any, error) {
	if source.Status != fanoutbarrier.StatusClosedPending {
		if source.Status == fanoutbarrier.StatusFired || source.Status == fanoutbarrier.StatusOutcomeDeadLettered {
			return registration.Handle.TaskID(), nil, nil, nil
		}
		return nil, nil, nil, nil
	}
	if transferred != nil {
		raw, err := json.Marshal(transferred)
		return registration.Handle.TaskID(), nil, string(raw), err
	}
	if schedules == nil || source.Summary == nil {
		return nil, nil, nil, fmt.Errorf("closed fork fan-out barrier requires generic schedule owner and summary")
	}
	command, err := runtimegenericschedule.FanOutBarrierAdmission(registration, *source.Summary, due)
	if err != nil {
		return nil, nil, nil, err
	}
	admitted, err := schedules.AdmitTx(ctx, attempt, command)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("admit fork fan-out barrier completion: %w", err)
	}
	return command.ScheduleKey, admitted.Activation.ID, nil, nil
}

func runForkFanOutBarrierDue(source fanoutbarrier.Barrier, schedule *runtimegenericschedule.Activation) (time.Time, error) {
	if source.Status != fanoutbarrier.StatusClosedPending {
		return time.Time{}, nil
	}
	if schedule == nil {
		return time.Time{}, fmt.Errorf("pending fork barrier requires its captured schedule")
	}
	if err := runtimegenericschedule.ValidateFanOutBarrierScheduleRelation(source, *schedule); err != nil {
		return time.Time{}, fmt.Errorf("pending fork barrier schedule: %w", err)
	}
	if schedule.Status != runtimegenericschedule.StatusActive {
		return time.Time{}, fmt.Errorf("published fork barrier occurrence requires historical event continuation, not rearming")
	}
	return schedule.CurrentDueAt, nil
}

func commitFanOutBarrierRegistrationTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	postgres bool,
	registration fanoutbarrier.Registration,
) error {
	if err := registration.Validate(); err != nil {
		return err
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
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
		fact, err := privaterunforkrevision.FanOutBarrierFact(registration.IntentKey)
		if err != nil {
			return err
		}
		return attempt.AddFacts(registration.IntentKey.RunID, fact)
	})
}

func commitFanOutBarrierCompletionTx(
	ctx context.Context,
	attempt *mutationprotocol.Attempt,
	postgres bool,
	runID string,
	completion fanoutbarrier.Completion,
	claim runtimedelivery.Claim,
	updatedAt time.Time,
) error {
	if updatedAt.IsZero() {
		return fmt.Errorf("fan-out barrier completion requires selected-store time")
	}
	key, err := completion.IntentKey(runID)
	if err != nil {
		return err
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := loadFanOutBarriers(ctx, tx, postgres, runID, "", &key, true)
		if err != nil {
			return err
		}
		if len(rows) != 1 {
			return fmt.Errorf("fan-out barrier completion requires one exact owner")
		}
		owner := rows[0]
		if owner.Summary == nil || *owner.Summary != completion.Summary || owner.Registration.Handle != completion.Handle || owner.ScheduleKey != completion.Handle.TaskID() {
			return fmt.Errorf("fan-out barrier completion contradicts its closed owner")
		}
		if owner.TransferredOccurrence != nil {
			if err := requireTransferredBarrierCompletionClaim(ctx, tx, postgres, owner, claim); err != nil {
				return err
			}
		} else if owner.ScheduleActivationID == "" {
			return fmt.Errorf("fan-out barrier completion lacks its original occurrence")
		}
		switch owner.Status {
		case fanoutbarrier.StatusFired:
			return nil
		case fanoutbarrier.StatusClosedPending:
		default:
			return fmt.Errorf("fan-out barrier completion requires closed_pending owner, got %q", owner.Status)
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
		ref, err := privaterunforkrevision.FanOutBarrierFact(key)
		if err != nil {
			return err
		}
		return attempt.AddFacts(key.RunID, ref)
	})
}

func addFanOutBarrierRevisionEffect(effects interface {
	AddFacts(string, ...privaterunforkrevision.FactRef) error
}, key fanoutobligation.IntentKey) error {
	ref, err := privaterunforkrevision.FanOutBarrierFact(key)
	if err != nil {
		return err
	}
	return effects.AddFacts(key.RunID, ref)
}
