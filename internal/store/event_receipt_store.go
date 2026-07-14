package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimerunquiescence "github.com/division-sh/swarm/internal/runtime/runquiescence"
	"github.com/lib/pq"
)

func (s *PostgresStore) MarkEventDeliveryInProgress(ctx context.Context, eventID, agentID, sessionID string) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return fmt.Errorf("mark event delivery in progress: eventID and agentID required")
	}
	sessionID = sanitizeOptionalUUID(sessionID)
	switch caps.Events.Deliveries {
	case SchemaFlavorCanonical:
		return s.markEventDeliveryInProgressSpec(ctx, eventID, agentID, sessionID)
	default:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	}
}

func (s *PostgresStore) UpsertEventReceipt(ctx context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, failure *runtimefailures.Envelope) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return fmt.Errorf("upsert event receipt: eventID and agentID required")
	}
	if status == "" {
		return fmt.Errorf("upsert event receipt: status required")
	}
	switch caps.Events.Receipts {
	case SchemaFlavorCanonical:
		return s.upsertAgentReceiptSpec(ctx, caps, eventID, agentID, status, failure)
	default:
		return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
}

func (s *PostgresStore) ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error) {
	if agentID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}
	page, err := s.ListPendingAgentDeliveryDetails(ctx, PendingAgentDeliveryListOptions{
		AgentID: agentID,
		Since:   since,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]events.Event, 0, len(page.PendingDeliveries))
	for _, detail := range page.PendingDeliveries {
		out = append(out, detail.Event)
	}
	return out, nil
}

func (s *PostgresStore) ListPendingSubscribedEvents(
	ctx context.Context,
	agentID string,
	subscriptions []events.EventType,
	since time.Time,
	limit int,
) ([]events.Event, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if agentID == "" || len(subscriptions) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}
	if err := RequireCanonicalPendingAgentDeliveryCapabilities(caps); err != nil {
		return nil, err
	}
	return s.listPendingSubscribedEventsSpec(ctx, agentID, subscriptions, since, limit)
}

func (s *PostgresStore) GetEventReceipt(ctx context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return runtimemanager.EventReceipt{}, false, err
	}
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("event_id and agent_id are required")
	}
	switch caps.Events.Receipts {
	case SchemaFlavorCanonical:
		return s.getEventReceiptSpec(ctx, eventID, agentID)
	default:
		return runtimemanager.EventReceipt{}, false, unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
}

type agentReceiptWriteState struct {
	finalStatus  runtimemanager.ReceiptStatus
	retryCount   int
	reasonCode   string
	failure      *runtimefailures.Envelope
	deliveryCode string
}

type lockedAgentDelivery struct {
	deliveryID      string
	runID           string
	eventType       string
	retryCount      int
	status          string
	reasonCode      string
	activeSessionID string
	entityID        string
	flowInstance    string
	startedAt       sql.NullTime
	found           bool
}

type deliveryBackedTerminalTransitionRequest struct {
	reasonCode   string
	failure      *runtimefailures.Envelope
	retryAdvance int
}

// Receipts are outcome-only for an existing agent delivery. This write path must
// never mint or repair delivery ownership from receipt state.
func (s *PostgresStore) upsertAgentReceiptSpec(ctx context.Context, caps StoreSchemaCapabilities, eventID, agentID string, status runtimemanager.ReceiptStatus, failure *runtimefailures.Envelope) error {
	if err := withEventStoreRetry(ctx, nil, func() error {
		return s.runAuthorActivityMutation(ctx, "postgres upsert event receipt", func(txctx context.Context, tx *sql.Tx) error {
			delivery, err := s.lockAgentDeliveryTx(txctx, tx, eventID, agentID)
			if err != nil {
				return err
			}
			if !delivery.found {
				return fmt.Errorf("upsert event receipt: delivery row required for event %s agent %s", strings.TrimSpace(eventID), strings.TrimSpace(agentID))
			}
			if !activeRunQuiescenceDeliveryTerminal(delivery.status, delivery.reasonCode) {
				state, err := buildAgentReceiptWriteState(delivery.retryCount, status, failure)
				if err != nil {
					return err
				}
				changed := false
				if state.finalStatus == runtimemanager.ReceiptStatusDeadLetter {
					if _, changed, err = s.applyDeliveryBackedTerminalTransitionTx(txctx, tx, eventID, agentID, delivery, deliveryBackedTerminalTransitionRequest{
						reasonCode:   state.reasonCode,
						failure:      state.failure,
						retryAdvance: state.retryCount - delivery.retryCount,
					}); err != nil {
						return err
					}
				} else {
					changed, err = s.updateAgentDeliveryRowTx(txctx, tx, eventID, agentID, state)
					if err != nil {
						return err
					}
					if changed {
						if err := s.upsertAgentReceiptRowTx(txctx, tx, eventID, agentID, state); err != nil {
							return err
						}
					}
				}
				if changed {
					if err := recordAgentDeliveryAuthorActivity(txctx, delivery, agentID, state, time.Now().UTC()); err != nil {
						return err
					}
				}
			}
			return s.convergeStandaloneRuntimePlatformRunByEventID(txctx, tx, caps, eventID)
		})
	}); err != nil {
		return err
	}
	return nil
}

func buildAgentReceiptWriteState(baseRetryCount int, status runtimemanager.ReceiptStatus, failure *runtimefailures.Envelope) (agentReceiptWriteState, error) {
	state := agentReceiptWriteState{
		finalStatus: status,
		failure:     runtimefailures.CloneEnvelope(failure),
	}
	if status == runtimemanager.ReceiptStatusProcessed && failure != nil {
		return agentReceiptWriteState{}, fmt.Errorf("processed receipt must not carry failure")
	}
	if status != runtimemanager.ReceiptStatusProcessed && failure == nil {
		return agentReceiptWriteState{}, fmt.Errorf("failed receipt requires canonical failure")
	}
	if state.failure != nil {
		if err := runtimefailures.ValidateEnvelope(*state.failure); err != nil {
			return agentReceiptWriteState{}, fmt.Errorf("failed receipt carries invalid failure: %w", err)
		}
	}
	retryCount := baseRetryCount
	if status == runtimemanager.ReceiptStatusError {
		retryCount++
	}
	finalStatus := status
	if status == runtimemanager.ReceiptStatusTerminal {
		finalStatus = runtimemanager.ReceiptStatusDeadLetter
	}
	if status == runtimemanager.ReceiptStatusError && retryCount >= 2 {
		finalStatus = runtimemanager.ReceiptStatusDeadLetter
	}
	if finalStatus == runtimemanager.ReceiptStatusDeadLetter && status == runtimemanager.ReceiptStatusError && state.failure != nil {
		terminal := runtimefailures.FromError(runtimefailures.New(
			runtimefailures.ClassRetryExhausted,
			"delivery_retry_exhausted",
			"event-delivery",
			"apply_retry_policy",
			map[string]any{"attempts": retryCount, "last_failure": *state.failure},
		), "event-delivery", "apply_retry_policy")
		state.failure = &terminal.Failure
	}
	state.finalStatus = finalStatus
	state.retryCount = retryCount
	state.reasonCode = managerReceiptReasonCode(status)
	if status == runtimemanager.ReceiptStatusError && finalStatus == runtimemanager.ReceiptStatusDeadLetter {
		state.reasonCode = "retry_exhausted"
	}
	state.deliveryCode = "delivered"
	switch status {
	case runtimemanager.ReceiptStatusError:
		state.deliveryCode = "failed"
	case runtimemanager.ReceiptStatusDeadLetter, runtimemanager.ReceiptStatusTerminal:
		state.deliveryCode = "dead_letter"
	}
	if state.finalStatus == runtimemanager.ReceiptStatusDeadLetter {
		state.deliveryCode = "dead_letter"
	}
	return state, nil
}

func (s *PostgresStore) lockAgentDeliveryTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID, agentID string,
) (lockedAgentDelivery, error) {
	var delivery lockedAgentDelivery
	err := tx.QueryRowContext(ctx, `
		SELECT
			d.delivery_id::text,
			COALESCE(d.run_id::text, e.run_id::text, ''),
			e.event_name,
			d.retry_count,
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.active_session_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			COALESCE(e.flow_instance, ''),
			d.started_at
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
		FOR UPDATE
	`, eventID, agentID).Scan(&delivery.deliveryID, &delivery.runID, &delivery.eventType, &delivery.retryCount, &delivery.status, &delivery.reasonCode, &delivery.activeSessionID, &delivery.entityID, &delivery.flowInstance, &delivery.startedAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return lockedAgentDelivery{}, nil
	case err != nil:
		return lockedAgentDelivery{}, fmt.Errorf("lock event delivery row: %w", err)
	default:
		delivery.found = true
		return delivery, nil
	}
}

func (s *PostgresStore) applyDeliveryBackedTerminalTransitionTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID, agentID string,
	delivery lockedAgentDelivery,
	req deliveryBackedTerminalTransitionRequest,
) (runtimemanager.EventReceipt, bool, error) {
	if !delivery.found {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("apply delivery-backed terminal transition: delivery row required")
	}
	reasonCode := strings.TrimSpace(req.reasonCode)
	if reasonCode == "" {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("apply delivery-backed terminal transition: reason_code required")
	}
	if req.retryAdvance < 0 {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("apply delivery-backed terminal transition: retry advance must be non-negative")
	}
	state := agentReceiptWriteState{
		finalStatus:  runtimemanager.ReceiptStatusDeadLetter,
		retryCount:   delivery.retryCount + req.retryAdvance,
		reasonCode:   reasonCode,
		failure:      runtimefailures.CloneEnvelope(req.failure),
		deliveryCode: "dead_letter",
	}
	changed, err := s.updateAgentDeliveryRowTx(ctx, tx, eventID, agentID, state)
	if err != nil {
		return runtimemanager.EventReceipt{}, false, err
	}
	if changed {
		if err := s.upsertAgentReceiptRowTx(ctx, tx, eventID, agentID, state); err != nil {
			return runtimemanager.EventReceipt{}, false, err
		}
	}
	return runtimemanager.EventReceipt{
		EventID:    strings.TrimSpace(eventID),
		AgentID:    strings.TrimSpace(agentID),
		Status:     runtimemanager.ReceiptStatusDeadLetter,
		RetryCount: state.retryCount,
		Failure:    runtimefailures.CloneEnvelope(state.failure),
	}, changed, nil
}

func (s *PostgresStore) updateAgentDeliveryRowTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID, agentID string,
	state agentReceiptWriteState,
) (bool, error) {
	const q = `
		UPDATE event_deliveries
		SET
			status = $3,
			retry_count = $4,
			reason_code = NULLIF($5, ''),
			failure = NULLIF($6, '')::jsonb,
			active_session_id = NULL,
			delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
		  AND (
			COALESCE(status, '') IS DISTINCT FROM $3
			OR COALESCE(retry_count, 0) IS DISTINCT FROM $4
			OR COALESCE(reason_code, '') IS DISTINCT FROM $5
			OR COALESCE(failure, 'null'::jsonb) IS DISTINCT FROM COALESCE(NULLIF($6, '')::jsonb, 'null'::jsonb)
			OR active_session_id IS NOT NULL
		  )
	`
	failureJSON, err := nullableFailureJSON(state.failure)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, q, eventID, agentID, state.deliveryCode, state.retryCount, state.reasonCode, failureJSON)
	if err != nil {
		return false, fmt.Errorf("sync event delivery: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read event delivery transition result: %w", err)
	}
	return rows > 0, nil
}

func (s *PostgresStore) upsertAgentReceiptRowTx(ctx context.Context, tx *sql.Tx, eventID, agentID string, state agentReceiptWriteState) error {
	sideEffects, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects(state.finalStatus, state.reasonCode, state.retryCount))
	if err != nil {
		return fmt.Errorf("marshal event receipt side effects: %w", err)
	}
	outcome := mapManagerReceiptStatusToOutcome(state.finalStatus)
	const q = `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT
			e.event_id, 'agent', $2, e.entity_id, e.flow_instance,
			$3, NULLIF($4,''), NULLIF($5, '')::jsonb, $6::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_type, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			failure = EXCLUDED.failure,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
	`
	failureJSON, err := nullableFailureJSON(state.failure)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, q, eventID, agentID, outcome, state.reasonCode, failureJSON, string(sideEffects))
	if err != nil {
		return fmt.Errorf("upsert event receipt: %w", err)
	}
	rows, err := result.RowsAffected()
	if err == nil && rows == 0 {
		return fmt.Errorf("upsert event receipt: event %s not found", strings.TrimSpace(eventID))
	}
	return nil
}

func (s *PostgresStore) markEventDeliveryInProgressSpec(ctx context.Context, eventID, agentID, sessionID string) error {
	const q = `
		UPDATE event_deliveries
		SET
			status = 'in_progress',
			reason_code = 'agent_processing',
			failure = NULL,
			active_session_id = COALESCE(NULLIF($3, '')::uuid, active_session_id),
			started_at = COALESCE(started_at, $5),
			delivered_at = NULL
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
		  AND (
			status IN ('pending', 'in_progress')
			OR (status = 'failed' AND COALESCE(retry_count, 0) < 2)
		  )
		  AND COALESCE(reason_code, '') <> ALL($4)
	`
	return s.runAuthorActivityMutation(ctx, "postgres mark event delivery in progress", func(txctx context.Context, tx *sql.Tx) error {
		delivery, err := s.lockAgentDeliveryTx(txctx, tx, eventID, agentID)
		if err != nil {
			return err
		}
		if !delivery.found {
			return fmt.Errorf("mark event delivery in progress: delivery row required for event %s agent %s", eventID, agentID)
		}
		if activeRunQuiescenceDeliveryTerminal(delivery.status, delivery.reasonCode) {
			return nil
		}
		transitionAt := time.Now().UTC()
		res, err := tx.ExecContext(txctx, q, eventID, agentID, sessionID, pq.Array(activeRunQuiescenceTerminalReasonCodes()), transitionAt)
		if err != nil {
			return fmt.Errorf("mark event delivery in progress: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			return fmt.Errorf("mark event delivery in progress: delivery transition conflict for event %s agent %s", eventID, agentID)
		}
		state := agentReceiptWriteState{deliveryCode: "in_progress", retryCount: delivery.retryCount, reasonCode: "agent_processing"}
		if delivery.startedAt.Valid {
			transitionAt = delivery.startedAt.Time.UTC()
		}
		return recordAgentDeliveryAuthorActivity(txctx, delivery, agentID, state, transitionAt)
	})
}

func recordAgentDeliveryAuthorActivity(ctx context.Context, delivery lockedAgentDelivery, agentID string, state agentReceiptWriteState, occurredAt time.Time) error {
	transition := strings.TrimSpace(state.deliveryCode)
	if transition == "" {
		return fmt.Errorf("agent delivery author activity transition is required")
	}
	retry := state.retryCount
	identity := delivery.deliveryID + ":" + transition + fmt.Sprintf(":%d", retry)
	return runtimeauthoractivity.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindDeliveryLifecycle, Transition: transition,
		SourceOwner: "event_deliveries", SourceIdentity: identity, DedupKey: "delivery:" + identity,
		OccurredAt: occurredAt.UTC(), RunID: delivery.runID, EntityID: delivery.entityID, FlowID: delivery.flowInstance,
		AgentID: agentID, Failure: state.failure,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "agent", SubjectID: agentID, EventType: delivery.eventType,
			SubscriberType: "agent", SubscriberID: agentID, RetryCount: &retry, ReasonCode: state.reasonCode,
		},
	})
}

func (s *PostgresStore) ActiveRunDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (string, bool, error) {
	if s == nil || s.DB == nil {
		return "", false, fmt.Errorf("postgres store is required")
	}
	eventID = strings.TrimSpace(eventID)
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	if eventID == "" || subscriberType == "" || subscriberID == "" {
		return "", false, nil
	}
	var reason string
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = $2
		  AND subscriber_id = $3
		  AND status = 'dead_letter'
		  AND reason_code = ANY($4)
		ORDER BY reason_code
		LIMIT 1
	`, eventID, subscriberType, subscriberID, pq.Array(activeRunQuiescenceTerminalReasonCodes())).Scan(&reason)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("check active run delivery quiescence: %w", err)
	default:
		return strings.TrimSpace(reason), true, nil
	}
}

func (s *PostgresStore) DestructiveResetDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (bool, error) {
	if s == nil || s.DB == nil {
		return false, fmt.Errorf("postgres store is required")
	}
	eventID = strings.TrimSpace(eventID)
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	if eventID == "" || subscriberType == "" || subscriberID == "" {
		return false, nil
	}
	var ok bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = $2
			  AND subscriber_id = $3
			  AND status = 'dead_letter'
			  AND reason_code = $4
		)
	`, eventID, subscriberType, subscriberID, destructivereset.QuiescenceReasonCode).Scan(&ok); err != nil {
		return false, fmt.Errorf("check destructive reset delivery quiescence: %w", err)
	}
	return ok, nil
}

func (s *PostgresStore) ServeAbandonDeliveryQuiesced(ctx context.Context, eventID, subscriberType, subscriberID string) (bool, error) {
	if s == nil || s.DB == nil {
		return false, fmt.Errorf("postgres store is required")
	}
	eventID = strings.TrimSpace(eventID)
	subscriberType = strings.TrimSpace(subscriberType)
	subscriberID = strings.TrimSpace(subscriberID)
	if eventID == "" || subscriberType == "" || subscriberID == "" {
		return false, nil
	}
	var ok bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = $2
			  AND subscriber_id = $3
			  AND status = 'dead_letter'
			  AND reason_code = $4
		)
	`, eventID, subscriberType, subscriberID, runtimerunquiescence.ServeAbandonReasonCode).Scan(&ok); err != nil {
		return false, fmt.Errorf("check serve abandon delivery quiescence: %w", err)
	}
	return ok, nil
}

func (s *PostgresStore) listPendingSubscribedEventsSpec(ctx context.Context, agentID string, subscriptions []events.EventType, since time.Time, limit int) ([]events.Event, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			$1,
			e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name, COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, e.created_at,
			COALESCE(e.source_event_id::text, ''),
			CASE WHEN d.delivery_id IS NULL THEN FALSE ELSE TRUE END,
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.created_at, e.created_at),
			d.delivered_at,
			CASE WHEN r.event_id IS NULL THEN FALSE ELSE TRUE END
		FROM events e
		LEFT JOIN event_deliveries d
			ON d.event_id = e.event_id
			AND d.subscriber_type = 'agent'
			AND d.subscriber_id = $1
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = $1
		WHERE e.created_at >= $2
		  AND EXISTS (
				SELECT 1
				FROM event_deliveries d_me
				WHERE d_me.event_id = e.event_id
				  AND d_me.subscriber_type = 'agent'
				  AND d_me.subscriber_id = $1
			)
		  AND `+canonicalPendingDeliveryPredicateSQL("d", "r")+`
		ORDER BY e.created_at ASC
	`, agentID, since)
	if err != nil {
		return nil, fmt.Errorf("query pending subscribed events for %s: %w", agentID, err)
	}
	defer rows.Close()
	records, err := scanPendingAgentDeliveryRecords(rows)
	if err != nil {
		return nil, err
	}
	pending := pendingEventsFromRecords(records, time.Now(), 0)
	filtered := make([]events.Event, 0, len(pending))
	for _, evt := range pending {
		for _, subscription := range subscriptions {
			if eventidentity.MatchPattern(string(subscription), string(evt.Type())) {
				filtered = append(filtered, evt)
				if limit > 0 && len(filtered) >= limit {
					return filtered, nil
				}
				break
			}
		}
	}
	return filtered, nil
}

func (s *PostgresStore) getEventReceiptSpec(ctx context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	var (
		outcome      string
		sideEffects  []byte
		delivery     string
		receiptRaw   []byte
		deliveryRaw  []byte
		deliverySeen bool
		retryCount   sql.NullInt64
	)
	if err := s.DB.QueryRowContext(ctx, `
		SELECT
			r.outcome,
			COALESCE(r.side_effects, '{}'::jsonb),
			COALESCE(r.failure, 'null'::jsonb),
			COALESCE(d.status, ''),
			COALESCE(d.failure, 'null'::jsonb),
			d.retry_count,
			CASE WHEN d.delivery_id IS NULL THEN FALSE ELSE TRUE END
		FROM event_receipts r
		LEFT JOIN event_deliveries d
			ON d.event_id = r.event_id
			AND d.subscriber_type = 'agent'
			AND d.subscriber_id = r.subscriber_id
		WHERE r.event_id = $1::uuid
		  AND r.subscriber_type = 'agent'
		  AND r.subscriber_id = $2
	`, eventID, agentID).Scan(&outcome, &sideEffects, &receiptRaw, &delivery, &deliveryRaw, &retryCount, &deliverySeen); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimemanager.EventReceipt{}, false, nil
		}
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("get event receipt: %w", err)
	}
	receipt := runtimemanager.EventReceipt{
		EventID: eventID,
		AgentID: agentID,
		Status:  mapOutcomeToManagerReceiptStatus(outcome),
	}
	if len(sideEffects) > 0 {
		payload, err := decodeAgentReceiptSideEffects(sideEffects)
		if err != nil {
			return runtimemanager.EventReceipt{}, false, fmt.Errorf("decode event receipt side effects: %w", err)
		}
		receipt.Status = payload.ManagerStatus
		receipt.RetryCount = payload.RetryCount
	}
	if string(receiptRaw) != "null" {
		failure, err := runtimefailures.UnmarshalEnvelope(receiptRaw)
		if err != nil {
			return runtimemanager.EventReceipt{}, false, fmt.Errorf("decode event receipt failure: %w", err)
		}
		receipt.Failure = &failure
	}
	if deliverySeen {
		mappedStatus, override, err := terminalManagerReceiptStatusFromDelivery(delivery)
		if err != nil {
			return runtimemanager.EventReceipt{}, false, fmt.Errorf("get event receipt: %w", err)
		}
		if override {
			receipt.Status = mappedStatus
			if retryCount.Valid {
				receipt.RetryCount = int(retryCount.Int64)
			}
			if string(deliveryRaw) != "null" {
				failure, err := runtimefailures.UnmarshalEnvelope(deliveryRaw)
				if err != nil {
					return runtimemanager.EventReceipt{}, false, fmt.Errorf("decode event delivery failure: %w", err)
				}
				receipt.Failure = &failure
			}
		}
	}
	return receipt, true, nil
}

func mapManagerReceiptStatusToOutcome(status runtimemanager.ReceiptStatus) string {
	switch status {
	case runtimemanager.ReceiptStatusError, runtimemanager.ReceiptStatusDeadLetter:
		return "dead_letter"
	default:
		return "success"
	}
}

func mapOutcomeToManagerReceiptStatus(outcome string) runtimemanager.ReceiptStatus {
	switch strings.TrimSpace(strings.ToLower(outcome)) {
	case "dead_letter":
		return runtimemanager.ReceiptStatusDeadLetter
	default:
		return runtimemanager.ReceiptStatusProcessed
	}
}

func terminalManagerReceiptStatusFromDelivery(status string) (runtimemanager.ReceiptStatus, bool, error) {
	switch strings.TrimSpace(strings.ToLower(status)) {
	case "delivered":
		return runtimemanager.ReceiptStatusProcessed, true, nil
	case "failed":
		return runtimemanager.ReceiptStatusError, true, nil
	case "dead_letter":
		return runtimemanager.ReceiptStatusDeadLetter, true, nil
	case "pending", "in_progress", "":
		return "", false, nil
	default:
		return "", false, fmt.Errorf("invalid delivery status %q", strings.TrimSpace(status))
	}
}

func managerReceiptReasonCode(status runtimemanager.ReceiptStatus) string {
	switch status {
	case runtimemanager.ReceiptStatusTerminal:
		return "terminal_failure"
	case runtimemanager.ReceiptStatusDeadLetter:
		return "dead_letter"
	case runtimemanager.ReceiptStatusError:
		return "handler_failure"
	default:
		return "agent_processed"
	}
}

func nullableFailureJSON(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", nil
	}
	raw, err := runtimefailures.MarshalEnvelope(*failure)
	if err != nil {
		return "", fmt.Errorf("encode canonical failure: %w", err)
	}
	return string(raw), nil
}
