package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"swarm/internal/events"
	"swarm/internal/runtime/core/eventidentity"
	"swarm/internal/runtime/destructivereset"
	runtimemanager "swarm/internal/runtime/manager"
	runtimerunquiescence "swarm/internal/runtime/runquiescence"
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

func (s *PostgresStore) UpsertEventReceipt(ctx context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, errText string) error {
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
		return s.upsertAgentReceiptSpec(ctx, caps, eventID, agentID, status, errText)
	default:
		return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
}

func (s *PostgresStore) ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if agentID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}
	if err := RequireCanonicalPendingAgentDeliveryCapabilities(caps); err != nil {
		return nil, err
	}
	return s.listPendingEventsForAgentSpec(ctx, agentID, since, limit)
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
	errorText    string
	deliveryCode string
}

type lockedAgentDelivery struct {
	retryCount      int
	status          string
	reasonCode      string
	activeSessionID string
	entityID        string
	flowInstance    string
	found           bool
}

type deliveryBackedTerminalTransitionRequest struct {
	reasonCode   string
	errorText    string
	retryAdvance int
}

// Receipts are outcome-only for an existing agent delivery. This write path must
// never mint or repair delivery ownership from receipt state.
func (s *PostgresStore) upsertAgentReceiptSpec(ctx context.Context, caps StoreSchemaCapabilities, eventID, agentID string, status runtimemanager.ReceiptStatus, errText string) error {
	if err := withEventStoreRetry(ctx, nil, func() error {
		tx, err := s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin event receipt tx: %w", err)
		}
		defer func() { _ = tx.Rollback() }()

		delivery, err := s.lockAgentDeliveryTx(ctx, tx, eventID, agentID)
		if err != nil {
			return err
		}
		if !delivery.found {
			return fmt.Errorf("upsert event receipt: delivery row required for event %s agent %s", strings.TrimSpace(eventID), strings.TrimSpace(agentID))
		}
		if activeRunQuiescenceDeliveryTerminal(delivery.status, delivery.reasonCode) {
			return nil
		}
		state := buildAgentReceiptWriteState(delivery.retryCount, status, errText)
		if state.finalStatus == runtimemanager.ReceiptStatusDeadLetter {
			if _, err := s.applyDeliveryBackedTerminalTransitionTx(ctx, tx, eventID, agentID, delivery, deliveryBackedTerminalTransitionRequest{
				reasonCode:   state.reasonCode,
				errorText:    state.errorText,
				retryAdvance: state.retryCount - delivery.retryCount,
			}); err != nil {
				return err
			}
		} else {
			if err := s.updateAgentDeliveryRowTx(ctx, tx, eventID, agentID, state); err != nil {
				return err
			}
			if err := s.upsertAgentReceiptRowTx(ctx, tx, eventID, agentID, state); err != nil {
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit event receipt tx: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return s.convergeStandaloneRuntimePlatformRunByEventID(ctx, s.DB, caps, eventID)
}

func buildAgentReceiptWriteState(baseRetryCount int, status runtimemanager.ReceiptStatus, errText string) agentReceiptWriteState {
	state := agentReceiptWriteState{
		finalStatus: status,
		errorText:   strings.TrimSpace(errText),
	}
	retryCount := baseRetryCount
	if status == runtimemanager.ReceiptStatusError {
		retryCount++
	}
	finalStatus := status
	if status == runtimemanager.ReceiptStatusError && retryCount >= 2 {
		finalStatus = runtimemanager.ReceiptStatusDeadLetter
	}
	state.finalStatus = finalStatus
	state.retryCount = retryCount
	state.reasonCode = managerReceiptReasonCode(finalStatus, state.errorText)
	state.deliveryCode = "delivered"
	switch status {
	case runtimemanager.ReceiptStatusError:
		state.deliveryCode = "failed"
	case runtimemanager.ReceiptStatusDeadLetter:
		state.deliveryCode = "dead_letter"
	}
	if state.finalStatus == runtimemanager.ReceiptStatusDeadLetter {
		state.deliveryCode = "dead_letter"
	}
	return state
}

func (s *PostgresStore) lockAgentDeliveryTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID, agentID string,
) (lockedAgentDelivery, error) {
	var delivery lockedAgentDelivery
	err := tx.QueryRowContext(ctx, `
		SELECT
			d.retry_count,
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.active_session_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			COALESCE(e.flow_instance, '')
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = $2
		FOR UPDATE
	`, eventID, agentID).Scan(&delivery.retryCount, &delivery.status, &delivery.reasonCode, &delivery.activeSessionID, &delivery.entityID, &delivery.flowInstance)
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
) (runtimemanager.EventReceipt, error) {
	if !delivery.found {
		return runtimemanager.EventReceipt{}, fmt.Errorf("apply delivery-backed terminal transition: delivery row required")
	}
	reasonCode := strings.TrimSpace(req.reasonCode)
	if reasonCode == "" {
		return runtimemanager.EventReceipt{}, fmt.Errorf("apply delivery-backed terminal transition: reason_code required")
	}
	if req.retryAdvance < 0 {
		return runtimemanager.EventReceipt{}, fmt.Errorf("apply delivery-backed terminal transition: retry advance must be non-negative")
	}
	state := agentReceiptWriteState{
		finalStatus:  runtimemanager.ReceiptStatusDeadLetter,
		retryCount:   delivery.retryCount + req.retryAdvance,
		reasonCode:   reasonCode,
		errorText:    strings.TrimSpace(req.errorText),
		deliveryCode: "dead_letter",
	}
	if err := s.updateAgentDeliveryRowTx(ctx, tx, eventID, agentID, state); err != nil {
		return runtimemanager.EventReceipt{}, err
	}
	if err := s.upsertAgentReceiptRowTx(ctx, tx, eventID, agentID, state); err != nil {
		return runtimemanager.EventReceipt{}, err
	}
	return runtimemanager.EventReceipt{
		EventID:    strings.TrimSpace(eventID),
		AgentID:    strings.TrimSpace(agentID),
		Status:     runtimemanager.ReceiptStatusDeadLetter,
		RetryCount: state.retryCount,
		Error:      state.errorText,
	}, nil
}

func (s *PostgresStore) updateAgentDeliveryRowTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID, agentID string,
	state agentReceiptWriteState,
) error {
	const q = `
		UPDATE event_deliveries
		SET
			status = $3,
			retry_count = $4,
			reason_code = NULLIF($5, ''),
			last_error = NULLIF($6, ''),
			active_session_id = NULL,
			delivered_at = now()
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $2
	`
	if _, err := tx.ExecContext(ctx, q, eventID, agentID, state.deliveryCode, state.retryCount, state.reasonCode, state.errorText); err != nil {
		return fmt.Errorf("sync event delivery: %w", err)
	}
	return nil
}

func (s *PostgresStore) upsertAgentReceiptRowTx(ctx context.Context, tx *sql.Tx, eventID, agentID string, state agentReceiptWriteState) error {
	sideEffects, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects(state.finalStatus, state.reasonCode, state.retryCount, state.errorText))
	if err != nil {
		return fmt.Errorf("marshal event receipt side effects: %w", err)
	}
	outcome := mapManagerReceiptStatusToOutcome(state.finalStatus)
	const q = `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			e.event_id, 'agent', $2, e.entity_id, e.flow_instance,
			$3, NULLIF($4,''), $5::jsonb, now()
		FROM events e
		WHERE e.event_id = $1::uuid
		ON CONFLICT (event_id, subscriber_id) DO UPDATE SET
			entity_id = EXCLUDED.entity_id,
			flow_instance = EXCLUDED.flow_instance,
			outcome = EXCLUDED.outcome,
			reason_code = EXCLUDED.reason_code,
			side_effects = EXCLUDED.side_effects,
			processed_at = now()
	`
	result, err := tx.ExecContext(ctx, q, eventID, agentID, outcome, state.reasonCode, string(sideEffects))
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
			last_error = NULL,
			active_session_id = COALESCE(NULLIF($3, '')::uuid, active_session_id),
			started_at = COALESCE(started_at, now()),
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
	if _, err := s.DB.ExecContext(ctx, q, eventID, agentID, sessionID, pq.Array(activeRunQuiescenceTerminalReasonCodes())); err != nil {
		return fmt.Errorf("mark event delivery in progress: %w", err)
	}
	return nil
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

func (s *PostgresStore) listPendingEventsForAgentSpec(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			d.subscriber_id,
			e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name, COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''), COALESCE(e.scope, 'global'),
			e.payload, e.created_at,
			COALESCE(e.source_event_id::text, ''),
			TRUE,
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			d.created_at,
			d.delivered_at,
			CASE WHEN r.event_id IS NULL THEN FALSE ELSE TRUE END
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		LEFT JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id = $1
		  AND e.created_at >= $2
		  AND `+canonicalPendingDeliveryPredicateSQL("d", "r")+`
		ORDER BY e.created_at ASC
		LIMIT $3
	`, agentID, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending events for %s: %w", agentID, err)
	}
	defer rows.Close()
	records, err := scanPendingAgentDeliveryRecords(rows)
	if err != nil {
		return nil, err
	}
	return pendingEventsFromRecords(records, time.Now(), limit), nil
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
			if eventidentity.MatchPattern(string(subscription), string(evt.Type)) {
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
		deliveryErr  string
		deliverySeen bool
		retryCount   sql.NullInt64
	)
	if err := s.DB.QueryRowContext(ctx, `
		SELECT
			r.outcome,
			COALESCE(r.side_effects, '{}'::jsonb),
			COALESCE(d.status, ''),
			COALESCE(d.last_error, ''),
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
	`, eventID, agentID).Scan(&outcome, &sideEffects, &delivery, &deliveryErr, &retryCount, &deliverySeen); err != nil {
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
		receipt.Error = payload.Error
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
			if strings.TrimSpace(deliveryErr) != "" {
				receipt.Error = strings.TrimSpace(deliveryErr)
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

func managerReceiptReasonCode(status runtimemanager.ReceiptStatus, errText string) string {
	if strings.TrimSpace(errText) != "" {
		switch status {
		case runtimemanager.ReceiptStatusDeadLetter:
			return "retry_exhausted"
		case runtimemanager.ReceiptStatusError:
			return "handler_error"
		default:
			return "runtime_handled"
		}
	}
	switch status {
	case runtimemanager.ReceiptStatusDeadLetter:
		return "retry_exhausted"
	default:
		return "agent_processed"
	}
}
