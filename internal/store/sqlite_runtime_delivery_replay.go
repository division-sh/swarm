package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"swarm/internal/events"
	"swarm/internal/runtime/core/eventidentity"
	runtimeownership "swarm/internal/runtime/core/ownership"
	runtimemanager "swarm/internal/runtime/manager"
	runtimereplayclaim "swarm/internal/runtime/replayclaim"
)

type sqliteReplayLease struct {
	store   *SQLiteRuntimeStore
	eventID string
}

func (s *SQLiteRuntimeStore) UpsertPipelineReceipt(ctx context.Context, eventID, status, errText string) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, errText)
}

func (s *SQLiteRuntimeStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status, errText string) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = "processed"
	}
	if strings.TrimSpace(errText) != "" && status == "processed" {
		status = "error"
	}
	reasonCode := pipelineReceiptReasonCode(status, errText)
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects(status, reasonCode, errText))
	if err != nil {
		return fmt.Errorf("marshal sqlite pipeline receipt side effects: %w", err)
	}
	outcome := mapPipelineStatusToOutcome(status)
	exec := s.DB.ExecContext
	if tx != nil {
		exec = tx.ExecContext
	}
	_, err = exec(ctx, `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		SELECT
			?, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance,
			?, ?, ?, ?
		FROM events e
		WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = excluded.outcome,
			reason_code = excluded.reason_code,
			side_effects = excluded.side_effects,
			processed_at = excluded.processed_at
	`, uuid.NewString(), outcome, sqliteNullString(reasonCode), string(sideEffects), s.now(), eventID)
	if err != nil {
		return fmt.Errorf("upsert sqlite pipeline receipt: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) InsertEventDeliveriesWithTargets(ctx context.Context, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	return s.InsertEventDeliveriesWithTargetsTx(ctx, nil, eventID, agentIDs, deliveryTargets)
}

func (s *SQLiteRuntimeStore) InsertEventDeliveriesWithTargetsTx(ctx context.Context, tx *sql.Tx, eventID string, agentIDs []string, deliveryTargets map[string]events.RouteIdentity) error {
	routes := make([]events.DeliveryRoute, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		routes = append(routes, events.DeliveryRoute{
			SubscriberType: "agent",
			SubscriberID:   agentID,
			Target:         deliveryTargets[agentID],
		})
	}
	return s.InsertEventDeliveryRoutesTx(ctx, tx, eventID, routes)
}

func (s *SQLiteRuntimeStore) InsertEventDeliveryRoutes(ctx context.Context, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	return s.InsertEventDeliveryRoutesTx(ctx, nil, eventID, deliveryRoutes)
}

func (s *SQLiteRuntimeStore) InsertEventDeliveryRoutesTx(ctx context.Context, tx *sql.Tx, eventID string, deliveryRoutes []events.DeliveryRoute) error {
	eventID = strings.TrimSpace(eventID)
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if eventID == "" || len(deliveryRoutes) == 0 {
		return nil
	}
	var runID sql.NullString
	if err := chooseRowQueryer(s.DB, tx).QueryRowContext(ctx, `SELECT run_id FROM events WHERE event_id = ?`, eventID).Scan(&runID); err != nil {
		return fmt.Errorf("load event run for sqlite delivery routes: %w", err)
	}
	ownedTx := tx == nil
	var err error
	if ownedTx {
		tx, err = s.DB.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin sqlite delivery routes tx: %w", err)
		}
	}
	committed := false
	defer func() {
		if ownedTx && !committed {
			_ = tx.Rollback()
		}
	}()
	for _, route := range deliveryRoutes {
		route = route.Normalized()
		if route.SubscriberType == "" || route.SubscriberID == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO event_deliveries (
				delivery_id, run_id, event_id, subscriber_type, subscriber_id, delivery_target_route,
				status, reason_code, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)
		`, uuid.NewString(), sqliteNullString(runID.String), eventID, route.SubscriberType, route.SubscriberID,
			string(routeIdentityJSON(route.Target)), deliveryRouteReasonCode(route), s.now()); err != nil {
			return fmt.Errorf("insert sqlite event delivery route (%s=%s): %w", route.SubscriberType, route.SubscriberID, err)
		}
	}
	if ownedTx {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit sqlite delivery routes tx: %w", err)
		}
		committed = true
	}
	return nil
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error {
	return s.PersistEventWithDeliveriesAndScope(ctx, evt, agentIDs, runtimereplayclaim.CommittedReplayScopeSubscribed)
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveriesAndScope(ctx context.Context, evt events.Event, agentIDs []string, scope runtimereplayclaim.CommittedReplayScope) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite event/delivery tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.AppendEventTx(ctx, tx, evt); err != nil {
		return err
	}
	if err := s.InsertEventDeliveriesTx(ctx, tx, evt.ID, agentIDs); err != nil {
		return err
	}
	if err := s.UpsertCommittedReplayScopeTx(ctx, tx, evt.ID, scope); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite event/delivery tx: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveryRoutesAndScope(ctx context.Context, evt events.Event, agentIDs []string, deliveryTargets map[string]events.RouteIdentity, scope runtimereplayclaim.CommittedReplayScope) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite event/routes tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.AppendEventTx(ctx, tx, evt); err != nil {
		return err
	}
	if err := s.InsertEventDeliveriesWithTargetsTx(ctx, tx, evt.ID, agentIDs, deliveryTargets); err != nil {
		return err
	}
	if err := s.UpsertCommittedReplayScopeTx(ctx, tx, evt.ID, scope); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite event/routes tx: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveryRouteSetAndScope(ctx context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, scope runtimereplayclaim.CommittedReplayScope) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite event/route-set tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.AppendEventTx(ctx, tx, evt); err != nil {
		return err
	}
	if err := s.InsertEventDeliveryRoutesTx(ctx, tx, evt.ID, deliveryRoutes); err != nil {
		return err
	}
	if err := s.UpsertCommittedReplayScopeTx(ctx, tx, evt.ID, scope); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite event/route-set tx: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) UpsertCommittedReplayScope(ctx context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	return s.UpsertCommittedReplayScopeTx(ctx, nil, eventID, scope)
}

func (s *SQLiteRuntimeStore) UpsertCommittedReplayScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	reasonCode, err := committedReplayScopeReasonCode(scope)
	if err != nil {
		return err
	}
	exec := s.DB.ExecContext
	if tx != nil {
		exec = tx.ExecContext
	}
	now := s.now()
	res, err := exec(ctx, `
		UPDATE event_deliveries
		SET reason_code = ?,
		    status = 'delivered',
		    delivered_at = ?
		WHERE event_id = ?
		  AND subscriber_type = ?
		  AND subscriber_id = ?
	`, reasonCode, now, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID)
	if err != nil {
		return fmt.Errorf("update sqlite committed replay scope: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows > 0 {
		return nil
	}
	_, err = exec(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id,
			status, reason_code, delivered_at, created_at
		)
		SELECT ?, e.run_id, e.event_id, ?, ?, 'delivered', ?, ?, ?
		FROM events e
		WHERE e.event_id = ?
	`, uuid.NewString(), replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID, reasonCode, now, now, eventID)
	if err != nil {
		return fmt.Errorf("insert sqlite committed replay scope: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) LoadCommittedReplayScope(ctx context.Context, eventID string) (runtimereplayclaim.CommittedReplayScope, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	}
	var reasonCode string
	err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(reason_code, '')
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = ?
		  AND subscriber_id = ?
		ORDER BY created_at DESC, delivery_id DESC
		LIMIT 1
	`, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID).Scan(&reasonCode)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", runtimereplayclaim.ErrMissingCommittedReplayScope
	case err != nil:
		return "", fmt.Errorf("load sqlite committed replay scope: %w", err)
	}
	scope, ok := committedReplayScopeFromReasonCode(reasonCode)
	if !ok {
		return "", fmt.Errorf("load sqlite committed replay scope: unrecognized reason_code %q", strings.TrimSpace(reasonCode))
	}
	return scope, nil
}

func (s *SQLiteRuntimeStore) ListEventDeliveryTargets(ctx context.Context, eventID string) (map[string]events.RouteIdentity, error) {
	routes, err := s.ListEventDeliveryRoutes(ctx, eventID)
	if err != nil {
		return nil, err
	}
	out := map[string]events.RouteIdentity{}
	for _, route := range routes {
		if route.SubscriberType == "agent" && !route.Target.Empty() {
			out[route.SubscriberID] = route.Target
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) ListEventDeliveryRoutes(ctx context.Context, eventID string) ([]events.DeliveryRoute, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, nil
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT subscriber_type, subscriber_id, COALESCE(delivery_target_route, '{}')
		FROM event_deliveries
		WHERE event_id = ?
		  AND NOT (subscriber_type = ? AND subscriber_id = ?)
		ORDER BY created_at ASC, delivery_id ASC
	`, eventID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID)
	if err != nil {
		return nil, fmt.Errorf("list sqlite event delivery routes: %w", err)
	}
	defer rows.Close()
	out := make([]events.DeliveryRoute, 0, 8)
	for rows.Next() {
		var route events.DeliveryRoute
		var raw json.RawMessage
		if err := rows.Scan(&route.SubscriberType, &route.SubscriberID, &raw); err != nil {
			return nil, fmt.Errorf("scan sqlite event delivery route: %w", err)
		}
		route.Target = decodeRouteIdentityJSON(raw)
		out = append(out, route)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite event delivery routes: %w", err)
	}
	return events.NormalizeDeliveryRoutes(out), nil
}

func (s *SQLiteRuntimeStore) ListEventsMissingPipelineReceipt(ctx context.Context, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	return s.listSQLiteEventsMissingPipelineReceipt(ctx, "e.created_at >= ?", []any{since.UTC()}, limit)
}

func (s *SQLiteRuntimeStore) ListEventsMissingPipelineReceiptForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 200
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	return s.listSQLiteEventsMissingPipelineReceipt(ctx, "e.run_id = ? AND e.created_at >= ?", []any{runID, since.UTC()}, limit)
}

func (s *SQLiteRuntimeStore) listSQLiteEventsMissingPipelineReceipt(ctx context.Context, where string, args []any, limit int) ([]events.PersistedReplayEvent, error) {
	args = append(append([]any{}, args...), limit)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id,
			COALESCE(e.run_id, ''),
			e.event_name,
			COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''),
			COALESCE(e.scope, 'global'),
			e.payload,
			e.created_at,
			COALESCE(e.source_event_id, ''),
			COALESCE(e.source_route, '{}'),
			COALESCE(e.target_route, '{}'),
			COALESCE(e.target_set, '[]')
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  AND `+where+`
		ORDER BY e.created_at ASC, e.event_id ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite events missing pipeline receipt: %w", err)
	}
	defer rows.Close()
	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var evt events.Event
		var entityID, flowInstance, scope string
		var payloadRaw, createdAtRaw, sourceRouteRaw, targetRouteRaw, targetSetRaw any
		if err := rows.Scan(
			&evt.ID,
			&evt.RunID,
			&evt.Type,
			&evt.SourceAgent,
			&entityID,
			&flowInstance,
			&scope,
			&payloadRaw,
			&createdAtRaw,
			&evt.ParentEventID,
			&sourceRouteRaw,
			&targetRouteRaw,
			&targetSetRaw,
		); err != nil {
			return nil, fmt.Errorf("scan sqlite missing pipeline receipt event: %w", err)
		}
		evt.Payload = sqliteJSONRawMessage(payloadRaw)
		if createdAt, ok, err := sqliteTimeValue(createdAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite missing pipeline receipt created_at: %w", err)
		} else if ok {
			evt.CreatedAt = createdAt
		}
		sourceRoute := sqliteJSONRawMessage(sourceRouteRaw)
		targetRoute := sqliteJSONRawMessage(targetRouteRaw)
		targetSet := sqliteJSONRawMessage(targetSetRaw)
		evt = evt.WithEnvelope(eventEnvelopeFromStorage(entityID, flowInstance, scope, sourceRoute, targetRoute, targetSet))
		record := events.PersistedReplayEvent{Event: evt}
		if strings.TrimSpace(evt.RunID) == "" {
			record.ReplayError = "missing canonical run_id"
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) ClaimPipelineReplay(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	var pending bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM events e
			LEFT JOIN event_receipts r
				ON r.event_id = e.event_id
				AND r.subscriber_type = 'platform'
				AND r.subscriber_id = 'pipeline'
			WHERE e.event_id = ?
			  AND r.event_id IS NULL
		)
	`, eventID).Scan(&pending); err != nil {
		return nil, false, fmt.Errorf("claim sqlite pipeline replay: %w", err)
	}
	if !pending {
		return nil, false, nil
	}
	s.replayMu.Lock()
	if s.replayClaims == nil {
		s.replayClaims = map[string]struct{}{}
	}
	if _, exists := s.replayClaims[eventID]; exists {
		s.replayMu.Unlock()
		return nil, false, nil
	}
	s.replayClaims[eventID] = struct{}{}
	s.replayMu.Unlock()
	return &sqliteReplayLease{store: s, eventID: eventID}, true, nil
}

func (l *sqliteReplayLease) Release(context.Context) error {
	if l == nil || l.store == nil {
		return nil
	}
	l.store.replayMu.Lock()
	delete(l.store.replayClaims, strings.TrimSpace(l.eventID))
	l.store.replayMu.Unlock()
	return nil
}

func (s *SQLiteRuntimeStore) MarkEventDeliveryInProgress(ctx context.Context, eventID, agentID, sessionID string) error {
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return fmt.Errorf("mark sqlite event delivery in progress: eventID and agentID required")
	}
	res, err := s.DB.ExecContext(ctx, `
		UPDATE event_deliveries
		SET status = 'in_progress',
		    active_session_id = ?,
		    started_at = COALESCE(started_at, ?)
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		  AND subscriber_id = ?
		  AND status IN ('pending', 'failed', 'in_progress')
	`, sqliteNullUUID(sessionID), s.now(), eventID, agentID)
	if err != nil {
		return fmt.Errorf("mark sqlite event delivery in progress: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return fmt.Errorf("mark sqlite event delivery in progress: delivery row required for event %s agent %s", eventID, agentID)
	}
	return nil
}

func (s *SQLiteRuntimeStore) UpsertEventReceipt(ctx context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, errText string) error {
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return fmt.Errorf("upsert sqlite event receipt: eventID and agentID required")
	}
	if status == "" {
		return fmt.Errorf("upsert sqlite event receipt: status required")
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin sqlite event receipt tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	delivery, err := s.sqliteLockAgentDeliveryTx(ctx, tx, eventID, agentID)
	if err != nil {
		return err
	}
	if !delivery.found {
		return fmt.Errorf("upsert sqlite event receipt: delivery row required for event %s agent %s", eventID, agentID)
	}
	state := buildAgentReceiptWriteState(delivery.retryCount, status, errText)
	now := s.now()
	if _, err := tx.ExecContext(ctx, `
		UPDATE event_deliveries
		SET status = ?,
		    retry_count = ?,
		    reason_code = ?,
		    last_error = ?,
		    delivered_at = ?
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		  AND subscriber_id = ?
	`, state.deliveryCode, state.retryCount, sqliteNullString(state.reasonCode), sqliteNullString(state.errorText), now, eventID, agentID); err != nil {
		return fmt.Errorf("update sqlite event delivery receipt state: %w", err)
	}
	sideEffects, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects(state.finalStatus, state.reasonCode, state.retryCount, state.errorText))
	if err != nil {
		return fmt.Errorf("marshal sqlite agent receipt side effects: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, side_effects, processed_at
		)
		VALUES (?, ?, 'agent', ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = excluded.outcome,
			reason_code = excluded.reason_code,
			side_effects = excluded.side_effects,
			processed_at = excluded.processed_at
	`, uuid.NewString(), eventID, agentID, sqliteNullUUID(delivery.entityID), sqliteNullString(delivery.flowInstance),
		mapManagerReceiptStatusToOutcome(state.finalStatus), sqliteNullString(state.reasonCode), string(sideEffects), now); err != nil {
		return fmt.Errorf("upsert sqlite event receipt row: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit sqlite event receipt tx: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) GetEventReceipt(ctx context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("event_id and agent_id are required")
	}
	var rec runtimemanager.EventReceipt
	var outcome string
	err := s.DB.QueryRowContext(ctx, `
		SELECT event_id, subscriber_id, outcome, COALESCE(reason_code, '')
		FROM event_receipts
		WHERE event_id = ?
		  AND subscriber_type = 'agent'
		  AND subscriber_id = ?
	`, eventID, agentID).Scan(&rec.EventID, &rec.AgentID, &outcome, &rec.Error)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimemanager.EventReceipt{}, false, nil
	}
	if err != nil {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("get sqlite event receipt: %w", err)
	}
	rec.Status = mapOutcomeToManagerReceiptStatus(outcome)
	return rec, true, nil
}

func (s *SQLiteRuntimeStore) ListPendingEventsForAgent(ctx context.Context, agentID string, since time.Time, limit int) ([]events.Event, error) {
	if strings.TrimSpace(agentID) == "" {
		return nil, nil
	}
	page, err := s.ListPendingAgentDeliveryDetails(ctx, PendingAgentDeliveryListOptions{
		AgentID: strings.TrimSpace(agentID),
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

func (s *SQLiteRuntimeStore) ListPendingSubscribedEvents(ctx context.Context, agentID string, subscriptions []events.EventType, since time.Time, limit int) ([]events.Event, error) {
	if strings.TrimSpace(agentID) == "" || len(subscriptions) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	if since.IsZero() {
		since = time.Now().Add(-30 * 24 * time.Hour)
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id,
			COALESCE(e.run_id, ''),
			e.event_name,
			COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''),
			COALESCE(e.scope, 'global'),
			e.payload,
			e.created_at,
			COALESCE(e.source_event_id, '')
		FROM events e
		LEFT JOIN event_deliveries d
			ON d.event_id = e.event_id
			AND d.subscriber_type = 'agent'
			AND d.subscriber_id = ?
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = ?
		WHERE e.created_at >= ?
		  AND d.delivery_id IS NULL
		  AND r.event_id IS NULL
		ORDER BY e.created_at ASC, e.event_id ASC
		LIMIT ?
	`, strings.TrimSpace(agentID), strings.TrimSpace(agentID), since.UTC(), limit*4)
	if err != nil {
		return nil, fmt.Errorf("query sqlite pending subscribed events: %w", err)
	}
	defer rows.Close()
	out := make([]events.Event, 0, limit)
	for rows.Next() {
		var evt events.Event
		var entityID, flowInstance, scope string
		var payloadRaw, createdAtRaw any
		if err := rows.Scan(&evt.ID, &evt.RunID, &evt.Type, &evt.SourceAgent, &entityID, &flowInstance, &scope, &payloadRaw, &createdAtRaw, &evt.ParentEventID); err != nil {
			return nil, fmt.Errorf("scan sqlite pending subscribed event: %w", err)
		}
		evt.Payload = sqliteJSONRawMessage(payloadRaw)
		if createdAt, ok, err := sqliteTimeValue(createdAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite pending subscribed event created_at: %w", err)
		} else if ok {
			evt.CreatedAt = createdAt
		}
		if !eventMatchesAnyPattern(evt.Type, subscriptions) {
			continue
		}
		out = append(out, evt.WithEnvelope(events.EventEnvelope{EntityID: entityID, FlowInstance: flowInstance, Scope: events.EventScope(scope)}))
		if len(out) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite pending subscribed events: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) ListPendingAgentDeliveryFacts(ctx context.Context, agentIDs []string, since time.Time) (map[string]PendingAgentDeliveryFacts, error) {
	normalized := normalizePendingAgentIDs(agentIDs)
	out := make(map[string]PendingAgentDeliveryFacts, len(normalized))
	for _, agentID := range normalized {
		out[agentID] = PendingAgentDeliveryFacts{}
	}
	if len(normalized) == 0 {
		return out, nil
	}
	records, err := s.listSQLitePendingAgentDeliveryRecords(ctx, normalized, since)
	if err != nil {
		return nil, err
	}
	grouped := make(map[string][]pendingAgentDeliveryRecord, len(normalized))
	for _, record := range records {
		grouped[record.AgentID] = append(grouped[record.AgentID], record)
	}
	now := s.now()
	for _, agentID := range normalized {
		out[agentID] = pendingAgentDeliveryFactsFromRecords(grouped[agentID], now)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) ListPendingAgentDeliveryDetails(ctx context.Context, opts PendingAgentDeliveryListOptions) (PendingAgentDeliveryPage, error) {
	opts.AgentID = strings.TrimSpace(opts.AgentID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.AgentID == "" {
		return PendingAgentDeliveryPage{PendingDeliveries: []PendingAgentDeliveryDetail{}}, nil
	}
	if opts.Limit == 0 {
		opts.Limit = DefaultPendingAgentDeliveryDetailLimit
	}
	if opts.Limit < 0 || opts.Limit > MaxPendingAgentDeliveryDetailLimit {
		return PendingAgentDeliveryPage{}, fmt.Errorf("pending agent delivery detail limit must be from 1 to %d", MaxPendingAgentDeliveryDetailLimit)
	}
	var cursor *pendingAgentDeliveryCursorPosition
	if opts.Cursor != "" {
		decoded, err := decodePendingAgentDeliveryCursor(opts.Cursor)
		if err != nil {
			return PendingAgentDeliveryPage{}, err
		}
		cursor = &decoded
	}
	records, err := s.listSQLitePendingAgentDeliveryRecords(ctx, []string{opts.AgentID}, opts.Since)
	if err != nil {
		return PendingAgentDeliveryPage{}, err
	}
	return pendingAgentDeliveryPageFromRecords(records, s.now(), opts.Limit, cursor)
}

func (s *SQLiteRuntimeStore) listSQLitePendingAgentDeliveryRecords(ctx context.Context, agentIDs []string, since time.Time) ([]pendingAgentDeliveryRecord, error) {
	if len(agentIDs) == 0 {
		return nil, nil
	}
	args := make([]any, 0, len(agentIDs)+1)
	placeholders := make([]string, 0, len(agentIDs))
	for _, agentID := range agentIDs {
		placeholders = append(placeholders, "?")
		args = append(args, agentID)
	}
	sinceClause := ""
	if !since.IsZero() {
		sinceClause = "AND e.created_at >= ?"
		args = append(args, since.UTC())
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			d.subscriber_id,
			e.event_id,
			COALESCE(e.run_id, ''),
			e.event_name,
			COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''),
			COALESCE(e.scope, 'global'),
			e.payload,
			e.created_at,
			COALESCE(e.source_event_id, ''),
			1,
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			d.created_at,
			d.delivered_at,
			CASE WHEN r.event_id IS NULL THEN 0 ELSE 1 END
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		LEFT JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.subscriber_type = 'agent'
		  AND d.subscriber_id IN (`+strings.Join(placeholders, ",")+`)
		  `+sinceClause+`
		ORDER BY d.subscriber_id ASC, e.created_at ASC, e.event_id ASC
	`, args...)
	if err != nil {
		return nil, fmt.Errorf("query sqlite pending agent delivery records: %w", err)
	}
	defer rows.Close()
	out := make([]pendingAgentDeliveryRecord, 0)
	for rows.Next() {
		var (
			record                 pendingAgentDeliveryRecord
			entityID, flowInstance string
			scope                  string
			payloadRaw             any
			eventCreatedRaw        any
			deliveryCreatedRaw     any
			deliveryDeliveredRaw   any
		)
		if err := rows.Scan(
			&record.AgentID,
			&record.Event.ID,
			&record.Event.RunID,
			&record.Event.Type,
			&record.Event.SourceAgent,
			&entityID,
			&flowInstance,
			&scope,
			&payloadRaw,
			&eventCreatedRaw,
			&record.Event.ParentEventID,
			&record.DeliveryFound,
			&record.DeliveryStatus,
			&record.DeliveryRetryCount,
			&deliveryCreatedRaw,
			&deliveryDeliveredRaw,
			&record.ReceiptFound,
		); err != nil {
			return nil, fmt.Errorf("scan pending agent delivery record: %w", err)
		}
		eventCreatedAt, ok, err := sqliteTimeValue(eventCreatedRaw)
		if err != nil {
			return nil, fmt.Errorf("scan pending agent event created_at: %w", err)
		}
		if ok {
			record.Event.CreatedAt = eventCreatedAt
		}
		deliveryCreatedAt, ok, err := sqliteTimeValue(deliveryCreatedRaw)
		if err != nil {
			return nil, fmt.Errorf("scan pending agent delivery created_at: %w", err)
		}
		if ok {
			record.DeliveryCreatedAt = deliveryCreatedAt
		}
		deliveryDeliveredAt, ok, err := sqliteTimeValue(deliveryDeliveredRaw)
		if err != nil {
			return nil, fmt.Errorf("scan pending agent delivery delivered_at: %w", err)
		}
		if ok {
			record.DeliveryDeliveredAt = sql.NullTime{Time: deliveryDeliveredAt, Valid: true}
		}
		record.AgentID = strings.TrimSpace(record.AgentID)
		record.Event.Payload = sqliteJSONRawMessage(payloadRaw)
		record.Event = record.Event.WithEnvelope(events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: flowInstance,
			Scope:        events.EventScope(scope),
		})
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending agent delivery records: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) sqliteLockAgentDeliveryTx(ctx context.Context, tx *sql.Tx, eventID, agentID string) (lockedAgentDelivery, error) {
	var delivery lockedAgentDelivery
	err := tx.QueryRowContext(ctx, `
		SELECT
			COALESCE(d.retry_count, 0),
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.active_session_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, '')
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE d.event_id = ?
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = ?
	`, eventID, agentID).Scan(&delivery.retryCount, &delivery.status, &delivery.reasonCode, &delivery.activeSessionID, &delivery.entityID, &delivery.flowInstance)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return lockedAgentDelivery{}, nil
	case err != nil:
		return lockedAgentDelivery{}, fmt.Errorf("lock sqlite event delivery row: %w", err)
	default:
		delivery.found = true
		return delivery, nil
	}
}

func eventMatchesAnyPattern(eventType events.EventType, subscriptions []events.EventType) bool {
	name := strings.TrimSpace(string(eventType))
	if name == "" {
		return false
	}
	for _, subscription := range subscriptions {
		if eventidentity.MatchPattern(strings.TrimSpace(string(subscription)), name) {
			return true
		}
	}
	return false
}

func sqliteJSONRawMessage(raw any) json.RawMessage {
	switch v := raw.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), v...)
	case []byte:
		return json.RawMessage(append([]byte(nil), v...))
	case string:
		return json.RawMessage(v)
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return encoded
	}
}
