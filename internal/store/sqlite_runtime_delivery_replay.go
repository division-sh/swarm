package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
)

type sqliteReplayLease struct {
	store   *SQLiteRuntimeStore
	eventID string
}

func (s *SQLiteRuntimeStore) UpsertPipelineReceipt(ctx context.Context, eventID, status string, failure *runtimefailures.Envelope) error {
	return s.UpsertPipelineReceiptTx(ctx, nil, eventID, status, failure)
}

func (s *SQLiteRuntimeStore) HasProcessedPipelineReceipt(ctx context.Context, eventID string) (bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false, nil
	}
	var processed bool
	err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM event_receipts
			WHERE event_id = ?
			  AND subscriber_type = 'platform'
			  AND subscriber_id = 'pipeline'
			  AND outcome = 'success'
			  AND reason_code = 'pipeline_persisted'
		)
	`, eventID).Scan(&processed)
	if err != nil {
		return false, fmt.Errorf("load sqlite processed pipeline receipt: %w", err)
	}
	return processed, nil
}

func (s *SQLiteRuntimeStore) UpsertPipelineReceiptTx(ctx context.Context, tx *sql.Tx, eventID, status string, failure *runtimefailures.Envelope) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	if tx == nil {
		return s.runRuntimeMutation(ctx, "sqlite pipeline receipt", func(txctx context.Context, tx *sql.Tx) error {
			return s.UpsertPipelineReceiptTx(txctx, tx, eventID, status, failure)
		})
	}
	if err := requireActiveRunForPipelineReceipt(ctx, tx, eventID, storerunlifecycle.DialectSQLite); err != nil {
		return err
	}
	status = strings.TrimSpace(strings.ToLower(status))
	if status == "" {
		status = "processed"
	}
	if failure != nil && status == "processed" {
		status = "error"
	}
	reasonCode := pipelineReceiptReasonCode(status, failure)
	failureJSON, err := encodeStoredFailure(failure)
	if err != nil {
		return err
	}
	sideEffects, err := marshalPipelineReceiptSideEffects(newPipelineReceiptSideEffects(status, reasonCode))
	if err != nil {
		return fmt.Errorf("marshal sqlite pipeline receipt side effects: %w", err)
	}
	outcome := mapPipelineStatusToOutcome(status)
	terminalReasons := activeRunQuiescenceTerminalReasonCodes()
	args := []any{uuid.NewString(), outcome, sqliteNullString(reasonCode), failureJSON, string(sideEffects), s.now(), eventID}
	for _, reason := range terminalReasons {
		args = append(args, reason)
	}
	for _, reason := range terminalReasons {
		args = append(args, reason)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT
			?, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance,
			?, ?, ?, ?, ?
		FROM events e
		WHERE e.event_id = ?
		  AND NOT EXISTS (
			SELECT 1
			FROM event_deliveries d
			WHERE d.event_id = e.event_id
			  AND d.status = 'dead_letter'
			  AND d.reason_code IN (`+sqlitePlaceholders(len(terminalReasons))+`)
		  )
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = excluded.outcome,
			reason_code = excluded.reason_code,
			failure = excluded.failure,
			side_effects = excluded.side_effects,
			processed_at = excluded.processed_at
		WHERE COALESCE(event_receipts.reason_code, '') NOT IN (`+sqlitePlaceholders(len(terminalReasons))+`)
	`, args...)
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
	if err := events.ValidateDeliveryRouteProjections(deliveryRoutes); err != nil {
		return err
	}
	deliveryRoutes = events.NormalizeDeliveryRoutes(deliveryRoutes)
	if eventID == "" || len(deliveryRoutes) == 0 {
		return nil
	}
	ownedTx := tx == nil
	if ownedTx {
		return s.runRuntimeMutation(ctx, "sqlite event delivery routes", func(txctx context.Context, tx *sql.Tx) error {
			return s.InsertEventDeliveryRoutesTx(txctx, tx, eventID, deliveryRoutes)
		})
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectSQLite); err != nil {
		return err
	}
	var runID sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT run_id FROM events WHERE event_id = ?`, eventID).Scan(&runID); err != nil {
		return fmt.Errorf("load event run for sqlite delivery routes: %w", err)
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if !caps.Events.DeliveryTargetRoute || !caps.Events.DeliveryContext || !caps.Events.DeliveryPayloadProjection {
		return fmt.Errorf("event_deliveries requires delivery_target_route, delivery_context, and delivery_payload_projection")
	}
	for _, route := range deliveryRoutes {
		route = route.Normalized()
		if route.SubscriberType == "" || route.SubscriberID == "" {
			continue
		}
		projectionRaw, err := deliveryPayloadProjectionJSON(route.PayloadProjection)
		if err != nil {
			return fmt.Errorf("encode sqlite event delivery projection (%s=%s): %w", route.SubscriberType, route.SubscriberID, err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO event_deliveries (
				delivery_id, run_id, event_id, subscriber_type, subscriber_id,
				delivery_target_route, delivery_context, delivery_payload_projection,
				status, reason_code, created_at
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
		`, uuid.NewString(), sqliteNullString(runID.String), eventID, route.SubscriberType, route.SubscriberID,
			string(routeIdentityJSON(route.Target)), string(deliveryContextJSON(route.Context)), string(projectionRaw), deliveryRouteReasonCode(route), s.now()); err != nil {
			return fmt.Errorf("insert sqlite event delivery route (%s=%s): %w", route.SubscriberType, route.SubscriberID, err)
		}
	}
	return nil
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveries(ctx context.Context, evt events.Event, agentIDs []string) error {
	_, err := s.PersistEventWithDeliveriesOutcome(ctx, evt, agentIDs)
	return err
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveriesOutcome(ctx context.Context, evt events.Event, agentIDs []string) (runtimebus.EventAppendOutcome, error) {
	if err := rejectDiagnosticDirectDeliveryPersistence(evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return s.PersistEventWithDeliveriesAndScopeOutcome(ctx, evt, agentIDs, runtimereplayclaim.CommittedReplayScopeSubscribed)
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveriesAndScope(ctx context.Context, evt events.Event, agentIDs []string, scope runtimereplayclaim.CommittedReplayScope) error {
	_, err := s.PersistEventWithDeliveriesAndScopeOutcome(ctx, evt, agentIDs, scope)
	return err
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveriesAndScopeOutcome(ctx context.Context, evt events.Event, agentIDs []string, scope runtimereplayclaim.CommittedReplayScope) (runtimebus.EventAppendOutcome, error) {
	if err := rejectDiagnosticDirectDeliveryPersistence(evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return s.persistEventAtomicOutcome(ctx, "sqlite event/delivery", evt, func(txctx context.Context, tx *sql.Tx) error {
		if err := s.InsertEventDeliveriesTx(txctx, tx, evt.ID(), agentIDs); err != nil {
			return err
		}
		return s.UpsertCommittedReplayScopeTx(txctx, tx, evt.ID(), scope)
	})
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveryRoutesAndScope(ctx context.Context, evt events.Event, agentIDs []string, deliveryTargets map[string]events.RouteIdentity, scope runtimereplayclaim.CommittedReplayScope) error {
	_, err := s.PersistEventWithDeliveryRoutesAndScopeOutcome(ctx, evt, agentIDs, deliveryTargets, scope)
	return err
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveryRoutesAndScopeOutcome(ctx context.Context, evt events.Event, agentIDs []string, deliveryTargets map[string]events.RouteIdentity, scope runtimereplayclaim.CommittedReplayScope) (runtimebus.EventAppendOutcome, error) {
	if err := rejectDiagnosticDirectDeliveryPersistence(evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return s.persistEventAtomicOutcome(ctx, "sqlite event/routes", evt, func(txctx context.Context, tx *sql.Tx) error {
		if err := s.InsertEventDeliveriesWithTargetsTx(txctx, tx, evt.ID(), agentIDs, deliveryTargets); err != nil {
			return err
		}
		return s.UpsertCommittedReplayScopeTx(txctx, tx, evt.ID(), scope)
	})
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveryRouteSetAndScope(ctx context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, scope runtimereplayclaim.CommittedReplayScope) error {
	_, err := s.PersistEventWithDeliveryRouteSetAndScopeOutcome(ctx, evt, deliveryRoutes, scope)
	return err
}

func (s *SQLiteRuntimeStore) PersistEventWithDeliveryRouteSetAndScopeOutcome(ctx context.Context, evt events.Event, deliveryRoutes []events.DeliveryRoute, scope runtimereplayclaim.CommittedReplayScope) (runtimebus.EventAppendOutcome, error) {
	if err := rejectDiagnosticDirectDeliveryPersistence(evt); err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	return s.persistEventAtomicOutcome(ctx, "sqlite event/route-set", evt, func(txctx context.Context, tx *sql.Tx) error {
		if err := s.InsertEventDeliveryRoutesTx(txctx, tx, evt.ID(), deliveryRoutes); err != nil {
			return err
		}
		return s.UpsertCommittedReplayScopeTx(txctx, tx, evt.ID(), scope)
	})
}

func (s *SQLiteRuntimeStore) persistEventAtomicOutcome(
	ctx context.Context,
	operation string,
	evt events.Event,
	persistSideEffects func(context.Context, *sql.Tx) error,
) (runtimebus.EventAppendOutcome, error) {
	outcome := runtimebus.EventAppendOutcomeUnknown
	err := s.runAuthorActivityMutation(ctx, operation, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		outcome, err = s.AppendEventTxOutcome(txctx, tx, evt)
		if err != nil || outcome == runtimebus.EventAppendExactDuplicate {
			return err
		}
		if persistSideEffects != nil {
			return persistSideEffects(txctx, tx)
		}
		return nil
	})
	if err != nil {
		return runtimebus.EventAppendOutcomeUnknown, err
	}
	if outcome == runtimebus.EventAppendOutcomeUnknown {
		return runtimebus.EventAppendOutcomeUnknown, fmt.Errorf("sqlite event transaction completed without append outcome")
	}
	return outcome, nil
}

func (s *SQLiteRuntimeStore) UpsertCommittedReplayScope(ctx context.Context, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	return s.UpsertCommittedReplayScopeTx(ctx, nil, eventID, scope)
}

func (s *SQLiteRuntimeStore) UpsertCommittedReplayScopeTx(ctx context.Context, tx *sql.Tx, eventID string, scope runtimereplayclaim.CommittedReplayScope) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil
	}
	if tx == nil {
		return s.runRuntimeMutation(ctx, "sqlite committed replay scope", func(txctx context.Context, tx *sql.Tx) error {
			return s.UpsertCommittedReplayScopeTx(txctx, tx, eventID, scope)
		})
	}
	if err := requireActiveRunForEvent(ctx, tx, eventID, storerunlifecycle.DialectSQLite); err != nil {
		return err
	}
	reasonCode, err := committedReplayScopeReasonCode(scope)
	if err != nil {
		return err
	}
	now := s.now()
	res, err := tx.ExecContext(ctx, `
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
	_, err = tx.ExecContext(ctx, `
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
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if !caps.Events.DeliveryTargetRoute || !caps.Events.DeliveryContext || !caps.Events.DeliveryPayloadProjection {
		return nil, fmt.Errorf("event_deliveries requires delivery_target_route, delivery_context, and delivery_payload_projection")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT subscriber_type, subscriber_id, COALESCE(delivery_target_route, '{}'),
		       COALESCE(delivery_context, '{}'), COALESCE(delivery_payload_projection, '{}')
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
		var rawValue, contextValue, projectionValue any
		if err := rows.Scan(&route.SubscriberType, &route.SubscriberID, &rawValue, &contextValue, &projectionValue); err != nil {
			return nil, fmt.Errorf("scan sqlite event delivery route: %w", err)
		}
		raw := jsonRawMessageValue(rawValue)
		contextRaw := jsonRawMessageValue(contextValue)
		route.Target = decodeRouteIdentityJSON(raw)
		route.Context = decodeDeliveryContextJSON(contextRaw)
		projection, err := decodeDeliveryPayloadProjectionJSON(jsonRawMessageValue(projectionValue))
		if err != nil {
			return nil, fmt.Errorf("decode sqlite event delivery route projection (%s=%s): %w", route.SubscriberType, route.SubscriberID, err)
		}
		route.PayloadProjection = projection
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

func (s *SQLiteRuntimeStore) ListEventsWithPendingDeliveriesForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
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
	return s.listSQLiteEventsWithPendingDeliveriesForRun(ctx, runID, since.UTC(), limit)
}

func (s *SQLiteRuntimeStore) listSQLiteEventsMissingPipelineReceipt(ctx context.Context, where string, args []any, limit int) ([]events.PersistedReplayEvent, error) {
	queryArgs := append([]any{}, args...)
	queryArgs = append(queryArgs, diagnosticDirectReplayEventArgs()...)
	queryArgs = append(queryArgs, limit)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id,
			COALESCE(e.run_id, ''),
			e.event_name,
			COALESCE(e.task_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''),
			COALESCE(e.scope, 'global'),
			e.payload,
			COALESCE(e.chain_depth, 0),
			COALESCE(e.produced_by, ''),
			COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id, ''),
			e.created_at,
			e.execution_mode,
			COALESCE(e.source_route, '{}'),
			COALESCE(e.target_route, '{}'),
			COALESCE(e.target_set, '[]')
		FROM events e
		LEFT JOIN runs run ON run.run_id = e.run_id
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE r.event_id IS NULL
		  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
		  AND `+where+`
		  AND NOT EXISTS (
			SELECT 1 FROM decision_card_route_obligations o
			WHERE o.event_id = e.event_id AND o.status <> 'completed'
		  )
		  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		ORDER BY e.created_at ASC, e.event_id ASC
		LIMIT ?
	`, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite events missing pipeline receipt: %w", err)
	}
	defer rows.Close()
	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var eventID string
		var row persistedEventIdentity
		var payloadRaw, createdAtRaw, sourceRouteRaw, targetRouteRaw, targetSetRaw any
		if err := rows.Scan(
			&eventID,
			&row.RunID,
			&row.EventName,
			&row.TaskID,
			&row.EntityID,
			&row.FlowInstance,
			&row.Scope,
			&payloadRaw,
			&row.ChainDepth,
			&row.ProducedBy,
			&row.ProducedByType,
			&row.SourceEventID,
			&createdAtRaw,
			&row.ExecutionMode,
			&sourceRouteRaw,
			&targetRouteRaw,
			&targetSetRaw,
		); err != nil {
			return nil, fmt.Errorf("scan sqlite missing pipeline receipt event: %w", err)
		}
		row.Payload = sqliteJSONRawMessage(payloadRaw)
		if parsedCreatedAt, ok, err := sqliteTimeValue(createdAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite missing pipeline receipt created_at: %w", err)
		} else if ok {
			row.CreatedAt = parsedCreatedAt.UTC()
		}
		row.SourceRoute = sqliteJSONRawMessage(sourceRouteRaw)
		row.TargetRoute = sqliteJSONRawMessage(targetRouteRaw)
		row.TargetSet = sqliteJSONRawMessage(targetSetRaw)
		row.EventID = eventID
		evt, err := eventFromPersistedIdentity(row)
		if err != nil {
			return nil, err
		}
		record := events.PersistedReplayEvent{Event: evt}
		if strings.TrimSpace(evt.RunID()) == "" {
			record.ReplayFailure = replayAdmissionFailure("missing_canonical_run_id")
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite missing pipeline receipt events: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) listSQLiteEventsWithPendingDeliveriesForRun(ctx context.Context, runID string, since time.Time, limit int) ([]events.PersistedReplayEvent, error) {
	queryArgs := append([]any{runID, since}, diagnosticDirectReplayEventArgs()...)
	queryArgs = append(queryArgs, limit)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id,
			COALESCE(e.run_id, ''),
			e.event_name,
			COALESCE(e.task_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''),
			COALESCE(e.scope, 'global'),
			e.payload,
			COALESCE(e.chain_depth, 0),
			COALESCE(e.produced_by, ''),
			COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id, ''),
			e.created_at,
			e.execution_mode,
			COALESCE(e.source_route, '{}'),
			COALESCE(e.target_route, '{}'),
			COALESCE(e.target_set, '[]')
		FROM events e
		JOIN runs run ON run.run_id = e.run_id
		WHERE e.run_id = ?
		  AND run.status IN ('running', 'paused')
		  AND e.created_at >= ?
		  AND EXISTS (
			SELECT 1
			FROM event_deliveries d
			WHERE d.event_id = e.event_id
			  AND d.run_id = e.run_id
			  AND d.status = 'pending'
		  )
		  AND `+sqliteDiagnosticDirectReplayExclusionSQL("e")+`
		ORDER BY e.created_at ASC, e.event_id ASC
		LIMIT ?
	`, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite events with pending deliveries: %w", err)
	}
	defer rows.Close()
	out := make([]events.PersistedReplayEvent, 0, limit)
	for rows.Next() {
		var eventID string
		var row persistedEventIdentity
		var payloadRaw, createdAtRaw, sourceRouteRaw, targetRouteRaw, targetSetRaw any
		if err := rows.Scan(
			&eventID,
			&row.RunID,
			&row.EventName,
			&row.TaskID,
			&row.EntityID,
			&row.FlowInstance,
			&row.Scope,
			&payloadRaw,
			&row.ChainDepth,
			&row.ProducedBy,
			&row.ProducedByType,
			&row.SourceEventID,
			&createdAtRaw,
			&row.ExecutionMode,
			&sourceRouteRaw,
			&targetRouteRaw,
			&targetSetRaw,
		); err != nil {
			return nil, fmt.Errorf("scan sqlite event with pending deliveries: %w", err)
		}
		row.Payload = sqliteJSONRawMessage(payloadRaw)
		if parsedCreatedAt, ok, err := sqliteTimeValue(createdAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite pending-delivery event created_at: %w", err)
		} else if ok {
			row.CreatedAt = parsedCreatedAt.UTC()
		}
		row.SourceRoute = sqliteJSONRawMessage(sourceRouteRaw)
		row.TargetRoute = sqliteJSONRawMessage(targetRouteRaw)
		row.TargetSet = sqliteJSONRawMessage(targetSetRaw)
		row.EventID = eventID
		evt, err := eventFromPersistedIdentity(row)
		if err != nil {
			return nil, err
		}
		record := events.PersistedReplayEvent{Event: evt}
		if strings.TrimSpace(evt.RunID()) == "" {
			record.ReplayFailure = replayAdmissionFailure("missing_canonical_run_id")
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite events with pending deliveries: %w", err)
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
			LEFT JOIN runs run ON run.run_id = e.run_id
			LEFT JOIN event_receipts r
				ON r.event_id = e.event_id
				AND r.subscriber_type = 'platform'
				AND r.subscriber_id = 'pipeline'
			WHERE e.event_id = ?
			  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
			  AND r.event_id IS NULL
		)
	`, eventID).Scan(&pending); err != nil {
		return nil, false, fmt.Errorf("claim sqlite pipeline replay: %w", err)
	}
	if !pending {
		return nil, false, nil
	}
	return s.claimPipelineEvent(eventID)
}

func (s *SQLiteRuntimeStore) ClaimPipelinePublication(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	// Publication ownership serializes same-ID attempts. Canonical append must
	// classify exact/conflicting duplicates before applying run-status admission.
	return s.claimPipelineEvent(eventID)
}

func (s *SQLiteRuntimeStore) ClaimPipelineSettlement(ctx context.Context, eventID string) (runtimeownership.Lease, bool, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return nil, false, fmt.Errorf("event_id is required")
	}
	if err := requireEventRunNotForked(ctx, s.DB, eventID, storerunlifecycle.DialectSQLite, true); err != nil {
		if errors.Is(err, storerunlifecycle.ErrRunNotActive) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return s.claimPipelineEvent(eventID)
}

func (s *SQLiteRuntimeStore) claimPipelineEvent(eventID string) (runtimeownership.Lease, bool, error) {
	s.replayMu.Lock()
	defer s.replayMu.Unlock()
	if s.replayClaims == nil {
		s.replayClaims = map[string]struct{}{}
	}
	if _, exists := s.replayClaims[eventID]; exists {
		return nil, false, nil
	}
	s.replayClaims[eventID] = struct{}{}
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
	terminalReasons := activeRunQuiescenceTerminalReasonCodes()
	transitionAt := s.now()
	args := []any{sqliteNullUUID(sessionID), transitionAt, eventID, agentID}
	for _, reason := range terminalReasons {
		args = append(args, reason)
	}
	var rows int64
	if err := s.runAuthorActivityMutation(ctx, "sqlite delivery in progress", func(txctx context.Context, tx *sql.Tx) error {
		delivery, err := s.sqliteLockAgentDeliveryTx(txctx, tx, eventID, agentID)
		if err != nil {
			return err
		}
		if !delivery.found {
			return fmt.Errorf("mark sqlite event delivery in progress: delivery row required for event %s agent %s", eventID, agentID)
		}
		if activeRunQuiescenceDeliveryTerminal(delivery.status, delivery.reasonCode) {
			return nil
		}
		if strings.TrimSpace(delivery.runID) != "" {
			if err := storerunlifecycle.RequireActive(txctx, tx, delivery.runID, storerunlifecycle.DialectSQLite); err != nil {
				return err
			}
		}
		res, err := tx.ExecContext(txctx, `
			UPDATE event_deliveries
			SET status = 'in_progress',
			    active_session_id = ?,
			    started_at = COALESCE(started_at, ?)
			WHERE event_id = ?
			  AND subscriber_type = 'agent'
			  AND subscriber_id = ?
			  AND status IN ('pending', 'failed', 'in_progress')
			  AND COALESCE(reason_code, '') NOT IN (`+sqlitePlaceholders(len(terminalReasons))+`)
		`, args...)
		if err != nil {
			return err
		}
		rows, _ = res.RowsAffected()
		if rows == 0 {
			return fmt.Errorf("mark sqlite event delivery in progress: delivery transition conflict for event %s agent %s", eventID, agentID)
		}
		state := agentReceiptWriteState{deliveryCode: "in_progress", retryCount: delivery.retryCount, reasonCode: "agent_processing"}
		if delivery.startedAt.Valid {
			transitionAt = delivery.startedAt.Time.UTC()
		}
		return recordAgentDeliveryAuthorActivity(txctx, delivery, agentID, state, transitionAt)
	}); err != nil {
		return fmt.Errorf("mark sqlite event delivery in progress: %w", err)
	}
	if rows == 0 {
		if _, ok, err := s.ActiveRunDeliveryQuiesced(ctx, eventID, "agent", agentID); err != nil {
			return err
		} else if ok {
			return nil
		}
		return fmt.Errorf("mark sqlite event delivery in progress: delivery row required for event %s agent %s", eventID, agentID)
	}
	return nil
}

func (s *SQLiteRuntimeStore) UpsertEventReceipt(ctx context.Context, eventID, agentID string, status runtimemanager.ReceiptStatus, failure *runtimefailures.Envelope) error {
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return fmt.Errorf("upsert sqlite event receipt: eventID and agentID required")
	}
	if status == "" {
		return fmt.Errorf("upsert sqlite event receipt: status required")
	}
	return s.runAuthorActivityMutation(ctx, "sqlite event receipt", func(txctx context.Context, tx *sql.Tx) error {
		delivery, err := s.sqliteLockAgentDeliveryTx(txctx, tx, eventID, agentID)
		if err != nil {
			return err
		}
		if !delivery.found {
			return fmt.Errorf("upsert sqlite event receipt: delivery row required for event %s agent %s", eventID, agentID)
		}
		if activeRunQuiescenceDeliveryTerminal(delivery.status, delivery.reasonCode) {
			return nil
		}
		if strings.TrimSpace(delivery.runID) != "" {
			if err := storerunlifecycle.RequireActive(txctx, tx, delivery.runID, storerunlifecycle.DialectSQLite); err != nil {
				return err
			}
		}
		state, err := buildAgentReceiptWriteState(delivery.retryCount, status, failure)
		if err != nil {
			return err
		}
		now := s.now()
		changed, err := s.updateAgentDeliveryRowTx(txctx, tx, eventID, agentID, state, now)
		if err != nil {
			return err
		}
		if !changed {
			return nil
		}
		if err := s.upsertAgentReceiptRowTx(txctx, tx, eventID, agentID, delivery, state, now); err != nil {
			return err
		}
		return recordAgentDeliveryAuthorActivity(txctx, delivery, agentID, state, now)
	})
}

func (s *SQLiteRuntimeStore) updateAgentDeliveryRowTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID, agentID string,
	state agentReceiptWriteState,
	transitionAt time.Time,
) (bool, error) {
	failureJSON, err := nullableFailureJSON(state.failure)
	if err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `
			UPDATE event_deliveries
			SET status = ?,
			    retry_count = ?,
			    reason_code = ?,
			    failure = NULLIF(?, ''),
			    active_session_id = NULL,
			    delivered_at = ?
			WHERE event_id = ?
			  AND subscriber_type = 'agent'
			  AND subscriber_id = ?
			  AND (
				COALESCE(status, '') <> ?
				OR COALESCE(retry_count, 0) <> ?
				OR COALESCE(reason_code, '') <> ?
				OR COALESCE(failure, '') <> ?
				OR active_session_id IS NOT NULL
			  )
		`, state.deliveryCode, state.retryCount, sqliteNullString(state.reasonCode), failureJSON, transitionAt.UTC(), eventID, agentID,
		state.deliveryCode, state.retryCount, strings.TrimSpace(state.reasonCode), failureJSON)
	if err != nil {
		return false, fmt.Errorf("update sqlite event delivery receipt state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read sqlite event delivery transition result: %w", err)
	}
	return rows > 0, nil
}

func (s *SQLiteRuntimeStore) upsertAgentReceiptRowTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID, agentID string,
	delivery lockedAgentDelivery,
	state agentReceiptWriteState,
	processedAt time.Time,
) error {
	failureJSON, err := nullableFailureJSON(state.failure)
	if err != nil {
		return err
	}
	sideEffects, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects(state.finalStatus, state.reasonCode, state.retryCount))
	if err != nil {
		return fmt.Errorf("marshal sqlite agent receipt side effects: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		VALUES (?, ?, 'agent', ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO UPDATE SET
			outcome = excluded.outcome,
			reason_code = excluded.reason_code,
			failure = excluded.failure,
			side_effects = excluded.side_effects,
			processed_at = excluded.processed_at
	`, uuid.NewString(), eventID, agentID, sqliteNullUUID(delivery.entityID), sqliteNullString(delivery.flowInstance),
		mapManagerReceiptStatusToOutcome(state.finalStatus), sqliteNullString(state.reasonCode), sqliteNullString(failureJSON), string(sideEffects), processedAt.UTC()); err != nil {
		return fmt.Errorf("upsert sqlite event receipt row: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) GetEventReceipt(ctx context.Context, eventID, agentID string) (runtimemanager.EventReceipt, bool, error) {
	eventID = strings.TrimSpace(eventID)
	agentID = strings.TrimSpace(agentID)
	if eventID == "" || agentID == "" {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("event_id and agent_id are required")
	}
	var (
		rec          runtimemanager.EventReceipt
		outcome      string
		sideEffects  any
		receiptRaw   any
		delivery     string
		deliveryRaw  any
		deliverySeen int
		retryCount   sql.NullInt64
	)
	err := s.DB.QueryRowContext(ctx, `
		SELECT
			r.event_id,
			r.subscriber_id,
			r.outcome,
			COALESCE(r.reason_code, ''),
			COALESCE(r.side_effects, '{}'),
			COALESCE(r.failure, 'null'),
			COALESCE(d.status, ''),
			COALESCE(d.failure, 'null'),
			d.retry_count,
			CASE WHEN d.delivery_id IS NULL THEN 0 ELSE 1 END
		FROM event_receipts r
		LEFT JOIN event_deliveries d
			ON d.event_id = r.event_id
			AND d.subscriber_type = 'agent'
			AND d.subscriber_id = r.subscriber_id
		WHERE r.event_id = ?
		  AND r.subscriber_type = 'agent'
		  AND r.subscriber_id = ?
	`, eventID, agentID).Scan(&rec.EventID, &rec.AgentID, &outcome, new(string), &sideEffects, &receiptRaw, &delivery, &deliveryRaw, &retryCount, &deliverySeen)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimemanager.EventReceipt{}, false, nil
	}
	if err != nil {
		return runtimemanager.EventReceipt{}, false, fmt.Errorf("get sqlite event receipt: %w", err)
	}
	rec.Status = mapOutcomeToManagerReceiptStatus(outcome)
	if raw := sqliteJSONRawMessage(sideEffects); len(raw) > 0 {
		payload, err := decodeAgentReceiptSideEffects(raw)
		if err != nil {
			return runtimemanager.EventReceipt{}, false, fmt.Errorf("decode sqlite event receipt side effects: %w", err)
		}
		rec.Status = payload.ManagerStatus
		rec.RetryCount = payload.RetryCount
	}
	if raw := sqliteJSONRawMessage(receiptRaw); len(raw) > 0 && string(raw) != "null" {
		failure, err := runtimefailures.UnmarshalEnvelope(raw)
		if err != nil {
			return runtimemanager.EventReceipt{}, false, fmt.Errorf("decode sqlite event receipt failure: %w", err)
		}
		rec.Failure = &failure
	}
	if deliverySeen != 0 {
		mappedStatus, override, err := terminalManagerReceiptStatusFromDelivery(delivery)
		if err != nil {
			return runtimemanager.EventReceipt{}, false, fmt.Errorf("get sqlite event receipt: %w", err)
		}
		if override {
			rec.Status = mappedStatus
			if retryCount.Valid {
				rec.RetryCount = int(retryCount.Int64)
			}
			if raw := sqliteJSONRawMessage(deliveryRaw); len(raw) > 0 && string(raw) != "null" {
				failure, err := runtimefailures.UnmarshalEnvelope(raw)
				if err != nil {
					return runtimemanager.EventReceipt{}, false, fmt.Errorf("decode sqlite event delivery failure: %w", err)
				}
				rec.Failure = &failure
			}
		}
	}
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
	agentID = strings.TrimSpace(agentID)
	if agentID == "" || len(subscriptions) == 0 {
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
			COALESCE(e.task_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''),
			COALESCE(e.scope, 'global'),
			e.payload,
			COALESCE(e.chain_depth, 0),
			COALESCE(e.produced_by, ''),
			COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id, ''),
			e.created_at,
			e.execution_mode,
			COALESCE(e.source_route, '{}'),
			COALESCE(e.target_route, '{}'),
			COALESCE(e.target_set, '[]'),
			CASE WHEN d.delivery_id IS NULL THEN 0 ELSE 1 END,
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			COALESCE(d.created_at, e.created_at),
			d.delivered_at,
			CASE WHEN r.event_id IS NULL THEN 0 ELSE 1 END
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
		  AND EXISTS (
				SELECT 1
				FROM event_deliveries d_me
				WHERE d_me.event_id = e.event_id
				  AND d_me.subscriber_type = 'agent'
				  AND d_me.subscriber_id = ?
			)
		ORDER BY e.created_at ASC, e.event_id ASC
	`, agentID, agentID, since.UTC(), agentID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite pending subscribed events: %w", err)
	}
	defer rows.Close()
	out := make([]events.Event, 0, limit)
	now := s.now()
	for rows.Next() {
		var eventID string
		var row persistedEventIdentity
		var payloadRaw, createdAtRaw, sourceRouteRaw, targetRouteRaw, targetSetRaw any
		var deliveryCreatedAtRaw, deliveryDeliveredAtRaw any
		var deliveryFound, receiptFound int
		record := pendingAgentDeliveryRecord{AgentID: agentID}
		if err := rows.Scan(
			&eventID,
			&row.RunID,
			&row.EventName,
			&row.TaskID,
			&row.EntityID,
			&row.FlowInstance,
			&row.Scope,
			&payloadRaw,
			&row.ChainDepth,
			&row.ProducedBy,
			&row.ProducedByType,
			&row.SourceEventID,
			&createdAtRaw,
			&row.ExecutionMode,
			&sourceRouteRaw,
			&targetRouteRaw,
			&targetSetRaw,
			&deliveryFound,
			&record.DeliveryStatus,
			&record.DeliveryRetryCount,
			&deliveryCreatedAtRaw,
			&deliveryDeliveredAtRaw,
			&receiptFound,
		); err != nil {
			return nil, fmt.Errorf("scan sqlite pending subscribed event: %w", err)
		}
		row.Payload = sqliteJSONRawMessage(payloadRaw)
		if parsedCreatedAt, ok, err := sqliteTimeValue(createdAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite pending subscribed event created_at: %w", err)
		} else if ok {
			row.CreatedAt = parsedCreatedAt.UTC()
		}
		if deliveryCreatedAt, ok, err := sqliteTimeValue(deliveryCreatedAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite pending subscribed delivery created_at: %w", err)
		} else if ok {
			record.DeliveryCreatedAt = deliveryCreatedAt
		}
		if deliveryDeliveredAt, ok, err := sqliteTimeValue(deliveryDeliveredAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite pending subscribed delivery delivered_at: %w", err)
		} else if ok {
			record.DeliveryDeliveredAt = sql.NullTime{Time: deliveryDeliveredAt, Valid: true}
		}
		record.DeliveryFound = deliveryFound != 0
		record.ReceiptFound = receiptFound != 0
		row.SourceRoute = sqliteJSONRawMessage(sourceRouteRaw)
		row.TargetRoute = sqliteJSONRawMessage(targetRouteRaw)
		row.TargetSet = sqliteJSONRawMessage(targetSetRaw)
		row.EventID = eventID
		record.Event, err = eventFromPersistedIdentity(row)
		if err != nil {
			return nil, err
		}
		if !record.isPending(now) {
			continue
		}
		if !eventMatchesAnyPattern(record.Event.Type(), subscriptions) {
			continue
		}
		out = append(out, record.Event)
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
			COALESCE(e.task_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''),
			COALESCE(e.scope, 'global'),
			e.payload,
			COALESCE(e.chain_depth, 0),
			COALESCE(e.produced_by, ''),
			COALESCE(e.produced_by_type, ''),
			COALESCE(e.source_event_id, ''),
			e.created_at,
			e.execution_mode,
			COALESCE(e.source_route, '{}'),
			COALESCE(e.target_route, '{}'),
			COALESCE(e.target_set, '[]'),
			1,
			COALESCE(d.status, ''),
			COALESCE(d.retry_count, 0),
			d.created_at,
			d.delivered_at,
			CASE WHEN r.event_id IS NULL THEN 0 ELSE 1 END
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		LEFT JOIN runs run ON run.run_id = e.run_id
		LEFT JOIN event_receipts r
			ON r.event_id = d.event_id
			AND r.subscriber_type = 'agent'
			AND r.subscriber_id = d.subscriber_id
		WHERE d.subscriber_type = 'agent'
		  AND (e.run_id IS NULL OR run.status IN ('running', 'paused'))
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
			record               pendingAgentDeliveryRecord
			eventID              string
			row                  persistedEventIdentity
			payloadRaw           any
			eventCreatedRaw      any
			sourceRouteRaw       any
			targetRouteRaw       any
			targetSetRaw         any
			deliveryCreatedRaw   any
			deliveryDeliveredRaw any
		)
		if err := rows.Scan(
			&record.AgentID,
			&eventID,
			&row.RunID,
			&row.EventName,
			&row.TaskID,
			&row.EntityID,
			&row.FlowInstance,
			&row.Scope,
			&payloadRaw,
			&row.ChainDepth,
			&row.ProducedBy,
			&row.ProducedByType,
			&row.SourceEventID,
			&eventCreatedRaw,
			&row.ExecutionMode,
			&sourceRouteRaw,
			&targetRouteRaw,
			&targetSetRaw,
			&record.DeliveryFound,
			&record.DeliveryStatus,
			&record.DeliveryRetryCount,
			&deliveryCreatedRaw,
			&deliveryDeliveredRaw,
			&record.ReceiptFound,
		); err != nil {
			return nil, fmt.Errorf("scan pending agent delivery record: %w", err)
		}
		eventCreatedAt, _, err := sqliteTimeValue(eventCreatedRaw)
		if err != nil {
			return nil, fmt.Errorf("scan pending agent event created_at: %w", err)
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
		row.Payload = sqliteJSONRawMessage(payloadRaw)
		row.CreatedAt = eventCreatedAt
		row.SourceRoute = sqliteJSONRawMessage(sourceRouteRaw)
		row.TargetRoute = sqliteJSONRawMessage(targetRouteRaw)
		row.TargetSet = sqliteJSONRawMessage(targetSetRaw)
		row.EventID = eventID
		record.Event, err = eventFromPersistedIdentity(row)
		if err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read pending agent delivery records: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) sqliteLockAgentDeliveryTx(ctx context.Context, tx *sql.Tx, eventID, agentID string) (lockedAgentDelivery, error) {
	var delivery lockedAgentDelivery
	var startedAtRaw any
	err := tx.QueryRowContext(ctx, `
		SELECT
			d.delivery_id,
			COALESCE(d.run_id, e.run_id, ''),
			e.event_name,
			COALESCE(d.retry_count, 0),
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.active_session_id, ''),
			COALESCE(e.entity_id, ''),
			COALESCE(e.flow_instance, ''),
			d.started_at
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE d.event_id = ?
		  AND d.subscriber_type = 'agent'
		  AND d.subscriber_id = ?
	`, eventID, agentID).Scan(&delivery.deliveryID, &delivery.runID, &delivery.eventType, &delivery.retryCount, &delivery.status, &delivery.reasonCode, &delivery.activeSessionID, &delivery.entityID, &delivery.flowInstance, &startedAtRaw)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return lockedAgentDelivery{}, nil
	case err != nil:
		return lockedAgentDelivery{}, fmt.Errorf("lock sqlite event delivery row: %w", err)
	default:
		delivery.eventID = strings.TrimSpace(eventID)
		if startedAt, valid, parseErr := sqliteTimeValue(startedAtRaw); parseErr != nil {
			return lockedAgentDelivery{}, fmt.Errorf("parse sqlite event delivery started_at: %w", parseErr)
		} else if valid {
			delivery.startedAt = sql.NullTime{Time: startedAt.UTC(), Valid: true}
		}
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
