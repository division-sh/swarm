package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func (s *SQLiteRuntimeStore) LoadRunDebugTracePage(ctx context.Context, runID string, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error) {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil, "", ErrRunNotFound
	}
	opts = defaultRunDebugTraceQueryOptions(opts)
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = ?)`, runID).Scan(&exists); err != nil {
		return nil, "", fmt.Errorf("load sqlite run trace run existence: %w", err)
	}
	if !exists {
		return nil, "", ErrRunNotFound
	}
	where := []string{"e.run_id = ?", "NOT (COALESCE(d.subscriber_type, '') = ? AND COALESCE(d.subscriber_id, '') = ?)"}
	args := []any{runID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID}
	if opts.Cursor != "" {
		cursor, err := decodeRunDebugTraceCursor(opts.Cursor)
		if err != nil {
			return nil, "", err
		}
		eventCreatedAt, err := sqliteRunTraceCursorTime(cursor.EventCreatedAt)
		if err != nil {
			return nil, "", err
		}
		eventID := strings.TrimSpace(cursor.EventID)
		deliveryCreatedAt, err := sqliteRunTraceCursorOptionalTime(cursor.DeliveryCreatedAt)
		if err != nil {
			return nil, "", err
		}
		deliveryID := strings.TrimSpace(cursor.DeliveryID)
		turnCreatedAt, err := sqliteRunTraceCursorOptionalTime(cursor.TurnCreatedAt)
		if err != nil {
			return nil, "", err
		}
		turnID := strings.TrimSpace(cursor.TurnID)
		where = append(where, `(
			e.created_at > ?
			OR (e.created_at = ? AND e.event_id > ?)
			OR (e.created_at = ? AND e.event_id = ? AND COALESCE(d.created_at, ?) > ?)
			OR (e.created_at = ? AND e.event_id = ? AND COALESCE(d.created_at, ?) = ? AND COALESCE(d.delivery_id, '') > ?)
			OR (e.created_at = ? AND e.event_id = ? AND COALESCE(d.created_at, ?) = ? AND COALESCE(d.delivery_id, '') = ? AND COALESCE(t.created_at, ?) > ?)
			OR (e.created_at = ? AND e.event_id = ? AND COALESCE(d.created_at, ?) = ? AND COALESCE(d.delivery_id, '') = ? AND COALESCE(t.created_at, ?) = ? AND COALESCE(t.turn_id, '') > ?)
		)`)
		args = append(args,
			eventCreatedAt,
			eventCreatedAt, eventID,
			eventCreatedAt, eventID, sqliteRunTraceCursorFloorTime(), deliveryCreatedAt,
			eventCreatedAt, eventID, sqliteRunTraceCursorFloorTime(), deliveryCreatedAt, deliveryID,
			eventCreatedAt, eventID, sqliteRunTraceCursorFloorTime(), deliveryCreatedAt, deliveryID, sqliteRunTraceCursorFloorTime(), turnCreatedAt,
			eventCreatedAt, eventID, sqliteRunTraceCursorFloorTime(), deliveryCreatedAt, deliveryID, sqliteRunTraceCursorFloorTime(), turnCreatedAt, turnID,
		)
	}
	if opts.Since != nil {
		where = append(where, sqliteRunTraceWatermarkExpression()+" > ?")
		args = append(args, opts.Since.UTC())
	}
	if opts.Until != nil {
		where = append(where, sqliteRunTraceWatermarkExpression()+" <= ?")
		args = append(args, opts.Until.UTC())
	}
	appendIn := func(column string, values []string) {
		if len(values) == 0 {
			return
		}
		placeholders := make([]string, 0, len(values))
		for _, value := range values {
			placeholders = append(placeholders, "?")
			args = append(args, value)
		}
		where = append(where, column+" IN ("+strings.Join(placeholders, ",")+")")
	}
	appendIn("e.event_name", opts.Filter.EventNames)
	appendIn("COALESCE(e.entity_id, '')", opts.Filter.EntityIDs)
	appendIn("COALESCE(d.status, '')", opts.Filter.DeliveryStatuses)
	appendIn("COALESCE(d.subscriber_id, '')", opts.Filter.SubscriberIDs)
	appendIn("COALESCE(d.subscriber_type, '')", opts.Filter.SubscriberTypes)
	if opts.ExcludeRuntimeLogs {
		where = append(where, "e.event_name <> 'platform.runtime_log'")
	}
	args = append(args, opts.Limit+1)
	rows, err := s.DB.QueryContext(ctx, `
		WITH trace_sessions AS (
			SELECT
				session_id,
				run_id,
				'live_session' AS session_kind,
				memory_enabled,
				memory_source,
				COALESCE(status, '') AS status,
				updated_at
			FROM agent_sessions
			UNION ALL
			SELECT
				session_id,
				run_id,
				'turn_audit' AS session_kind,
				memory_enabled,
				memory_source,
				COALESCE(status, '') AS status,
				updated_at
			FROM agent_conversation_audits
		)
		SELECT
			e.event_id, e.event_name, COALESCE(e.source_event_id, ''), COALESCE(e.entity_id, ''),
			COALESCE(e.produced_by, ''), COALESCE(e.produced_by_type, ''), e.created_at,
				COALESCE(d.delivery_id, ''), COALESCE(d.subscriber_type, ''), COALESCE(d.subscriber_id, ''),
				COALESCE(d.status, ''), COALESCE(d.reason_code, ''), COALESCE(d.failure, 'null'), COALESCE(d.retry_count, 0),
				COALESCE(d.active_session_id, ''),
				d.created_at, d.started_at, d.delivered_at,
			COALESCE(ses.session_id, ''), COALESCE(ses.session_kind, ''), COALESCE(ses.memory_enabled, 0), COALESCE(ses.memory_source, ''),
			COALESCE(ses.status, ''), ses.updated_at,
			COALESCE(t.turn_id, ''), COALESCE(t.trigger_event_id, ''), COALESCE(t.trigger_event_type, ''),
			COALESCE(t.flow_instance, ''), COALESCE(t.memory_enabled, 0), COALESCE(t.memory_source, ''), COALESCE(t.entity_id, ''),
			COALESCE(t.task_id, ''), COALESCE(t.parse_ok, 0), COALESCE(t.retry_count, 0),
			COALESCE(t.failure, 'null'), t.created_at
		FROM events e
		LEFT JOIN event_deliveries d ON d.event_id = e.event_id
		LEFT JOIN agent_turns t
			ON t.run_id = e.run_id
		   AND t.trigger_event_id = e.event_id
		   AND (
				d.delivery_id IS NULL
				OR (
					COALESCE(d.subscriber_type, '') = 'agent'
					AND COALESCE(d.subscriber_id, '') <> ''
					AND t.agent_id = d.subscriber_id
				)
		   )
		LEFT JOIN trace_sessions ses
			ON ses.session_id = COALESCE(t.session_id, d.active_session_id)
		   AND (
				ses.run_id = e.run_id
				OR ses.run_id IS NULL
		   )
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY e.created_at ASC, e.event_id ASC, d.created_at ASC, d.delivery_id ASC, t.created_at ASC, t.turn_id ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, "", fmt.Errorf("query sqlite run trace: %w", err)
	}
	defer rows.Close()
	out := make([]RunDebugTraceRow, 0, opts.Limit+1)
	for rows.Next() {
		var row RunDebugTraceRow
		var rawDeliveryFailure, rawTurnFailure any
		var eventCreatedRaw, deliveryCreatedRaw, deliveryStartedRaw, deliveryDeliveredRaw, sessionUpdatedRaw, turnCreatedRaw any
		if err := rows.Scan(
			&row.EventID, &row.EventName, &row.SourceEventID, &row.EntityID,
			&row.EventSource, &row.EventSourceType, &eventCreatedRaw,
			&row.DeliveryID, &row.SubscriberType, &row.SubscriberID,
			&row.DeliveryStatus, &row.DeliveryReasonCode, &rawDeliveryFailure, &row.DeliveryRetryCount, &row.ActiveSessionID,
			&deliveryCreatedRaw, &deliveryStartedRaw, &deliveryDeliveredRaw,
			&row.SessionID, &row.SessionKind, &row.SessionMemory, &row.SessionMemorySource,
			&row.SessionStatus, &sessionUpdatedRaw,
			&row.TurnID, &row.TurnTriggerEventID, &row.TurnTriggerEventType,
			&row.TurnFlowInstance, &row.TurnMemory, &row.TurnMemorySource, &row.TurnEntityID,
			&row.TurnTaskID, &row.TurnParseOK, &row.TurnRetryCount,
			&rawTurnFailure, &turnCreatedRaw,
		); err != nil {
			return nil, "", fmt.Errorf("scan sqlite run trace: %w", err)
		}
		row.DeliveryFailure, err = decodeStoredFailure(rawDeliveryFailure)
		if err != nil {
			return nil, "", fmt.Errorf("decode sqlite run trace delivery failure: %w", err)
		}
		row.TurnFailure, err = decodeStoredFailure(rawTurnFailure)
		if err != nil {
			return nil, "", fmt.Errorf("decode sqlite run trace turn failure: %w", err)
		}
		if at, ok, err := sqliteTimeValue(eventCreatedRaw); err != nil {
			return nil, "", err
		} else if ok {
			row.EventCreatedAt = at
		}
		row.DeliveryCreatedAt = sqliteTraceTimePtr(deliveryCreatedRaw)
		row.DeliveryStartedAt = sqliteTraceTimePtr(deliveryStartedRaw)
		row.DeliveryDeliveredAt = sqliteTraceTimePtr(deliveryDeliveredRaw)
		row.DeliveryRetryEligible = OperatorDeliveryRetryEligible(row.DeliveryStatus)
		row.DeliveryTerminal = OperatorDeliveryTerminal(row.DeliveryStatus)
		row.SessionUpdatedAt = sqliteTraceTimePtr(sessionUpdatedRaw)
		row.TurnCreatedAt = sqliteTraceTimePtr(turnCreatedRaw)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read sqlite run trace: %w", err)
	}
	nextCursor := ""
	if len(out) > opts.Limit {
		out = out[:opts.Limit]
		nextCursor = encodeRunDebugTraceCursor(out[len(out)-1])
	}
	return out, nextCursor, nil
}

const sqliteRunTraceCursorFloor = "0001-01-01T00:00:00Z"

func sqliteRunTraceCursorFloorTime() time.Time {
	return time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
}

func sqliteRunTraceCursorTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, ErrInvalidObservabilityCursor
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, ErrInvalidObservabilityCursor
	}
	return parsed.UTC(), nil
}

func sqliteRunTraceCursorOptionalTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return sqliteRunTraceCursorFloorTime(), nil
	}
	return sqliteRunTraceCursorTime(raw)
}

func sqliteRunTraceWatermarkExpression() string {
	return `max(
		e.created_at,
		COALESCE(d.created_at, '` + sqliteRunTraceCursorFloor + `'),
		COALESCE(d.started_at, '` + sqliteRunTraceCursorFloor + `'),
		COALESCE(d.delivered_at, '` + sqliteRunTraceCursorFloor + `'),
		COALESCE(ses.updated_at, '` + sqliteRunTraceCursorFloor + `'),
		COALESCE(t.created_at, '` + sqliteRunTraceCursorFloor + `')
	)`
}

func (s *SQLiteRuntimeStore) ListOperatorEvents(ctx context.Context, opts OperatorEventListOptions) (OperatorEventListResult, error) {
	if opts.Cursor != "" {
		return OperatorEventListResult{}, ErrInvalidObservabilityCursor
	}
	opts = defaultOperatorEventListOptions(opts)
	where := []string{"1=1"}
	args := []any{}
	if opts.Filter.RunID != "" {
		where = append(where, "e.run_id = ?")
		args = append(args, nullUUIDString(opts.Filter.RunID))
	}
	if opts.Filter.EventName != "" {
		where = append(where, "e.event_name = ?")
		args = append(args, opts.Filter.EventName)
	}
	if opts.Filter.EntityID != "" {
		where = append(where, "COALESCE(e.entity_id, '') = ?")
		args = append(args, nullUUIDString(opts.Filter.EntityID))
	}
	if opts.Source != "" {
		where = append(where, "COALESCE(e.produced_by, '') = ?")
		args = append(args, opts.Source)
	}
	if opts.Since != nil {
		where = append(where, "e.created_at >= ?")
		args = append(args, opts.Since.UTC())
	}
	if opts.Until != nil {
		where = append(where, "e.created_at <= ?")
		args = append(args, opts.Until.UTC())
	}
	if opts.ExcludeRuntimeLogs {
		where = append(where, "e.event_name <> 'platform.runtime_log'")
	}
	if opts.Filter.DeliveryStatus != "" || opts.Filter.SubscriberID != "" || opts.Filter.SubscriberType != "" || opts.Filter.ReasonCode != "" {
		where = append(where, `EXISTS (
			SELECT 1 FROM event_deliveries d
			WHERE d.event_id = e.event_id
			  AND (? = '' OR d.status = ?)
			  AND (? = '' OR d.subscriber_id = ?)
			  AND (? = '' OR d.subscriber_type = ?)
			  AND (? = '' OR COALESCE(d.reason_code, '') = ?)
		)`)
		args = append(args,
			opts.Filter.DeliveryStatus, opts.Filter.DeliveryStatus,
			opts.Filter.SubscriberID, opts.Filter.SubscriberID,
			opts.Filter.SubscriberType, opts.Filter.SubscriberType,
			opts.Filter.ReasonCode, opts.Filter.ReasonCode,
		)
	}
	order := "DESC"
	if strings.EqualFold(opts.Order, "asc") {
		order = "ASC"
	}
	args = append(args, opts.Limit)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT e.event_id
		FROM events e
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY e.created_at `+order+`, e.event_id `+order+`
		LIMIT ?
	`, args...)
	if err != nil {
		return OperatorEventListResult{}, fmt.Errorf("query sqlite operator events: %w", err)
	}
	defer rows.Close()
	result := OperatorEventListResult{Events: []OperatorEventFull{}}
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			return OperatorEventListResult{}, fmt.Errorf("scan sqlite operator event id: %w", err)
		}
		full, err := s.LoadOperatorEvent(ctx, eventID)
		if err != nil {
			return OperatorEventListResult{}, err
		}
		result.Events = append(result.Events, full)
	}
	if err := rows.Err(); err != nil {
		return OperatorEventListResult{}, fmt.Errorf("read sqlite operator events: %w", err)
	}
	return result, nil
}

func (s *SQLiteRuntimeStore) LoadOperatorEvent(ctx context.Context, eventID string) (OperatorEventFull, error) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return OperatorEventFull{}, ErrEventNotFound
	}
	var event OperatorEventFull
	var payloadRaw, createdRaw any
	err := s.DB.QueryRowContext(ctx, `
		SELECT event_id, event_name, COALESCE(entity_id, ''), COALESCE(run_id, ''), COALESCE(source_event_id, ''), execution_mode,
		       created_at, COALESCE(produced_by, ''), payload
		FROM events
		WHERE event_id = ?
	`, eventID).Scan(&event.EventID, &event.EventName, &event.EntityID, &event.RunID, &event.SourceEventID, &event.ExecutionMode, &createdRaw, &event.Source, &payloadRaw)
	if err == sql.ErrNoRows {
		return OperatorEventFull{}, ErrEventNotFound
	}
	if err != nil {
		return OperatorEventFull{}, fmt.Errorf("load sqlite operator event: %w", err)
	}
	if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
		return OperatorEventFull{}, err
	} else if ok {
		event.CreatedAt = at
	}
	event.Payload = map[string]any{}
	_ = json.Unmarshal(sqliteJSONRawMessage(payloadRaw), &event.Payload)
	deadLetters, err := s.sqliteOperatorEventDeadLetters(ctx, eventID)
	if err != nil {
		return OperatorEventFull{}, err
	}
	deliveries, err := s.sqliteOperatorEventDeliveries(ctx, eventID)
	if err != nil {
		return OperatorEventFull{}, err
	}
	event.Deliveries = EnrichOperatorDeliveryFailureEvidence(deliveries, deadLetters)
	event.DeadLetters = deadLetters
	if event.DeadLetters == nil {
		event.DeadLetters = []OperatorDeadLetterRecord{}
	}
	return event, nil
}

func (s *SQLiteRuntimeStore) ListOperatorRuntimeLogs(ctx context.Context, opts OperatorRuntimeLogListOptions) (OperatorRuntimeLogListResult, error) {
	if opts.Cursor != "" {
		return OperatorRuntimeLogListResult{}, ErrInvalidObservabilityCursor
	}
	opts = defaultOperatorRuntimeLogListOptions(opts)
	where := []string{"event_name = 'platform.runtime_log'"}
	args := []any{}
	if opts.RunID != "" {
		where = append(where, "run_id = ?")
		args = append(args, nullUUIDString(opts.RunID))
	}
	if opts.EntityID != "" {
		where = append(where, "COALESCE(entity_id, '') = ?")
		args = append(args, nullUUIDString(opts.EntityID))
	}
	if opts.Level != "" {
		where = append(where, "json_extract(payload, '$.log_level') = ?")
		args = append(args, strings.ToLower(opts.Level))
	}
	if opts.Component != "" {
		where = append(where, "json_extract(payload, '$.details.component') = ?")
		args = append(args, opts.Component)
	}
	if opts.Source != "" {
		where = append(where, "COALESCE(NULLIF(TRIM(json_extract(payload, '$.details.agent_id')), ''), NULLIF(TRIM(produced_by), ''), 'runtime') = ?")
		args = append(args, opts.Source)
	}
	if opts.SessionID != "" {
		where = append(where, "json_extract(payload, '$.details.session_id') = ?")
		args = append(args, opts.SessionID)
	}
	if opts.Since != nil {
		where = append(where, "created_at >= ?")
		args = append(args, opts.Since.UTC())
	}
	if opts.Until != nil {
		where = append(where, "created_at <= ?")
		args = append(args, opts.Until.UTC())
	}
	order := "DESC"
	if strings.EqualFold(opts.Order, "asc") {
		order = "ASC"
	}
	args = append(args, opts.Limit)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT event_id, created_at, COALESCE(run_id, ''), COALESCE(entity_id, ''), COALESCE(produced_by, ''), payload
		FROM events
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY created_at `+order+`, event_id `+order+`
		LIMIT ?
	`, args...)
	if err != nil {
		return OperatorRuntimeLogListResult{}, fmt.Errorf("query sqlite runtime logs: %w", err)
	}
	defer rows.Close()
	result := OperatorRuntimeLogListResult{Logs: []OperatorRuntimeLogEntry{}}
	for rows.Next() {
		var createdRaw, payloadRaw any
		var logID, runID, entityID, producedBy string
		if err := rows.Scan(&logID, &createdRaw, &runID, &entityID, &producedBy, &payloadRaw); err != nil {
			return OperatorRuntimeLogListResult{}, fmt.Errorf("scan sqlite runtime log: %w", err)
		}
		var createdAt time.Time
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			return OperatorRuntimeLogListResult{}, err
		} else if ok {
			createdAt = at
		}
		log, err := operatorRuntimeLogEntry(logID, runID, entityID, producedBy, createdAt, sqliteJSONRawMessage(payloadRaw))
		if err != nil {
			return OperatorRuntimeLogListResult{}, err
		}
		result.Logs = append(result.Logs, log)
	}
	if err := rows.Err(); err != nil {
		return OperatorRuntimeLogListResult{}, fmt.Errorf("read sqlite runtime logs: %w", err)
	}
	return result, nil
}

func (s *SQLiteRuntimeStore) ListOperatorRuntimeIncidents(ctx context.Context, opts OperatorRuntimeIncidentListOptions) (OperatorRuntimeIncidentListResult, error) {
	logs, err := s.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
		Component: opts.Component,
		Level:     coalesceRuntimeIncidentLevel(opts.Level),
		Limit:     opts.Limit,
	})
	if err != nil {
		return OperatorRuntimeIncidentListResult{}, err
	}
	result := OperatorRuntimeIncidentListResult{Incidents: []OperatorRuntimeIncident{}}
	for _, log := range logs.Logs {
		result.Incidents = append(result.Incidents, OperatorRuntimeIncident{
			IncidentID:    log.LogID,
			FirstSeen:     log.TS,
			LastSeen:      log.TS,
			Count:         1,
			Level:         log.Level,
			Component:     log.Component,
			ErrorCode:     log.ErrorCode,
			SampleMessage: log.Message,
			SampleLogIDs:  []string{log.LogID},
		})
	}
	return result, nil
}

func (s *SQLiteRuntimeStore) sqliteOperatorEventDeliveries(ctx context.Context, eventID string) ([]OperatorEventDelivery, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT delivery_id, subscriber_type, subscriber_id, COALESCE(active_session_id, ''),
		       status, COALESCE(reason_code, ''), COALESCE(failure, 'null'), COALESCE(retry_count, 0),
		       created_at, started_at, delivered_at
		FROM event_deliveries
		WHERE event_id = ?
		ORDER BY created_at ASC, delivery_id ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite operator event deliveries: %w", err)
	}
	defer rows.Close()
	out := []OperatorEventDelivery{}
	for rows.Next() {
		var delivery OperatorEventDelivery
		var rawFailure any
		var createdRaw, startedRaw, finishedRaw any
		if err := rows.Scan(&delivery.DeliveryID, &delivery.SubscriberType, &delivery.SubscriberID, &delivery.SessionID, &delivery.Status, &delivery.ReasonCode, &rawFailure, &delivery.RetryCount, &createdRaw, &startedRaw, &finishedRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite operator event delivery: %w", err)
		}
		delivery.Failure, err = decodeStoredFailure(rawFailure)
		if err != nil {
			return nil, fmt.Errorf("decode sqlite operator event delivery failure: %w", err)
		}
		delivery.CreatedAt = sqliteTraceTimePtr(createdRaw)
		delivery.StartedAt = sqliteTraceTimePtr(startedRaw)
		delivery.FinishedAt = sqliteTraceTimePtr(finishedRaw)
		out = append(out, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite operator event deliveries: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) sqliteOperatorEventDeadLetters(ctx context.Context, eventID string) ([]OperatorDeadLetterRecord, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT dead_letter_id, failure, COALESCE(retry_count, 0), COALESCE(chain_depth, 0), COALESCE(handler_node, ''), created_at
		FROM dead_letters
		WHERE original_event_id = ?
		ORDER BY created_at ASC, dead_letter_id ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("query sqlite operator event dead letters: %w", err)
	}
	defer rows.Close()
	out := []OperatorDeadLetterRecord{}
	for rows.Next() {
		var item OperatorDeadLetterRecord
		var rawFailure any
		var createdRaw any
		if err := rows.Scan(&item.DeadLetterID, &rawFailure, &item.RetryCount, &item.ChainDepth, &item.HandlerNode, &createdRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite operator event dead letter: %w", err)
		}
		failure, err := decodeStoredFailure(rawFailure)
		if err != nil || failure == nil {
			return nil, fmt.Errorf("decode sqlite operator dead-letter failure")
		}
		item.Failure = *failure
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			return nil, err
		} else if ok {
			item.CreatedAt = at
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite operator event dead letters: %w", err)
	}
	return out, nil
}

func sqliteTraceTimePtr(raw any) *time.Time {
	at, ok, err := sqliteTimeValue(raw)
	if err != nil || !ok {
		return nil
	}
	return &at
}

func applySQLiteRuntimeLogPayload(log *OperatorRuntimeLogEntry, raw json.RawMessage) error {
	if log == nil {
		return fmt.Errorf("runtime log target is required")
	}
	payload, err := runtimepkg.DecodeCanonicalRuntimeLogPayload(raw)
	if err != nil {
		return err
	}
	log.Level = strings.TrimSpace(payload.LogLevel)
	log.Message = strings.TrimSpace(payload.Message)
	log.Details = payload.Detail
	log.Component = strings.TrimSpace(payload.Component)
	log.Source = strings.TrimSpace(payload.AgentID)
	log.SessionID = strings.TrimSpace(payload.SessionID)
	log.ErrorCode = strings.TrimSpace(payload.ErrorCode)
	log.Failure = runtimefailures.CloneEnvelope(payload.Failure)
	return nil
}

func coalesceRuntimeIncidentLevel(level string) string {
	level = strings.TrimSpace(strings.ToLower(level))
	if level != "" {
		return level
	}
	return "error"
}

func sqliteObservabilityString(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprint(typed)
	}
}
