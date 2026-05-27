package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (s *SQLiteRuntimeStore) LoadRunDebugTracePage(ctx context.Context, runID string, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error) {
	runID = nullUUIDString(runID)
	if runID == "" {
		return nil, "", ErrRunNotFound
	}
	if opts.Cursor != "" {
		return nil, "", ErrInvalidObservabilityCursor
	}
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = ?)`, runID).Scan(&exists); err != nil {
		return nil, "", fmt.Errorf("load sqlite run trace run existence: %w", err)
	}
	if !exists {
		return nil, "", ErrRunNotFound
	}
	opts = defaultRunDebugTraceQueryOptions(opts)
	where := []string{"e.run_id = ?", "NOT (COALESCE(d.subscriber_type, '') = ? AND COALESCE(d.subscriber_id, '') = ?)"}
	args := []any{runID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID}
	if opts.Since != nil {
		where = append(where, "e.created_at >= ?")
		args = append(args, opts.Since.UTC())
	}
	if opts.Until != nil {
		where = append(where, "e.created_at <= ?")
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
	args = append(args, opts.Limit)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id, e.event_name, COALESCE(e.source_event_id, ''), COALESCE(e.entity_id, ''),
			COALESCE(e.produced_by, ''), COALESCE(e.produced_by_type, ''), e.created_at,
			COALESCE(d.delivery_id, ''), COALESCE(d.subscriber_type, ''), COALESCE(d.subscriber_id, ''),
			COALESCE(d.status, ''), COALESCE(d.reason_code, ''), COALESCE(d.active_session_id, ''),
			d.created_at, d.started_at, d.delivered_at,
			COALESCE(ses.session_id, ''), COALESCE(ses.scope, ''), COALESCE(ses.runtime_mode, ''),
			COALESCE(ses.status, ''), ses.updated_at,
			COALESCE(t.turn_id, ''), COALESCE(t.trigger_event_id, ''), COALESCE(t.trigger_event_type, ''),
			COALESCE(t.runtime_mode, ''), COALESCE(t.scope_key, ''), COALESCE(t.entity_id, ''),
			COALESCE(t.task_id, ''), COALESCE(t.parse_ok, 0), COALESCE(t.retry_count, 0),
			COALESCE(t.error, ''), t.created_at
		FROM events e
		LEFT JOIN event_deliveries d ON d.event_id = e.event_id
		LEFT JOIN agent_sessions ses ON ses.session_id = d.active_session_id
		LEFT JOIN agent_turns t ON t.trigger_event_id = e.event_id
		WHERE `+strings.Join(where, " AND ")+`
		ORDER BY e.created_at ASC, e.event_id ASC, d.created_at ASC, d.delivery_id ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, "", fmt.Errorf("query sqlite run trace: %w", err)
	}
	defer rows.Close()
	out := make([]RunDebugTraceRow, 0, opts.Limit)
	for rows.Next() {
		var row RunDebugTraceRow
		var eventCreatedRaw, deliveryCreatedRaw, deliveryStartedRaw, deliveryDeliveredRaw, sessionUpdatedRaw, turnCreatedRaw any
		if err := rows.Scan(
			&row.EventID, &row.EventName, &row.SourceEventID, &row.EntityID,
			&row.EventSource, &row.EventSourceType, &eventCreatedRaw,
			&row.DeliveryID, &row.SubscriberType, &row.SubscriberID,
			&row.DeliveryStatus, &row.DeliveryReasonCode, &row.ActiveSessionID,
			&deliveryCreatedRaw, &deliveryStartedRaw, &deliveryDeliveredRaw,
			&row.SessionID, &row.SessionKind, &row.SessionRuntimeMode,
			&row.SessionStatus, &sessionUpdatedRaw,
			&row.TurnID, &row.TurnTriggerEventID, &row.TurnTriggerEventType,
			&row.TurnRuntimeMode, &row.TurnScopeKey, &row.TurnEntityID,
			&row.TurnTaskID, &row.TurnParseOK, &row.TurnRetryCount,
			&row.TurnError, &turnCreatedRaw,
		); err != nil {
			return nil, "", fmt.Errorf("scan sqlite run trace: %w", err)
		}
		if at, ok, err := sqliteTimeValue(eventCreatedRaw); err != nil {
			return nil, "", err
		} else if ok {
			row.EventCreatedAt = at
		}
		row.DeliveryCreatedAt = sqliteTraceTimePtr(deliveryCreatedRaw)
		row.DeliveryStartedAt = sqliteTraceTimePtr(deliveryStartedRaw)
		row.DeliveryDeliveredAt = sqliteTraceTimePtr(deliveryDeliveredRaw)
		row.SessionUpdatedAt = sqliteTraceTimePtr(sessionUpdatedRaw)
		row.TurnCreatedAt = sqliteTraceTimePtr(turnCreatedRaw)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read sqlite run trace: %w", err)
	}
	return out, "", nil
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
		SELECT event_id, event_name, COALESCE(entity_id, ''), COALESCE(run_id, ''), COALESCE(source_event_id, ''),
		       created_at, COALESCE(produced_by, ''), payload
		FROM events
		WHERE event_id = ?
	`, eventID).Scan(&event.EventID, &event.EventName, &event.EntityID, &event.RunID, &event.SourceEventID, &createdRaw, &event.Source, &payloadRaw)
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
	deliveries, err := s.sqliteOperatorEventDeliveries(ctx, eventID)
	if err != nil {
		return OperatorEventFull{}, err
	}
	event.Deliveries = deliveries
	event.DeadLetters = []OperatorDeadLetterRecord{}
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
		SELECT event_id, created_at, COALESCE(run_id, ''), COALESCE(entity_id, ''), payload
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
		var log OperatorRuntimeLogEntry
		var createdRaw, payloadRaw any
		if err := rows.Scan(&log.LogID, &createdRaw, &log.RunID, &log.EntityID, &payloadRaw); err != nil {
			return OperatorRuntimeLogListResult{}, fmt.Errorf("scan sqlite runtime log: %w", err)
		}
		if at, ok, err := sqliteTimeValue(createdRaw); err != nil {
			return OperatorRuntimeLogListResult{}, err
		} else if ok {
			log.TS = at
		}
		applySQLiteRuntimeLogPayload(&log, sqliteJSONRawMessage(payloadRaw))
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
		       status, COALESCE(reason_code, ''), COALESCE(last_error, ''), COALESCE(retry_count, 0)
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
		if err := rows.Scan(&delivery.DeliveryID, &delivery.SubscriberType, &delivery.SubscriberID, &delivery.SessionID, &delivery.Status, &delivery.ReasonCode, &delivery.LastError, &delivery.RetryCount); err != nil {
			return nil, fmt.Errorf("scan sqlite operator event delivery: %w", err)
		}
		out = append(out, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read sqlite operator event deliveries: %w", err)
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

func applySQLiteRuntimeLogPayload(log *OperatorRuntimeLogEntry, raw json.RawMessage) {
	if log == nil {
		return
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		payload = map[string]any{}
	}
	log.Level = strings.TrimSpace(sqliteObservabilityString(payload["log_level"]))
	if log.Level == "" {
		log.Level = "info"
	}
	log.Message = strings.TrimSpace(sqliteObservabilityString(payload["message"]))
	details, _ := payload["details"].(map[string]any)
	if details == nil {
		details = map[string]any{}
	}
	log.Details = details
	log.Component = strings.TrimSpace(sqliteObservabilityString(details["component"]))
	log.Source = strings.TrimSpace(sqliteObservabilityString(details["source"]))
	log.SessionID = strings.TrimSpace(sqliteObservabilityString(details["session_id"]))
	log.ErrorCode = strings.TrimSpace(sqliteObservabilityString(details["error_code"]))
	if log.Message == "" {
		log.Message = strings.TrimSpace(sqliteObservabilityString(details["message"]))
	}
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
