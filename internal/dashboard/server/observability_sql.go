package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimepkg "swarm/internal/runtime"
)

type EventFilter struct {
	Type       string
	Source     string
	EntityID   string
	Subscriber string
	After      time.Time
}

type RuntimeLogFilter struct {
	Type      string
	Source    string
	EntityID  string
	Component string
	Level     string
	ErrorCode string
	Order     string
	After     time.Time
}

type IncidentFilter struct {
	SinceHours int
	MCPOnly    bool
	Level      string
	Component  string
	Limit      int
}

type eventDeliveryRecord struct {
	AgentID    string `json:"agent_id,omitempty"`
	Status     string `json:"status,omitempty"`
	Error      string `json:"error,omitempty"`
	RetryCount int    `json:"retry_count,omitempty"`
}

type deliveryLifecycleSummary struct {
	Pending    int `json:"pending,omitempty"`
	InProgress int `json:"in_progress,omitempty"`
	Delivered  int `json:"delivered,omitempty"`
	Failed     int `json:"failed,omitempty"`
	DeadLetter int `json:"dead_letter,omitempty"`
}

func (s *deliveryLifecycleSummary) record(status string) {
	if s == nil {
		return
	}
	switch strings.TrimSpace(status) {
	case "pending":
		s.Pending++
	case "in_progress":
		s.InProgress++
	case "delivered":
		s.Delivered++
	case "failed":
		s.Failed++
	case "dead_letter":
		s.DeadLetter++
	}
}

type eventRecord struct {
	ID                string                   `json:"id"`
	EventID           string                   `json:"event_id,omitempty"`
	Type              string                   `json:"type,omitempty"`
	CreatedAt         string                   `json:"created_at,omitempty"`
	SourceAgent       string                   `json:"source_agent,omitempty"`
	EntityID          string                   `json:"entity_id,omitempty"`
	Scope             string                   `json:"scope,omitempty"`
	ParentEventID     string                   `json:"parent_event_id,omitempty"`
	Payload           any                      `json:"payload,omitempty"`
	DeliveryLifecycle deliveryLifecycleSummary `json:"delivery_lifecycle,omitempty"`
	Deliveries        []eventDeliveryRecord    `json:"deliveries,omitempty"`
	ErrorCount        int                      `json:"error_count,omitempty"`
	DeadCount         int                      `json:"dead_count,omitempty"`
	PendingCount      int                      `json:"pending_count,omitempty"`
}

type runtimeLogRecord struct {
	ID            string `json:"id"`
	EventID       string `json:"event_id,omitempty"`
	TS            string `json:"ts,omitempty"`
	Level         string `json:"level,omitempty"`
	Component     string `json:"component,omitempty"`
	Action        string `json:"action,omitempty"`
	EventType     string `json:"event_type,omitempty"`
	ParentEventID string `json:"parent_event_id,omitempty"`
	HandlerID     string `json:"handler_id,omitempty"`
	Error         string `json:"error,omitempty"`
	ErrorCode     string `json:"error_code,omitempty"`
	AgentID       string `json:"agent_id,omitempty"`
	EntityID      string `json:"entity_id,omitempty"`
	SessionID     string `json:"session_id,omitempty"`
	DurationUS    int    `json:"duration_us,omitempty"`
	Source        string `json:"source,omitempty"`
	Message       string `json:"message,omitempty"`
	DeliveryState string `json:"delivery_state,omitempty"`
	PreviousState string `json:"delivery_previous_state,omitempty"`
	Transition    string `json:"delivery_transition,omitempty"`
	Reason        string `json:"delivery_reason,omitempty"`
	Terminal      string `json:"delivery_terminal_outcome,omitempty"`
	RetryCount    int    `json:"delivery_retry_count,omitempty"`
	Detail        any    `json:"detail,omitempty"`
	Correlation   any    `json:"correlation,omitempty"`
}

type incidentRecord struct {
	Code       string   `json:"code"`
	Count      int      `json:"count,omitempty"`
	RootCause  string   `json:"root_cause,omitempty"`
	Component  string   `json:"component,omitempty"`
	Level      string   `json:"level,omitempty"`
	Agents     []string `json:"agents,omitempty"`
	Components []string `json:"components,omitempty"`
	Actions    []string `json:"actions,omitempty"`
	FirstSeen  string   `json:"first_seen,omitempty"`
	LastSeen   string   `json:"last_seen,omitempty"`
}

type SQLObservabilityReader struct {
	db *sql.DB
}

func NewSQLObservabilityReader(db *sql.DB) *SQLObservabilityReader {
	if db == nil {
		return nil
	}
	return &SQLObservabilityReader{db: db}
}

func applyDeliveryLifecycle(record *eventRecord, lifecycle deliveryLifecycleSummary) {
	if record == nil {
		return
	}
	record.DeliveryLifecycle = lifecycle
	record.PendingCount = lifecycle.Pending
	record.ErrorCount = lifecycle.Failed
	record.DeadCount = lifecycle.DeadLetter
}

func (r *SQLObservabilityReader) ListEvents(ctx context.Context, filter EventFilter, limit int) ([]eventRecord, error) {
	if r == nil || r.db == nil {
		return []eventRecord{}, nil
	}
	if limit <= 0 {
		limit = 200
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			e.event_id::text,
			e.event_name,
			e.created_at,
			COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, COALESCE(e.payload->>'entity_id', '')),
			COALESCE(e.scope, ''),
			COALESCE(e.source_event_id::text, '') AS parent_event_id,
			COALESCE(e.payload, '{}'::jsonb),
			COALESCE(dl.pending_count, 0),
			COALESCE(dl.in_progress_count, 0),
			COALESCE(dl.delivered_count, 0),
			COALESCE(dl.failed_count, 0),
			COALESCE(dl.dead_letter_count, 0)
		FROM events e
		LEFT JOIN LATERAL (
			SELECT
				COUNT(*) FILTER (WHERE d.status = 'pending')::int AS pending_count,
				COUNT(*) FILTER (WHERE d.status = 'in_progress')::int AS in_progress_count,
				COUNT(*) FILTER (WHERE d.status = 'delivered')::int AS delivered_count,
				COUNT(*) FILTER (WHERE d.status = 'failed')::int AS failed_count,
				COUNT(*) FILTER (WHERE d.status = 'dead_letter')::int AS dead_letter_count
			FROM event_deliveries d
			WHERE d.event_id = e.event_id
		) dl ON TRUE
		WHERE e.event_name <> 'platform.runtime_log'
		  AND ($1 = '' OR e.event_name = $1)
		  AND ($2 = '' OR COALESCE(e.produced_by, '') = $2)
		  AND ($3 = '' OR EXISTS (
				SELECT 1
				FROM event_deliveries d
				WHERE d.event_id = e.event_id
				  AND d.subscriber_id = $3
		  ))
		  AND ($4 = '' OR COALESCE(e.entity_id::text, COALESCE(e.payload->>'entity_id', '')) = $4)
		  AND ($5::timestamptz IS NULL OR e.created_at > $5)
		ORDER BY e.created_at DESC
		LIMIT $6
	`, strings.TrimSpace(filter.Type), strings.TrimSpace(filter.Source), strings.TrimSpace(filter.Subscriber), strings.TrimSpace(filter.EntityID), nullableTime(filter.After), limit)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	out := make([]eventRecord, 0, limit)
	for rows.Next() {
		var (
			item       eventRecord
			payloadRaw []byte
			lifecycle  deliveryLifecycleSummary
		)
		if err := rows.Scan(
			&item.ID,
			&item.Type,
			&item.CreatedAt,
			&item.SourceAgent,
			&item.EntityID,
			&item.Scope,
			&item.ParentEventID,
			&payloadRaw,
			&lifecycle.Pending,
			&lifecycle.InProgress,
			&lifecycle.Delivered,
			&lifecycle.Failed,
			&lifecycle.DeadLetter,
		); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		item.EventID = item.ID
		payloadMap, err := decodeJSONMap(payloadRaw)
		if err != nil {
			return nil, fmt.Errorf("decode event payload: %w", err)
		}
		item.Payload = payloadMap
		if strings.TrimSpace(item.EntityID) == "" {
			item.EntityID = readString(payloadMap["entity_id"])
		}
		applyDeliveryLifecycle(&item, lifecycle)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list event rows: %w", err)
	}
	return out, nil
}

func (r *SQLObservabilityReader) GetEvent(ctx context.Context, id string) (eventRecord, bool, error) {
	if r == nil || r.db == nil {
		return eventRecord{}, false, nil
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return eventRecord{}, false, nil
	}
	row := r.db.QueryRowContext(ctx, `
		SELECT
			e.event_id::text,
			e.event_name,
			e.created_at,
			COALESCE(e.produced_by, ''),
			COALESCE(e.entity_id::text, COALESCE(e.payload->>'entity_id', '')),
			COALESCE(e.scope, ''),
			COALESCE(e.source_event_id::text, '') AS parent_event_id,
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		WHERE e.event_id::text = $1
		LIMIT 1
	`, id)
	var (
		item       eventRecord
		payloadRaw []byte
	)
	if err := row.Scan(&item.ID, &item.Type, &item.CreatedAt, &item.SourceAgent, &item.EntityID, &item.Scope, &item.ParentEventID, &payloadRaw); err == sql.ErrNoRows {
		return eventRecord{}, false, nil
	} else if err != nil {
		return eventRecord{}, false, fmt.Errorf("get event: %w", err)
	}
	item.EventID = item.ID
	payloadMap, err := decodeJSONMap(payloadRaw)
	if err != nil {
		return eventRecord{}, false, fmt.Errorf("decode event payload: %w", err)
	}
	item.Payload = payloadMap
	if strings.TrimSpace(item.EntityID) == "" {
		item.EntityID = readString(payloadMap["entity_id"])
	}

	deliveries, lifecycle, err := r.loadEventDeliveries(ctx, id)
	if err != nil {
		return eventRecord{}, false, err
	}
	item.Deliveries = deliveries
	applyDeliveryLifecycle(&item, lifecycle)
	return item, true, nil
}

func (r *SQLObservabilityReader) loadEventDeliveries(ctx context.Context, id string) ([]eventDeliveryRecord, deliveryLifecycleSummary, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			d.subscriber_id,
			COALESCE(d.status, 'pending'),
			COALESCE(d.last_error, ''),
			COALESCE(d.retry_count, 0)
		FROM event_deliveries d
		WHERE d.event_id::text = $1
		ORDER BY d.created_at ASC, d.subscriber_id ASC
	`, id)
	if err != nil {
		return nil, deliveryLifecycleSummary{}, fmt.Errorf("load event deliveries: %w", err)
	}
	defer rows.Close()

	out := []eventDeliveryRecord{}
	var lifecycle deliveryLifecycleSummary
	for rows.Next() {
		var item eventDeliveryRecord
		if err := rows.Scan(&item.AgentID, &item.Status, &item.Error, &item.RetryCount); err != nil {
			return nil, deliveryLifecycleSummary{}, fmt.Errorf("scan event delivery: %w", err)
		}
		lifecycle.record(item.Status)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, deliveryLifecycleSummary{}, fmt.Errorf("event delivery rows: %w", err)
	}
	return out, lifecycle, nil
}

func (r *SQLObservabilityReader) ListRuntimeLogs(ctx context.Context, filter RuntimeLogFilter, limit int) ([]runtimeLogRecord, error) {
	if r == nil || r.db == nil {
		return []runtimeLogRecord{}, nil
	}
	if limit <= 0 {
		limit = 200
	}
	order := "DESC"
	if strings.EqualFold(strings.TrimSpace(filter.Order), "asc") {
		order = "ASC"
	}
	query := fmt.Sprintf(`
		SELECT
			e.event_id::text,
			e.created_at,
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		WHERE e.event_name = 'platform.runtime_log'
		  AND ($1::timestamptz IS NULL OR e.created_at > $1)
		ORDER BY e.created_at %s
	`, order)
	rows, err := r.db.QueryContext(ctx, query,
		nullableTime(filter.After),
	)
	if err != nil {
		return nil, fmt.Errorf("list runtime logs: %w", err)
	}
	defer rows.Close()

	out := make([]runtimeLogRecord, 0, limit)
	for rows.Next() {
		var (
			eventID    string
			createdAt  time.Time
			payloadRaw []byte
		)
		if err := rows.Scan(&eventID, &createdAt, &payloadRaw); err != nil {
			return nil, fmt.Errorf("scan runtime log: %w", err)
		}
		item, err := decodeRuntimeLogRecord(eventID, createdAt, payloadRaw)
		if err != nil {
			return nil, err
		}
		if !matchesRuntimeLogFilter(item, filter) {
			continue
		}
		out = append(out, item)
		if len(out) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("runtime log rows: %w", err)
	}
	return out, nil
}

func (r *SQLObservabilityReader) ListIncidents(ctx context.Context, filter IncidentFilter) ([]incidentRecord, error) {
	if r == nil || r.db == nil {
		return []incidentRecord{}, nil
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 2000
	}
	sinceHours := filter.SinceHours
	if sinceHours <= 0 {
		sinceHours = 24
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			e.event_id::text,
			e.created_at,
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		WHERE e.event_name = 'platform.runtime_log'
		  AND e.created_at >= now() - make_interval(hours => $1)
		ORDER BY e.created_at DESC
	`, sinceHours)
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	defer rows.Close()

	type incidentAggregate struct {
		record        incidentRecord
		firstSeen     time.Time
		lastSeen      time.Time
		agentsSet     map[string]struct{}
		componentsSet map[string]struct{}
		actionsSet    map[string]struct{}
	}

	aggregates := map[string]*incidentAggregate{}
	for rows.Next() {
		var (
			eventID    string
			createdAt  time.Time
			payloadRaw []byte
		)
		if err := rows.Scan(&eventID, &createdAt, &payloadRaw); err != nil {
			return nil, fmt.Errorf("scan runtime log for incidents: %w", err)
		}
		logRecord, err := decodeRuntimeLogRecord(eventID, createdAt, payloadRaw)
		if err != nil {
			return nil, err
		}
		if !matchesIncidentFilter(logRecord, filter) {
			continue
		}
		if strings.TrimSpace(logRecord.ErrorCode) == "" {
			continue
		}
		agg := aggregates[logRecord.ErrorCode]
		if agg == nil {
			agg = &incidentAggregate{
				record: incidentRecord{
					Code: logRecord.ErrorCode,
				},
				firstSeen:     createdAt,
				lastSeen:      createdAt,
				agentsSet:     map[string]struct{}{},
				componentsSet: map[string]struct{}{},
				actionsSet:    map[string]struct{}{},
			}
			aggregates[logRecord.ErrorCode] = agg
		}
		agg.record.Count++
		if createdAt.Before(agg.firstSeen) {
			agg.firstSeen = createdAt
		}
		if createdAt.After(agg.lastSeen) {
			agg.lastSeen = createdAt
		}
		if strings.TrimSpace(logRecord.Error) > strings.TrimSpace(agg.record.RootCause) {
			agg.record.RootCause = strings.TrimSpace(logRecord.Error)
		}
		if strings.TrimSpace(logRecord.Component) > strings.TrimSpace(agg.record.Component) {
			agg.record.Component = strings.TrimSpace(logRecord.Component)
		}
		if strings.TrimSpace(logRecord.Level) > strings.TrimSpace(agg.record.Level) {
			agg.record.Level = strings.TrimSpace(logRecord.Level)
		}
		if v := strings.TrimSpace(logRecord.AgentID); v != "" {
			agg.agentsSet[v] = struct{}{}
		}
		if v := strings.TrimSpace(logRecord.Component); v != "" {
			agg.componentsSet[v] = struct{}{}
		}
		if v := strings.TrimSpace(logRecord.Action); v != "" {
			agg.actionsSet[v] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("incident runtime log rows: %w", err)
	}

	out := make([]incidentRecord, 0, len(aggregates))
	for _, agg := range aggregates {
		agg.record.Agents = sortedStringSet(agg.agentsSet)
		agg.record.Components = sortedStringSet(agg.componentsSet)
		agg.record.Actions = sortedStringSet(agg.actionsSet)
		agg.record.FirstSeen = formatTime(agg.firstSeen)
		agg.record.LastSeen = formatTime(agg.lastSeen)
		out = append(out, agg.record)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].LastSeen != out[j].LastSeen {
			return out[i].LastSeen > out[j].LastSeen
		}
		return out[i].Code < out[j].Code
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func nullableTime(ts time.Time) any {
	if ts.IsZero() {
		return nil
	}
	return ts.UTC()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func decodeJSONAny(raw []byte) (any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeRuntimeLogRecord(rowID string, createdAt time.Time, payloadRaw []byte) (runtimeLogRecord, error) {
	payload, err := runtimepkg.DecodeCanonicalRuntimeLogPayload(payloadRaw)
	if err != nil {
		return runtimeLogRecord{}, fmt.Errorf("decode canonical runtime log payload: %w", err)
	}
	record := runtimeLogRecord{
		ID:            strings.TrimSpace(rowID),
		EventID:       payload.EventID,
		TS:            formatTime(createdAt),
		Level:         payload.LogLevel,
		Component:     payload.Component,
		Action:        payload.Action,
		EventType:     payload.EventType,
		ParentEventID: payload.ParentEventID,
		HandlerID:     payload.HandlerID,
		Error:         payload.Error,
		ErrorCode:     payload.ErrorCode,
		AgentID:       payload.AgentID,
		EntityID:      payload.EntityID,
		SessionID:     payload.SessionID,
		DurationUS:    payload.DurationUS,
		Source:        payload.AgentID,
		Message:       payload.Message,
		DeliveryState: payload.DeliveryState,
		PreviousState: payload.PreviousState,
		Transition:    payload.Transition,
		Reason:        payload.Reason,
		Terminal:      payload.Terminal,
		RetryCount:    payload.RetryCount,
		Detail:        payload.Detail,
		Correlation:   payload.Correlation,
	}
	return record, nil
}

func matchesRuntimeLogFilter(item runtimeLogRecord, filter RuntimeLogFilter) bool {
	typeFilter := strings.TrimSpace(filter.Type)
	if typeFilter != "" && strings.TrimSpace(item.EventType) != typeFilter && strings.TrimSpace(item.Action) != typeFilter {
		return false
	}
	sourceFilter := strings.TrimSpace(filter.Source)
	if sourceFilter != "" && strings.TrimSpace(item.Source) != sourceFilter {
		return false
	}
	entityFilter := strings.TrimSpace(filter.EntityID)
	if entityFilter != "" && strings.TrimSpace(item.EntityID) != entityFilter {
		return false
	}
	componentFilter := strings.TrimSpace(filter.Component)
	if componentFilter != "" && strings.TrimSpace(item.Component) != componentFilter {
		return false
	}
	levelFilter := strings.TrimSpace(filter.Level)
	if levelFilter != "" && strings.TrimSpace(item.Level) != levelFilter {
		return false
	}
	errorCodeFilter := strings.TrimSpace(filter.ErrorCode)
	if errorCodeFilter != "" && strings.TrimSpace(item.ErrorCode) != errorCodeFilter {
		return false
	}
	return true
}

func matchesIncidentFilter(item runtimeLogRecord, filter IncidentFilter) bool {
	levelFilter := strings.TrimSpace(filter.Level)
	if levelFilter != "" && strings.TrimSpace(item.Level) != levelFilter {
		return false
	}
	componentFilter := strings.TrimSpace(filter.Component)
	if componentFilter != "" && strings.TrimSpace(item.Component) != componentFilter {
		return false
	}
	if filter.MCPOnly && !strings.HasPrefix(strings.TrimSpace(item.Component), "mcp") {
		return false
	}
	return true
}

func sortedStringSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(values))
	for value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}
