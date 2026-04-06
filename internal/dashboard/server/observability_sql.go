package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
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
			COALESCE(e.payload->'details'->>'event_id', ''),
			e.created_at,
			COALESCE(e.payload->>'log_level', ''),
			COALESCE(e.payload->'details'->>'component', ''),
			COALESCE(e.payload->'details'->>'action', ''),
			COALESCE(e.payload->'details'->>'event_name', COALESCE(e.payload->'details'->>'event_type', '')),
			COALESCE(e.payload->'details'->>'parent_event_id', ''),
			COALESCE(e.payload->'details'->>'handler_id', ''),
			COALESCE(e.payload->'details'->>'error', ''),
			COALESCE(e.payload->'details'->>'error_code', ''),
			COALESCE(e.payload->'details'->>'agent_id', ''),
			COALESCE(e.payload->'details'->>'agent_id', ''),
			COALESCE(e.payload->'details'->>'entity_id', ''),
			COALESCE(e.payload->'details'->>'session_id', ''),
			COALESCE(NULLIF(e.payload->'details'->>'duration_us', ''), '0')::int,
			COALESCE(e.payload->'details', '{}'::jsonb),
			COALESCE(e.payload->'details'->'correlation', '{}'::jsonb),
			COALESCE(e.payload->>'message', '')
		FROM events e
		WHERE e.event_name = 'platform.runtime_log'
		  AND ($1 = '' OR COALESCE(e.payload->'details'->>'event_name', COALESCE(e.payload->'details'->>'event_type', '')) = $1 OR COALESCE(e.payload->'details'->>'action', '') = $1)
		  AND ($2 = '' OR COALESCE(e.payload->'details'->>'agent_id', '') = $2)
		  AND ($3 = '' OR COALESCE(e.payload->'details'->>'entity_id', '') = $3)
		  AND ($4 = '' OR COALESCE(e.payload->'details'->>'component', '') = $4)
		  AND ($5 = '' OR COALESCE(e.payload->>'log_level', '') = $5)
		  AND ($6 = '' OR COALESCE(e.payload->'details'->>'error_code', '') = $6)
		  AND ($7::timestamptz IS NULL OR e.created_at > $7)
		ORDER BY e.created_at %s
		LIMIT $8
	`, order)
	rows, err := r.db.QueryContext(ctx, query,
		strings.TrimSpace(filter.Type),
		strings.TrimSpace(filter.Source),
		strings.TrimSpace(filter.EntityID),
		strings.TrimSpace(filter.Component),
		strings.TrimSpace(filter.Level),
		strings.TrimSpace(filter.ErrorCode),
		nullableTime(filter.After),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list runtime logs: %w", err)
	}
	defer rows.Close()

	out := make([]runtimeLogRecord, 0, limit)
	for rows.Next() {
		var item runtimeLogRecord
		var (
			detailRaw      []byte
			correlationRaw []byte
		)
		if err := rows.Scan(&item.ID, &item.EventID, &item.TS, &item.Level, &item.Component, &item.Action, &item.EventType, &item.ParentEventID, &item.HandlerID, &item.Error, &item.ErrorCode, &item.AgentID, &item.Source, &item.EntityID, &item.SessionID, &item.DurationUS, &detailRaw, &correlationRaw, &item.Message); err != nil {
			return nil, fmt.Errorf("scan runtime log: %w", err)
		}
		detailMap, err := decodeJSONMap(detailRaw)
		if err != nil {
			return nil, fmt.Errorf("decode runtime log detail: %w", err)
		}
		correlationMap, err := decodeJSONMap(correlationRaw)
		if err != nil {
			return nil, fmt.Errorf("decode runtime log correlation: %w", err)
		}
		item.Detail = detailMap
		item.Correlation = correlationMap
		out = append(out, item)
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
		WITH logs AS (
			SELECT
				COALESCE(e.payload->'details'->>'error_code', '') AS code,
				COALESCE(e.payload->'details'->>'component', '') AS component,
				COALESCE(e.payload->>'log_level', '') AS level,
				COALESCE(e.payload->'details'->>'agent_id', '') AS agent_id,
				COALESCE(e.payload->'details'->>'action', '') AS action,
				COALESCE(e.payload->'details'->>'error', COALESCE(e.payload->>'message', '')) AS error,
				e.created_at
			FROM events e
			WHERE e.event_name = 'platform.runtime_log'
			  AND e.created_at >= now() - make_interval(hours => $1)
			  AND COALESCE(e.payload->'details'->>'error_code', '') <> ''
			  AND ($2 = '' OR COALESCE(e.payload->>'log_level', '') = $2)
			  AND ($3 = '' OR COALESCE(e.payload->'details'->>'component', '') = $3)
			  AND ($4 = FALSE OR COALESCE(e.payload->'details'->>'component', '') LIKE 'mcp%%')
		)
		SELECT
			code,
			COUNT(*)::int,
			MIN(created_at),
			MAX(created_at),
			COALESCE(NULLIF(MAX(error), ''), ''),
			COALESCE(NULLIF(MAX(component), ''), ''),
			COALESCE(NULLIF(MAX(level), ''), ''),
			COALESCE(array_remove(array_agg(DISTINCT NULLIF(agent_id, '')), NULL), ARRAY[]::text[]),
			COALESCE(array_remove(array_agg(DISTINCT NULLIF(component, '')), NULL), ARRAY[]::text[]),
			COALESCE(array_remove(array_agg(DISTINCT NULLIF(action, '')), NULL), ARRAY[]::text[])
		FROM logs
		GROUP BY code
		ORDER BY MAX(created_at) DESC, code ASC
		LIMIT $5
	`, sinceHours, strings.TrimSpace(filter.Level), strings.TrimSpace(filter.Component), filter.MCPOnly, limit)
	if err != nil {
		return nil, fmt.Errorf("list incidents: %w", err)
	}
	defer rows.Close()

	out := make([]incidentRecord, 0, limit)
	for rows.Next() {
		var (
			item       incidentRecord
			firstSeen  time.Time
			lastSeen   time.Time
			agents     []string
			components []string
			actions    []string
		)
		if err := rows.Scan(&item.Code, &item.Count, &firstSeen, &lastSeen, &item.RootCause, &item.Component, &item.Level, pqArray(&agents), pqArray(&components), pqArray(&actions)); err != nil {
			return nil, fmt.Errorf("scan incident: %w", err)
		}
		item.Agents = agents
		item.Components = components
		item.Actions = actions
		item.FirstSeen = formatTime(firstSeen)
		item.LastSeen = formatTime(lastSeen)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("incident rows: %w", err)
	}
	return out, nil
}

type textArrayScanner struct {
	target *[]string
}

func pqArray(target *[]string) textArrayScanner {
	return textArrayScanner{target: target}
}

func (s textArrayScanner) Scan(src any) error {
	if s.target == nil {
		return nil
	}
	switch typed := src.(type) {
	case nil:
		*s.target = nil
		return nil
	case []byte:
		return parsePGTextArray(string(typed), s.target)
	case string:
		return parsePGTextArray(typed, s.target)
	default:
		return fmt.Errorf("unsupported text array source %T", src)
	}
}

func parsePGTextArray(raw string, target *[]string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" {
		*target = []string{}
		return nil
	}
	raw = strings.TrimPrefix(raw, "{")
	raw = strings.TrimSuffix(raw, "}")
	if strings.TrimSpace(raw) == "" {
		*target = []string{}
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(strings.TrimSpace(part), `"`)
		if part == "" {
			continue
		}
		out = append(out, part)
	}
	*target = out
	return nil
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
