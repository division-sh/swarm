package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
)

type OperatorEventListFilter struct {
	RunID          string
	EntityID       string
	EventName      string
	DeliveryStatus string
	SubscriberID   string
	SubscriberType string
	ReasonCode     string
	HasDeadLetter  *bool
}

type OperatorEventListOptions struct {
	Filter             OperatorEventListFilter
	Source             string
	Since              *time.Time
	Until              *time.Time
	Limit              int
	Cursor             string
	Order              string
	ExcludeRuntimeLogs bool
}

type OperatorEventListResult struct {
	Events     []OperatorEventFull `json:"events"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

type OperatorEventFull struct {
	EventID       string                     `json:"event_id"`
	EventName     string                     `json:"event_name"`
	EntityID      string                     `json:"entity_id,omitempty"`
	RunID         string                     `json:"run_id,omitempty"`
	SourceEventID string                     `json:"source_event_id,omitempty"`
	CreatedAt     time.Time                  `json:"created_at"`
	Source        string                     `json:"source"`
	Payload       map[string]any             `json:"payload"`
	Deliveries    []OperatorEventDelivery    `json:"deliveries"`
	DeadLetters   []OperatorDeadLetterRecord `json:"dead_letters"`
}

type OperatorEventDelivery struct {
	DeliveryID     string                     `json:"delivery_id"`
	SubscriberType string                     `json:"subscriber_type"`
	SubscriberID   string                     `json:"subscriber_id"`
	SessionID      string                     `json:"session_id,omitempty"`
	Status         string                     `json:"status"`
	ReasonCode     string                     `json:"reason_code,omitempty"`
	LastError      string                     `json:"last_error,omitempty"`
	RetryCount     int                        `json:"retry_count"`
	RetryEligible  bool                       `json:"retry_eligible"`
	Terminal       bool                       `json:"terminal"`
	CreatedAt      *time.Time                 `json:"created_at,omitempty"`
	StartedAt      *time.Time                 `json:"started_at,omitempty"`
	FinishedAt     *time.Time                 `json:"finished_at,omitempty"`
	DeadLetters    []OperatorDeadLetterRecord `json:"dead_letters,omitempty"`
}

type OperatorDeadLetterRecord struct {
	DeadLetterID string    `json:"dead_letter_id"`
	FailureType  string    `json:"failure_type"`
	ErrorMessage string    `json:"error_message,omitempty"`
	RetryCount   int       `json:"retry_count"`
	ChainDepth   int       `json:"chain_depth"`
	HandlerNode  string    `json:"handler_node,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

type OperatorRuntimeLogListOptions struct {
	RunID             string
	BundleHash        string
	EntityID          string
	SessionID         string
	Component         string
	Level             string
	ErrorCode         string
	Source            string
	ActionOrEventType string
	Since             *time.Time
	Until             *time.Time
	Limit             int
	Cursor            string
	Order             string
}

type OperatorRuntimeLogListResult struct {
	Logs       []OperatorRuntimeLogEntry `json:"logs"`
	NextCursor string                    `json:"next_cursor,omitempty"`
}

type OperatorRuntimeLogEntry struct {
	LogID     string         `json:"log_id"`
	TS        time.Time      `json:"ts"`
	Level     string         `json:"level"`
	Component string         `json:"component"`
	Source    string         `json:"source"`
	RunID     string         `json:"run_id,omitempty"`
	EntityID  string         `json:"entity_id,omitempty"`
	SessionID string         `json:"session_id,omitempty"`
	ErrorCode string         `json:"error_code,omitempty"`
	Message   string         `json:"message"`
	Details   map[string]any `json:"details,omitempty"`
}

type OperatorRuntimeIncidentListOptions struct {
	SinceHours int
	BundleHash string
	Component  string
	Level      string
	MCPOnly    bool
	Limit      int
	Cursor     string
}

type OperatorRuntimeIncidentListResult struct {
	Incidents  []OperatorRuntimeIncident `json:"incidents"`
	NextCursor string                    `json:"next_cursor,omitempty"`
}

type OperatorRuntimeIncident struct {
	IncidentID    string    `json:"incident_id"`
	FirstSeen     time.Time `json:"first_seen"`
	LastSeen      time.Time `json:"last_seen"`
	Count         int       `json:"count"`
	Level         string    `json:"level"`
	Component     string    `json:"component"`
	ErrorCode     string    `json:"error_code,omitempty"`
	SampleMessage string    `json:"sample_message"`
	SampleLogIDs  []string  `json:"sample_log_ids"`

	Agents     []string `json:"-"`
	Actions    []string `json:"-"`
	Components []string `json:"-"`
}

type observabilityPositionCursor struct {
	Kind      string `json:"kind"`
	CreatedAt string `json:"created_at,omitempty"`
	ID        string `json:"id,omitempty"`
	Order     string `json:"order,omitempty"`
	LastSeen  string `json:"last_seen,omitempty"`
}

func (s *PostgresStore) requireOperatorObservabilityCapabilities(ctx context.Context) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	switch {
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case !caps.Events.LogRunID:
		return fmt.Errorf("operator observability read surface requires canonical events.run_id")
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case !caps.Events.DeliveryRunID:
		return fmt.Errorf("operator observability read surface requires canonical event_deliveries.run_id")
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	required := map[string][]string{
		"events": {
			"event_id", "run_id", "event_name", "entity_id", "scope", "payload",
			"produced_by", "produced_by_type", "source_event_id", "created_at",
		},
		"runs": {
			"run_id", "bundle_hash",
		},
		"event_deliveries": {
			"delivery_id", "run_id", "event_id", "subscriber_type", "subscriber_id",
			"status", "retry_count", "reason_code", "last_error", "active_session_id", "created_at", "started_at", "delivered_at",
		},
		"dead_letters": {
			"dead_letter_id", "original_event_id", "failure_type", "error_message",
			"retry_count", "chain_depth", "handler_node", "created_at",
		},
	}
	for tableName, columns := range required {
		if catalog.hasColumns(tableName, columns...) {
			continue
		}
		return fmt.Errorf("operator observability read surface requires %s columns %v", tableName, columns)
	}
	return nil
}

func (s *PostgresStore) ListOperatorEvents(ctx context.Context, opts OperatorEventListOptions) (OperatorEventListResult, error) {
	if err := s.requireOperatorObservabilityCapabilities(ctx); err != nil {
		return OperatorEventListResult{}, err
	}
	opts = defaultOperatorEventListOptions(opts)
	args := make([]any, 0, 16)
	where := []string{"TRUE"}
	add := func(value any) int {
		args = append(args, value)
		return len(args)
	}
	if opts.Filter.RunID != "" {
		n := add(opts.Filter.RunID)
		where = append(where, fmt.Sprintf("e.run_id::text = $%d", n))
	}
	if opts.Filter.EntityID != "" {
		n := add(opts.Filter.EntityID)
		where = append(where, fmt.Sprintf("e.entity_id::text = $%d", n))
	}
	if opts.Filter.EventName != "" {
		n := add(opts.Filter.EventName)
		where = append(where, fmt.Sprintf("e.event_name = $%d", n))
	}
	if opts.Source != "" {
		n := add(opts.Source)
		where = append(where, fmt.Sprintf("COALESCE(e.produced_by, '') = $%d", n))
	}
	deliveryWhere := make([]string, 0, 3)
	if opts.Filter.DeliveryStatus != "" {
		n := add(opts.Filter.DeliveryStatus)
		deliveryWhere = append(deliveryWhere, fmt.Sprintf("d.status = $%d", n))
	}
	if opts.Filter.SubscriberID != "" {
		n := add(opts.Filter.SubscriberID)
		deliveryWhere = append(deliveryWhere, fmt.Sprintf("d.subscriber_id = $%d", n))
	}
	if opts.Filter.SubscriberType != "" {
		n := add(opts.Filter.SubscriberType)
		deliveryWhere = append(deliveryWhere, fmt.Sprintf("d.subscriber_type = $%d", n))
	}
	if len(deliveryWhere) > 0 {
		where = append(where, fmt.Sprintf(
			"EXISTS (SELECT 1 FROM event_deliveries d WHERE d.event_id = e.event_id AND %s)",
			strings.Join(deliveryWhere, " AND "),
		))
	}
	if opts.Filter.ReasonCode != "" {
		n := add(opts.Filter.ReasonCode)
		where = append(where, fmt.Sprintf(`(
			EXISTS (SELECT 1 FROM event_deliveries d WHERE d.event_id = e.event_id AND d.reason_code = $%d)
			OR EXISTS (SELECT 1 FROM dead_letters dl WHERE dl.original_event_id = e.event_id AND dl.failure_type = $%d)
		)`, n, n))
	}
	if opts.Filter.HasDeadLetter != nil {
		exists := "EXISTS"
		if !*opts.Filter.HasDeadLetter {
			exists = "NOT EXISTS"
		}
		where = append(where, fmt.Sprintf("%s (SELECT 1 FROM dead_letters dl WHERE dl.original_event_id = e.event_id)", exists))
	}
	if opts.ExcludeRuntimeLogs {
		where = append(where, "e.event_name <> 'platform.runtime_log'")
	}
	if opts.Since != nil {
		n := add(opts.Since.UTC())
		where = append(where, fmt.Sprintf("e.created_at > $%d", n))
	}
	if opts.Until != nil {
		n := add(opts.Until.UTC())
		where = append(where, fmt.Sprintf("e.created_at <= $%d", n))
	}
	if opts.Cursor != "" {
		cursor, err := decodeObservabilityPositionCursor(opts.Cursor, "event.list")
		if err != nil {
			return OperatorEventListResult{}, err
		}
		if cursor.Order != "" && cursor.Order != opts.Order {
			return OperatorEventListResult{}, ErrInvalidObservabilityCursor
		}
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil || strings.TrimSpace(cursor.ID) == "" {
			return OperatorEventListResult{}, ErrInvalidObservabilityCursor
		}
		nTime := add(createdAt.UTC())
		nID := add(cursor.ID)
		if opts.Order == "asc" {
			where = append(where, fmt.Sprintf("(e.created_at > $%d OR (e.created_at = $%d AND e.event_id::text > $%d))", nTime, nTime, nID))
		} else {
			where = append(where, fmt.Sprintf("(e.created_at < $%d OR (e.created_at = $%d AND e.event_id::text < $%d))", nTime, nTime, nID))
		}
	}
	limitArg := add(opts.Limit + 1)
	orderSQL := "DESC"
	if opts.Order == "asc" {
		orderSQL = "ASC"
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT e.event_id::text
		FROM events e
		WHERE `+strings.Join(where, " AND ")+fmt.Sprintf(`
		ORDER BY e.created_at %s, e.event_id::text %s
		LIMIT $%d
	`, orderSQL, orderSQL, limitArg), args...)
	if err != nil {
		return OperatorEventListResult{}, fmt.Errorf("list operator events: %w", err)
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return OperatorEventListResult{}, fmt.Errorf("scan operator event id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return OperatorEventListResult{}, fmt.Errorf("read operator event ids: %w", err)
	}
	events := make([]OperatorEventFull, 0, minInt(len(ids), opts.Limit))
	for _, id := range ids {
		event, err := s.LoadOperatorEvent(ctx, id)
		if err != nil {
			return OperatorEventListResult{}, err
		}
		events = append(events, event)
	}
	nextCursor := ""
	if len(events) > opts.Limit {
		events = events[:opts.Limit]
		last := events[len(events)-1]
		nextCursor = encodeObservabilityPositionCursor(observabilityPositionCursor{
			Kind:      "event.list",
			CreatedAt: last.CreatedAt.UTC().Format(time.RFC3339Nano),
			ID:        last.EventID,
			Order:     opts.Order,
		})
	}
	if events == nil {
		events = []OperatorEventFull{}
	}
	return OperatorEventListResult{Events: events, NextCursor: nextCursor}, nil
}

func (s *PostgresStore) LoadOperatorEvent(ctx context.Context, eventID string) (OperatorEventFull, error) {
	if err := s.requireOperatorObservabilityCapabilities(ctx); err != nil {
		return OperatorEventFull{}, err
	}
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return OperatorEventFull{}, ErrEventNotFound
	}
	row := s.DB.QueryRowContext(ctx, `
		SELECT
			e.event_id::text,
			COALESCE(e.event_name, ''),
			COALESCE(e.entity_id::text, ''),
			COALESCE(e.run_id::text, ''),
			COALESCE(e.source_event_id::text, ''),
			e.created_at,
			COALESCE(e.produced_by, ''),
			COALESCE(e.produced_by_type, ''),
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		WHERE e.event_id::text = $1
	`, eventID)
	var (
		event          OperatorEventFull
		producedBy     string
		producedByType string
		payloadRaw     []byte
	)
	if err := row.Scan(&event.EventID, &event.EventName, &event.EntityID, &event.RunID, &event.SourceEventID, &event.CreatedAt, &producedBy, &producedByType, &payloadRaw); err == sql.ErrNoRows {
		return OperatorEventFull{}, ErrEventNotFound
	} else if err != nil {
		return OperatorEventFull{}, fmt.Errorf("load operator event: %w", err)
	}
	event.Source = firstNonEmptyStore(producedBy, producedByType, "unknown")
	payload, err := decodeStoreJSONMap(payloadRaw)
	if err != nil {
		return OperatorEventFull{}, fmt.Errorf("decode operator event payload: %w", err)
	}
	event.Payload = payload
	deadLetters, err := s.loadOperatorEventDeadLetters(ctx, event.EventID)
	if err != nil {
		return OperatorEventFull{}, err
	}
	deliveries, err := s.loadOperatorEventDeliveries(ctx, event.EventID)
	if err != nil {
		return OperatorEventFull{}, err
	}
	event.Deliveries = EnrichOperatorDeliveryFailureEvidence(deliveries, deadLetters)
	event.DeadLetters = deadLetters
	if event.Deliveries == nil {
		event.Deliveries = []OperatorEventDelivery{}
	}
	if event.DeadLetters == nil {
		event.DeadLetters = []OperatorDeadLetterRecord{}
	}
	return event, nil
}

func (s *PostgresStore) loadOperatorEventDeliveries(ctx context.Context, eventID string) ([]OperatorEventDelivery, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			d.delivery_id::text,
			COALESCE(d.subscriber_type, ''),
			COALESCE(d.subscriber_id, ''),
			COALESCE(d.active_session_id::text, ''),
				COALESCE(d.status, ''),
				COALESCE(d.reason_code, ''),
				COALESCE(d.last_error, ''),
				COALESCE(d.retry_count, 0),
				d.created_at,
				d.started_at,
				d.delivered_at
			FROM event_deliveries d
		WHERE d.event_id::text = $1
		ORDER BY d.created_at ASC, d.delivery_id::text ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("load operator event deliveries: %w", err)
	}
	defer rows.Close()
	out := []OperatorEventDelivery{}
	for rows.Next() {
		var item OperatorEventDelivery
		var createdAt, startedAt, finishedAt sql.NullTime
		if err := rows.Scan(&item.DeliveryID, &item.SubscriberType, &item.SubscriberID, &item.SessionID, &item.Status, &item.ReasonCode, &item.LastError, &item.RetryCount, &createdAt, &startedAt, &finishedAt); err != nil {
			return nil, fmt.Errorf("scan operator event delivery: %w", err)
		}
		item.CreatedAt = nullTimePtr(createdAt)
		item.StartedAt = nullTimePtr(startedAt)
		item.FinishedAt = nullTimePtr(finishedAt)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read operator event deliveries: %w", err)
	}
	return out, nil
}

func EnrichOperatorEventFailureEvidence(event OperatorEventFull) OperatorEventFull {
	event.Deliveries = EnrichOperatorDeliveryFailureEvidence(event.Deliveries, event.DeadLetters)
	if event.Deliveries == nil {
		event.Deliveries = []OperatorEventDelivery{}
	}
	if event.DeadLetters == nil {
		event.DeadLetters = []OperatorDeadLetterRecord{}
	}
	return event
}

func EnrichOperatorDeliveryFailureEvidence(deliveries []OperatorEventDelivery, deadLetters []OperatorDeadLetterRecord) []OperatorEventDelivery {
	out := make([]OperatorEventDelivery, 0, len(deliveries))
	for _, delivery := range deliveries {
		delivery.Status = strings.TrimSpace(delivery.Status)
		delivery.ReasonCode = strings.TrimSpace(delivery.ReasonCode)
		delivery.LastError = strings.TrimSpace(delivery.LastError)
		delivery.RetryEligible = OperatorDeliveryRetryEligible(delivery.Status)
		delivery.Terminal = OperatorDeliveryTerminal(delivery.Status)
		if delivery.Status == "dead_letter" && len(deadLetters) > 0 {
			delivery.DeadLetters = append([]OperatorDeadLetterRecord(nil), deadLetters...)
		}
		out = append(out, delivery)
	}
	if out == nil {
		return []OperatorEventDelivery{}
	}
	return out
}

func OperatorDeliveryRetryEligible(status string) bool {
	return strings.TrimSpace(status) == "failed"
}

func OperatorDeliveryTerminal(status string) bool {
	switch strings.TrimSpace(status) {
	case "delivered", "dead_letter":
		return true
	default:
		return false
	}
}

func nullTimePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	at := value.Time.UTC()
	return &at
}

func (s *PostgresStore) loadOperatorEventDeadLetters(ctx context.Context, eventID string) ([]OperatorDeadLetterRecord, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			dl.dead_letter_id::text,
			COALESCE(dl.failure_type, ''),
			COALESCE(dl.error_message, ''),
			COALESCE(dl.retry_count, 0),
			COALESCE(dl.chain_depth, 0),
			COALESCE(dl.handler_node, ''),
			dl.created_at
		FROM dead_letters dl
		WHERE dl.original_event_id::text = $1
		ORDER BY dl.created_at ASC, dl.dead_letter_id::text ASC
	`, eventID)
	if err != nil {
		return nil, fmt.Errorf("load operator event dead letters: %w", err)
	}
	defer rows.Close()
	out := []OperatorDeadLetterRecord{}
	for rows.Next() {
		var item OperatorDeadLetterRecord
		if err := rows.Scan(&item.DeadLetterID, &item.FailureType, &item.ErrorMessage, &item.RetryCount, &item.ChainDepth, &item.HandlerNode, &item.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan operator event dead letter: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read operator event dead letters: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListOperatorRuntimeLogs(ctx context.Context, opts OperatorRuntimeLogListOptions) (OperatorRuntimeLogListResult, error) {
	if err := s.requireOperatorObservabilityCapabilities(ctx); err != nil {
		return OperatorRuntimeLogListResult{}, err
	}
	opts = defaultOperatorRuntimeLogListOptions(opts)
	cursorClause := ""
	args := []any{opts.RunID, opts.EntityID, opts.Component, opts.Level, opts.ErrorCode, opts.Source, opts.ActionOrEventType, opts.SessionID, opts.BundleHash}
	if opts.Since != nil {
		args = append(args, opts.Since.UTC())
		cursorClause += fmt.Sprintf(" AND e.created_at > $%d", len(args))
	}
	if opts.Until != nil {
		args = append(args, opts.Until.UTC())
		cursorClause += fmt.Sprintf(" AND e.created_at <= $%d", len(args))
	}
	if opts.Cursor != "" {
		cursor, err := decodeObservabilityPositionCursor(opts.Cursor, "runtime.logs")
		if err != nil {
			return OperatorRuntimeLogListResult{}, err
		}
		if cursor.Order != opts.Order {
			return OperatorRuntimeLogListResult{}, ErrInvalidObservabilityCursor
		}
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil || strings.TrimSpace(cursor.ID) == "" {
			return OperatorRuntimeLogListResult{}, ErrInvalidObservabilityCursor
		}
		args = append(args, createdAt.UTC(), cursor.ID)
		if opts.Order == "asc" {
			cursorClause += fmt.Sprintf(" AND (e.created_at > $%d OR (e.created_at = $%d AND e.event_id::text > $%d))", len(args)-1, len(args)-1, len(args))
		} else {
			cursorClause += fmt.Sprintf(" AND (e.created_at < $%d OR (e.created_at = $%d AND e.event_id::text < $%d))", len(args)-1, len(args)-1, len(args))
		}
	}
	orderSQL := "DESC"
	compareOrder := "DESC"
	if opts.Order == "asc" {
		orderSQL = "ASC"
		compareOrder = "ASC"
	}
	args = append(args, opts.Limit+1)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			e.event_id::text,
			COALESCE(e.run_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			e.created_at,
			COALESCE(e.produced_by, ''),
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		WHERE e.event_name = 'platform.runtime_log'
		  AND ($1 = '' OR e.run_id::text = $1)
		  AND ($2 = '' OR COALESCE(e.entity_id::text, e.payload->'details'->>'entity_id', '') = $2)
		  AND ($3 = '' OR COALESCE(e.payload->'details'->>'component', '') = $3)
		  AND ($4 = '' OR COALESCE(e.payload->>'log_level', '') = $4)
		  AND ($5 = '' OR COALESCE(e.payload->'details'->>'error_code', '') = $5)
		  AND ($6 = '' OR COALESCE(NULLIF(BTRIM(e.payload->'details'->>'agent_id'), ''), NULLIF(BTRIM(e.produced_by), ''), 'runtime') = $6)
		  AND ($7 = '' OR COALESCE(e.payload->'details'->>'action', '') = $7 OR COALESCE(e.payload->'details'->>'event_name', e.payload->'details'->>'event_type', '') = $7)
		  AND ($8 = '' OR COALESCE(e.payload->'details'->>'session_id', '') = $8)
		  AND ($9 = '' OR EXISTS (
		  	SELECT 1
		  	FROM runs r
		  	WHERE r.run_id = e.run_id
		  	  AND r.bundle_hash = $9
		  ))
		  %s
		ORDER BY e.created_at %s, e.event_id::text %s
		LIMIT $%d
	`, cursorClause, orderSQL, compareOrder, len(args)), args...)
	if err != nil {
		return OperatorRuntimeLogListResult{}, fmt.Errorf("list operator runtime logs: %w", err)
	}
	defer rows.Close()
	logs := []OperatorRuntimeLogEntry{}
	for rows.Next() {
		var (
			eventID    string
			runID      string
			entityID   string
			createdAt  time.Time
			producedBy string
			payloadRaw []byte
		)
		if err := rows.Scan(&eventID, &runID, &entityID, &createdAt, &producedBy, &payloadRaw); err != nil {
			return OperatorRuntimeLogListResult{}, fmt.Errorf("scan operator runtime log: %w", err)
		}
		entry, err := operatorRuntimeLogEntry(eventID, runID, entityID, producedBy, createdAt, payloadRaw)
		if err != nil {
			return OperatorRuntimeLogListResult{}, err
		}
		logs = append(logs, entry)
	}
	if err := rows.Err(); err != nil {
		return OperatorRuntimeLogListResult{}, fmt.Errorf("read operator runtime logs: %w", err)
	}
	nextCursor := ""
	if len(logs) > opts.Limit {
		logs = logs[:opts.Limit]
		last := logs[len(logs)-1]
		nextCursor = encodeObservabilityPositionCursor(observabilityPositionCursor{
			Kind:      "runtime.logs",
			CreatedAt: last.TS.UTC().Format(time.RFC3339Nano),
			ID:        last.LogID,
			Order:     opts.Order,
		})
	}
	if logs == nil {
		logs = []OperatorRuntimeLogEntry{}
	}
	return OperatorRuntimeLogListResult{Logs: logs, NextCursor: nextCursor}, nil
}

func (s *PostgresStore) ListOperatorRuntimeIncidents(ctx context.Context, opts OperatorRuntimeIncidentListOptions) (OperatorRuntimeIncidentListResult, error) {
	if err := s.requireOperatorObservabilityCapabilities(ctx); err != nil {
		return OperatorRuntimeIncidentListResult{}, err
	}
	opts = defaultOperatorRuntimeIncidentListOptions(opts)
	cutoff := time.Now().UTC().Add(-time.Duration(opts.SinceHours) * time.Hour)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id::text,
			COALESCE(e.run_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			e.created_at,
			COALESCE(e.produced_by, ''),
			COALESCE(e.payload, '{}'::jsonb)
		FROM events e
		WHERE e.event_name = 'platform.runtime_log'
		  AND e.created_at >= $1
		  AND ($2 = '' OR COALESCE(e.payload->'details'->>'component', '') = $2)
		  AND ($3 = '' OR COALESCE(e.payload->>'log_level', '') = $3)
		  AND ($4 = '' OR EXISTS (
		  	SELECT 1
		  	FROM runs r
		  	WHERE r.run_id = e.run_id
		  	  AND r.bundle_hash = $4
		  ))
		ORDER BY e.created_at DESC, e.event_id::text DESC
	`, cutoff, opts.Component, opts.Level, opts.BundleHash)
	if err != nil {
		return OperatorRuntimeIncidentListResult{}, fmt.Errorf("list operator runtime incident logs: %w", err)
	}
	defer rows.Close()
	type aggregate struct {
		item       OperatorRuntimeIncident
		agents     map[string]struct{}
		actions    map[string]struct{}
		components map[string]struct{}
	}
	aggregates := map[string]*aggregate{}
	for rows.Next() {
		var (
			eventID    string
			runID      string
			entityID   string
			createdAt  time.Time
			producedBy string
			payloadRaw []byte
		)
		if err := rows.Scan(&eventID, &runID, &entityID, &createdAt, &producedBy, &payloadRaw); err != nil {
			return OperatorRuntimeIncidentListResult{}, fmt.Errorf("scan operator runtime incident log: %w", err)
		}
		logEntry, err := operatorRuntimeLogEntry(eventID, runID, entityID, producedBy, createdAt, payloadRaw)
		if err != nil {
			return OperatorRuntimeIncidentListResult{}, err
		}
		if opts.MCPOnly && !strings.HasPrefix(strings.TrimSpace(logEntry.Component), "mcp") {
			continue
		}
		if strings.TrimSpace(logEntry.ErrorCode) == "" {
			continue
		}
		key := strings.Join([]string{logEntry.ErrorCode, logEntry.Component, logEntry.Level}, "\x00")
		agg := aggregates[key]
		if agg == nil {
			agg = &aggregate{
				item: OperatorRuntimeIncident{
					IncidentID:    operatorIncidentID(key),
					FirstSeen:     logEntry.TS,
					LastSeen:      logEntry.TS,
					Level:         logEntry.Level,
					Component:     logEntry.Component,
					ErrorCode:     logEntry.ErrorCode,
					SampleMessage: firstNonEmptyStore(readStoreString(logEntry.Details["error"]), logEntry.Message),
					SampleLogIDs:  []string{},
				},
				agents:     map[string]struct{}{},
				actions:    map[string]struct{}{},
				components: map[string]struct{}{},
			}
			aggregates[key] = agg
		}
		agg.item.Count++
		if logEntry.TS.Before(agg.item.FirstSeen) {
			agg.item.FirstSeen = logEntry.TS
		}
		if logEntry.TS.After(agg.item.LastSeen) {
			agg.item.LastSeen = logEntry.TS
		}
		if len(agg.item.SampleLogIDs) < 5 {
			agg.item.SampleLogIDs = append(agg.item.SampleLogIDs, logEntry.LogID)
		}
		if agg.item.SampleMessage == "" {
			agg.item.SampleMessage = firstNonEmptyStore(readStoreString(logEntry.Details["error"]), logEntry.Message)
		}
		if agentID := readStoreString(logEntry.Details["agent_id"]); agentID != "" {
			agg.agents[agentID] = struct{}{}
		}
		if action := readStoreString(logEntry.Details["action"]); action != "" {
			agg.actions[action] = struct{}{}
		}
		if logEntry.Component != "" {
			agg.components[logEntry.Component] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return OperatorRuntimeIncidentListResult{}, fmt.Errorf("read operator runtime incident logs: %w", err)
	}
	out := make([]OperatorRuntimeIncident, 0, len(aggregates))
	for _, agg := range aggregates {
		agg.item.Agents = sortedStoreStringSet(agg.agents)
		agg.item.Actions = sortedStoreStringSet(agg.actions)
		agg.item.Components = sortedStoreStringSet(agg.components)
		out = append(out, agg.item)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].IncidentID < out[j].IncidentID
	})
	if opts.Cursor != "" {
		cursor, err := decodeObservabilityPositionCursor(opts.Cursor, "runtime.incidents")
		if err != nil {
			return OperatorRuntimeIncidentListResult{}, err
		}
		lastSeen, err := time.Parse(time.RFC3339Nano, cursor.LastSeen)
		if err != nil || cursor.ID == "" {
			return OperatorRuntimeIncidentListResult{}, ErrInvalidObservabilityCursor
		}
		filtered := out[:0]
		for _, item := range out {
			if item.LastSeen.Before(lastSeen) || (item.LastSeen.Equal(lastSeen) && item.IncidentID > cursor.ID) {
				filtered = append(filtered, item)
			}
		}
		out = filtered
	}
	nextCursor := ""
	if len(out) > opts.Limit {
		out = out[:opts.Limit]
		last := out[len(out)-1]
		nextCursor = encodeObservabilityPositionCursor(observabilityPositionCursor{
			Kind:     "runtime.incidents",
			LastSeen: last.LastSeen.UTC().Format(time.RFC3339Nano),
			ID:       last.IncidentID,
		})
	}
	if out == nil {
		out = []OperatorRuntimeIncident{}
	}
	return OperatorRuntimeIncidentListResult{Incidents: out, NextCursor: nextCursor}, nil
}

func operatorRuntimeLogEntry(eventID, runID, rowEntityID, producedBy string, createdAt time.Time, payloadRaw []byte) (OperatorRuntimeLogEntry, error) {
	payload, err := runtimepkg.DecodeCanonicalRuntimeLogPayload(payloadRaw)
	if err != nil {
		return OperatorRuntimeLogEntry{}, fmt.Errorf("decode canonical runtime log payload: %w", err)
	}
	details := map[string]any{}
	for key, value := range payload.Detail {
		details[key] = value
	}
	return OperatorRuntimeLogEntry{
		LogID:     strings.TrimSpace(eventID),
		TS:        createdAt.UTC(),
		Level:     strings.TrimSpace(payload.LogLevel),
		Component: strings.TrimSpace(payload.Component),
		Source:    firstNonEmptyStore(payload.AgentID, producedBy, "runtime"),
		RunID:     strings.TrimSpace(runID),
		EntityID:  firstNonEmptyStore(payload.EntityID, rowEntityID),
		SessionID: strings.TrimSpace(payload.SessionID),
		ErrorCode: strings.TrimSpace(payload.ErrorCode),
		Message:   strings.TrimSpace(payload.Message),
		Details:   details,
	}, nil
}

func defaultOperatorEventListOptions(opts OperatorEventListOptions) OperatorEventListOptions {
	opts.Filter.RunID = strings.TrimSpace(opts.Filter.RunID)
	opts.Filter.EntityID = strings.TrimSpace(opts.Filter.EntityID)
	opts.Filter.EventName = strings.TrimSpace(opts.Filter.EventName)
	opts.Filter.DeliveryStatus = strings.TrimSpace(opts.Filter.DeliveryStatus)
	opts.Filter.SubscriberID = strings.TrimSpace(opts.Filter.SubscriberID)
	opts.Filter.SubscriberType = strings.TrimSpace(opts.Filter.SubscriberType)
	opts.Filter.ReasonCode = strings.TrimSpace(opts.Filter.ReasonCode)
	opts.Source = strings.TrimSpace(opts.Source)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	opts.Order = strings.ToLower(strings.TrimSpace(opts.Order))
	if opts.Order == "" {
		opts.Order = "desc"
	}
	if opts.Order != "asc" && opts.Order != "desc" {
		opts.Order = "desc"
	}
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	if opts.Limit > 1000 {
		opts.Limit = 1000
	}
	return opts
}

func defaultOperatorRuntimeLogListOptions(opts OperatorRuntimeLogListOptions) OperatorRuntimeLogListOptions {
	opts.RunID = strings.TrimSpace(opts.RunID)
	opts.BundleHash = strings.TrimSpace(opts.BundleHash)
	opts.EntityID = strings.TrimSpace(opts.EntityID)
	opts.SessionID = strings.TrimSpace(opts.SessionID)
	opts.Component = strings.TrimSpace(opts.Component)
	opts.Level = strings.TrimSpace(opts.Level)
	opts.ErrorCode = strings.TrimSpace(opts.ErrorCode)
	opts.Source = strings.TrimSpace(opts.Source)
	opts.ActionOrEventType = strings.TrimSpace(opts.ActionOrEventType)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	opts.Order = strings.ToLower(strings.TrimSpace(opts.Order))
	if opts.Order == "" {
		opts.Order = "desc"
	}
	if opts.Order != "asc" {
		opts.Order = "desc"
	}
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	if opts.Limit > 1000 {
		opts.Limit = 1000
	}
	return opts
}

func defaultOperatorRuntimeIncidentListOptions(opts OperatorRuntimeIncidentListOptions) OperatorRuntimeIncidentListOptions {
	opts.BundleHash = strings.TrimSpace(opts.BundleHash)
	opts.Component = strings.TrimSpace(opts.Component)
	opts.Level = strings.TrimSpace(opts.Level)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.SinceHours <= 0 {
		opts.SinceHours = 24
	}
	if opts.SinceHours > 720 {
		opts.SinceHours = 720
	}
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	return opts
}

func encodeObservabilityPositionCursor(cursor observabilityPositionCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeObservabilityPositionCursor(raw string, kind string) (observabilityPositionCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return observabilityPositionCursor{}, ErrInvalidObservabilityCursor
	}
	var cursor observabilityPositionCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return observabilityPositionCursor{}, ErrInvalidObservabilityCursor
	}
	if strings.TrimSpace(cursor.Kind) != kind {
		return observabilityPositionCursor{}, ErrInvalidObservabilityCursor
	}
	return cursor, nil
}

func operatorIncidentID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return "inc_" + hex.EncodeToString(sum[:8])
}

func decodeStoreJSONMap(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out == nil {
		out = map[string]any{}
	}
	return out, nil
}

func readStoreString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func firstNonEmptyStore(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func sortedStoreStringSet(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		if strings.TrimSpace(value) != "" {
			out = append(out, strings.TrimSpace(value))
		}
	}
	sort.Strings(out)
	return out
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
