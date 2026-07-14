package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type RunDebugQueryOptions struct {
	LogsAllLevels   bool
	Component       string
	EventLimit      int
	MutationLimit   int
	RuntimeLogLimit int
	DeadLetterLimit int
}

type RunDebugRunSummary struct {
	RunID          string     `json:"run_id"`
	RunTableStatus string     `json:"run_table_status,omitempty"`
	RootEventID    string     `json:"root_event_id,omitempty"`
	RootEventType  string     `json:"root_event_type,omitempty"`
	StartedAt      time.Time  `json:"started_at,omitempty"`
	LastEventAt    time.Time  `json:"last_event_at,omitempty"`
	EndedAt        *time.Time `json:"ended_at,omitempty"`
	EventCount     int        `json:"event_count"`
	EntityCount    int        `json:"entity_count"`
}

type RunDebugEventCount struct {
	EventName string `json:"event_name"`
	Count     int    `json:"count"`
}

type RunDebugDeliveryCount struct {
	SubscriberID string `json:"subscriber_id"`
	Status       string `json:"status"`
	Count        int    `json:"count"`
}

type RunDebugEvent struct {
	EventID    string          `json:"event_id,omitempty"`
	EventName  string          `json:"event_name"`
	EntityID   string          `json:"entity_id,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	Source     string          `json:"source,omitempty"`
	SourceType string          `json:"source_type,omitempty"`
	Payload    json.RawMessage `json:"payload,omitempty"`
}

type RunDebugMutation struct {
	MutationID    string          `json:"mutation_id,omitempty"`
	EntityID      string          `json:"entity_id,omitempty"`
	Field         string          `json:"field"`
	OldValue      json.RawMessage `json:"old_value,omitempty"`
	NewValue      json.RawMessage `json:"new_value,omitempty"`
	WriterType    string          `json:"writer_type,omitempty"`
	WriterID      string          `json:"writer_id,omitempty"`
	HandlerStep   string          `json:"handler_step,omitempty"`
	CausedByEvent string          `json:"caused_by_event,omitempty"`
	CreatedAt     time.Time       `json:"created_at"`
}

type RunDebugDeadLetter struct {
	OriginalEvent string                   `json:"original_event"`
	EntityID      string                   `json:"entity_id,omitempty"`
	Failure       runtimefailures.Envelope `json:"failure"`
	HandlerNode   string                   `json:"handler_node,omitempty"`
	CreatedAt     time.Time                `json:"created_at"`
}

type RunDebugFailureDelivery struct {
	EventID        string                     `json:"event_id"`
	EventName      string                     `json:"event_name"`
	EntityID       string                     `json:"entity_id,omitempty"`
	DeliveryID     string                     `json:"delivery_id"`
	SubscriberType string                     `json:"subscriber_type"`
	SubscriberID   string                     `json:"subscriber_id"`
	SessionID      string                     `json:"session_id,omitempty"`
	Status         string                     `json:"status"`
	ReasonCode     string                     `json:"reason_code,omitempty"`
	Failure        *runtimefailures.Envelope  `json:"failure,omitempty"`
	RetryCount     int                        `json:"retry_count"`
	RetryEligible  bool                       `json:"retry_eligible"`
	Terminal       bool                       `json:"terminal"`
	CreatedAt      *time.Time                 `json:"created_at,omitempty"`
	StartedAt      *time.Time                 `json:"started_at,omitempty"`
	FinishedAt     *time.Time                 `json:"finished_at,omitempty"`
	DeadLetters    []OperatorDeadLetterRecord `json:"dead_letters,omitempty"`
}

type RunDebugAgentTurn struct {
	AgentID    string    `json:"agent_id"`
	Turns      int       `json:"turns"`
	ErrorCount int       `json:"error_count"`
	LastAt     time.Time `json:"last_at"`
}

type RunDebugRuntimeLog struct {
	EventID   string                    `json:"event_id,omitempty"`
	Level     string                    `json:"level"`
	Message   string                    `json:"message,omitempty"`
	Component string                    `json:"component"`
	Action    string                    `json:"action"`
	EventType string                    `json:"event_type,omitempty"`
	AgentID   string                    `json:"agent_id,omitempty"`
	EntityID  string                    `json:"entity_id,omitempty"`
	Failure   *runtimefailures.Envelope `json:"failure,omitempty"`
	Detail    json.RawMessage           `json:"detail,omitempty"`
	CreatedAt time.Time                 `json:"created_at"`
}

type RunDebugRuntimeSummary struct {
	Level     string `json:"level"`
	Component string `json:"component"`
	Action    string `json:"action"`
	Count     int    `json:"count"`
}

type RunTestQuiescence struct {
	Ready                   bool `json:"ready"`
	ActiveDeliveries        int  `json:"active_deliveries"`
	UnsettledPipelineEvents int  `json:"unsettled_pipeline_events"`
	DueTimers               int  `json:"due_timers"`
	ActiveSessionLeases     int  `json:"active_session_leases"`
}

type RunDebugReport struct {
	RunID             string                    `json:"run_id"`
	RunTableStatus    string                    `json:"run_table_status,omitempty"`
	RootEventID       string                    `json:"root_event_id,omitempty"`
	RootEventType     string                    `json:"root_event_type,omitempty"`
	Failure           *runtimefailures.Envelope `json:"failure,omitempty"`
	ControlReason     string                    `json:"control_reason,omitempty"`
	StartedAt         time.Time                 `json:"started_at,omitempty"`
	LastEventAt       time.Time                 `json:"last_event_at,omitempty"`
	EndedAt           *time.Time                `json:"ended_at,omitempty"`
	EventCount        int                       `json:"event_count"`
	EntityCount       int                       `json:"entity_count"`
	WarnErrorLogCount int                       `json:"warn_error_log_count"`
	EventCounts       []RunDebugEventCount      `json:"event_counts,omitempty"`
	Deliveries        []RunDebugDeliveryCount   `json:"deliveries,omitempty"`
	Events            []RunDebugEvent           `json:"events,omitempty"`
	FailedDeliveries  []RunDebugFailureDelivery `json:"failed_deliveries,omitempty"`
	DeadLetters       []RunDebugDeadLetter      `json:"dead_letters,omitempty"`
	AgentTurns        []RunDebugAgentTurn       `json:"agent_turns,omitempty"`
	Mutations         []RunDebugMutation        `json:"mutations,omitempty"`
	RuntimeLogSummary []RunDebugRuntimeSummary  `json:"runtime_log_summary,omitempty"`
	RuntimeLogs       []RunDebugRuntimeLog      `json:"runtime_logs,omitempty"`
	TestQuiescence    RunTestQuiescence         `json:"test_quiescence"`
}

type RunDebugTraceQueryOptions struct {
	Limit              int
	Cursor             string
	Since              *time.Time
	Until              *time.Time
	Filter             RunDebugTraceFilter
	ExcludeRuntimeLogs bool
}

type RunDebugTraceFilter struct {
	EventNames       []string
	EntityIDs        []string
	DeliveryStatuses []string
	SubscriberIDs    []string
	SubscriberTypes  []string
}

type RunDebugTraceRow struct {
	EventID                   string                            `json:"event_id,omitempty"`
	EventName                 string                            `json:"event_name,omitempty"`
	SourceEventID             string                            `json:"source_event_id,omitempty"`
	EntityID                  string                            `json:"entity_id,omitempty"`
	EventSource               string                            `json:"event_source,omitempty"`
	EventSourceType           string                            `json:"event_source_type,omitempty"`
	EventCreatedAt            time.Time                         `json:"event_created_at"`
	DeliveryID                string                            `json:"delivery_id,omitempty"`
	SubscriberType            string                            `json:"subscriber_type,omitempty"`
	SubscriberID              string                            `json:"subscriber_id,omitempty"`
	DeliveryStatus            string                            `json:"delivery_status,omitempty"`
	DeliveryReasonCode        string                            `json:"delivery_reason_code,omitempty"`
	ReplyContextID            string                            `json:"reply_context_id,omitempty"`
	DeliveryPayloadProjection *events.DeliveryPayloadProjection `json:"delivery_payload_projection,omitempty"`
	DeliveryFailure           *runtimefailures.Envelope         `json:"delivery_failure,omitempty"`
	DeliveryRetryCount        int                               `json:"delivery_retry_count,omitempty"`
	DeliveryRetryEligible     bool                              `json:"delivery_retry_eligible,omitempty"`
	DeliveryTerminal          bool                              `json:"delivery_terminal,omitempty"`
	ActiveSessionID           string                            `json:"active_session_id,omitempty"`
	DeliveryCreatedAt         *time.Time                        `json:"delivery_created_at,omitempty"`
	DeliveryStartedAt         *time.Time                        `json:"delivery_started_at,omitempty"`
	DeliveryDeliveredAt       *time.Time                        `json:"delivery_delivered_at,omitempty"`
	SessionID                 string                            `json:"session_id,omitempty"`
	SessionKind               string                            `json:"session_kind,omitempty"`
	SessionRuntimeMode        string                            `json:"session_runtime_mode,omitempty"`
	SessionStatus             string                            `json:"session_status,omitempty"`
	SessionUpdatedAt          *time.Time                        `json:"session_updated_at,omitempty"`
	TurnID                    string                            `json:"turn_id,omitempty"`
	TurnTriggerEventID        string                            `json:"turn_trigger_event_id,omitempty"`
	TurnTriggerEventType      string                            `json:"turn_trigger_event_type,omitempty"`
	TurnRuntimeMode           string                            `json:"turn_runtime_mode,omitempty"`
	TurnScopeKey              string                            `json:"turn_scope_key,omitempty"`
	TurnEntityID              string                            `json:"turn_entity_id,omitempty"`
	TurnTaskID                string                            `json:"turn_task_id,omitempty"`
	TurnParseOK               bool                              `json:"turn_parse_ok,omitempty"`
	TurnRetryCount            int                               `json:"turn_retry_count,omitempty"`
	TurnFailure               *runtimefailures.Envelope         `json:"turn_failure,omitempty"`
	TurnCreatedAt             *time.Time                        `json:"turn_created_at,omitempty"`
}

func defaultRunDebugQueryOptions(opts RunDebugQueryOptions) RunDebugQueryOptions {
	if opts.EventLimit <= 0 {
		opts.EventLimit = 15
	}
	if opts.MutationLimit <= 0 {
		opts.MutationLimit = 20
	}
	if opts.RuntimeLogLimit <= 0 {
		opts.RuntimeLogLimit = 20
	}
	if opts.DeadLetterLimit <= 0 {
		opts.DeadLetterLimit = 10
	}
	opts.Component = strings.TrimSpace(opts.Component)
	return opts
}

func defaultRunDebugTraceQueryOptions(opts RunDebugTraceQueryOptions) RunDebugTraceQueryOptions {
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.Limit <= 0 {
		opts.Limit = 200
	}
	if opts.Limit > 2000 {
		opts.Limit = 2000
	}
	opts.Filter.EventNames = normalizedUniqueStrings(opts.Filter.EventNames)
	opts.Filter.EntityIDs = normalizedUniqueStrings(opts.Filter.EntityIDs)
	opts.Filter.DeliveryStatuses = normalizedUniqueStrings(opts.Filter.DeliveryStatuses)
	opts.Filter.SubscriberIDs = normalizedUniqueStrings(opts.Filter.SubscriberIDs)
	opts.Filter.SubscriberTypes = normalizedUniqueStrings(opts.Filter.SubscriberTypes)
	return opts
}

func normalizedUniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func RequireCanonicalRunDebugCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	switch {
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case !caps.Events.LogRunID:
		return fmt.Errorf("run debug read surface requires canonical events.run_id")
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case !caps.Events.DeliveryRunID:
		return fmt.Errorf("run debug read surface requires canonical event_deliveries.run_id")
	case caps.EntityState != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("entity_state", caps.EntityState)
	case !caps.EntityRunID:
		return fmt.Errorf("run debug read surface requires canonical entity_state.run_id")
	case caps.Conversations.Turns != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	case !caps.Conversations.TurnRunID:
		return fmt.Errorf("run debug read surface requires canonical agent_turns.run_id")
	}
	required := map[string][]string{
		"runs":              {"run_id", "status", "failure", "started_at", "ended_at", "entity_count"},
		"run_control_state": {"run_id", "reason"},
		"entity_state":      {"run_id", "entity_id"},
		"event_deliveries":  {"delivery_id", "run_id", "event_id", "subscriber_type", "subscriber_id", "delivery_payload_projection", "status", "retry_count", "reason_code", "failure", "active_session_id", "created_at", "started_at", "delivered_at"},
		"dead_letters":      {"dead_letter_id", "original_event_id", "original_event", "entity_id", "failure", "retry_count", "chain_depth", "handler_node", "created_at"},
		"entity_mutations":  {"mutation_id", "run_id", "entity_id", "field", "old_value", "new_value", "caused_by_event", "writer_type", "writer_id", "handler_step", "created_at"},
	}
	for tableName, columns := range required {
		if catalog.hasColumns(tableName, columns...) {
			continue
		}
		return fmt.Errorf("run debug read surface requires %s columns %v", tableName, columns)
	}
	return nil
}

func (s *PostgresStore) requireRunDebugCapabilities(ctx context.Context) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return err
	}
	return RequireCanonicalRunDebugCapabilities(caps, catalog)
}

func (s *PostgresStore) ResolveLatestRunDebugRunID(ctx context.Context) (string, error) {
	if s == nil || s.DB == nil {
		return "", fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunDebugCapabilities(ctx); err != nil {
		return "", err
	}
	var runID string
	if err := s.DB.QueryRowContext(ctx, `
		SELECT r.run_id::text
		FROM runs r
		WHERE EXISTS (
			SELECT 1
			FROM events e
			WHERE e.run_id = r.run_id
		)
		ORDER BY r.started_at DESC, r.run_id DESC
		LIMIT 1
	`).Scan(&runID); err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("no current run found")
		}
		return "", fmt.Errorf("resolve latest run: %w", err)
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", fmt.Errorf("no current run found")
	}
	return runID, nil
}

func (s *PostgresStore) ListRunDebugRuns(ctx context.Context, limit int) ([]RunDebugRunSummary, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunDebugCapabilities(ctx); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			r.run_id::text,
			COALESCE(r.status, ''),
			COALESCE(root.event_id::text, ''),
			COALESCE(root.event_name, ''),
			COALESCE(root.created_at, r.started_at, now()),
			summary.last_event_at,
			r.ended_at,
			COALESCE(summary.event_count, 0),
			COALESCE(entity_summary.entity_count, 0)
		FROM runs r
		LEFT JOIN LATERAL (
			SELECT e.event_id, e.event_name, e.created_at
			FROM events e
			WHERE e.run_id = r.run_id
			ORDER BY e.created_at ASC, e.event_id ASC
			LIMIT 1
		) root ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(*)::int AS event_count, MAX(created_at) AS last_event_at
			FROM events
			WHERE run_id = r.run_id
		) summary ON TRUE
		LEFT JOIN LATERAL (
			SELECT COUNT(DISTINCT es.entity_id)::int AS entity_count
			FROM entity_state es
			WHERE es.run_id = r.run_id
		) entity_summary ON TRUE
		WHERE root.event_id IS NOT NULL
		ORDER BY COALESCE(summary.last_event_at, root.created_at, r.started_at) DESC, r.run_id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list run debug runs: %w", err)
	}
	defer rows.Close()
	out := make([]RunDebugRunSummary, 0, limit)
	for rows.Next() {
		var (
			item      RunDebugRunSummary
			lastEvent sql.NullTime
			endedAt   sql.NullTime
		)
		if err := rows.Scan(
			&item.RunID,
			&item.RunTableStatus,
			&item.RootEventID,
			&item.RootEventType,
			&item.StartedAt,
			&lastEvent,
			&endedAt,
			&item.EventCount,
			&item.EntityCount,
		); err != nil {
			return nil, fmt.Errorf("scan run debug summary: %w", err)
		}
		if lastEvent.Valid {
			item.LastEventAt = lastEvent.Time
		}
		if endedAt.Valid {
			tm := endedAt.Time
			item.EndedAt = &tm
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run debug summaries: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) LoadRunDebugReport(ctx context.Context, runID string, opts RunDebugQueryOptions) (RunDebugReport, error) {
	if s == nil || s.DB == nil {
		return RunDebugReport{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunDebugCapabilities(ctx); err != nil {
		return RunDebugReport{}, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return RunDebugReport{}, fmt.Errorf("run_id is required")
	}
	opts = defaultRunDebugQueryOptions(opts)
	report := RunDebugReport{RunID: runID}

	var (
		runStatus     string
		failureRaw    []byte
		controlReason string
		started       sql.NullTime
		ended         sql.NullTime
		entityCount   sql.NullInt64
	)
	if err := s.DB.QueryRowContext(ctx, `
		SELECT
			COALESCE(r.status, ''),
			r.failure,
			COALESCE(rc.reason, ''),
			r.started_at,
			r.ended_at,
			COALESCE(entity_summary.entity_count, 0)
		FROM runs r
		LEFT JOIN run_control_state rc ON rc.run_id = r.run_id
		LEFT JOIN LATERAL (
			SELECT COUNT(DISTINCT es.entity_id)::int AS entity_count
			FROM entity_state es
			WHERE es.run_id = r.run_id
		) entity_summary ON TRUE
		WHERE r.run_id = $1::uuid
	`, report.RunID).Scan(&runStatus, &failureRaw, &controlReason, &started, &ended, &entityCount); err == nil {
		report.RunTableStatus = strings.TrimSpace(runStatus)
		report.ControlReason = strings.TrimSpace(controlReason)
		failure, decodeErr := decodeStoredFailure(failureRaw)
		if decodeErr != nil {
			return RunDebugReport{}, decodeErr
		}
		report.Failure = failure
		if evidenceErr := storerunlifecycle.ValidateStatusFailure(report.RunTableStatus, report.Failure); evidenceErr != nil {
			return RunDebugReport{}, fmt.Errorf("run %s terminal evidence: %w", report.RunID, evidenceErr)
		}
		if started.Valid {
			report.StartedAt = started.Time
		}
		if ended.Valid {
			tm := ended.Time
			report.EndedAt = &tm
		}
		if entityCount.Valid {
			report.EntityCount = int(entityCount.Int64)
		}
	} else if err != sql.ErrNoRows {
		return RunDebugReport{}, fmt.Errorf("load run row: %w", err)
	}

	if err := s.DB.QueryRowContext(ctx, `
		SELECT event_id::text, event_name, created_at
		FROM events
		WHERE run_id = $1::uuid
		ORDER BY created_at ASC, event_id ASC
		LIMIT 1
	`, report.RunID).Scan(&report.RootEventID, &report.RootEventType, &report.StartedAt); err != nil {
		return RunDebugReport{}, fmt.Errorf("load root event: %w", err)
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*), MAX(created_at)
		FROM events
		WHERE run_id = $1::uuid
	`, report.RunID).Scan(&report.EventCount, &report.LastEventAt); err != nil {
		return RunDebugReport{}, fmt.Errorf("load event summary: %w", err)
	}

	eventCountRows, err := s.DB.QueryContext(ctx, `
		SELECT event_name, COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		GROUP BY event_name
		ORDER BY event_name
	`, report.RunID)
	if err != nil {
		return RunDebugReport{}, fmt.Errorf("load event counts: %w", err)
	}
	defer eventCountRows.Close()
	for eventCountRows.Next() {
		var item RunDebugEventCount
		if err := eventCountRows.Scan(&item.EventName, &item.Count); err != nil {
			return RunDebugReport{}, fmt.Errorf("scan event counts: %w", err)
		}
		report.EventCounts = append(report.EventCounts, item)
	}
	if err := eventCountRows.Err(); err != nil {
		return RunDebugReport{}, fmt.Errorf("read event counts: %w", err)
	}

	deliveryRows, err := s.DB.QueryContext(ctx, `
		SELECT COALESCE(subscriber_id, ''), COALESCE(status, ''), COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		GROUP BY subscriber_id, status
		ORDER BY subscriber_id, status
	`, report.RunID)
	if err != nil {
		return RunDebugReport{}, fmt.Errorf("load deliveries: %w", err)
	}
	defer deliveryRows.Close()
	for deliveryRows.Next() {
		var item RunDebugDeliveryCount
		if err := deliveryRows.Scan(&item.SubscriberID, &item.Status, &item.Count); err != nil {
			return RunDebugReport{}, fmt.Errorf("scan deliveries: %w", err)
		}
		report.Deliveries = append(report.Deliveries, item)
	}
	if err := deliveryRows.Err(); err != nil {
		return RunDebugReport{}, fmt.Errorf("read deliveries: %w", err)
	}
	failedDeliveries, err := s.loadRunDebugFailureDeliveries(ctx, report.RunID, opts.DeadLetterLimit)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.FailedDeliveries = failedDeliveries
	testQuiescence, err := s.loadRunTestQuiescence(ctx, report.RunID)
	if err != nil {
		return RunDebugReport{}, err
	}
	report.TestQuiescence = testQuiescence

	eventRows, err := s.DB.QueryContext(ctx, `
		SELECT
			event_id::text,
			event_name,
			COALESCE(entity_id::text, ''),
			created_at,
			COALESCE(produced_by, ''),
			COALESCE(produced_by_type, ''),
			COALESCE(payload, '{}'::jsonb)
		FROM events
		WHERE run_id = $1::uuid
		ORDER BY created_at DESC, event_id DESC
		LIMIT $2
	`, report.RunID, opts.EventLimit)
	if err != nil {
		return RunDebugReport{}, fmt.Errorf("load run events: %w", err)
	}
	defer eventRows.Close()
	for eventRows.Next() {
		var item RunDebugEvent
		var payload []byte
		if err := eventRows.Scan(&item.EventID, &item.EventName, &item.EntityID, &item.CreatedAt, &item.Source, &item.SourceType, &payload); err != nil {
			return RunDebugReport{}, fmt.Errorf("scan run events: %w", err)
		}
		item.Payload = append(json.RawMessage(nil), payload...)
		report.Events = append(report.Events, item)
	}
	if err := eventRows.Err(); err != nil {
		return RunDebugReport{}, fmt.Errorf("read run events: %w", err)
	}

	deadRows, err := s.DB.QueryContext(ctx, `
		SELECT
			COALESCE(dl.original_event, ''),
			COALESCE(dl.entity_id::text, ''),
			dl.failure,
			COALESCE(dl.handler_node, ''),
			dl.created_at
		FROM dead_letters dl
		INNER JOIN events e ON e.event_id = dl.original_event_id
		WHERE e.run_id = $1::uuid
		ORDER BY dl.created_at DESC
		LIMIT $2
	`, report.RunID, opts.DeadLetterLimit)
	if err != nil {
		return RunDebugReport{}, fmt.Errorf("load dead letters: %w", err)
	}
	defer deadRows.Close()
	for deadRows.Next() {
		var item RunDebugDeadLetter
		var rawFailure []byte
		if err := deadRows.Scan(&item.OriginalEvent, &item.EntityID, &rawFailure, &item.HandlerNode, &item.CreatedAt); err != nil {
			return RunDebugReport{}, fmt.Errorf("scan dead letters: %w", err)
		}
		failure, err := decodeStoredFailure(rawFailure)
		if err != nil || failure == nil {
			return RunDebugReport{}, fmt.Errorf("decode run dead letter failure")
		}
		item.Failure = *failure
		report.DeadLetters = append(report.DeadLetters, item)
	}
	if err := deadRows.Err(); err != nil {
		return RunDebugReport{}, fmt.Errorf("read dead letters: %w", err)
	}

	turnRows, err := s.DB.QueryContext(ctx, `
		SELECT agent_id, COUNT(*), COUNT(*) FILTER (WHERE failure IS NOT NULL), MAX(created_at)
		FROM agent_turns
		WHERE run_id = $1::uuid
		GROUP BY agent_id
		ORDER BY agent_id
	`, report.RunID)
	if err != nil {
		return RunDebugReport{}, fmt.Errorf("load agent turns: %w", err)
	}
	defer turnRows.Close()
	for turnRows.Next() {
		var item RunDebugAgentTurn
		if err := turnRows.Scan(&item.AgentID, &item.Turns, &item.ErrorCount, &item.LastAt); err != nil {
			return RunDebugReport{}, fmt.Errorf("scan agent turns: %w", err)
		}
		report.AgentTurns = append(report.AgentTurns, item)
	}
	if err := turnRows.Err(); err != nil {
		return RunDebugReport{}, fmt.Errorf("read agent turns: %w", err)
	}

	mutationRows, err := s.DB.QueryContext(ctx, `
		SELECT
			mutation_id::text,
			COALESCE(entity_id::text, ''),
			COALESCE(field, ''),
			COALESCE(old_value, 'null'::jsonb),
			COALESCE(new_value, 'null'::jsonb),
			COALESCE(writer_type, ''),
			COALESCE(writer_id, ''),
			COALESCE(handler_step, ''),
			COALESCE(caused_by_event::text, ''),
			created_at
		FROM entity_mutations
		WHERE run_id = $1::uuid
		ORDER BY created_at DESC, mutation_id DESC
		LIMIT $2
	`, report.RunID, opts.MutationLimit)
	if err != nil {
		return RunDebugReport{}, fmt.Errorf("load run mutations: %w", err)
	}
	defer mutationRows.Close()
	for mutationRows.Next() {
		var (
			item     RunDebugMutation
			oldValue []byte
			newValue []byte
		)
		if err := mutationRows.Scan(
			&item.MutationID,
			&item.EntityID,
			&item.Field,
			&oldValue,
			&newValue,
			&item.WriterType,
			&item.WriterID,
			&item.HandlerStep,
			&item.CausedByEvent,
			&item.CreatedAt,
		); err != nil {
			return RunDebugReport{}, fmt.Errorf("scan run mutations: %w", err)
		}
		item.OldValue = append(json.RawMessage(nil), oldValue...)
		item.NewValue = append(json.RawMessage(nil), newValue...)
		report.Mutations = append(report.Mutations, item)
	}
	if err := mutationRows.Err(); err != nil {
		return RunDebugReport{}, fmt.Errorf("read run mutations: %w", err)
	}

	if err := s.loadRunDebugRuntimeLogs(ctx, report.RunID, opts, &report); err != nil {
		return RunDebugReport{}, err
	}

	return report, nil
}

func (s *PostgresStore) loadRunTestQuiescence(ctx context.Context, runID string) (RunTestQuiescence, error) {
	var out RunTestQuiescence
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries d
		WHERE d.run_id = $1::uuid
		  AND NOT (COALESCE(d.subscriber_type, '') = $2 AND COALESCE(d.subscriber_id, '') = $3)
		  AND `+activeRunQuiescenceDeliveryPredicateSQL("d")+`
	`, runID, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID).Scan(&out.ActiveDeliveries); err != nil {
		return RunTestQuiescence{}, fmt.Errorf("load run test quiescence active deliveries: %w", err)
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events e
		LEFT JOIN event_receipts r
			ON r.event_id = e.event_id
			AND r.subscriber_type = 'platform'
			AND r.subscriber_id = 'pipeline'
		WHERE e.run_id = $1::uuid
		  AND e.event_name <> '`+runtimeLogEventName+`'
		  AND (r.event_id IS NULL OR COALESCE(r.outcome, '') <> 'success')
	`, runID).Scan(&out.UnsettledPipelineEvents); err != nil {
		return RunTestQuiescence{}, fmt.Errorf("load run test quiescence unsettled pipeline events: %w", err)
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = $1::uuid
		  AND status = 'active'
		  AND fire_at <= now()
	`, runID).Scan(&out.DueTimers); err != nil {
		return RunTestQuiescence{}, fmt.Errorf("load run test quiescence due timers: %w", err)
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE run_id = $1::uuid
		  AND status = 'active'
		  AND lease_holder IS NOT NULL
		  AND lease_expires_at IS NOT NULL
		  AND lease_expires_at > now()
	`, runID).Scan(&out.ActiveSessionLeases); err != nil {
		return RunTestQuiescence{}, fmt.Errorf("load run test quiescence active session leases: %w", err)
	}
	out.Ready = runTestQuiescenceReady(out)
	return out, nil
}

func runTestQuiescenceReady(value RunTestQuiescence) bool {
	return value.ActiveDeliveries == 0 &&
		value.UnsettledPipelineEvents == 0 &&
		value.DueTimers == 0 &&
		value.ActiveSessionLeases == 0
}

func (s *PostgresStore) loadRunDebugFailureDeliveries(ctx context.Context, runID string, limit int) ([]RunDebugFailureDelivery, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			e.event_id::text,
			COALESCE(e.event_name, ''),
			COALESCE(e.entity_id::text, ''),
			d.delivery_id::text,
			COALESCE(d.subscriber_type, ''),
			COALESCE(d.subscriber_id, ''),
			COALESCE(d.active_session_id::text, ''),
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			COALESCE(d.failure, 'null'::jsonb),
			COALESCE(d.retry_count, 0),
			d.created_at,
			d.started_at,
			d.delivered_at
		FROM event_deliveries d
		INNER JOIN events e ON e.event_id = d.event_id
		WHERE d.run_id = $1::uuid
		  AND NOT (COALESCE(d.subscriber_type, '') = $3 AND COALESCE(d.subscriber_id, '') = $4)
		  AND COALESCE(d.status, '') IN ('failed', 'dead_letter')
		ORDER BY COALESCE(d.delivered_at, d.started_at, d.created_at, e.created_at) DESC, d.delivery_id::text DESC
		LIMIT $2
	`, runID, limit, replayScopeMarkerSubscriberType, replayScopeMarkerSubscriberID)
	if err != nil {
		return nil, fmt.Errorf("load run failed deliveries: %w", err)
	}
	defer rows.Close()
	out := []RunDebugFailureDelivery{}
	for rows.Next() {
		var item RunDebugFailureDelivery
		var rawFailure []byte
		var createdAt, startedAt, finishedAt sql.NullTime
		if err := rows.Scan(
			&item.EventID,
			&item.EventName,
			&item.EntityID,
			&item.DeliveryID,
			&item.SubscriberType,
			&item.SubscriberID,
			&item.SessionID,
			&item.Status,
			&item.ReasonCode,
			&rawFailure,
			&item.RetryCount,
			&createdAt,
			&startedAt,
			&finishedAt,
		); err != nil {
			return nil, fmt.Errorf("scan run failed delivery: %w", err)
		}
		item.Failure, err = decodeStoredFailure(rawFailure)
		if err != nil {
			return nil, fmt.Errorf("decode run failed delivery failure: %w", err)
		}
		item.CreatedAt = nullTimePtr(createdAt)
		item.StartedAt = nullTimePtr(startedAt)
		item.FinishedAt = nullTimePtr(finishedAt)
		normalizeRunDebugFailureDelivery(&item)
		if item.Status == "dead_letter" {
			deadLetters, err := s.operatorObservabilityReadSurface().loadOperatorEventDeadLetters(ctx, item.EventID)
			if err != nil {
				return nil, err
			}
			item.DeadLetters = deadLetters
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run failed deliveries: %w", err)
	}
	return out, nil
}

func normalizeRunDebugFailureDelivery(item *RunDebugFailureDelivery) {
	if item == nil {
		return
	}
	item.Status = strings.TrimSpace(item.Status)
	item.ReasonCode = strings.TrimSpace(item.ReasonCode)
	item.RetryEligible = OperatorDeliveryRetryEligible(item.Status)
	item.Terminal = OperatorDeliveryTerminal(item.Status)
}

func (s *PostgresStore) LoadRunDebugTrace(ctx context.Context, runID string, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, error) {
	rows, _, err := s.LoadRunDebugTracePage(ctx, runID, opts)
	return rows, err
}

func runDebugTraceWatermarkWhere(operator string, argIndex int) string {
	return fmt.Sprintf(`
			  AND GREATEST(
				e.created_at,
				COALESCE(d.created_at, '-infinity'::timestamptz),
				COALESCE(d.started_at, '-infinity'::timestamptz),
				COALESCE(d.delivered_at, '-infinity'::timestamptz),
				COALESCE(sess.updated_at, '-infinity'::timestamptz),
				COALESCE(t.created_at, '-infinity'::timestamptz)
			  ) %s $%d::timestamptz`, operator, argIndex)
}

func (s *PostgresStore) LoadRunDebugTracePage(ctx context.Context, runID string, opts RunDebugTraceQueryOptions) ([]RunDebugTraceRow, string, error) {
	if s == nil || s.DB == nil {
		return nil, "", fmt.Errorf("postgres store is required")
	}
	if err := s.requireRunDebugCapabilities(ctx); err != nil {
		return nil, "", err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, "", ErrRunNotFound
	}
	if _, err := uuid.Parse(runID); err != nil {
		return nil, "", ErrRunNotFound
	}
	opts = defaultRunDebugTraceQueryOptions(opts)
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM runs WHERE run_id = $1::uuid)`, runID).Scan(&exists); err != nil {
		return nil, "", fmt.Errorf("check run debug trace run: %w", err)
	}
	if !exists {
		return nil, "", ErrRunNotFound
	}

	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, "", err
	}
	sessionSources := runDebugTraceSessionSources(caps)
	replyContextSelect := "''"
	if caps.Events.DeliveryContext {
		replyContextSelect = "COALESCE(d.delivery_context->'reply'->>'id', '')"
	}
	args := []any{runID}
	cursorWhere := ""
	if opts.Cursor != "" {
		cursor, err := decodeRunDebugTraceCursor(opts.Cursor)
		if err != nil {
			return nil, "", err
		}
		args = append(args,
			cursor.EventCreatedAt,
			cursor.EventID,
			nullableCursorTimestamp(cursor.DeliveryCreatedAt),
			cursor.DeliveryID,
			nullableCursorTimestamp(cursor.TurnCreatedAt),
			cursor.TurnID,
		)
		cursorWhere = fmt.Sprintf(`
		  AND (
			e.created_at > $%d::timestamptz
			OR (e.created_at = $%d::timestamptz AND e.event_id::text > $%d)
			OR (e.created_at = $%d::timestamptz AND e.event_id::text = $%d AND COALESCE(d.created_at, '-infinity'::timestamptz) > $%d::timestamptz)
			OR (e.created_at = $%d::timestamptz AND e.event_id::text = $%d AND COALESCE(d.created_at, '-infinity'::timestamptz) = $%d::timestamptz AND COALESCE(d.delivery_id::text, '') > $%d)
			OR (e.created_at = $%d::timestamptz AND e.event_id::text = $%d AND COALESCE(d.created_at, '-infinity'::timestamptz) = $%d::timestamptz AND COALESCE(d.delivery_id::text, '') = $%d AND COALESCE(t.created_at, '-infinity'::timestamptz) > $%d::timestamptz)
			OR (e.created_at = $%d::timestamptz AND e.event_id::text = $%d AND COALESCE(d.created_at, '-infinity'::timestamptz) = $%d::timestamptz AND COALESCE(d.delivery_id::text, '') = $%d AND COALESCE(t.created_at, '-infinity'::timestamptz) = $%d::timestamptz AND COALESCE(t.turn_id::text, '') > $%d)
		  )`,
			2, 2, 3,
			2, 3, 4,
			2, 3, 4, 5,
			2, 3, 4, 5, 6,
			2, 3, 4, 5, 6, 7,
		)
	}
	sinceWhere := ""
	if opts.Since != nil {
		args = append(args, opts.Since.UTC())
		sinceWhere = runDebugTraceWatermarkWhere(">", len(args))
	}
	untilWhere := ""
	if opts.Until != nil {
		args = append(args, opts.Until.UTC())
		untilWhere = runDebugTraceWatermarkWhere("<=", len(args))
	}
	filterWhere := ""
	addTextArrayFilter := func(values []string, expression string) {
		if len(values) == 0 {
			return
		}
		args = append(args, pq.Array(values))
		filterWhere += fmt.Sprintf(`
			  AND %s = ANY($%d::text[])`, expression, len(args))
	}
	addTextArrayFilter(opts.Filter.EventNames, "e.event_name")
	addTextArrayFilter(opts.Filter.EntityIDs, "e.entity_id::text")
	addTextArrayFilter(opts.Filter.DeliveryStatuses, "d.status")
	addTextArrayFilter(opts.Filter.SubscriberIDs, "d.subscriber_id")
	addTextArrayFilter(opts.Filter.SubscriberTypes, "d.subscriber_type")
	if opts.ExcludeRuntimeLogs {
		filterWhere += `
			  AND e.event_name <> 'platform.runtime_log'`
	}
	args = append(args, opts.Limit+1)
	limitArg := len(args)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		WITH trace_sessions AS (
			%s
		)
		SELECT
			e.event_id::text,
			COALESCE(e.event_name, ''),
			COALESCE(e.source_event_id::text, ''),
			COALESCE(e.entity_id::text, ''),
			COALESCE(e.produced_by, ''),
			COALESCE(e.produced_by_type, ''),
			e.created_at,
			COALESCE(d.delivery_id::text, ''),
			COALESCE(d.subscriber_type, ''),
				COALESCE(d.subscriber_id, ''),
			COALESCE(d.status, ''),
			COALESCE(d.reason_code, ''),
			%s,
			COALESCE(d.delivery_payload_projection, '{}'::jsonb),
			COALESCE(d.failure, 'null'::jsonb),
				COALESCE(d.retry_count, 0),
				COALESCE(d.active_session_id::text, ''),
				d.created_at,
			d.started_at,
			d.delivered_at,
			COALESCE(sess.session_id::text, ''),
			COALESCE(sess.session_kind, ''),
			COALESCE(sess.runtime_mode, ''),
			COALESCE(sess.status, ''),
			sess.updated_at,
			COALESCE(t.turn_id::text, ''),
			COALESCE(t.trigger_event_id::text, ''),
			COALESCE(t.trigger_event_type, ''),
			COALESCE(t.runtime_mode, ''),
			COALESCE(t.scope_key, ''),
			COALESCE(t.entity_id::text, ''),
			COALESCE(t.task_id, ''),
			COALESCE(t.parse_ok, false),
			COALESCE(t.retry_count, 0),
			COALESCE(t.failure, 'null'::jsonb),
			t.created_at
		FROM events e
		LEFT JOIN event_deliveries d
			ON d.event_id = e.event_id
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
		LEFT JOIN trace_sessions sess
			ON sess.session_id = COALESCE(t.session_id, d.active_session_id)
		   AND (
				sess.run_id = e.run_id
				OR sess.run_id IS NULL
		   )
			WHERE e.run_id = $1::uuid
			%s
			%s
			%s
			%s
			ORDER BY
				e.created_at ASC,
				e.event_id ASC,
			d.created_at ASC NULLS FIRST,
			d.delivery_id ASC NULLS FIRST,
			t.created_at ASC NULLS FIRST,
			t.turn_id ASC NULLS FIRST
		LIMIT $%d
		`, sessionSources, replyContextSelect, cursorWhere, sinceWhere, untilWhere, filterWhere, limitArg), args...)
	if err != nil {
		return nil, "", fmt.Errorf("load run debug trace: %w", err)
	}
	defer rows.Close()

	out := make([]RunDebugTraceRow, 0, opts.Limit+1)
	for rows.Next() {
		var (
			item                  RunDebugTraceRow
			deliveryCreatedAt     sql.NullTime
			deliveryStartedAt     sql.NullTime
			deliveryDeliveredAt   sql.NullTime
			sessionUpdatedAt      sql.NullTime
			turnCreatedAt         sql.NullTime
			rawDeliveryProjection []byte
			rawDeliveryFailure    []byte
			rawTurnFailure        []byte
		)
		if err := rows.Scan(
			&item.EventID,
			&item.EventName,
			&item.SourceEventID,
			&item.EntityID,
			&item.EventSource,
			&item.EventSourceType,
			&item.EventCreatedAt,
			&item.DeliveryID,
			&item.SubscriberType,
			&item.SubscriberID,
			&item.DeliveryStatus,
			&item.DeliveryReasonCode,
			&item.ReplyContextID,
			&rawDeliveryProjection,
			&rawDeliveryFailure,
			&item.DeliveryRetryCount,
			&item.ActiveSessionID,
			&deliveryCreatedAt,
			&deliveryStartedAt,
			&deliveryDeliveredAt,
			&item.SessionID,
			&item.SessionKind,
			&item.SessionRuntimeMode,
			&item.SessionStatus,
			&sessionUpdatedAt,
			&item.TurnID,
			&item.TurnTriggerEventID,
			&item.TurnTriggerEventType,
			&item.TurnRuntimeMode,
			&item.TurnScopeKey,
			&item.TurnEntityID,
			&item.TurnTaskID,
			&item.TurnParseOK,
			&item.TurnRetryCount,
			&rawTurnFailure,
			&turnCreatedAt,
		); err != nil {
			return nil, "", fmt.Errorf("scan run debug trace: %w", err)
		}
		projection, err := decodeDeliveryPayloadProjectionJSON(rawDeliveryProjection)
		if err != nil {
			return nil, "", fmt.Errorf("decode run trace delivery payload projection (%s): %w", item.DeliveryID, err)
		}
		if !projection.Empty() {
			item.DeliveryPayloadProjection = &projection
		}
		item.DeliveryFailure, err = decodeStoredFailure(rawDeliveryFailure)
		if err != nil {
			return nil, "", fmt.Errorf("decode run trace delivery failure: %w", err)
		}
		item.TurnFailure, err = decodeStoredFailure(rawTurnFailure)
		if err != nil {
			return nil, "", fmt.Errorf("decode run trace turn failure: %w", err)
		}
		item.DeliveryCreatedAt = nullableTimePtr(deliveryCreatedAt)
		item.DeliveryStartedAt = nullableTimePtr(deliveryStartedAt)
		item.DeliveryDeliveredAt = nullableTimePtr(deliveryDeliveredAt)
		item.DeliveryRetryEligible = OperatorDeliveryRetryEligible(item.DeliveryStatus)
		item.DeliveryTerminal = OperatorDeliveryTerminal(item.DeliveryStatus)
		item.SessionUpdatedAt = nullableTimePtr(sessionUpdatedAt)
		item.TurnCreatedAt = nullableTimePtr(turnCreatedAt)
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("read run debug trace: %w", err)
	}
	nextCursor := ""
	if len(out) > opts.Limit {
		out = out[:opts.Limit]
		nextCursor = encodeRunDebugTraceCursor(out[len(out)-1])
	}
	return out, nextCursor, nil
}

type runDebugTraceCursor struct {
	EventCreatedAt    string `json:"event_created_at"`
	EventID           string `json:"event_id"`
	DeliveryCreatedAt string `json:"delivery_created_at,omitempty"`
	DeliveryID        string `json:"delivery_id,omitempty"`
	TurnCreatedAt     string `json:"turn_created_at,omitempty"`
	TurnID            string `json:"turn_id,omitempty"`
}

func encodeRunDebugTraceCursor(row RunDebugTraceRow) string {
	cursor := runDebugTraceCursor{
		EventCreatedAt: row.EventCreatedAt.UTC().Format(time.RFC3339Nano),
		EventID:        strings.TrimSpace(row.EventID),
		DeliveryID:     strings.TrimSpace(row.DeliveryID),
		TurnID:         strings.TrimSpace(row.TurnID),
	}
	if row.DeliveryCreatedAt != nil {
		cursor.DeliveryCreatedAt = row.DeliveryCreatedAt.UTC().Format(time.RFC3339Nano)
	}
	if row.TurnCreatedAt != nil {
		cursor.TurnCreatedAt = row.TurnCreatedAt.UTC().Format(time.RFC3339Nano)
	}
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeRunDebugTraceCursor(cursor string) (runDebugTraceCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(cursor))
	if err != nil {
		return runDebugTraceCursor{}, ErrInvalidObservabilityCursor
	}
	var decoded runDebugTraceCursor
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return runDebugTraceCursor{}, ErrInvalidObservabilityCursor
	}
	if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(decoded.EventCreatedAt)); err != nil {
		return runDebugTraceCursor{}, ErrInvalidObservabilityCursor
	}
	if strings.TrimSpace(decoded.EventID) == "" {
		return runDebugTraceCursor{}, ErrInvalidObservabilityCursor
	}
	for _, timestamp := range []string{decoded.DeliveryCreatedAt, decoded.TurnCreatedAt} {
		if strings.TrimSpace(timestamp) == "" {
			continue
		}
		if _, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(timestamp)); err != nil {
			return runDebugTraceCursor{}, ErrInvalidObservabilityCursor
		}
	}
	return decoded, nil
}

func nullableCursorTimestamp(timestamp string) string {
	if strings.TrimSpace(timestamp) == "" {
		return "-infinity"
	}
	return strings.TrimSpace(timestamp)
}

func runDebugTraceSessionSources(caps StoreSchemaCapabilities) string {
	sources := []string{}
	if caps.Conversations.Sessions == SchemaFlavorCanonical {
		runIDExpr := "NULL::uuid"
		if caps.Conversations.SessionRunID {
			runIDExpr = "run_id"
		}
		sources = append(sources, `
			SELECT
				session_id,
				`+runIDExpr+`,
				'live_session' AS session_kind,
				COALESCE(runtime_mode, '') AS runtime_mode,
				COALESCE(status, '') AS status,
				updated_at
			FROM agent_sessions
		`)
	}
	if caps.Conversations.Audits == SchemaFlavorCanonical {
		runIDExpr := "NULL::uuid"
		if caps.Conversations.AuditRunID {
			runIDExpr = "run_id"
		}
		sources = append(sources, `
			SELECT
				session_id,
				`+runIDExpr+`,
				'turn_audit' AS session_kind,
				COALESCE(runtime_mode, '') AS runtime_mode,
				COALESCE(status, '') AS status,
				updated_at
			FROM agent_conversation_audits
		`)
	}
	if len(sources) == 0 {
		return `
			SELECT
				NULL::uuid AS session_id,
				NULL::uuid AS run_id,
				''::text AS session_kind,
				''::text AS runtime_mode,
				''::text AS status,
				NULL::timestamptz AS updated_at
			WHERE FALSE
		`
	}
	return strings.Join(sources, "\nUNION ALL\n")
}

func nullableTimePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	tm := value.Time
	return &tm
}

func (s *PostgresStore) loadRunDebugRuntimeLogs(ctx context.Context, runID string, opts RunDebugQueryOptions, report *RunDebugReport) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	if report == nil {
		return fmt.Errorf("report is required")
	}
	logLevels := []string{"warn", "error"}
	if opts.LogsAllLevels {
		logLevels = []string{"info", "warn", "error"}
	}
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
	`, runID, pq.Array(logLevels), opts.Component).Scan(&report.WarnErrorLogCount); err != nil {
		return fmt.Errorf("load runtime log summary: %w", err)
	}
	logSummaryRows, err := s.DB.QueryContext(ctx, `
		SELECT
			COALESCE(payload->>'log_level', ''),
			COALESCE(payload->'details'->>'component', ''),
			COALESCE(payload->'details'->>'action', ''),
			COUNT(*)
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
		GROUP BY payload->>'log_level', payload->'details'->>'component', payload->'details'->>'action'
		ORDER BY COUNT(*) DESC, payload->'details'->>'component', payload->'details'->>'action'
		LIMIT 12
	`, runID, pq.Array(logLevels), opts.Component)
	if err != nil {
		return fmt.Errorf("load runtime log rollup: %w", err)
	}
	defer logSummaryRows.Close()
	for logSummaryRows.Next() {
		var item RunDebugRuntimeSummary
		if err := logSummaryRows.Scan(&item.Level, &item.Component, &item.Action, &item.Count); err != nil {
			return fmt.Errorf("scan runtime log rollup: %w", err)
		}
		report.RuntimeLogSummary = append(report.RuntimeLogSummary, item)
	}
	if err := logSummaryRows.Err(); err != nil {
		return fmt.Errorf("read runtime log rollup: %w", err)
	}
	logRows, err := s.DB.QueryContext(ctx, `
		SELECT
			event_id::text,
			COALESCE(payload->>'log_level', ''),
			COALESCE(payload->>'message', ''),
			COALESCE(payload->'details'->>'component', ''),
			COALESCE(payload->'details'->>'action', ''),
			COALESCE(payload->'details'->>'event_type', ''),
			COALESCE(payload->'details'->>'agent_id', ''),
			COALESCE(payload->'details'->>'entity_id', ''),
			COALESCE(payload->'details'->'failure', 'null'::jsonb),
			COALESCE(payload->'details', '{}'::jsonb),
			created_at
		FROM events
		WHERE event_name = 'platform.runtime_log'
		  AND run_id = $1::uuid
		  AND payload->>'log_level' = ANY($2::text[])
		  AND ($3 = '' OR payload->'details'->>'component' = $3)
		ORDER BY created_at DESC
		LIMIT $4
	`, runID, pq.Array(logLevels), opts.Component, opts.RuntimeLogLimit)
	if err != nil {
		return fmt.Errorf("load runtime logs: %w", err)
	}
	defer logRows.Close()
	for logRows.Next() {
		var item RunDebugRuntimeLog
		var detail []byte
		var failureRaw []byte
		if err := logRows.Scan(&item.EventID, &item.Level, &item.Message, &item.Component, &item.Action, &item.EventType, &item.AgentID, &item.EntityID, &failureRaw, &detail, &item.CreatedAt); err != nil {
			return fmt.Errorf("scan runtime logs: %w", err)
		}
		failure, err := decodeStoredFailure(failureRaw)
		if err != nil {
			return fmt.Errorf("decode runtime log failure: %w", err)
		}
		item.Failure = failure
		item.Detail = append(json.RawMessage(nil), detail...)
		report.RuntimeLogs = append(report.RuntimeLogs, item)
	}
	if err := logRows.Err(); err != nil {
		return fmt.Errorf("read runtime logs: %w", err)
	}
	return nil
}
