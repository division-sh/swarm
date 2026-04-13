package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

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
	OriginalEvent string    `json:"original_event"`
	EntityID      string    `json:"entity_id,omitempty"`
	FailureType   string    `json:"failure_type"`
	ErrorMessage  string    `json:"error_message,omitempty"`
	HandlerNode   string    `json:"handler_node,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

type RunDebugAgentTurn struct {
	AgentID    string    `json:"agent_id"`
	Turns      int       `json:"turns"`
	ErrorCount int       `json:"error_count"`
	LastAt     time.Time `json:"last_at"`
}

type RunDebugRuntimeLog struct {
	EventID   string          `json:"event_id,omitempty"`
	Level     string          `json:"level"`
	Message   string          `json:"message,omitempty"`
	Component string          `json:"component"`
	Action    string          `json:"action"`
	EventType string          `json:"event_type,omitempty"`
	AgentID   string          `json:"agent_id,omitempty"`
	EntityID  string          `json:"entity_id,omitempty"`
	Error     string          `json:"error,omitempty"`
	Detail    json.RawMessage `json:"detail,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

type RunDebugRuntimeSummary struct {
	Level     string `json:"level"`
	Component string `json:"component"`
	Action    string `json:"action"`
	Count     int    `json:"count"`
}

type RunDebugReport struct {
	RunID             string                   `json:"run_id"`
	RunTableStatus    string                   `json:"run_table_status,omitempty"`
	RootEventID       string                   `json:"root_event_id,omitempty"`
	RootEventType     string                   `json:"root_event_type,omitempty"`
	ErrorSummary      string                   `json:"error_summary,omitempty"`
	StartedAt         time.Time                `json:"started_at,omitempty"`
	LastEventAt       time.Time                `json:"last_event_at,omitempty"`
	EndedAt           *time.Time               `json:"ended_at,omitempty"`
	EventCount        int                      `json:"event_count"`
	EntityCount       int                      `json:"entity_count"`
	WarnErrorLogCount int                      `json:"warn_error_log_count"`
	EventCounts       []RunDebugEventCount     `json:"event_counts,omitempty"`
	Deliveries        []RunDebugDeliveryCount  `json:"deliveries,omitempty"`
	Events            []RunDebugEvent          `json:"events,omitempty"`
	DeadLetters       []RunDebugDeadLetter     `json:"dead_letters,omitempty"`
	AgentTurns        []RunDebugAgentTurn      `json:"agent_turns,omitempty"`
	Mutations         []RunDebugMutation       `json:"mutations,omitempty"`
	RuntimeLogSummary []RunDebugRuntimeSummary `json:"runtime_log_summary,omitempty"`
	RuntimeLogs       []RunDebugRuntimeLog     `json:"runtime_logs,omitempty"`
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
	case caps.Conversations.Turns != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	case !caps.Conversations.TurnRunID:
		return fmt.Errorf("run debug read surface requires canonical agent_turns.run_id")
	}
	required := map[string][]string{
		"runs":             {"run_id", "status", "error_summary", "started_at", "ended_at", "entity_count"},
		"dead_letters":     {"original_event_id", "original_event", "entity_id", "failure_type", "error_message", "handler_node", "created_at"},
		"entity_mutations": {"mutation_id", "run_id", "entity_id", "field", "old_value", "new_value", "caused_by_event", "writer_type", "writer_id", "handler_step", "created_at"},
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
		SELECT COALESCE(run_id::text, '')
		FROM events
		WHERE event_name = 'scan.requested'
		  AND run_id IS NOT NULL
		ORDER BY created_at DESC
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
			COALESCE(r.entity_count, 0)
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
		runStatus    string
		errorSummary string
		started      sql.NullTime
		ended        sql.NullTime
		entityCount  sql.NullInt64
	)
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(error_summary, ''), started_at, ended_at, COALESCE(entity_count, 0)
		FROM runs
		WHERE run_id = $1::uuid
	`, report.RunID).Scan(&runStatus, &errorSummary, &started, &ended, &entityCount); err == nil {
		report.RunTableStatus = strings.TrimSpace(runStatus)
		report.ErrorSummary = strings.TrimSpace(errorSummary)
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
			COALESCE(dl.failure_type, ''),
			COALESCE(dl.error_message, ''),
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
		if err := deadRows.Scan(&item.OriginalEvent, &item.EntityID, &item.FailureType, &item.ErrorMessage, &item.HandlerNode, &item.CreatedAt); err != nil {
			return RunDebugReport{}, fmt.Errorf("scan dead letters: %w", err)
		}
		report.DeadLetters = append(report.DeadLetters, item)
	}
	if err := deadRows.Err(); err != nil {
		return RunDebugReport{}, fmt.Errorf("read dead letters: %w", err)
	}

	turnRows, err := s.DB.QueryContext(ctx, `
		SELECT agent_id, COUNT(*), COUNT(*) FILTER (WHERE COALESCE(error, '') <> ''), MAX(created_at)
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
			COALESCE(payload->'details'->>'error', ''),
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
		if err := logRows.Scan(&item.EventID, &item.Level, &item.Message, &item.Component, &item.Action, &item.EventType, &item.AgentID, &item.EntityID, &item.Error, &detail, &item.CreatedAt); err != nil {
			return fmt.Errorf("scan runtime logs: %w", err)
		}
		item.Detail = append(json.RawMessage(nil), detail...)
		report.RuntimeLogs = append(report.RuntimeLogs, item)
	}
	if err := logRows.Err(); err != nil {
		return fmt.Errorf("read runtime logs: %w", err)
	}
	return nil
}
