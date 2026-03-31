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

type TraceEvent struct {
	EventID        string
	EventName      string
	EntityID       string
	FlowInstance   string
	Scope          string
	TraceID        string
	SourceEventID  string
	ProducedBy     string
	ProducedByType string
	Payload        map[string]any
	CreatedAt      time.Time
}

type TraceDelivery struct {
	EventID        string
	SubscriberType string
	SubscriberID   string
	Status         string
	ReasonCode     string
	RetryCount     int
	LastError      string
	DeliveredAt    sql.NullTime
	CreatedAt      time.Time
}

type TraceReceipt struct {
	EventID        string
	SubscriberType string
	SubscriberID   string
	Outcome        string
	ReasonCode     string
	SideEffects    map[string]any
	ProcessedAt    time.Time
}

type TraceDeadLetter struct {
	OriginalEventID string
	FailureType     string
	ErrorMessage    string
	RetryCount      int
	ChainDepth      int
	HandlerNode     string
	CreatedAt       time.Time
}

type TraceTurn struct {
	TurnID           string
	AgentID          string
	SessionID        string
	RuntimeMode      string
	ScopeKey         string
	TraceID          string
	EntityID         string
	TriggerEventID   string
	TriggerEventType string
	AvailableTools   []string
	ToolCalls        []string
	EmittedEvents    []string
	MCPServers       map[string]string
	MCPToolsListed   []string
	MCPToolsVisible  []string
	ParseOK          bool
	LatencyMS        int
	RetryCount       int
	Error            string
	CreatedAt        time.Time
}

type TraceReport struct {
	TraceID     string
	Events      []TraceEvent
	Deliveries  []TraceDelivery
	Receipts    []TraceReceipt
	DeadLetters []TraceDeadLetter
	Turns       []TraceTurn
}

func (s *PostgresStore) TraceReport(ctx context.Context, traceID string) (TraceReport, error) {
	traceID = strings.TrimSpace(traceID)
	if traceID == "" {
		return TraceReport{}, fmt.Errorf("trace id is required")
	}
	report := TraceReport{TraceID: traceID}

	eventRows, err := s.DB.QueryContext(ctx, `
		SELECT
			event_id::text,
			event_name,
			COALESCE(entity_id::text, ''),
			COALESCE(flow_instance, ''),
			scope,
			COALESCE(trace_id, ''),
			COALESCE(source_event_id::text, ''),
			COALESCE(produced_by, ''),
			COALESCE(produced_by_type, ''),
			payload,
			created_at
		FROM events
		WHERE trace_id = $1
		ORDER BY created_at ASC, event_id ASC
	`, traceID)
	if err != nil {
		return TraceReport{}, fmt.Errorf("query trace events: %w", err)
	}
	defer eventRows.Close()

	eventIDs := make([]string, 0, 64)
	for eventRows.Next() {
		var (
			row     TraceEvent
			payload []byte
		)
		if err := eventRows.Scan(
			&row.EventID,
			&row.EventName,
			&row.EntityID,
			&row.FlowInstance,
			&row.Scope,
			&row.TraceID,
			&row.SourceEventID,
			&row.ProducedBy,
			&row.ProducedByType,
			&payload,
			&row.CreatedAt,
		); err != nil {
			return TraceReport{}, fmt.Errorf("scan trace event: %w", err)
		}
		row.Payload = map[string]any{}
		_ = json.Unmarshal(payload, &row.Payload)
		report.Events = append(report.Events, row)
		eventIDs = append(eventIDs, row.EventID)
	}
	if err := eventRows.Err(); err != nil {
		return TraceReport{}, fmt.Errorf("read trace events: %w", err)
	}
	if len(eventIDs) == 0 {
		return report, nil
	}

	deliveryRows, err := s.DB.QueryContext(ctx, `
		SELECT
			event_id::text,
			subscriber_type,
			subscriber_id,
			status,
			COALESCE(reason_code, ''),
			retry_count,
			COALESCE(last_error, ''),
			delivered_at,
			created_at
		FROM event_deliveries
		WHERE event_id = ANY($1::uuid[])
		ORDER BY created_at ASC, subscriber_id ASC
	`, pq.Array(eventIDs))
	if err != nil {
		return TraceReport{}, fmt.Errorf("query trace deliveries: %w", err)
	}
	defer deliveryRows.Close()
	for deliveryRows.Next() {
		var row TraceDelivery
		if err := deliveryRows.Scan(
			&row.EventID,
			&row.SubscriberType,
			&row.SubscriberID,
			&row.Status,
			&row.ReasonCode,
			&row.RetryCount,
			&row.LastError,
			&row.DeliveredAt,
			&row.CreatedAt,
		); err != nil {
			return TraceReport{}, fmt.Errorf("scan trace delivery: %w", err)
		}
		report.Deliveries = append(report.Deliveries, row)
	}
	if err := deliveryRows.Err(); err != nil {
		return TraceReport{}, fmt.Errorf("read trace deliveries: %w", err)
	}

	receiptRows, err := s.DB.QueryContext(ctx, `
		SELECT
			event_id::text,
			subscriber_type,
			subscriber_id,
			outcome,
			COALESCE(reason_code, ''),
			side_effects,
			processed_at
		FROM event_receipts
		WHERE event_id = ANY($1::uuid[])
		ORDER BY processed_at ASC, subscriber_id ASC
	`, pq.Array(eventIDs))
	if err != nil {
		return TraceReport{}, fmt.Errorf("query trace receipts: %w", err)
	}
	defer receiptRows.Close()
	for receiptRows.Next() {
		var (
			row         TraceReceipt
			sideEffects []byte
		)
		if err := receiptRows.Scan(
			&row.EventID,
			&row.SubscriberType,
			&row.SubscriberID,
			&row.Outcome,
			&row.ReasonCode,
			&sideEffects,
			&row.ProcessedAt,
		); err != nil {
			return TraceReport{}, fmt.Errorf("scan trace receipt: %w", err)
		}
		row.SideEffects = map[string]any{}
		_ = json.Unmarshal(sideEffects, &row.SideEffects)
		report.Receipts = append(report.Receipts, row)
	}
	if err := receiptRows.Err(); err != nil {
		return TraceReport{}, fmt.Errorf("read trace receipts: %w", err)
	}

	deadLetterRows, err := s.DB.QueryContext(ctx, `
		SELECT
			original_event_id::text,
			failure_type,
			COALESCE(error_message, ''),
			retry_count,
			chain_depth,
			COALESCE(handler_node, ''),
			created_at
		FROM dead_letters
		WHERE original_event_id = ANY($1::uuid[])
		ORDER BY created_at ASC, original_event_id ASC
	`, pq.Array(eventIDs))
	if err != nil {
		return TraceReport{}, fmt.Errorf("query trace dead letters: %w", err)
	}
	defer deadLetterRows.Close()
	for deadLetterRows.Next() {
		var row TraceDeadLetter
		if err := deadLetterRows.Scan(
			&row.OriginalEventID,
			&row.FailureType,
			&row.ErrorMessage,
			&row.RetryCount,
			&row.ChainDepth,
			&row.HandlerNode,
			&row.CreatedAt,
		); err != nil {
			return TraceReport{}, fmt.Errorf("scan trace dead letter: %w", err)
		}
		report.DeadLetters = append(report.DeadLetters, row)
	}
	if err := deadLetterRows.Err(); err != nil {
		return TraceReport{}, fmt.Errorf("read trace dead letters: %w", err)
	}

	turnRows, err := s.DB.QueryContext(ctx, `
		SELECT
			turn_id::text,
			agent_id,
			session_id::text,
			runtime_mode,
			COALESCE(scope_key, ''),
			COALESCE(trace_id, ''),
			COALESCE(entity_id::text, ''),
			COALESCE(trigger_event_id::text, ''),
			COALESCE(trigger_event_type, ''),
			COALESCE(available_tools, '[]'::jsonb),
			COALESCE(tool_calls, '[]'::jsonb),
			COALESCE(emitted_events, '[]'::jsonb),
			COALESCE(mcp_servers, '{}'::jsonb),
			COALESCE(mcp_tools_listed, '[]'::jsonb),
			COALESCE(mcp_tools_visible, '[]'::jsonb),
			parse_ok,
			latency_ms,
			retry_count,
			COALESCE(error, ''),
			created_at
		FROM agent_turns
		WHERE trace_id = $1
		ORDER BY created_at ASC, turn_id ASC
	`, traceID)
	if err != nil {
		return TraceReport{}, fmt.Errorf("query trace turns: %w", err)
	}
	defer turnRows.Close()
	for turnRows.Next() {
		var (
			row             TraceTurn
			availableTools  []byte
			toolCalls       []byte
			emittedEvents   []byte
			mcpServers      []byte
			mcpToolsListed  []byte
			mcpToolsVisible []byte
		)
		if err := turnRows.Scan(
			&row.TurnID,
			&row.AgentID,
			&row.SessionID,
			&row.RuntimeMode,
			&row.ScopeKey,
			&row.TraceID,
			&row.EntityID,
			&row.TriggerEventID,
			&row.TriggerEventType,
			&availableTools,
			&toolCalls,
			&emittedEvents,
			&mcpServers,
			&mcpToolsListed,
			&mcpToolsVisible,
			&row.ParseOK,
			&row.LatencyMS,
			&row.RetryCount,
			&row.Error,
			&row.CreatedAt,
		); err != nil {
			return TraceReport{}, fmt.Errorf("scan trace turn: %w", err)
		}
		_ = json.Unmarshal(availableTools, &row.AvailableTools)
		row.ToolCalls = decodeTraceToolCallNames(toolCalls)
		_ = json.Unmarshal(emittedEvents, &row.EmittedEvents)
		row.MCPServers = map[string]string{}
		_ = json.Unmarshal(mcpServers, &row.MCPServers)
		_ = json.Unmarshal(mcpToolsListed, &row.MCPToolsListed)
		_ = json.Unmarshal(mcpToolsVisible, &row.MCPToolsVisible)
		report.Turns = append(report.Turns, row)
	}
	if err := turnRows.Err(); err != nil {
		return TraceReport{}, fmt.Errorf("read trace turns: %w", err)
	}

	return report, nil
}

func decodeTraceToolCallNames(raw []byte) []string {
	var items []map[string]any
	if err := json.Unmarshal(raw, &items); err == nil {
		out := make([]string, 0, len(items))
		for _, item := range items {
			name := strings.TrimSpace(asString(item["name"]))
			if name != "" {
				out = append(out, name)
			}
		}
		return out
	}
	var names []string
	if err := json.Unmarshal(raw, &names); err == nil {
		return names
	}
	return nil
}

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}
