package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"swarm/internal/store"
)

const (
	conversationKindLiveSession = "live_session"
	conversationKindTurnAudit   = "turn_audit"
)

type SQLConversationReader struct {
	db        *sql.DB
	capSource conversationCapabilitySource
}

type conversationCapabilitySource interface {
	ResolveSchemaCapabilities(ctx context.Context) (store.StoreSchemaCapabilities, error)
}

func NewSQLConversationReader(db *sql.DB, capSource conversationCapabilitySource) *SQLConversationReader {
	if db == nil {
		return nil
	}
	return &SQLConversationReader{db: db, capSource: capSource}
}

func (r *SQLConversationReader) List(ctx context.Context, limit int) ([]ConversationSummary, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	caps, err := r.resolveCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	sources := conversationQuerySources(caps)
	if len(sources) == 0 {
		return []ConversationSummary{}, nil
	}
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			session_id,
			agent_id,
			kind,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			COALESCE(runtime_state, '{}'::jsonb),
			updated_at
		FROM (
			%s
		) conversations
		ORDER BY updated_at DESC, agent_id ASC
		LIMIT $1
	`, strings.Join(sources, "\nUNION ALL\n")), limit)
	if err != nil {
		return nil, fmt.Errorf("list conversations: %w", err)
	}
	defer rows.Close()

	out := []ConversationSummary{}
	for rows.Next() {
		item, err := scanConversationSummary(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list conversations rows: %w", err)
	}
	return out, nil
}

func (r *SQLConversationReader) Get(ctx context.Context, sessionID string) (ConversationDetail, bool, error) {
	if r == nil || r.db == nil {
		return ConversationDetail{}, false, nil
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return ConversationDetail{}, false, nil
	}
	caps, err := r.resolveCapabilities(ctx)
	if err != nil {
		return ConversationDetail{}, false, err
	}
	sources := conversationQuerySources(caps)
	if len(sources) == 0 {
		return ConversationDetail{}, false, nil
	}

	row := r.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT
			session_id,
			agent_id,
			kind,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			COALESCE(runtime_state, '{}'::jsonb),
			COALESCE(conversation, '[]'::jsonb),
			updated_at
		FROM (
			%s
		) conversations
		WHERE session_id = $1
		LIMIT 1
	`, strings.Join(sources, "\nUNION ALL\n")), sessionID)

	item, err := scanConversationDetail(row)
	if err == sql.ErrNoRows {
		return ConversationDetail{}, false, nil
	}
	if err != nil {
		return ConversationDetail{}, false, fmt.Errorf("get conversation: %w", err)
	}
	item.Turns, err = r.loadConversationTurns(ctx, item.AgentID, item.SessionID)
	if err != nil {
		return ConversationDetail{}, false, fmt.Errorf("load conversation turns: %w", err)
	}
	return item, true, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanConversationSummary(scanner rowScanner) (ConversationSummary, error) {
	var (
		item            ConversationSummary
		runtimeStateRaw []byte
	)
	if err := scanner.Scan(
		&item.SessionID,
		&item.AgentID,
		&item.Kind,
		&item.ScopeKey,
		&item.Scope,
		&item.RuntimeMode,
		&item.Status,
		&item.TurnCount,
		&runtimeStateRaw,
		&item.UpdatedAt,
	); err != nil {
		return ConversationSummary{}, err
	}
	runtimeState, err := store.DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return ConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = runtimeState.Summary
	item.Metadata = projectConversationSummaryMetadata(runtimeState)
	return item, nil
}

func scanConversationDetail(scanner rowScanner) (ConversationDetail, error) {
	var (
		item            ConversationDetail
		runtimeStateRaw []byte
		messagesRaw     []byte
	)
	if err := scanner.Scan(
		&item.SessionID,
		&item.AgentID,
		&item.Kind,
		&item.ScopeKey,
		&item.Scope,
		&item.RuntimeMode,
		&item.Status,
		&item.TurnCount,
		&runtimeStateRaw,
		&messagesRaw,
		&item.UpdatedAt,
	); err != nil {
		return ConversationDetail{}, err
	}
	runtimeState, err := store.DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return ConversationDetail{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = runtimeState.Summary
	item.RuntimeState = projectConversationRuntimeState(runtimeState)
	item.Messages, err = decodeJSONArray[ConversationMessage](messagesRaw)
	if err != nil {
		return ConversationDetail{}, fmt.Errorf("decode conversation messages: %w", err)
	}
	if item.Messages == nil {
		item.Messages = []ConversationMessage{}
	}
	return item, nil
}

func decodeJSONObjectRaw(raw []byte) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), raw...), nil
}

func decodeJSONMap(raw []byte) (map[string]any, error) {
	if len(raw) == 0 {
		return map[string]any{}, nil
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeJSONArray[T any](raw []byte) ([]T, error) {
	if len(raw) == 0 {
		return []T{}, nil
	}
	out := []T{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func decodeJSONStringMap(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return map[string]string{}, nil
	}
	out := map[string]string{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *SQLConversationReader) loadConversationTurns(ctx context.Context, agentID, sessionID string) ([]ConversationTurn, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	agentID = strings.TrimSpace(agentID)
	sessionID = strings.TrimSpace(sessionID)
	if agentID == "" || sessionID == "" {
		return []ConversationTurn{}, nil
	}
	caps, err := r.resolveCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if caps.Conversations.Turns != store.SchemaFlavorCanonical {
		return []ConversationTurn{}, nil
	}
	query := `
		SELECT
			turn_id::text,
			agent_id,
			session_id::text,
			COALESCE(runtime_mode, ''),
			COALESCE(scope_key, ''),
			COALESCE(entity_id::text, ''),
			COALESCE(trigger_event_id::text, ''),
			COALESCE(trigger_event_type, ''),
			COALESCE(task_id, ''),
			COALESCE(available_tools, '[]'::jsonb),
			COALESCE(tool_calls, '[]'::jsonb),
			COALESCE(emitted_events, '[]'::jsonb),
			COALESCE(mcp_servers, '{}'::jsonb),
			COALESCE(mcp_tools_listed, '[]'::jsonb),
			COALESCE(mcp_tools_visible, '[]'::jsonb),
			COALESCE(request_payload, '{}'::jsonb),
			COALESCE(response_payload, '{}'::jsonb),
			COALESCE(turn_blocks, '[]'::jsonb),
			parse_ok,
			COALESCE(latency_ms, 0),
			COALESCE(retry_count, 0),
			COALESCE(error, ''),
			created_at
		FROM agent_turns
		WHERE agent_id = $1
		  AND session_id = $2::uuid
		ORDER BY created_at ASC, turn_id ASC
	`
	if !caps.Conversations.TurnBlocks {
		query = `
		SELECT
			turn_id::text,
			agent_id,
			session_id::text,
			COALESCE(runtime_mode, ''),
			COALESCE(scope_key, ''),
			COALESCE(entity_id::text, ''),
			COALESCE(trigger_event_id::text, ''),
			COALESCE(trigger_event_type, ''),
			COALESCE(task_id, ''),
			COALESCE(available_tools, '[]'::jsonb),
			COALESCE(tool_calls, '[]'::jsonb),
			COALESCE(emitted_events, '[]'::jsonb),
			COALESCE(mcp_servers, '{}'::jsonb),
			COALESCE(mcp_tools_listed, '[]'::jsonb),
			COALESCE(mcp_tools_visible, '[]'::jsonb),
			COALESCE(request_payload, '{}'::jsonb),
			COALESCE(response_payload, '{}'::jsonb),
			'[]'::jsonb AS turn_blocks,
			parse_ok,
			COALESCE(latency_ms, 0),
			COALESCE(retry_count, 0),
			COALESCE(error, ''),
			created_at
		FROM agent_turns
		WHERE agent_id = $1
		  AND session_id = $2::uuid
		ORDER BY created_at ASC, turn_id ASC
		`
	}
	rows, err := r.db.QueryContext(ctx, query, agentID, sessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []ConversationTurn{}
	for rows.Next() {
		item, err := scanConversationTurn(rows)
		if err != nil {
			return nil, err
		}
		assistantText, outcome, reasoning, progress, toolResults := summarizeConversationTurnBlocks(item.TurnBlocks)
		item.AssistantVisibleOutput = assistantText
		item.Outcome = outcome
		item.ReasoningBlocks = reasoning
		item.ProgressUpdates = progress
		item.ToolResults = toolResults
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *SQLConversationReader) resolveCapabilities(ctx context.Context) (store.StoreSchemaCapabilities, error) {
	if r == nil || r.capSource == nil {
		return store.StoreSchemaCapabilities{
			Conversations: store.ConversationSchemaCapabilities{
				Sessions:   store.SchemaFlavorCanonical,
				Audits:     store.SchemaFlavorCanonical,
				Turns:      store.SchemaFlavorCanonical,
				TurnBlocks: true,
			},
		}, nil
	}
	return r.capSource.ResolveSchemaCapabilities(ctx)
}

func conversationQuerySources(caps store.StoreSchemaCapabilities) []string {
	sources := []string{}
	if caps.Conversations.Sessions == store.SchemaFlavorCanonical {
		sources = append(sources, `
			SELECT
				session_id::text AS session_id,
				agent_id,
				'live_session' AS kind,
				scope_key,
				scope,
				runtime_mode,
				status,
				turn_count,
				runtime_state,
				conversation,
				updated_at,
				created_at
			FROM agent_sessions
			WHERE status = 'active'
			  AND runtime_mode IN ('session', 'session_per_entity')
		`)
	}
	if taskSource := store.CanonicalTaskConversationVisibilitySourceSQL(caps.Conversations); taskSource != "" {
		sources = append(sources, fmt.Sprintf(`
			SELECT
				session_id,
				agent_id,
				'turn_audit' AS kind,
				scope_key,
				scope,
				runtime_mode,
				status,
				turn_count,
				runtime_state,
				conversation,
				updated_at,
				created_at
			FROM (
				%s
			) task_conversations
		`, taskSource))
	}
	return sources
}

func scanConversationTurn(scanner rowScanner) (ConversationTurn, error) {
	var (
		item                                  ConversationTurn
		availableToolsRaw, toolCallsRaw       []byte
		emittedEventsRaw, mcpServersRaw       []byte
		mcpToolsListedRaw, mcpToolsVisibleRaw []byte
		requestPayloadRaw, responsePayloadRaw []byte
		turnBlocksRaw                         []byte
		createdAt                             time.Time
	)
	if err := scanner.Scan(
		&item.TurnID,
		&item.AgentID,
		&item.SessionID,
		&item.RuntimeMode,
		&item.ScopeKey,
		&item.EntityID,
		&item.TriggerEventID,
		&item.TriggerEventType,
		&item.TaskID,
		&availableToolsRaw,
		&toolCallsRaw,
		&emittedEventsRaw,
		&mcpServersRaw,
		&mcpToolsListedRaw,
		&mcpToolsVisibleRaw,
		&requestPayloadRaw,
		&responsePayloadRaw,
		&turnBlocksRaw,
		&item.ParseOK,
		&item.LatencyMS,
		&item.RetryCount,
		&item.Error,
		&createdAt,
	); err != nil {
		return ConversationTurn{}, err
	}
	item.CreatedAt = createdAt.Format(time.RFC3339Nano)
	var err error
	summary, hasSummary, err := decodeTurnSummaryProjection(turnBlocksRaw)
	if err != nil {
		return ConversationTurn{}, err
	}
	if item.AvailableTools, err = decodeJSONArray[string](availableToolsRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn available_tools: %w", err)
	}
	if item.ToolCalls, err = decodeJSONArray[ConversationToolCall](toolCallsRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn tool_calls: %w", err)
	}
	if item.EmittedEvents, err = decodeJSONArray[string](emittedEventsRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn emitted_events: %w", err)
	}
	if item.MCPToolsListed, err = decodeJSONArray[string](mcpToolsListedRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn mcp_tools_listed: %w", err)
	}
	if item.MCPToolsVisible, err = decodeJSONArray[string](mcpToolsVisibleRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn mcp_tools_visible: %w", err)
	}
	if item.MCPServers, err = decodeJSONStringMap(mcpServersRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn mcp_servers: %w", err)
	}
	if item.RequestPayload, err = decodeJSONObjectRaw(requestPayloadRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn request_payload: %w", err)
	}
	if item.ResponsePayload, err = decodeJSONObjectRaw(responsePayloadRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn response_payload: %w", err)
	}
	if len(turnBlocksRaw) > 0 {
		if item.TurnBlocks, err = decodeJSONArray[ConversationTurnBlock](turnBlocksRaw); err != nil {
			return ConversationTurn{}, fmt.Errorf("decode turn turn_blocks: %w", err)
		}
	}
	if hasSummary {
		item.AssistantVisibleOutput, item.Outcome, item.ReasoningBlocks, item.ProgressUpdates, item.ToolResults = projectedTurnSummaryConversationFields(summary)
	}
	return item, nil
}

func summarizeConversationTurnBlocks(blocks []ConversationTurnBlock) (string, string, []string, []string, []ConversationToolResult) {
	raw, err := json.Marshal(blocks)
	if err != nil {
		return "", "", nil, nil, nil
	}
	summary, ok, err := decodeTurnSummaryProjection(raw)
	if err != nil || !ok {
		return "", "", nil, nil, nil
	}
	return projectedTurnSummaryConversationFields(summary)
}

func readString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}
