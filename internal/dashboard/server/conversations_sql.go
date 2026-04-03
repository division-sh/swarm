package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type SQLConversationReader struct {
	db *sql.DB
}

func NewSQLConversationReader(db *sql.DB) *SQLConversationReader {
	if db == nil {
		return nil
	}
	return &SQLConversationReader{db: db}
}

func (r *SQLConversationReader) List(ctx context.Context, limit int) ([]ConversationSummary, error) {
	if r == nil || r.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			agent_id,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			COALESCE(runtime_state, '{}'::jsonb),
			updated_at
		FROM agent_sessions
		WHERE status = 'active'
		ORDER BY updated_at DESC, agent_id ASC
		LIMIT $1
	`, limit)
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

func (r *SQLConversationReader) Get(ctx context.Context, agentID string) (ConversationDetail, bool, error) {
	if r == nil || r.db == nil {
		return ConversationDetail{}, false, nil
	}
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return ConversationDetail{}, false, nil
	}

	row := r.db.QueryRowContext(ctx, `
		SELECT
			session_id::text,
			agent_id,
			COALESCE(scope_key, ''),
			COALESCE(scope, ''),
			COALESCE(runtime_mode, ''),
			COALESCE(status, ''),
			COALESCE(turn_count, 0),
			COALESCE(runtime_state, '{}'::jsonb),
			COALESCE(conversation, '[]'::jsonb),
			updated_at
		FROM agent_sessions
		WHERE agent_id = $1
		  AND status = 'active'
		ORDER BY updated_at DESC, created_at DESC
		LIMIT 1
	`, agentID)

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
		&item.AgentID,
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
	runtimeState, err := decodeJSONMap(runtimeStateRaw)
	if err != nil {
		return ConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = readString(runtimeState["summary"])
	item.Metadata = compactMap(runtimeState, "summary", "last_turn")
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
	runtimeState, err := decodeJSONMap(runtimeStateRaw)
	if err != nil {
		return ConversationDetail{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = readString(runtimeState["summary"])
	item.RuntimeState = runtimeState
	item.Messages, err = decodeJSONArray(messagesRaw)
	if err != nil {
		return ConversationDetail{}, fmt.Errorf("decode conversation messages: %w", err)
	}
	if item.Messages == nil {
		item.Messages = []any{}
	}
	return item, nil
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

func decodeJSONArray(raw []byte) ([]any, error) {
	if len(raw) == 0 {
		return []any{}, nil
	}
	out := []any{}
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
	rows, err := r.db.QueryContext(ctx, `
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
	`, agentID, sessionID)
	if err != nil && shouldIgnoreConversationTurnBlocksColumn(err) {
		rows, err = r.db.QueryContext(ctx, `
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
	`, agentID, sessionID)
	}
	if err != nil {
		if shouldIgnoreConversationTurnsQuery(err) {
			return []ConversationTurn{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	out := []ConversationTurn{}
	for rows.Next() {
		item, err := scanConversationTurn(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, normalizeConversationTurn(item))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
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
	if item.AvailableTools, err = decodeJSONArray(availableToolsRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn available_tools: %w", err)
	}
	if item.ToolCalls, err = decodeJSONArray(toolCallsRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn tool_calls: %w", err)
	}
	if item.EmittedEvents, err = decodeJSONArray(emittedEventsRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn emitted_events: %w", err)
	}
	if item.MCPToolsListed, err = decodeJSONArray(mcpToolsListedRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn mcp_tools_listed: %w", err)
	}
	if item.MCPToolsVisible, err = decodeJSONArray(mcpToolsVisibleRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn mcp_tools_visible: %w", err)
	}
	if item.MCPServers, err = decodeJSONMap(mcpServersRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn mcp_servers: %w", err)
	}
	if item.RequestPayload, err = decodeJSONMap(requestPayloadRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn request_payload: %w", err)
	}
	if item.ResponsePayload, err = decodeJSONMap(responsePayloadRaw); err != nil {
		return ConversationTurn{}, fmt.Errorf("decode turn response_payload: %w", err)
	}
	if len(turnBlocksRaw) > 0 {
		if item.TurnBlocks, err = decodeJSONArray(turnBlocksRaw); err != nil {
			return ConversationTurn{}, fmt.Errorf("decode turn turn_blocks: %w", err)
		}
	}
	return item, nil
}

func normalizeConversationTurn(item ConversationTurn) ConversationTurn {
	assistantText, outcome, reasoning, progress, toolResults := summarizeConversationTurn(item)
	item.AssistantVisibleOutput = assistantText
	item.Outcome = outcome
	item.ReasoningBlocks = reasoning
	item.ProgressUpdates = progress
	item.ToolResults = toolResults
	return item
}

func summarizeConversationTurn(item ConversationTurn) (string, string, []string, []string, []any) {
	if len(item.TurnBlocks) > 0 {
		return summarizeConversationTurnBlocks(item.TurnBlocks)
	}
	return summarizeConversationTurnResponse(item.ResponsePayload)
}

func summarizeConversationTurnResponse(payload map[string]any) (string, string, []string, []string, []any) {
	if len(payload) == 0 {
		return "", "", nil, nil, nil
	}
	assistantText := strings.TrimSpace(readString(payload["result"]))
	if assistantText == "" {
		assistantText = strings.TrimSpace(readString(payload["assistant_text"]))
	}
	outcome := assistantText
	reasoning := []string{}
	progress := []string{}
	toolResults := []any{}

	raw := strings.TrimSpace(readString(payload["raw"]))
	if raw == "" {
		return assistantText, outcome, reasoning, progress, toolResults
	}
	lines := strings.Split(raw, "\n")
	assistantSeen := map[string]struct{}{}
	reasoningSeen := map[string]struct{}{}
	progressSeen := map[string]struct{}{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		obj := map[string]any{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		switch strings.TrimSpace(readString(obj["type"])) {
		case "assistant":
			blocks := cliContentBlocks(obj)
			texts, thinks := parseConversationBlocks(blocks)
			for _, text := range texts {
				if text == "" {
					continue
				}
				if _, ok := assistantSeen[text]; ok {
					continue
				}
				assistantSeen[text] = struct{}{}
				if assistantText == "" {
					assistantText = text
				} else if looksLikeProgressUpdate(text) {
					if _, ok := progressSeen[text]; !ok {
						progressSeen[text] = struct{}{}
						progress = append(progress, text)
					}
				}
			}
			for _, thought := range thinks {
				if thought == "" {
					continue
				}
				if _, ok := reasoningSeen[thought]; ok {
					continue
				}
				reasoningSeen[thought] = struct{}{}
				reasoning = append(reasoning, thought)
			}
		case "user":
			for _, entry := range cliToolResults(obj) {
				toolResults = append(toolResults, entry)
			}
		case "result":
			if text := strings.TrimSpace(readString(obj["result"])); text != "" {
				if assistantText == "" {
					assistantText = text
				}
				outcome = text
			}
		}
	}
	if outcome == "" {
		outcome = assistantText
	}
	return assistantText, outcome, reasoning, progress, toolResults
}

func summarizeConversationTurnBlocks(blocks []any) (string, string, []string, []string, []any) {
	assistantText := ""
	outcome := ""
	reasoning := []string{}
	progress := []string{}
	toolResults := []any{}
	for _, raw := range blocks {
		entry, _ := raw.(map[string]any)
		switch strings.TrimSpace(readString(entry["kind"])) {
		case "assistant_text":
			if text := strings.TrimSpace(readString(entry["text"])); text != "" {
				assistantText = text
			}
		case "outcome":
			if text := strings.TrimSpace(readString(entry["text"])); text != "" {
				outcome = text
				if assistantText == "" {
					assistantText = text
				}
			}
		case "reasoning":
			if text := strings.TrimSpace(readString(entry["text"])); text != "" {
				reasoning = append(reasoning, text)
			}
		case "progress":
			if text := strings.TrimSpace(readString(entry["text"])); text != "" {
				progress = append(progress, text)
			}
		case "tool_result":
			toolResults = append(toolResults, entry)
		}
	}
	if outcome == "" {
		outcome = assistantText
	}
	return assistantText, outcome, reasoning, progress, toolResults
}

func cliContentBlocks(obj map[string]any) []any {
	if content, ok := obj["content"].([]any); ok {
		return content
	}
	msg, _ := obj["message"].(map[string]any)
	if content, ok := msg["content"].([]any); ok {
		return content
	}
	return nil
}

func parseConversationBlocks(blocks []any) ([]string, []string) {
	texts := []string{}
	reasoning := []string{}
	for _, block := range blocks {
		entry, _ := block.(map[string]any)
		if len(entry) == 0 {
			continue
		}
		switch strings.TrimSpace(readString(entry["type"])) {
		case "text":
			if text := strings.TrimSpace(readString(entry["text"])); text != "" {
				texts = append(texts, text)
			}
		case "thinking":
			if thought := strings.TrimSpace(firstReadableString(
				readString(entry["thinking"]),
				readString(entry["text"]),
			)); thought != "" {
				reasoning = append(reasoning, thought)
			}
		default:
			if thought := strings.TrimSpace(readString(entry["thinking"])); thought != "" {
				reasoning = append(reasoning, thought)
			}
		}
	}
	return texts, reasoning
}

func cliToolResults(obj map[string]any) []any {
	blocks := cliContentBlocks(obj)
	if len(blocks) == 0 {
		return nil
	}
	out := []any{}
	for _, item := range blocks {
		entry, _ := item.(map[string]any)
		if strings.TrimSpace(readString(entry["type"])) != "tool_result" {
			continue
		}
		result := map[string]any{
			"tool_use_id": readString(entry["tool_use_id"]),
		}
		if content, ok := entry["content"].([]any); ok && len(content) > 0 {
			result["content"] = content
			if first, ok := content[0].(map[string]any); ok {
				if text := strings.TrimSpace(readString(first["text"])); text != "" {
					var decoded any
					if json.Unmarshal([]byte(text), &decoded) == nil {
						result["output"] = decoded
					} else {
						result["output"] = text
					}
				}
			}
		}
		out = append(out, result)
	}
	return out
}

func looksLikeProgressUpdate(text string) bool {
	text = strings.TrimSpace(strings.ToLower(text))
	if text == "" {
		return false
	}
	for _, prefix := range []string{
		"i'll ",
		"i will ",
		"starting ",
		"checking ",
		"reviewing ",
		"analyzing ",
		"searching ",
		"scheduling ",
	} {
		if strings.HasPrefix(text, prefix) {
			return true
		}
	}
	return false
}

func firstReadableString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func shouldIgnoreConversationTurnsQuery(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, `relation "agent_turns" does not exist`) ||
		strings.Contains(msg, `column "session_id" does not exist`)
}

func shouldIgnoreConversationTurnBlocksColumn(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, `column "turn_blocks" does not exist`)
}

func compactMap(in map[string]any, keys ...string) map[string]any {
	if len(in) == 0 {
		return nil
	}
	drop := map[string]struct{}{}
	for _, key := range keys {
		drop[key] = struct{}{}
	}
	out := map[string]any{}
	for key, value := range in {
		if _, skip := drop[key]; skip {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func readString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	default:
		return ""
	}
}
