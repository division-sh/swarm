package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
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
