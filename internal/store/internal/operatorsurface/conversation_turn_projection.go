package operatorsurface

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimerunfork "github.com/division-sh/swarm/internal/runtime/runfork"
	runtimeturnactivity "github.com/division-sh/swarm/internal/runtime/turnactivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

const (
	defaultOperatorConversationTurnPageSize = 50
	maxOperatorConversationTurnPageSize     = 500
)

type conversationTurnCursor struct {
	Kind        string `json:"kind"`
	CompletedAt string `json:"completed_at"`
	TurnID      string `json:"turn_id"`
}

type conversationTurnRecord struct {
	TurnID           string
	Ordinal          int
	RunID            string
	AgentID          string
	SessionID        string
	EntityID         string
	TriggerEventID   string
	TriggerEventType string
	TaskID           string
	TurnBlocksRaw    []byte
	ParseOK          bool
	LatencyMS        int
	RetryCount       int
	UsageExactness   string
	ExecutionMode    string
	InputTokens      sql.NullInt64
	OutputTokens     sql.NullInt64
	Failure          *runtimefailures.Envelope
	CreatedAt        time.Time
}

type ConversationTurnRecord = conversationTurnRecord

type conversationProjection struct {
	postgres    *postgresbackend.Backend
	sqlite      *sqlitebackend.Backend
	schemaGuard func() error
}

func (s conversationProjection) requireCurrentSchema() error {
	if (s.postgres == nil) == (s.sqlite == nil) {
		return fmt.Errorf("conversation projection requires exactly one backend")
	}
	return requireSchema("conversation projection", s.schemaGuard)
}

func (s conversationProjection) bind(query string) string {
	if s.sqlite != nil {
		return query
	}
	var out strings.Builder
	out.Grow(len(query) + 16)
	index := 1
	for _, r := range query {
		if r == '?' {
			fmt.Fprintf(&out, "$%d", index)
			index++
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

func (s conversationProjection) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	query = s.bind(query)
	if s.sqlite != nil {
		return s.sqlite.QueryRowContext(ctx, query, args...)
	}
	return s.postgres.QueryRowContext(ctx, query, args...)
}

func (s conversationProjection) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	query = s.bind(query)
	if s.sqlite != nil {
		return s.sqlite.QueryContext(ctx, query, args...)
	}
	return s.postgres.QueryContext(ctx, query, args...)
}

func (s conversationProjection) conversationQuerySources() []string {
	if s.sqlite != nil {
		return sqliteOperatorConversationQuerySources()
	}
	return operatorConversationQuerySources()
}

func (s *ConversationPostgres) projection() conversationProjection {
	return conversationProjection{postgres: s.backend, schemaGuard: s.schemaGuard}
}

func (s *ConversationSQLite) projection() conversationProjection {
	return conversationProjection{sqlite: s.backend, schemaGuard: s.schemaGuard}
}

func (s *ConversationPostgres) ListOperatorConversationTurns(ctx context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	return s.projection().listOperatorConversationTurns(ctx, opts)
}

func (s *ConversationSQLite) ListOperatorConversationTurns(ctx context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	return s.projection().listOperatorConversationTurns(ctx, opts)
}

func (s *ConversationPostgres) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	return s.projection().loadOperatorConversationTurn(ctx, sessionID, turnID)
}

func (s *ConversationSQLite) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	return s.projection().loadOperatorConversationTurn(ctx, sessionID, turnID)
}

func (s *ConversationPostgres) LoadLatestPublicConversationTurn(ctx context.Context, sessionID string) (*operatorread.OperatorPublicConversationTurn, error) {
	return s.projection().loadLatestPublicConversationTurn(ctx, sessionID)
}

func (s *ConversationSQLite) LoadLatestPublicConversationTurn(ctx context.Context, sessionID string) (*operatorread.OperatorPublicConversationTurn, error) {
	return s.projection().loadLatestPublicConversationTurn(ctx, sessionID)
}

func (s *ConversationPostgres) LoadConversationForkSource(ctx context.Context, sessionID string) (runtimerunfork.ConversationForkSource, error) {
	return s.projection().loadConversationForkSource(ctx, sessionID)
}

func (s *ConversationSQLite) LoadConversationForkSource(ctx context.Context, sessionID string) (runtimerunfork.ConversationForkSource, error) {
	return s.projection().loadConversationForkSource(ctx, sessionID)
}

func (s conversationProjection) loadConversationForkSource(ctx context.Context, rawSessionID string) (runtimerunfork.ConversationForkSource, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(rawSessionID))
	if err != nil {
		return runtimerunfork.ConversationForkSource{}, &operatorread.EntityReadParamError{Field: "source_session_id", Reason: "must be a UUID"}
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunfork.ConversationForkSource{}, err
	}
	rows, err := s.query(ctx, fmt.Sprintf(`
		SELECT
			session_id, agent_id, agent_name_owner, agent_name_source, agent_route_presence,
			flow_scope_key, flow_instance_id, flow_instance, run_id
		FROM (
			%s
		) conversations
		WHERE session_id = ?
		LIMIT 2
	`, strings.Join(s.conversationQuerySources(), "\nUNION ALL\n")), parsed.String())
	if err != nil {
		return runtimerunfork.ConversationForkSource{}, fmt.Errorf("load conversation fork source: %w", err)
	}
	defer rows.Close()

	items := []runtimerunfork.ConversationForkSource{}
	for rows.Next() {
		var item runtimerunfork.ConversationForkSource
		var identityFields runtimeagentidentity.StorageFields
		if err := rows.Scan(
			&item.SessionID,
			&identityFields.AgentID,
			&identityFields.NameOwner,
			&identityFields.NameSource,
			&identityFields.RoutePresence,
			&identityFields.FlowScopeKey,
			&identityFields.FlowInstanceID,
			&identityFields.FlowInstancePath,
			&item.RunID,
		); err != nil {
			return runtimerunfork.ConversationForkSource{}, err
		}
		item.SessionID = strings.TrimSpace(item.SessionID)
		item.RunID = strings.TrimSpace(item.RunID)
		item.Identity, err = runtimeagentidentity.FromStorageFields(identityFields)
		if err != nil {
			return runtimerunfork.ConversationForkSource{}, fmt.Errorf("load conversation fork source identity: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return runtimerunfork.ConversationForkSource{}, err
	}
	if len(items) == 0 {
		return runtimerunfork.ConversationForkSource{}, operatorread.ErrSessionNotFound
	}
	if len(items) > 1 {
		return runtimerunfork.ConversationForkSource{}, &operatorread.EntityReadParamError{Field: "source_session_id", Reason: "ambiguous source session"}
	}
	return items[0], nil
}

func (s conversationProjection) loadOperatorConversationSummary(ctx context.Context, sessionID string) (operatorread.OperatorConversationSummary, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return operatorread.OperatorConversationSummary{}, operatorread.ErrSessionNotFound
	}
	if err := s.requireCurrentSchema(); err != nil {
		return operatorread.OperatorConversationSummary{}, err
	}
	sources := s.conversationQuerySources()
	if len(sources) == 0 {
		return operatorread.OperatorConversationSummary{}, operatorread.ErrSessionNotFound
	}
	row := s.queryRow(ctx, fmt.Sprintf(`
		SELECT CAST(session_id AS TEXT), agent_id, COALESCE(CAST(run_id AS TEXT), ''), kind,
			COALESCE(flow_instance, ''), memory_enabled, memory_source,
			COALESCE(status, ''), COALESCE(turn_count, 0), COALESCE(message_count, 0),
			COALESCE(CAST(runtime_state AS TEXT), '{}'), started_at, ended_at, updated_at
		FROM (
			%s
		) conversations
		WHERE CAST(session_id AS TEXT) = ?
		LIMIT 2
	`, strings.Join(sources, "\nUNION ALL\n")), sessionID)
	var (
		item            operatorread.OperatorConversationSummary
		runtimeStateRaw []byte
		startedAtRaw    any
		endedAtRaw      any
		updatedAtRaw    any
	)
	if err := row.Scan(
		&item.SessionID, &item.AgentID, &item.RunID, &item.Kind, &item.FlowInstance,
		&item.Memory, &item.MemorySource, &item.Status, &item.TurnCount, &item.MessageCount,
		&runtimeStateRaw, &startedAtRaw, &endedAtRaw, &updatedAtRaw,
	); errors.Is(err, sql.ErrNoRows) {
		return operatorread.OperatorConversationSummary{}, operatorread.ErrSessionNotFound
	} else if err != nil {
		return operatorread.OperatorConversationSummary{}, operatorConversationReadQueryError("load conversation summary", err)
	}
	startedAt, err := requiredConversationTime(startedAtRaw, "started_at")
	if err != nil {
		return operatorread.OperatorConversationSummary{}, err
	}
	item.StartedAt = startedAt
	if endedAt, valid, scanErr := sqliteTimeValue(endedAtRaw); scanErr != nil {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("decode conversation ended_at: %w", scanErr)
	} else if valid {
		endedAt = endedAt.UTC()
		item.EndedAt = &endedAt
	}
	updatedAt, err := requiredConversationTime(updatedAtRaw, "updated_at")
	if err != nil {
		return operatorread.OperatorConversationSummary{}, err
	}
	item.UpdatedAt = updatedAt
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = runtimeState.Summary
	item.Metadata = projectOperatorConversationSummaryMetadata(runtimeState)
	if err := s.queryRow(ctx, `
		SELECT execution_mode
		FROM agent_turns
		WHERE session_id = ?
		ORDER BY created_at DESC, turn_id DESC
		LIMIT 1
	`, sessionID).Scan(&item.ExecutionMode); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("load conversation execution mode: %w", err)
	}
	if item.ExecutionMode != "" && item.ExecutionMode != "live" && item.ExecutionMode != "mock" {
		return operatorread.OperatorConversationSummary{}, fmt.Errorf("invalid persisted conversation execution_mode %q", item.ExecutionMode)
	}
	return item, nil
}

func requiredConversationTime(raw any, field string) (time.Time, error) {
	value, valid, err := sqliteTimeValue(raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("decode conversation %s: %w", field, err)
	}
	if !valid {
		return time.Time{}, fmt.Errorf("decode conversation %s: value is required", field)
	}
	return value.UTC(), nil
}

func (s conversationProjection) listOperatorConversationTurns(ctx context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	if err := s.requirePublicConversationProjectionAccess(); err != nil {
		return operatorread.OperatorConversationTurnListResult{}, err
	}
	runIDProjection := "COALESCE(CAST(run_id AS TEXT), '')"
	opts, cursor, err := normalizeOperatorConversationTurnListOptions(opts)
	if err != nil {
		return operatorread.OperatorConversationTurnListResult{}, err
	}
	summary, err := s.loadOperatorConversationSummary(ctx, opts.SessionID)
	if err != nil {
		return operatorread.OperatorConversationTurnListResult{}, err
	}

	where := []string{"1 = 1"}
	args := []any{opts.SessionID}
	if cursor != nil {
		cursorAt, parseErr := time.Parse(time.RFC3339Nano, cursor.CompletedAt)
		if parseErr != nil {
			return operatorread.OperatorConversationTurnListResult{}, operatorread.ErrInvalidConversationCursor
		}
		where = append(where, "(created_at < ? OR (created_at = ? AND CAST(turn_id AS TEXT) < ?))")
		args = append(args, cursorAt.UTC(), cursorAt.UTC(), cursor.TurnID)
	}
	args = append(args, opts.Limit+1)
	rows, err := s.query(ctx, fmt.Sprintf(`
		WITH ordered AS (
			SELECT
				ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC) AS ordinal,
				CAST(turn_id AS TEXT) AS turn_id, %s AS run_id,
				agent_id, CAST(session_id AS TEXT) AS session_id,
				COALESCE(CAST(entity_id AS TEXT), '') AS entity_id,
				COALESCE(CAST(trigger_event_id AS TEXT), '') AS trigger_event_id,
				COALESCE(trigger_event_type, '') AS trigger_event_type,
				COALESCE(task_id, '') AS task_id, COALESCE(CAST(turn_blocks AS TEXT), '[]') AS turn_blocks,
				parse_ok, COALESCE(latency_ms, 0) AS latency_ms, COALESCE(retry_count, 0) AS retry_count,
				COALESCE(usage_exactness, '') AS usage_exactness, execution_mode, input_tokens, output_tokens,
				COALESCE(CAST(failure AS TEXT), 'null') AS failure, created_at
			FROM agent_turns
			WHERE session_id = ?
		)
		SELECT ordinal, turn_id, run_id, agent_id, session_id, entity_id,
			trigger_event_id, trigger_event_type, task_id, turn_blocks, parse_ok, latency_ms,
			retry_count, usage_exactness, execution_mode, input_tokens, output_tokens, failure, created_at
		FROM ordered
		WHERE %s
		ORDER BY created_at DESC, turn_id DESC
		LIMIT ?
	`, runIDProjection, strings.Join(where, " AND ")), args...)
	if err != nil {
		return operatorread.OperatorConversationTurnListResult{}, fmt.Errorf("list conversation turns: %w", err)
	}
	defer rows.Close()

	turns := make([]operatorread.OperatorConversationTurnListItem, 0, opts.Limit+1)
	for rows.Next() {
		record, err := scanConversationTurnRecord(rows)
		if err != nil {
			return operatorread.OperatorConversationTurnListResult{}, err
		}
		turn, err := projectPublicConversationTurnListItem(record)
		if err != nil {
			return operatorread.OperatorConversationTurnListResult{}, err
		}
		turns = append(turns, turn)
	}
	if err := rows.Err(); err != nil {
		return operatorread.OperatorConversationTurnListResult{}, err
	}

	nextCursor := ""
	if len(turns) > opts.Limit {
		turns = turns[:opts.Limit]
		last := turns[len(turns)-1]
		nextCursor = encodeConversationTurnCursor(conversationTurnCursor{
			Kind:        "conversation.list_turns",
			CompletedAt: last.CompletedAt.UTC().Format(time.RFC3339Nano),
			TurnID:      last.TurnID,
		})
	}
	return operatorread.OperatorConversationTurnListResult{Conversation: summary, Turns: turns, NextCursor: nextCursor}, nil
}

func (s conversationProjection) loadOperatorConversationTurn(ctx context.Context, sessionID, turnID string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	if err := s.requirePublicConversationProjectionAccess(); err != nil {
		return operatorread.OperatorPublicConversationTurnDetail{}, err
	}
	runIDProjection := "COALESCE(CAST(run_id AS TEXT), '')"
	sessionID = strings.TrimSpace(sessionID)
	turnID = strings.TrimSpace(turnID)
	if sessionID == "" {
		return operatorread.OperatorPublicConversationTurnDetail{}, operatorread.ErrSessionNotFound
	}
	if turnID == "" {
		return operatorread.OperatorPublicConversationTurnDetail{}, operatorread.ErrTurnNotFound
	}
	summary, err := s.loadOperatorConversationSummary(ctx, sessionID)
	if err != nil {
		return operatorread.OperatorPublicConversationTurnDetail{}, err
	}
	row := s.queryRow(ctx, fmt.Sprintf(`
		WITH ordered AS (
			SELECT
				ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC) AS ordinal,
				CAST(turn_id AS TEXT) AS turn_id, %s AS run_id,
				agent_id, CAST(session_id AS TEXT) AS session_id,
				COALESCE(CAST(entity_id AS TEXT), '') AS entity_id,
				COALESCE(CAST(trigger_event_id AS TEXT), '') AS trigger_event_id,
				COALESCE(trigger_event_type, '') AS trigger_event_type,
				COALESCE(task_id, '') AS task_id, COALESCE(CAST(turn_blocks AS TEXT), '[]') AS turn_blocks,
				parse_ok, COALESCE(latency_ms, 0) AS latency_ms, COALESCE(retry_count, 0) AS retry_count,
				COALESCE(usage_exactness, '') AS usage_exactness, execution_mode, input_tokens, output_tokens,
				COALESCE(CAST(failure AS TEXT), 'null') AS failure, created_at, agent_frame_bytes
			FROM agent_turns
			WHERE session_id = ?
		)
		SELECT ordinal, turn_id, run_id, agent_id, session_id, entity_id, trigger_event_id,
			trigger_event_type, task_id, turn_blocks, parse_ok, latency_ms, retry_count,
			usage_exactness, execution_mode, input_tokens, output_tokens, failure, created_at, agent_frame_bytes
		FROM ordered
		WHERE turn_id = ?
	`, runIDProjection), sessionID, turnID)
	record, frameBytes, err := scanConversationTurnDetailRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return operatorread.OperatorPublicConversationTurnDetail{}, operatorread.ErrTurnNotFound
	}
	if err != nil {
		return operatorread.OperatorPublicConversationTurnDetail{}, fmt.Errorf("load conversation turn: %w", err)
	}
	turn, err := projectPublicConversationTurn(record)
	if err != nil {
		return operatorread.OperatorPublicConversationTurnDetail{}, err
	}
	frame, err := projectConversationFrameFacts(frameBytes, turn)
	if err != nil {
		return operatorread.OperatorPublicConversationTurnDetail{}, err
	}
	return operatorread.OperatorPublicConversationTurnDetail{Session: summary, Turn: turn, Frame: frame}, nil
}

func (s conversationProjection) loadLatestPublicConversationTurn(ctx context.Context, sessionID string) (*operatorread.OperatorPublicConversationTurn, error) {
	if err := s.requirePublicConversationProjectionAccess(); err != nil {
		return nil, err
	}
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, operatorread.ErrSessionNotFound
	}
	row := s.queryRow(ctx, `
		WITH ordered AS (
			SELECT
				ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC) AS ordinal,
				CAST(turn_id AS TEXT) AS turn_id,
				COALESCE(CAST(run_id AS TEXT), '') AS run_id,
				agent_id, CAST(session_id AS TEXT) AS session_id,
				COALESCE(CAST(entity_id AS TEXT), '') AS entity_id,
				COALESCE(CAST(trigger_event_id AS TEXT), '') AS trigger_event_id,
				COALESCE(trigger_event_type, '') AS trigger_event_type,
				COALESCE(task_id, '') AS task_id,
				COALESCE(CAST(turn_blocks AS TEXT), '[]') AS turn_blocks,
				parse_ok, COALESCE(latency_ms, 0) AS latency_ms,
				COALESCE(retry_count, 0) AS retry_count,
				COALESCE(usage_exactness, '') AS usage_exactness,
				execution_mode, input_tokens, output_tokens,
				COALESCE(CAST(failure AS TEXT), 'null') AS failure,
				created_at
			FROM agent_turns
			WHERE session_id = ?
		)
		SELECT ordinal, turn_id, run_id, agent_id, session_id, entity_id,
			trigger_event_id, trigger_event_type, task_id, turn_blocks, parse_ok,
			latency_ms, retry_count, usage_exactness, execution_mode, input_tokens,
			output_tokens, failure, created_at
		FROM ordered
		ORDER BY created_at DESC, turn_id DESC
		LIMIT 1
	`, sessionID)
	record, err := scanConversationTurnRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load latest conversation turn: %w", err)
	}
	turn, err := projectPublicConversationTurn(record)
	if err != nil {
		return nil, err
	}
	return &turn, nil
}

func (s conversationProjection) requirePublicConversationProjectionAccess() error {
	return requireSchema("conversation projection", s.schemaGuard)
}

func normalizeOperatorConversationTurnListOptions(opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListOptions, *conversationTurnCursor, error) {
	opts.SessionID = strings.TrimSpace(opts.SessionID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.SessionID == "" {
		return opts, nil, operatorread.ErrSessionNotFound
	}
	if opts.Limit == 0 {
		opts.Limit = defaultOperatorConversationTurnPageSize
	}
	if opts.Limit < 1 || opts.Limit > maxOperatorConversationTurnPageSize {
		return opts, nil, &operatorread.EntityReadParamError{Field: "limit", Reason: "must be between 1 and 500"}
	}
	if opts.Cursor == "" {
		return opts, nil, nil
	}
	cursor, err := decodeConversationTurnCursor(opts.Cursor)
	if err != nil || cursor.Kind != "conversation.list_turns" || strings.TrimSpace(cursor.TurnID) == "" {
		return opts, nil, operatorread.ErrInvalidConversationCursor
	}
	if _, err := time.Parse(time.RFC3339Nano, cursor.CompletedAt); err != nil {
		return opts, nil, operatorread.ErrInvalidConversationCursor
	}
	return opts, &cursor, nil
}

func encodeConversationTurnCursor(cursor conversationTurnCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeConversationTurnCursor(raw string) (conversationTurnCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return conversationTurnCursor{}, err
	}
	var cursor conversationTurnCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return conversationTurnCursor{}, err
	}
	return cursor, nil
}

func scanConversationTurnRecord(scanner operatorRowScanner) (conversationTurnRecord, error) {
	var (
		record       conversationTurnRecord
		failureRaw   []byte
		createdAtRaw any
	)
	if err := scanner.Scan(
		&record.Ordinal, &record.TurnID, &record.RunID, &record.AgentID, &record.SessionID,
		&record.EntityID, &record.TriggerEventID, &record.TriggerEventType, &record.TaskID,
		&record.TurnBlocksRaw, &record.ParseOK, &record.LatencyMS, &record.RetryCount,
		&record.UsageExactness, &record.ExecutionMode, &record.InputTokens, &record.OutputTokens, &failureRaw, &createdAtRaw,
	); err != nil {
		return conversationTurnRecord{}, err
	}
	failure, err := decodeStoredFailure(failureRaw)
	if err != nil {
		return conversationTurnRecord{}, fmt.Errorf("decode conversation turn failure: %w", err)
	}
	record.Failure = failure
	createdAt, valid, err := sqliteTimeValue(createdAtRaw)
	if err != nil || !valid {
		if err == nil {
			err = fmt.Errorf("created_at is required")
		}
		return conversationTurnRecord{}, fmt.Errorf("decode conversation turn created_at: %w", err)
	}
	record.CreatedAt = createdAt.UTC()
	return record, nil
}

func scanConversationTurnDetailRecord(scanner operatorRowScanner) (conversationTurnRecord, []byte, error) {
	var (
		record       conversationTurnRecord
		failureRaw   []byte
		createdAtRaw any
		frameBytes   []byte
	)
	if err := scanner.Scan(
		&record.Ordinal, &record.TurnID, &record.RunID, &record.AgentID, &record.SessionID,
		&record.EntityID, &record.TriggerEventID, &record.TriggerEventType, &record.TaskID,
		&record.TurnBlocksRaw, &record.ParseOK, &record.LatencyMS, &record.RetryCount,
		&record.UsageExactness, &record.ExecutionMode, &record.InputTokens, &record.OutputTokens,
		&failureRaw, &createdAtRaw, &frameBytes,
	); err != nil {
		return conversationTurnRecord{}, nil, err
	}
	failure, err := decodeStoredFailure(failureRaw)
	if err != nil {
		return conversationTurnRecord{}, nil, fmt.Errorf("decode conversation turn failure: %w", err)
	}
	record.Failure = failure
	createdAt, valid, err := sqliteTimeValue(createdAtRaw)
	if err != nil || !valid {
		if err == nil {
			err = fmt.Errorf("created_at is required")
		}
		return conversationTurnRecord{}, nil, fmt.Errorf("decode conversation turn created_at: %w", err)
	}
	record.CreatedAt = createdAt.UTC()
	return record, append([]byte(nil), frameBytes...), nil
}

func projectConversationFrameFacts(raw []byte, turn operatorread.OperatorPublicConversationTurn) (operatorread.OperatorConversationFrameFacts, error) {
	frame, err := agentframe.DecodeDurable(raw)
	if err != nil {
		return operatorread.OperatorConversationFrameFacts{}, fmt.Errorf("hydrate public conversation frame facts: %w", err)
	}
	if frame.FrameID != "agent-frame:v1:"+strings.TrimSpace(turn.TurnID) || frame.Turn.Event.ID != strings.TrimSpace(turn.TriggerEventID) ||
		frame.Turn.Event.Type != strings.TrimSpace(turn.TriggerEventType) || frame.Turn.Event.RunID != strings.TrimSpace(turn.RunID) ||
		frame.Session.AgentIdentity.AgentID() != strings.TrimSpace(turn.AgentID) {
		return operatorread.OperatorConversationFrameFacts{}, fmt.Errorf("persisted execution frame does not match public turn identity")
	}
	return operatorread.OperatorConversationFrameFacts{
		Version: frame.Version, FrameID: frame.FrameID, ContentHash: frame.ContentHash,
		TurnKind: string(frame.Turn.Kind), ParentFrameID: frame.Turn.ParentFrameID,
	}, nil
}

func projectPublicConversationTurn(record conversationTurnRecord) (operatorread.OperatorPublicConversationTurn, error) {
	var blocks []runtimellm.TurnBlock
	raw := bytes.TrimSpace(record.TurnBlocksRaw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		raw = []byte("[]")
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return operatorread.OperatorPublicConversationTurn{}, fmt.Errorf("decode public conversation turn blocks: %w", err)
	}
	activity, assistantOutput, outcome, err := projectAuthorSafeTurnActivity(blocks, record.ParseOK)
	if err != nil {
		return operatorread.OperatorPublicConversationTurn{}, err
	}
	if record.Failure != nil {
		activity = append(activity, operatorread.OperatorConversationActivity{Kind: "failure"})
	}
	if activity == nil {
		activity = []operatorread.OperatorConversationActivity{}
	}
	turn := operatorread.OperatorPublicConversationTurn{
		TurnID:                 strings.TrimSpace(record.TurnID),
		ExecutionMode:          strings.TrimSpace(record.ExecutionMode),
		Ordinal:                record.Ordinal,
		CompletedAt:            record.CreatedAt,
		DurationMS:             record.LatencyMS,
		TriggerEventID:         strings.TrimSpace(record.TriggerEventID),
		TriggerEventType:       strings.TrimSpace(record.TriggerEventType),
		EntityID:               strings.TrimSpace(record.EntityID),
		TaskID:                 strings.TrimSpace(record.TaskID),
		Activity:               activity,
		Outcome:                strings.TrimSpace(outcome),
		ParseOK:                record.ParseOK,
		Failure:                runtimefailures.CloneEnvelope(record.Failure),
		AssistantVisibleOutput: strings.TrimSpace(assistantOutput),
		RetryCount:             record.RetryCount,
		AgentID:                strings.TrimSpace(record.AgentID),
		SessionID:              strings.TrimSpace(record.SessionID),
		RunID:                  strings.TrimSpace(record.RunID),
	}
	if turn.ExecutionMode != "live" && turn.ExecutionMode != "mock" {
		return operatorread.OperatorPublicConversationTurn{}, fmt.Errorf("invalid persisted conversation execution_mode %q", turn.ExecutionMode)
	}
	if record.UsageExactness != "" {
		switch record.UsageExactness {
		case "exact", "estimated":
			if !record.InputTokens.Valid || !record.OutputTokens.Valid || record.InputTokens.Int64 < 0 || record.OutputTokens.Int64 < 0 {
				return operatorread.OperatorPublicConversationTurn{}, fmt.Errorf("invalid persisted conversation token usage")
			}
			turn.Tokens = &operatorread.OperatorConversationTokenUsage{Input: record.InputTokens.Int64, Output: record.OutputTokens.Int64, Exactness: record.UsageExactness}
		case "unavailable":
			if record.InputTokens.Valid || record.OutputTokens.Valid {
				return operatorread.OperatorPublicConversationTurn{}, fmt.Errorf("unavailable persisted conversation usage includes token totals")
			}
		default:
			return operatorread.OperatorPublicConversationTurn{}, fmt.Errorf("invalid persisted conversation usage exactness %q", record.UsageExactness)
		}
	}
	return turn, nil
}

func ProjectPublicConversationTurn(record ConversationTurnRecord) (operatorread.OperatorPublicConversationTurn, error) {
	return projectPublicConversationTurn(record)
}

func projectPublicConversationTurnListItem(record conversationTurnRecord) (operatorread.OperatorConversationTurnListItem, error) {
	turn, err := projectPublicConversationTurn(record)
	if err != nil {
		return operatorread.OperatorConversationTurnListItem{}, err
	}
	return operatorConversationTurnListItemFromPublic(turn), nil
}

func operatorConversationTurnListItemFromPublic(turn operatorread.OperatorPublicConversationTurn) operatorread.OperatorConversationTurnListItem {
	counts := operatorread.OperatorConversationActivityCounts{}
	for _, item := range turn.Activity {
		switch item.Kind {
		case "dispatch":
			counts.Dispatch++
		case "tool":
			counts.Tool++
		case "tool_result":
			counts.ToolResult++
		case "publish":
			counts.Publish++
		case "output":
			counts.Output++
		case "failure":
			counts.Failure++
		}
	}
	return operatorread.OperatorConversationTurnListItem{
		TurnID:           turn.TurnID,
		ExecutionMode:    turn.ExecutionMode,
		Ordinal:          turn.Ordinal,
		CompletedAt:      turn.CompletedAt,
		DurationMS:       turn.DurationMS,
		TriggerEventID:   turn.TriggerEventID,
		TriggerEventType: turn.TriggerEventType,
		ActivityCounts:   counts,
		Tokens:           turn.Tokens,
		Outcome:          turn.Outcome,
		ParseOK:          turn.ParseOK,
		Failure:          runtimefailures.CloneEnvelope(turn.Failure),
	}
}

func OperatorConversationTurnListItemFromPublic(turn operatorread.OperatorPublicConversationTurn) operatorread.OperatorConversationTurnListItem {
	return operatorConversationTurnListItemFromPublic(turn)
}

func projectAuthorSafeTurnActivity(blocks []runtimellm.TurnBlock, parseOK bool) ([]operatorread.OperatorConversationActivity, string, string, error) {
	projected, assistantOutput, outcome, err := runtimeturnactivity.Project(blocks, parseOK)
	if err != nil {
		return nil, "", "", err
	}
	activity := make([]operatorread.OperatorConversationActivity, 0, len(projected))
	for _, item := range projected {
		activity = append(activity, operatorread.OperatorConversationActivity{
			Kind: item.Kind, EventID: item.EventID, EventType: item.EventType,
			ToolName: item.ToolName, ToolUseID: item.ToolUseID, Text: item.Text,
			OK: item.OK, BlockOrdinal: item.BlockOrdinal,
		})
	}
	return activity, assistantOutput, outcome, nil
}
