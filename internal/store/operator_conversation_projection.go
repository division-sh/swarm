package store

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

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
)

const (
	defaultOperatorConversationTurnPageSize = 50
	maxOperatorConversationTurnPageSize     = 500
)

type OperatorConversationTurnListOptions struct {
	SessionID string
	Limit     int
	Cursor    string
}

type OperatorConversationTurnListResult struct {
	Conversation OperatorConversationSummary      `json:"conversation"`
	Turns        []OperatorPublicConversationTurn `json:"turns"`
	NextCursor   string                           `json:"next_cursor,omitempty"`
}

type OperatorConversationTokenUsage struct {
	Input     int64  `json:"input"`
	Output    int64  `json:"output"`
	Exactness string `json:"exactness"`
}

type OperatorConversationActivity struct {
	Kind      string `json:"kind"`
	ToolName  string `json:"tool_name,omitempty"`
	ToolUseID string `json:"tool_use_id,omitempty"`
	EventID   string `json:"event_id,omitempty"`
	EventType string `json:"event_type,omitempty"`
	Text      string `json:"text,omitempty"`
	OK        *bool  `json:"ok,omitempty"`
}

// OperatorPublicConversationTurn is the closed public projection of one persisted
// turn. Raw provider payloads and private evidence never enter this type.
type OperatorPublicConversationTurn struct {
	TurnID                 string                          `json:"turn_id"`
	Ordinal                int                             `json:"ordinal"`
	CompletedAt            time.Time                       `json:"completed_at"`
	DurationMS             int                             `json:"duration_ms"`
	TriggerEventID         string                          `json:"trigger_event_id,omitempty"`
	TriggerEventType       string                          `json:"trigger_event_type,omitempty"`
	EntityID               string                          `json:"entity_id,omitempty"`
	TaskID                 string                          `json:"task_id,omitempty"`
	Activity               []OperatorConversationActivity  `json:"activity"`
	Tokens                 *OperatorConversationTokenUsage `json:"tokens,omitempty"`
	Outcome                string                          `json:"outcome,omitempty"`
	ParseOK                bool                            `json:"parse_ok"`
	Failure                *runtimefailures.Envelope       `json:"failure,omitempty"`
	AssistantVisibleOutput string                          `json:"assistant_visible_output,omitempty"`
	RetryCount             int                             `json:"retry_count,omitempty"`
	AgentID                string                          `json:"-"`
	SessionID              string                          `json:"-"`
	RunID                  string                          `json:"-"`
}

type OperatorPublicConversationTurnDetail struct {
	Session OperatorConversationSummary    `json:"session"`
	Turn    OperatorPublicConversationTurn `json:"turn"`
}

type operatorPublicConversationProjectionSource interface {
	ListOperatorConversationTurns(context.Context, OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error)
}

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
	InputTokens      sql.NullInt64
	OutputTokens     sql.NullInt64
	Failure          *runtimefailures.Envelope
	CreatedAt        time.Time
}

func (s *PostgresStore) ListOperatorConversationTurns(ctx context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return OperatorConversationTurnListResult{}, err
	}
	return owner.listOperatorConversationTurns(ctx, opts)
}

func (s *SQLiteRuntimeStore) ListOperatorConversationTurns(ctx context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return OperatorConversationTurnListResult{}, err
	}
	return owner.listOperatorConversationTurns(ctx, opts)
}

func (s *PostgresStore) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return OperatorPublicConversationTurnDetail{}, err
	}
	return owner.loadOperatorConversationTurn(ctx, sessionID, turnID)
}

func (s *SQLiteRuntimeStore) LoadOperatorPublicConversationTurn(ctx context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return OperatorPublicConversationTurnDetail{}, err
	}
	return owner.loadOperatorConversationTurn(ctx, sessionID, turnID)
}

func loadOperatorLatestConversationTurn(ctx context.Context, source operatorPublicConversationProjectionSource, sessionID string) (*OperatorPublicConversationTurn, error) {
	if source == nil || strings.TrimSpace(sessionID) == "" {
		return nil, nil
	}
	page, err := source.ListOperatorConversationTurns(ctx, OperatorConversationTurnListOptions{SessionID: sessionID, Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(page.Turns) == 0 {
		return nil, nil
	}
	turn := page.Turns[0]
	return &turn, nil
}

func operatorLiveTurnFromPublic(turn *OperatorPublicConversationTurn) *OperatorLiveTurn {
	if turn == nil {
		return nil
	}
	out := &OperatorLiveTurn{
		TurnID:                 turn.TurnID,
		TaskID:                 turn.TaskID,
		ParseOK:                turn.ParseOK,
		AssistantVisibleOutput: turn.AssistantVisibleOutput,
		Outcome:                turn.Outcome,
	}
	for i := len(turn.Activity) - 1; i >= 0; i-- {
		activity := turn.Activity[i]
		if activity.Kind != "tool_result" && activity.Kind != "tool" {
			continue
		}
		ok := turn.ParseOK
		if activity.OK != nil {
			ok = *activity.OK
		}
		out.LastTool = &OperatorAgentTool{Name: activity.ToolName, ToolUseID: activity.ToolUseID, OK: ok}
		break
	}
	return out
}

func enrichOperatorProjectionWithPublicTurn(projection *operatorAgentProjection, turn *OperatorPublicConversationTurn) {
	if projection == nil || turn == nil {
		return
	}
	projection.LiveTurn = operatorLiveTurnFromPublic(turn)
	projection.LastTool = projection.LiveTurn.LastTool
	projection.CurrentTaskID = turn.TaskID
	projection.DiagnosisActive = operatorAgentDiagnosisActiveFromLatestTurn(turn.TurnID, turn.TaskID, turn.EntityID)
	projection.LastTurnRef = &OperatorTurnRef{
		TurnID:      turn.TurnID,
		CompletedAt: turn.CompletedAt,
		ParseOK:     turn.ParseOK,
		Failure:     runtimefailures.CloneEnvelope(turn.Failure),
	}
}

func (s conversationForkStore) loadOperatorConversationSummary(ctx context.Context, sessionID string) (OperatorConversationSummary, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return OperatorConversationSummary{}, ErrSessionNotFound
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return OperatorConversationSummary{}, err
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return OperatorConversationSummary{}, unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	sources := s.conversationQuerySources(caps)
	if len(sources) == 0 {
		return OperatorConversationSummary{}, ErrSessionNotFound
	}
	row := s.queryRow(ctx, s.db, fmt.Sprintf(`
		SELECT CAST(session_id AS TEXT), agent_id, COALESCE(CAST(run_id AS TEXT), ''), kind,
			COALESCE(scope_key, ''), COALESCE(scope, ''), COALESCE(runtime_mode, ''),
			COALESCE(status, ''), COALESCE(turn_count, 0), COALESCE(message_count, 0),
			COALESCE(CAST(runtime_state AS TEXT), '{}'), started_at, ended_at, updated_at
		FROM (
			%s
		) conversations
		WHERE CAST(session_id AS TEXT) = ?
		LIMIT 2
	`, strings.Join(sources, "\nUNION ALL\n")), sessionID)
	var (
		item            OperatorConversationSummary
		runtimeStateRaw []byte
		startedAtRaw    any
		endedAtRaw      any
		updatedAtRaw    any
	)
	if err := row.Scan(
		&item.SessionID, &item.AgentID, &item.RunID, &item.Kind, &item.ScopeKey,
		&item.Scope, &item.RuntimeMode, &item.Status, &item.TurnCount, &item.MessageCount,
		&runtimeStateRaw, &startedAtRaw, &endedAtRaw, &updatedAtRaw,
	); errors.Is(err, sql.ErrNoRows) {
		return OperatorConversationSummary{}, ErrSessionNotFound
	} else if err != nil {
		return OperatorConversationSummary{}, operatorConversationReadQueryError("load conversation summary", err)
	}
	if item.StartedAt, err = requiredConversationTime(startedAtRaw, "started_at"); err != nil {
		return OperatorConversationSummary{}, err
	}
	if endedAt, valid, scanErr := sqliteTimeValue(endedAtRaw); scanErr != nil {
		return OperatorConversationSummary{}, fmt.Errorf("decode conversation ended_at: %w", scanErr)
	} else if valid {
		endedAt = endedAt.UTC()
		item.EndedAt = &endedAt
	}
	if item.UpdatedAt, err = requiredConversationTime(updatedAtRaw, "updated_at"); err != nil {
		return OperatorConversationSummary{}, err
	}
	runtimeState, err := DecodeConversationRuntimeStateDescriptor(runtimeStateRaw)
	if err != nil {
		return OperatorConversationSummary{}, fmt.Errorf("decode conversation runtime_state: %w", err)
	}
	item.Summary = runtimeState.Summary
	item.Metadata = projectOperatorConversationSummaryMetadata(runtimeState)
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

func (s conversationForkStore) listOperatorConversationTurns(ctx context.Context, opts OperatorConversationTurnListOptions) (OperatorConversationTurnListResult, error) {
	if err := s.requirePublicConversationProjectionCapabilities(ctx); err != nil {
		return OperatorConversationTurnListResult{}, err
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return OperatorConversationTurnListResult{}, err
	}
	runIDProjection := "''"
	if caps.Conversations.TurnRunID {
		runIDProjection = "COALESCE(CAST(run_id AS TEXT), '')"
	}
	opts, cursor, err := normalizeOperatorConversationTurnListOptions(opts)
	if err != nil {
		return OperatorConversationTurnListResult{}, err
	}
	summary, err := s.loadOperatorConversationSummary(ctx, opts.SessionID)
	if err != nil {
		return OperatorConversationTurnListResult{}, err
	}

	where := []string{"1 = 1"}
	args := []any{opts.SessionID}
	if cursor != nil {
		cursorAt, parseErr := time.Parse(time.RFC3339Nano, cursor.CompletedAt)
		if parseErr != nil {
			return OperatorConversationTurnListResult{}, ErrInvalidConversationCursor
		}
		where = append(where, "(created_at < ? OR (created_at = ? AND CAST(turn_id AS TEXT) < ?))")
		args = append(args, cursorAt.UTC(), cursorAt.UTC(), cursor.TurnID)
	}
	args = append(args, opts.Limit+1)
	rows, err := s.query(ctx, s.db, fmt.Sprintf(`
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
				COALESCE(usage_exactness, '') AS usage_exactness, input_tokens, output_tokens,
				COALESCE(CAST(failure AS TEXT), 'null') AS failure, created_at
			FROM agent_turns
			WHERE session_id = ?
		)
		SELECT ordinal, turn_id, run_id, agent_id, session_id, entity_id,
			trigger_event_id, trigger_event_type, task_id, turn_blocks, parse_ok, latency_ms,
			retry_count, usage_exactness, input_tokens, output_tokens, failure, created_at
		FROM ordered
		WHERE %s
		ORDER BY created_at DESC, turn_id DESC
		LIMIT ?
	`, runIDProjection, strings.Join(where, " AND ")), args...)
	if err != nil {
		return OperatorConversationTurnListResult{}, fmt.Errorf("list conversation turns: %w", err)
	}
	defer rows.Close()

	turns := make([]OperatorPublicConversationTurn, 0, opts.Limit+1)
	for rows.Next() {
		record, err := scanConversationTurnRecord(rows)
		if err != nil {
			return OperatorConversationTurnListResult{}, err
		}
		turn, err := projectPublicConversationTurn(record)
		if err != nil {
			return OperatorConversationTurnListResult{}, err
		}
		turns = append(turns, turn)
	}
	if err := rows.Err(); err != nil {
		return OperatorConversationTurnListResult{}, err
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
	return OperatorConversationTurnListResult{Conversation: summary, Turns: turns, NextCursor: nextCursor}, nil
}

func (s conversationForkStore) loadOperatorConversationTurn(ctx context.Context, sessionID, turnID string) (OperatorPublicConversationTurnDetail, error) {
	if err := s.requirePublicConversationProjectionCapabilities(ctx); err != nil {
		return OperatorPublicConversationTurnDetail{}, err
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return OperatorPublicConversationTurnDetail{}, err
	}
	runIDProjection := "''"
	if caps.Conversations.TurnRunID {
		runIDProjection = "COALESCE(CAST(run_id AS TEXT), '')"
	}
	sessionID = strings.TrimSpace(sessionID)
	turnID = strings.TrimSpace(turnID)
	if sessionID == "" {
		return OperatorPublicConversationTurnDetail{}, ErrSessionNotFound
	}
	if turnID == "" {
		return OperatorPublicConversationTurnDetail{}, ErrTurnNotFound
	}
	summary, err := s.loadOperatorConversationSummary(ctx, sessionID)
	if err != nil {
		return OperatorPublicConversationTurnDetail{}, err
	}
	row := s.queryRow(ctx, s.db, fmt.Sprintf(`
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
				COALESCE(usage_exactness, '') AS usage_exactness, input_tokens, output_tokens,
				COALESCE(CAST(failure AS TEXT), 'null') AS failure, created_at
			FROM agent_turns
			WHERE session_id = ?
		)
		SELECT ordinal, turn_id, run_id, agent_id, session_id, entity_id, trigger_event_id,
			trigger_event_type, task_id, turn_blocks, parse_ok, latency_ms, retry_count,
			usage_exactness, input_tokens, output_tokens, failure, created_at
		FROM ordered
		WHERE turn_id = ?
	`, runIDProjection), sessionID, turnID)
	record, err := scanConversationTurnRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return OperatorPublicConversationTurnDetail{}, ErrTurnNotFound
	}
	if err != nil {
		return OperatorPublicConversationTurnDetail{}, fmt.Errorf("load conversation turn: %w", err)
	}
	turn, err := projectPublicConversationTurn(record)
	if err != nil {
		return OperatorPublicConversationTurnDetail{}, err
	}
	return OperatorPublicConversationTurnDetail{Session: summary, Turn: turn}, nil
}

func (s conversationForkStore) requirePublicConversationProjectionCapabilities(ctx context.Context) error {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agent_turns", caps.Conversations.Turns)
	}
	if !caps.Conversations.TurnBlocks {
		return errors.New("operator public conversation turn owner requires canonical agent_turns.turn_blocks")
	}
	return nil
}

func (s conversationForkStore) resolveConversationTurnCoordinateByID(ctx context.Context, sessionID, turnID string) (ConversationForkPointDescriptor, error) {
	sessionID = strings.TrimSpace(sessionID)
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return ConversationForkPointDescriptor{}, ErrTurnNotFound
	}
	row := s.queryRow(ctx, s.db, `
		WITH ordered AS (
			SELECT ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC) AS ordinal,
				CAST(turn_id AS TEXT) AS turn_id,
				COALESCE(CAST(trigger_event_id AS TEXT), '') AS event_id,
				created_at
			FROM agent_turns
			WHERE session_id = ?
		)
		SELECT ordinal, turn_id, event_id, created_at
		FROM ordered
		WHERE turn_id = ?
	`, sessionID, turnID)
	item, err := scanConversationTurnCoordinate(row, "turn", "", nil)
	if errors.Is(err, sql.ErrNoRows) {
		return ConversationForkPointDescriptor{}, ErrTurnNotFound
	}
	if err != nil {
		return ConversationForkPointDescriptor{}, err
	}
	return item, nil
}

func (s conversationForkStore) resolveConversationTurnCoordinateByEvent(ctx context.Context, sessionID, eventID string) (ConversationForkPointDescriptor, error) {
	rows, err := s.query(ctx, s.db, `
		WITH ordered AS (
			SELECT ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC) AS ordinal,
				CAST(turn_id AS TEXT) AS turn_id,
				COALESCE(CAST(trigger_event_id AS TEXT), '') AS event_id,
				created_at
			FROM agent_turns
			WHERE session_id = ?
		)
		SELECT ordinal, turn_id, event_id, created_at
		FROM ordered
		WHERE event_id = ?
		ORDER BY created_at ASC, turn_id ASC
		LIMIT 2
	`, sessionID, eventID)
	if err != nil {
		return ConversationForkPointDescriptor{}, fmt.Errorf("resolve conversation event coordinate: %w", err)
	}
	defer rows.Close()
	matches := make([]ConversationForkPointDescriptor, 0, 2)
	for rows.Next() {
		item, err := scanConversationTurnCoordinate(rows, "event", eventID, nil)
		if err != nil {
			return ConversationForkPointDescriptor{}, err
		}
		matches = append(matches, item)
	}
	if err := rows.Err(); err != nil {
		return ConversationForkPointDescriptor{}, err
	}
	if len(matches) == 0 {
		return ConversationForkPointDescriptor{}, ErrEventNotFound
	}
	if len(matches) > 1 {
		return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point.event_id", Reason: "event matches multiple source turns"}
	}
	return matches[0], nil
}

func (s conversationForkStore) resolveConversationTurnCoordinateAt(ctx context.Context, sessionID string, at time.Time) (ConversationForkPointDescriptor, error) {
	at = at.UTC()
	row := s.queryRow(ctx, s.db, `
		WITH ordered AS (
			SELECT ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC) AS ordinal,
				CAST(turn_id AS TEXT) AS turn_id,
				COALESCE(CAST(trigger_event_id AS TEXT), '') AS event_id,
				created_at
			FROM agent_turns
			WHERE session_id = ?
		)
		SELECT ordinal, turn_id, event_id, created_at
		FROM ordered
		WHERE created_at <= ?
		ORDER BY created_at DESC, turn_id DESC
		LIMIT 1
	`, sessionID, at)
	item, err := scanConversationTurnCoordinate(row, "time", "", &at)
	if errors.Is(err, sql.ErrNoRows) {
		return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point.at", Reason: "does not select a source turn"}
	}
	if err != nil {
		return ConversationForkPointDescriptor{}, err
	}
	return item, nil
}

func scanConversationTurnCoordinate(scanner operatorRowScanner, kind, selectedEventID string, at *time.Time) (ConversationForkPointDescriptor, error) {
	var (
		item          ConversationForkPointDescriptor
		storedEventID string
		selectedAtRaw any
	)
	if err := scanner.Scan(&item.TurnIndex, &item.TurnID, &storedEventID, &selectedAtRaw); err != nil {
		return ConversationForkPointDescriptor{}, err
	}
	selectedAt, err := requiredConversationTime(selectedAtRaw, "selected_at")
	if err != nil {
		return ConversationForkPointDescriptor{}, err
	}
	item.Kind = kind
	item.EventID = strings.TrimSpace(selectedEventID)
	if item.EventID == "" && kind != "turn" {
		item.EventID = strings.TrimSpace(storedEventID)
	}
	item.At = at
	item.SelectedAt = selectedAt
	return item, nil
}

func normalizeOperatorConversationTurnListOptions(opts OperatorConversationTurnListOptions) (OperatorConversationTurnListOptions, *conversationTurnCursor, error) {
	opts.SessionID = strings.TrimSpace(opts.SessionID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	if opts.SessionID == "" {
		return opts, nil, ErrSessionNotFound
	}
	if opts.Limit == 0 {
		opts.Limit = defaultOperatorConversationTurnPageSize
	}
	if opts.Limit < 1 || opts.Limit > maxOperatorConversationTurnPageSize {
		return opts, nil, &EntityReadParamError{Field: "limit", Reason: "must be between 1 and 500"}
	}
	if opts.Cursor == "" {
		return opts, nil, nil
	}
	cursor, err := decodeConversationTurnCursor(opts.Cursor)
	if err != nil || cursor.Kind != "conversation.list_turns" || strings.TrimSpace(cursor.TurnID) == "" {
		return opts, nil, ErrInvalidConversationCursor
	}
	if _, err := time.Parse(time.RFC3339Nano, cursor.CompletedAt); err != nil {
		return opts, nil, ErrInvalidConversationCursor
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
		&record.UsageExactness, &record.InputTokens, &record.OutputTokens, &failureRaw, &createdAtRaw,
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

func projectPublicConversationTurn(record conversationTurnRecord) (OperatorPublicConversationTurn, error) {
	var blocks []runtimellm.TurnBlock
	raw := bytes.TrimSpace(record.TurnBlocksRaw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		raw = []byte("[]")
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return OperatorPublicConversationTurn{}, fmt.Errorf("decode public conversation turn blocks: %w", err)
	}
	activity, assistantOutput, outcome, err := projectAuthorSafeTurnActivity(blocks, record.ParseOK)
	if err != nil {
		return OperatorPublicConversationTurn{}, err
	}
	if record.Failure != nil {
		activity = append(activity, OperatorConversationActivity{Kind: "failure"})
	}
	if activity == nil {
		activity = []OperatorConversationActivity{}
	}
	turn := OperatorPublicConversationTurn{
		TurnID:                 strings.TrimSpace(record.TurnID),
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
	if record.UsageExactness != "" {
		switch record.UsageExactness {
		case "exact", "estimated":
			if !record.InputTokens.Valid || !record.OutputTokens.Valid || record.InputTokens.Int64 < 0 || record.OutputTokens.Int64 < 0 {
				return OperatorPublicConversationTurn{}, fmt.Errorf("invalid persisted conversation token usage")
			}
			turn.Tokens = &OperatorConversationTokenUsage{Input: record.InputTokens.Int64, Output: record.OutputTokens.Int64, Exactness: record.UsageExactness}
		case "unavailable":
			if record.InputTokens.Valid || record.OutputTokens.Valid {
				return OperatorPublicConversationTurn{}, fmt.Errorf("unavailable persisted conversation usage includes token totals")
			}
		default:
			return OperatorPublicConversationTurn{}, fmt.Errorf("invalid persisted conversation usage exactness %q", record.UsageExactness)
		}
	}
	return turn, nil
}

func projectAuthorSafeTurnActivity(blocks []runtimellm.TurnBlock, parseOK bool) ([]OperatorConversationActivity, string, string, error) {
	activity := make([]OperatorConversationActivity, 0, len(blocks))
	assistantOutput := ""
	outcome := ""
	for _, block := range blocks {
		switch strings.TrimSpace(block.Kind) {
		case "dispatch":
			data, ok, err := block.DispatchData()
			if err != nil || !ok {
				return nil, "", "", fmt.Errorf("decode public dispatch activity: %w", firstNonNilError(err, errors.New("dispatch data is required")))
			}
			activity = append(activity, OperatorConversationActivity{Kind: "dispatch", EventID: strings.TrimSpace(data.TriggerEventID), EventType: strings.TrimSpace(data.TriggerEventType)})
		case "tool_use":
			name := strings.TrimSpace(block.ToolName)
			if name == "" {
				return nil, "", "", errors.New("decode public tool activity: tool_name is required")
			}
			link, _, err := block.ToolLinkData()
			if err != nil {
				return nil, "", "", fmt.Errorf("decode public tool activity: %w", err)
			}
			activity = append(activity, OperatorConversationActivity{Kind: "tool", ToolName: name, ToolUseID: strings.TrimSpace(link.ToolUseID)})
		case "tool_result":
			name := strings.TrimSpace(block.ToolName)
			if name == "" {
				return nil, "", "", errors.New("decode public tool result activity: tool_name is required")
			}
			link, _, err := block.ToolLinkData()
			if err != nil {
				return nil, "", "", fmt.Errorf("decode public tool result activity: %w", err)
			}
			ok := parseOK
			activity = append(activity, OperatorConversationActivity{Kind: "tool_result", ToolName: name, ToolUseID: strings.TrimSpace(link.ToolUseID), OK: &ok})
		case "publish":
			data, ok, err := block.PublishData()
			if err != nil || !ok {
				return nil, "", "", fmt.Errorf("decode public publish activity: %w", firstNonNilError(err, errors.New("publish data is required")))
			}
			activity = append(activity, OperatorConversationActivity{Kind: "publish", EventID: strings.TrimSpace(data.EventID), EventType: strings.TrimSpace(block.Title)})
		case "assistant_text":
			if text := strings.TrimSpace(block.Text); text != "" {
				assistantOutput = text
				activity = append(activity, OperatorConversationActivity{Kind: "output", Text: text})
			}
		case "outcome":
			if text := strings.TrimSpace(block.Text); text != "" {
				outcome = text
			}
		case "turn_summary":
			summary, ok, err := block.TurnSummaryData()
			if err != nil || !ok {
				return nil, "", "", fmt.Errorf("decode public turn summary: %w", firstNonNilError(err, errors.New("turn summary data is required")))
			}
			if assistantOutput == "" {
				assistantOutput = strings.TrimSpace(summary.AssistantVisibleOutput)
			}
			if outcome == "" {
				outcome = strings.TrimSpace(summary.Outcome)
			}
		case "reasoning", "progress", "runtime_log":
			// These blocks are explicitly not author-visible public facts.
		default:
			// Unknown/private block kinds fail closed by omission.
		}
	}
	if outcome == "" {
		outcome = assistantOutput
	}
	return activity, assistantOutput, outcome, nil
}

func firstNonNilError(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}
