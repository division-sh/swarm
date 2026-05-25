package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const ConversationForkLifecycleTTL = 24 * time.Hour

type ConversationForkPointSelector struct {
	Kind      string
	TurnIndex int
	EventID   string
	At        *time.Time
}

type ConversationForkPointDescriptor struct {
	Kind       string     `json:"kind"`
	TurnIndex  int        `json:"turn_index"`
	TurnID     string     `json:"turn_id"`
	EventID    string     `json:"event_id,omitempty"`
	At         *time.Time `json:"at,omitempty"`
	SelectedAt time.Time  `json:"selected_at"`
}

type OperatorConversationForkSession struct {
	ForkID          string                          `json:"fork_id"`
	SourceSessionID string                          `json:"source_session_id"`
	SourceRunID     string                          `json:"source_run_id,omitempty"`
	SourceAgentID   string                          `json:"source_agent_id"`
	ForkPoint       ConversationForkPointDescriptor `json:"fork_point"`
	CreatedBy       string                          `json:"created_by"`
	CreatedAt       time.Time                       `json:"created_at"`
	ExpiresAt       time.Time                       `json:"expires_at"`
	DeletedAt       *time.Time                      `json:"deleted_at,omitempty"`
	State           string                          `json:"state"`
	Turns           []OperatorConversationTurn      `json:"turns"`
}

type ConversationForkCreateRequest struct {
	SourceSessionID string
	ForkPoint       ConversationForkPointSelector
	CreatedBy       string
	Now             time.Time
}

type ConversationForkListOptions struct {
	SourceSessionID string
	Limit           int
	Cursor          string
	Now             time.Time
}

type ConversationForkListResult struct {
	Forks      []OperatorConversationForkSession `json:"forks"`
	NextCursor string                            `json:"next_cursor,omitempty"`
}

type ConversationForkDeleteResult struct {
	ForkID         string `json:"fork_id"`
	Deleted        bool   `json:"deleted"`
	AlreadyDeleted bool   `json:"already_deleted"`
}

type conversationForkSource struct {
	SessionID string
	AgentID   string
	RunID     string
}

type conversationForkCursor struct {
	Kind      string `json:"kind"`
	CreatedAt string `json:"created_at"`
	ForkID    string `json:"fork_id"`
}

func (s *PostgresStore) CreateOperatorConversationFork(ctx context.Context, req ConversationForkCreateRequest) (OperatorConversationForkSession, error) {
	if s == nil || s.DB == nil {
		return OperatorConversationForkSession{}, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	if err := requireConversationForkLifecycleCapabilities(caps); err != nil {
		return OperatorConversationForkSession{}, err
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	createdBy := strings.TrimSpace(req.CreatedBy)
	if createdBy == "" {
		return OperatorConversationForkSession{}, &EntityReadParamError{Field: "created_by", Reason: "is required"}
	}
	source, err := s.loadConversationForkSource(ctx, caps, req.SourceSessionID)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	descriptor, err := s.resolveConversationForkPoint(ctx, req.SourceSessionID, req.ForkPoint)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	expiresAt := now.Add(ConversationForkLifecycleTTL).UTC()
	row := s.DB.QueryRowContext(ctx, `
		INSERT INTO conversation_forks (
			source_session_id, source_run_id, source_agent_id,
			fork_point_kind, fork_point_turn_index, fork_point_turn_id,
			fork_point_event_id, fork_point_at, fork_point_selected_at,
			created_by, created_at, expires_at
		)
		VALUES (
			$1::uuid, nullif($2, '')::uuid, $3,
			$4, $5, $6::uuid,
			nullif($7, '')::uuid, $8, $9,
			$10, $11, $12
		)
		RETURNING
			fork_id::text, source_session_id::text, COALESCE(source_run_id::text, ''),
			source_agent_id, fork_point_kind, fork_point_turn_index,
			COALESCE(fork_point_turn_id::text, ''), COALESCE(fork_point_event_id::text, ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
	`, source.SessionID, source.RunID, source.AgentID, descriptor.Kind, descriptor.TurnIndex, descriptor.TurnID,
		descriptor.EventID, descriptor.At, descriptor.SelectedAt, createdBy, now, expiresAt)
	return scanConversationForkSession(row, now)
}

func (s *PostgresStore) ListOperatorConversationForks(ctx context.Context, opts ConversationForkListOptions) (ConversationForkListResult, error) {
	if s == nil || s.DB == nil {
		return ConversationForkListResult{}, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return ConversationForkListResult{}, err
	}
	if err := requireConversationForkLifecycleCapabilities(caps); err != nil {
		return ConversationForkListResult{}, err
	}
	opts, err = defaultConversationForkListOptions(opts)
	if err != nil {
		return ConversationForkListResult{}, err
	}
	args := make([]any, 0, 6)
	where := []string{"deleted_at IS NULL", "expires_at > $1"}
	args = append(args, opts.Now.UTC())
	if opts.SourceSessionID != "" {
		sessionID, err := normalizeUUIDParam(opts.SourceSessionID, "source_session_id")
		if err != nil {
			return ConversationForkListResult{}, err
		}
		args = append(args, sessionID)
		where = append(where, fmt.Sprintf("source_session_id = $%d::uuid", len(args)))
	}
	if opts.Cursor != "" {
		cursor, err := decodeConversationForkCursor(opts.Cursor)
		if err != nil {
			return ConversationForkListResult{}, err
		}
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil || strings.TrimSpace(cursor.ForkID) == "" {
			return ConversationForkListResult{}, ErrInvalidConversationForkCursor
		}
		forkID, err := normalizeUUIDParam(cursor.ForkID, "cursor.fork_id")
		if err != nil {
			return ConversationForkListResult{}, ErrInvalidConversationForkCursor
		}
		args = append(args, createdAt.UTC(), forkID)
		nTime := len(args) - 1
		nFork := len(args)
		where = append(where, fmt.Sprintf(`(
			created_at < $%d
			OR (created_at = $%d AND fork_id > $%d::uuid)
		)`, nTime, nTime, nFork))
	}
	args = append(args, opts.Limit+1)
	limitArg := len(args)
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			fork_id::text, source_session_id::text, COALESCE(source_run_id::text, ''),
			source_agent_id, fork_point_kind, fork_point_turn_index,
			COALESCE(fork_point_turn_id::text, ''), COALESCE(fork_point_event_id::text, ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
		FROM conversation_forks
		WHERE %s
		ORDER BY created_at DESC, fork_id ASC
		LIMIT $%d
	`, strings.Join(where, " AND "), limitArg), args...)
	if err != nil {
		return ConversationForkListResult{}, fmt.Errorf("list conversation forks: %w", err)
	}
	defer rows.Close()
	forks := []OperatorConversationForkSession{}
	for rows.Next() {
		item, err := scanConversationForkSession(rows, opts.Now)
		if err != nil {
			return ConversationForkListResult{}, err
		}
		forks = append(forks, item)
	}
	if err := rows.Err(); err != nil {
		return ConversationForkListResult{}, fmt.Errorf("read conversation forks: %w", err)
	}
	nextCursor := ""
	if len(forks) > opts.Limit {
		forks = forks[:opts.Limit]
		last := forks[len(forks)-1]
		nextCursor = encodeConversationForkCursor(conversationForkCursor{
			Kind:      "conversation.fork_list",
			CreatedAt: last.CreatedAt.UTC().Format(time.RFC3339Nano),
			ForkID:    last.ForkID,
		})
	}
	if forks == nil {
		forks = []OperatorConversationForkSession{}
	}
	return ConversationForkListResult{Forks: forks, NextCursor: nextCursor}, nil
}

func (s *PostgresStore) LoadOperatorConversationFork(ctx context.Context, forkID string) (OperatorConversationForkSession, error) {
	if s == nil || s.DB == nil {
		return OperatorConversationForkSession{}, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	if err := requireConversationForkLifecycleCapabilities(caps); err != nil {
		return OperatorConversationForkSession{}, err
	}
	id, err := normalizeUUIDParam(forkID, "fork_id")
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	row := s.DB.QueryRowContext(ctx, `
		SELECT
			fork_id::text, source_session_id::text, COALESCE(source_run_id::text, ''),
			source_agent_id, fork_point_kind, fork_point_turn_index,
			COALESCE(fork_point_turn_id::text, ''), COALESCE(fork_point_event_id::text, ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
		FROM conversation_forks
		WHERE fork_id = $1::uuid
	`, id)
	item, err := scanConversationForkSession(row, time.Now().UTC())
	if errors.Is(err, sql.ErrNoRows) {
		return OperatorConversationForkSession{}, ErrConversationForkNotFound
	}
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	return item, nil
}

func (s *PostgresStore) DeleteOperatorConversationFork(ctx context.Context, forkID string, now time.Time) (ConversationForkDeleteResult, error) {
	if s == nil || s.DB == nil {
		return ConversationForkDeleteResult{}, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return ConversationForkDeleteResult{}, err
	}
	if err := requireConversationForkLifecycleCapabilities(caps); err != nil {
		return ConversationForkDeleteResult{}, err
	}
	id, err := normalizeUUIDParam(forkID, "fork_id")
	if err != nil {
		return ConversationForkDeleteResult{}, err
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var existingDeleted sql.NullTime
	if err := s.DB.QueryRowContext(ctx, `SELECT deleted_at FROM conversation_forks WHERE fork_id = $1::uuid`, id).Scan(&existingDeleted); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ConversationForkDeleteResult{}, ErrConversationForkNotFound
		}
		return ConversationForkDeleteResult{}, fmt.Errorf("load conversation fork delete state: %w", err)
	}
	if existingDeleted.Valid {
		return ConversationForkDeleteResult{ForkID: id, Deleted: false, AlreadyDeleted: true}, nil
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE conversation_forks SET deleted_at = $2 WHERE fork_id = $1::uuid AND deleted_at IS NULL`, id, now); err != nil {
		return ConversationForkDeleteResult{}, fmt.Errorf("delete conversation fork: %w", err)
	}
	return ConversationForkDeleteResult{ForkID: id, Deleted: true, AlreadyDeleted: false}, nil
}

func requireConversationForkLifecycleCapabilities(caps StoreSchemaCapabilities) error {
	if caps.Conversations.Forks != SchemaFlavorCanonical {
		return fmt.Errorf("store: conversation_forks schema is unavailable")
	}
	if caps.Conversations.Turns != SchemaFlavorCanonical {
		return fmt.Errorf("store: conversation fork lifecycle requires canonical agent_turns")
	}
	if caps.Conversations.Sessions != SchemaFlavorCanonical && caps.Conversations.Audits != SchemaFlavorCanonical {
		return fmt.Errorf("store: conversation fork lifecycle requires canonical conversation source")
	}
	return nil
}

func defaultConversationForkListOptions(opts ConversationForkListOptions) (ConversationForkListOptions, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	opts.SourceSessionID = strings.TrimSpace(opts.SourceSessionID)
	opts.Cursor = strings.TrimSpace(opts.Cursor)
	opts.Now = opts.Now.UTC()
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	return opts, nil
}

func (s *PostgresStore) loadConversationForkSource(ctx context.Context, caps StoreSchemaCapabilities, sourceSessionID string) (conversationForkSource, error) {
	sessionID, err := normalizeUUIDParam(sourceSessionID, "source_session_id")
	if err != nil {
		return conversationForkSource{}, err
	}
	sources := operatorConversationQuerySources(caps)
	if len(sources) == 0 {
		return conversationForkSource{}, ErrSessionNotFound
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT session_id, agent_id, run_id
		FROM (
			%s
		) conversations
		WHERE session_id = $1
		LIMIT 2
	`, strings.Join(sources, "\nUNION ALL\n")), sessionID)
	if err != nil {
		return conversationForkSource{}, fmt.Errorf("load conversation fork source: %w", err)
	}
	defer rows.Close()
	items := []conversationForkSource{}
	for rows.Next() {
		var item conversationForkSource
		if err := rows.Scan(&item.SessionID, &item.AgentID, &item.RunID); err != nil {
			return conversationForkSource{}, err
		}
		item.SessionID = strings.TrimSpace(item.SessionID)
		item.AgentID = strings.TrimSpace(item.AgentID)
		item.RunID = strings.TrimSpace(item.RunID)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return conversationForkSource{}, err
	}
	if len(items) == 0 {
		return conversationForkSource{}, ErrSessionNotFound
	}
	if len(items) > 1 {
		return conversationForkSource{}, &EntityReadParamError{Field: "source_session_id", Reason: "ambiguous source session"}
	}
	return items[0], nil
}

func (s *PostgresStore) resolveConversationForkPoint(ctx context.Context, sourceSessionID string, selector ConversationForkPointSelector) (ConversationForkPointDescriptor, error) {
	sessionID, err := normalizeUUIDParam(sourceSessionID, "source_session_id")
	if err != nil {
		return ConversationForkPointDescriptor{}, err
	}
	kind := strings.ToLower(strings.TrimSpace(selector.Kind))
	switch kind {
	case "turn":
		if selector.TurnIndex <= 0 || strings.TrimSpace(selector.EventID) != "" || selector.At != nil {
			return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point", Reason: "turn selector requires only turn_index"}
		}
		return s.resolveConversationForkTurnPoint(ctx, sessionID, selector.TurnIndex, kind, "", nil)
	case "event":
		eventID, err := normalizeUUIDParam(selector.EventID, "fork_point.event_id")
		if err != nil {
			return ConversationForkPointDescriptor{}, err
		}
		if selector.TurnIndex != 0 || selector.At != nil {
			return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point", Reason: "event selector requires only event_id"}
		}
		return s.resolveConversationForkEventPoint(ctx, sessionID, eventID)
	case "time":
		if selector.At == nil || selector.TurnIndex != 0 || strings.TrimSpace(selector.EventID) != "" {
			return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point", Reason: "time selector requires only at"}
		}
		return s.resolveConversationForkTimePoint(ctx, sessionID, selector.At.UTC())
	default:
		return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point.kind", Reason: "must be one of turn, event, time"}
	}
}

func (s *PostgresStore) resolveConversationForkTurnPoint(ctx context.Context, sessionID string, turnIndex int, kind string, eventID string, at *time.Time) (ConversationForkPointDescriptor, error) {
	row := s.DB.QueryRowContext(ctx, `
		WITH ordered AS (
			SELECT
				ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC)::int AS turn_index,
				turn_id::text AS turn_id,
				COALESCE(trigger_event_id::text, '') AS event_id,
				created_at
			FROM agent_turns
			WHERE session_id = $1::uuid
		)
		SELECT turn_id, event_id, created_at
		FROM ordered
		WHERE turn_index = $2
	`, sessionID, turnIndex)
	var turnID string
	var triggerEventID string
	var selectedAt time.Time
	if err := row.Scan(&turnID, &triggerEventID, &selectedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ConversationForkPointDescriptor{}, ErrTurnNotFound
		}
		return ConversationForkPointDescriptor{}, fmt.Errorf("resolve conversation fork turn point: %w", err)
	}
	if eventID == "" && kind != "turn" {
		eventID = triggerEventID
	}
	return ConversationForkPointDescriptor{
		Kind:       kind,
		TurnIndex:  turnIndex,
		TurnID:     turnID,
		EventID:    eventID,
		At:         at,
		SelectedAt: selectedAt.UTC(),
	}, nil
}

func (s *PostgresStore) resolveConversationForkEventPoint(ctx context.Context, sessionID string, eventID string) (ConversationForkPointDescriptor, error) {
	rows, err := s.DB.QueryContext(ctx, `
		WITH ordered AS (
			SELECT
				ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC)::int AS turn_index,
				turn_id::text AS turn_id,
				COALESCE(trigger_event_id::text, '') AS event_id,
				created_at
			FROM agent_turns
			WHERE session_id = $1::uuid
		)
		SELECT turn_index, turn_id, created_at
		FROM ordered
		WHERE event_id = $2
		LIMIT 2
	`, sessionID, eventID)
	if err != nil {
		return ConversationForkPointDescriptor{}, fmt.Errorf("resolve conversation fork event point: %w", err)
	}
	defer rows.Close()
	type match struct {
		turnIndex int
		turnID    string
		createdAt time.Time
	}
	matches := []match{}
	for rows.Next() {
		var item match
		if err := rows.Scan(&item.turnIndex, &item.turnID, &item.createdAt); err != nil {
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
	return ConversationForkPointDescriptor{
		Kind:       "event",
		TurnIndex:  matches[0].turnIndex,
		TurnID:     matches[0].turnID,
		EventID:    eventID,
		SelectedAt: matches[0].createdAt.UTC(),
	}, nil
}

func (s *PostgresStore) resolveConversationForkTimePoint(ctx context.Context, sessionID string, at time.Time) (ConversationForkPointDescriptor, error) {
	row := s.DB.QueryRowContext(ctx, `
		WITH ordered AS (
			SELECT
				ROW_NUMBER() OVER (ORDER BY created_at ASC, turn_id ASC)::int AS turn_index,
				turn_id::text AS turn_id,
				COALESCE(trigger_event_id::text, '') AS event_id,
				created_at
			FROM agent_turns
			WHERE session_id = $1::uuid
		)
		SELECT turn_index, turn_id, event_id, created_at
		FROM ordered
		WHERE created_at <= $2
		ORDER BY created_at DESC, turn_id DESC
		LIMIT 1
	`, sessionID, at.UTC())
	var turnIndex int
	var turnID string
	var eventID string
	var selectedAt time.Time
	if err := row.Scan(&turnIndex, &turnID, &eventID, &selectedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ConversationForkPointDescriptor{}, ErrTurnNotFound
		}
		return ConversationForkPointDescriptor{}, fmt.Errorf("resolve conversation fork time point: %w", err)
	}
	atCopy := at.UTC()
	return ConversationForkPointDescriptor{
		Kind:       "time",
		TurnIndex:  turnIndex,
		TurnID:     turnID,
		EventID:    eventID,
		At:         &atCopy,
		SelectedAt: selectedAt.UTC(),
	}, nil
}

func scanConversationForkSession(scanner interface {
	Scan(dest ...any) error
}, now time.Time) (OperatorConversationForkSession, error) {
	var item OperatorConversationForkSession
	var turnIndex sql.NullInt64
	var turnID string
	var eventID string
	var at sql.NullTime
	var deletedAt sql.NullTime
	if err := scanner.Scan(
		&item.ForkID,
		&item.SourceSessionID,
		&item.SourceRunID,
		&item.SourceAgentID,
		&item.ForkPoint.Kind,
		&turnIndex,
		&turnID,
		&eventID,
		&at,
		&item.ForkPoint.SelectedAt,
		&item.CreatedBy,
		&item.CreatedAt,
		&item.ExpiresAt,
		&deletedAt,
	); err != nil {
		return OperatorConversationForkSession{}, err
	}
	if turnIndex.Valid {
		item.ForkPoint.TurnIndex = int(turnIndex.Int64)
	}
	item.ForkPoint.TurnID = strings.TrimSpace(turnID)
	item.ForkPoint.EventID = strings.TrimSpace(eventID)
	if at.Valid {
		atValue := at.Time.UTC()
		item.ForkPoint.At = &atValue
	}
	if deletedAt.Valid {
		value := deletedAt.Time.UTC()
		item.DeletedAt = &value
	}
	item.CreatedAt = item.CreatedAt.UTC()
	item.ExpiresAt = item.ExpiresAt.UTC()
	item.ForkPoint.SelectedAt = item.ForkPoint.SelectedAt.UTC()
	item.State = conversationForkState(item, now)
	item.Turns = []OperatorConversationTurn{}
	return item, nil
}

func conversationForkState(item OperatorConversationForkSession, now time.Time) string {
	if item.DeletedAt != nil {
		return "deleted"
	}
	if now.UTC().IsZero() {
		now = time.Now().UTC()
	}
	if !item.ExpiresAt.IsZero() && !item.ExpiresAt.After(now.UTC()) {
		return "expired"
	}
	return "active"
}

func normalizeUUIDParam(value, field string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", &EntityReadParamError{Field: field, Reason: "is required"}
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", &EntityReadParamError{Field: field, Reason: "must be a UUID"}
	}
	return parsed.String(), nil
}

func encodeConversationForkCursor(cursor conversationForkCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeConversationForkCursor(raw string) (conversationForkCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(raw))
	if err != nil {
		return conversationForkCursor{}, ErrInvalidConversationForkCursor
	}
	var cursor conversationForkCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return conversationForkCursor{}, ErrInvalidConversationForkCursor
	}
	if strings.TrimSpace(cursor.Kind) != "conversation.fork_list" {
		return conversationForkCursor{}, ErrInvalidConversationForkCursor
	}
	return cursor, nil
}
