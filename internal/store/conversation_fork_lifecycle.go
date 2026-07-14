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
	Kind    string
	TurnID  string
	EventID string
	At      *time.Time
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
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	return owner.createOperatorConversationFork(ctx, req)
}

func (s *SQLiteRuntimeStore) CreateOperatorConversationFork(ctx context.Context, req ConversationForkCreateRequest) (OperatorConversationForkSession, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	return owner.createOperatorConversationFork(ctx, req)
}

func (s conversationForkStore) createOperatorConversationFork(ctx context.Context, req ConversationForkCreateRequest) (OperatorConversationForkSession, error) {
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
	var created OperatorConversationForkSession
	err = s.runMutation(ctx, false, func(txctx context.Context, tx *sql.Tx) error {
		row := s.queryRow(txctx, tx, `
		INSERT INTO conversation_forks (
			source_session_id, source_run_id, source_agent_id,
			fork_point_kind, fork_point_turn_index, fork_point_turn_id,
			fork_point_event_id, fork_point_at, fork_point_selected_at,
			created_by, created_at, expires_at
		)
		VALUES (
			?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?
		)
		RETURNING
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, fork_point_kind, fork_point_turn_index,
			COALESCE(CAST(fork_point_turn_id AS TEXT), ''), COALESCE(CAST(fork_point_event_id AS TEXT), ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
	`, source.SessionID, nullableConversationForkID(source.RunID), source.AgentID, descriptor.Kind, descriptor.TurnIndex, descriptor.TurnID,
			nullableConversationForkID(descriptor.EventID), descriptor.At, descriptor.SelectedAt, createdBy, now, expiresAt)
		created, err = scanConversationForkSession(row, now)
		return err
	})
	return created, err
}

func (s *PostgresStore) ListOperatorConversationForks(ctx context.Context, opts ConversationForkListOptions) (ConversationForkListResult, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return ConversationForkListResult{}, err
	}
	return owner.listOperatorConversationForks(ctx, opts)
}

func (s *SQLiteRuntimeStore) ListOperatorConversationForks(ctx context.Context, opts ConversationForkListOptions) (ConversationForkListResult, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return ConversationForkListResult{}, err
	}
	return owner.listOperatorConversationForks(ctx, opts)
}

func (s conversationForkStore) listOperatorConversationForks(ctx context.Context, opts ConversationForkListOptions) (ConversationForkListResult, error) {
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
	where := []string{"deleted_at IS NULL", "expires_at > ?"}
	args = append(args, opts.Now.UTC())
	if opts.SourceSessionID != "" {
		sessionID, err := normalizeUUIDParam(opts.SourceSessionID, "source_session_id")
		if err != nil {
			return ConversationForkListResult{}, err
		}
		args = append(args, sessionID)
		where = append(where, "source_session_id = ?")
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
		where = append(where, `(
			created_at < ?
			OR (created_at = ? AND fork_id > ?)
		)`)
		args = append(args, createdAt.UTC(), createdAt.UTC(), forkID)
	}
	args = append(args, opts.Limit+1)
	rows, err := s.query(ctx, s.db, fmt.Sprintf(`
		SELECT
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, fork_point_kind, fork_point_turn_index,
			COALESCE(CAST(fork_point_turn_id AS TEXT), ''), COALESCE(CAST(fork_point_event_id AS TEXT), ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
		FROM conversation_forks
		WHERE %s
		ORDER BY created_at DESC, fork_id ASC
		LIMIT ?
	`, strings.Join(where, " AND ")), args...)
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
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	return owner.loadOperatorConversationFork(ctx, forkID)
}

func (s *SQLiteRuntimeStore) LoadOperatorConversationFork(ctx context.Context, forkID string) (OperatorConversationForkSession, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	return owner.loadOperatorConversationFork(ctx, forkID)
}

func (s conversationForkStore) loadOperatorConversationFork(ctx context.Context, forkID string) (OperatorConversationForkSession, error) {
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
	row := s.queryRow(ctx, s.db, `
		SELECT
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, fork_point_kind, fork_point_turn_index,
			COALESCE(CAST(fork_point_turn_id AS TEXT), ''), COALESCE(CAST(fork_point_event_id AS TEXT), ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
		FROM conversation_forks
		WHERE fork_id = ?
	`, id)
	item, err := scanConversationForkSession(row, time.Now().UTC())
	if errors.Is(err, sql.ErrNoRows) {
		return OperatorConversationForkSession{}, ErrConversationForkNotFound
	}
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	if caps.Conversations.ForkTurns != SchemaFlavorCanonical {
		return OperatorConversationForkSession{}, fmt.Errorf("store: conversation_fork_turns schema is unavailable")
	}
	turns, err := loadConversationForkTurns(ctx, s, s.db, item.ForkID)
	if err != nil {
		return OperatorConversationForkSession{}, err
	}
	item.Turns = turns
	return item, nil
}

func (s *PostgresStore) DeleteOperatorConversationFork(ctx context.Context, forkID string, now time.Time) (ConversationForkDeleteResult, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return ConversationForkDeleteResult{}, err
	}
	return owner.deleteOperatorConversationFork(ctx, forkID, now)
}

func (s *SQLiteRuntimeStore) DeleteOperatorConversationFork(ctx context.Context, forkID string, now time.Time) (ConversationForkDeleteResult, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return ConversationForkDeleteResult{}, err
	}
	return owner.deleteOperatorConversationFork(ctx, forkID, now)
}

func (s conversationForkStore) deleteOperatorConversationFork(ctx context.Context, forkID string, now time.Time) (ConversationForkDeleteResult, error) {
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
	var result ConversationForkDeleteResult
	err = s.runForkMutation(ctx, id, false, func(txctx context.Context, tx *sql.Tx) error {
		res, err := s.exec(txctx, tx, `UPDATE conversation_forks SET deleted_at = ? WHERE fork_id = ? AND deleted_at IS NULL`, now, id)
		if err != nil {
			return fmt.Errorf("delete conversation fork: %w", err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("read conversation fork delete affected rows: %w", err)
		}
		if affected > 0 {
			result = ConversationForkDeleteResult{ForkID: id, Deleted: true}
			return nil
		}
		var existingDeleted conversationForkTimeValue
		if err := s.queryRow(txctx, tx, `SELECT deleted_at FROM conversation_forks WHERE fork_id = ?`, id).Scan(&existingDeleted); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrConversationForkNotFound
			}
			return fmt.Errorf("load conversation fork delete state: %w", err)
		}
		if existingDeleted.Valid {
			result = ConversationForkDeleteResult{ForkID: id, AlreadyDeleted: true}
			return nil
		}
		return fmt.Errorf("conversation fork delete state changed concurrently")
	})
	return result, err
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

func (s conversationForkStore) loadConversationForkSource(ctx context.Context, caps StoreSchemaCapabilities, sourceSessionID string) (conversationForkSource, error) {
	sessionID, err := normalizeUUIDParam(sourceSessionID, "source_session_id")
	if err != nil {
		return conversationForkSource{}, err
	}
	sources := s.conversationQuerySources(caps)
	if len(sources) == 0 {
		return conversationForkSource{}, ErrSessionNotFound
	}
	rows, err := s.query(ctx, s.db, fmt.Sprintf(`
		SELECT session_id, agent_id, run_id
		FROM (
			%s
		) conversations
		WHERE session_id = ?
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

func (s conversationForkStore) resolveConversationForkPoint(ctx context.Context, sourceSessionID string, selector ConversationForkPointSelector) (ConversationForkPointDescriptor, error) {
	sessionID, err := normalizeUUIDParam(sourceSessionID, "source_session_id")
	if err != nil {
		return ConversationForkPointDescriptor{}, err
	}
	kind := strings.ToLower(strings.TrimSpace(selector.Kind))
	switch kind {
	case "turn":
		turnID := strings.TrimSpace(selector.TurnID)
		if turnID == "" || strings.TrimSpace(selector.EventID) != "" || selector.At != nil {
			return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point", Reason: "turn selector requires only turn_id"}
		}
		return s.resolveConversationTurnCoordinateByID(ctx, sessionID, turnID)
	case "event":
		eventID, err := normalizeUUIDParam(selector.EventID, "fork_point.event_id")
		if err != nil {
			return ConversationForkPointDescriptor{}, err
		}
		if strings.TrimSpace(selector.TurnID) != "" || selector.At != nil {
			return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point", Reason: "event selector requires only event_id"}
		}
		return s.resolveConversationTurnCoordinateByEvent(ctx, sessionID, eventID)
	case "time":
		if selector.At == nil || strings.TrimSpace(selector.TurnID) != "" || strings.TrimSpace(selector.EventID) != "" {
			return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point", Reason: "time selector requires only at"}
		}
		return s.resolveConversationTurnCoordinateAt(ctx, sessionID, selector.At.UTC())
	default:
		return ConversationForkPointDescriptor{}, &EntityReadParamError{Field: "fork_point.kind", Reason: "must be one of turn, event, time"}
	}
}

func scanConversationForkSession(scanner interface {
	Scan(dest ...any) error
}, now time.Time) (OperatorConversationForkSession, error) {
	var item OperatorConversationForkSession
	var turnIndex sql.NullInt64
	var turnID string
	var eventID string
	var at conversationForkTimeValue
	var selectedAt conversationForkTimeValue
	var createdAt conversationForkTimeValue
	var expiresAt conversationForkTimeValue
	var deletedAt conversationForkTimeValue
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
		&selectedAt,
		&item.CreatedBy,
		&createdAt,
		&expiresAt,
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
		atValue := at.Time
		item.ForkPoint.At = &atValue
	}
	if deletedAt.Valid {
		value := deletedAt.Time
		item.DeletedAt = &value
	}
	item.CreatedAt = createdAt.Time
	item.ExpiresAt = expiresAt.Time
	item.ForkPoint.SelectedAt = selectedAt.Time
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
