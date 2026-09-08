package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimerunfork "github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

type conversationForkCursor struct {
	Kind      string `json:"kind"`
	CreatedAt string `json:"created_at"`
	ForkID    string `json:"fork_id"`
}

func (s *RunForkPostgresOwner) CreateOperatorConversationFork(ctx context.Context, req runtimerunfork.ConversationForkCreateRequest) (runtimerunfork.OperatorConversationForkSession, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	return owner.createOperatorConversationFork(ctx, req)
}

func (s *RunForkSQLiteOwner) CreateOperatorConversationFork(ctx context.Context, req runtimerunfork.ConversationForkCreateRequest) (runtimerunfork.OperatorConversationForkSession, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	return owner.createOperatorConversationFork(ctx, req)
}

func (s *RunForkPostgresOwner) ResolveConversationForkPoint(ctx context.Context, sourceSessionID string, selector runtimerunfork.ConversationForkPointSelector) (runtimerunfork.ConversationForkPointDescriptor, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkPointDescriptor{}, err
	}
	return owner.resolveConversationForkPoint(ctx, sourceSessionID, selector)
}

func (s *RunForkSQLiteOwner) ResolveConversationForkPoint(ctx context.Context, sourceSessionID string, selector runtimerunfork.ConversationForkPointSelector) (runtimerunfork.ConversationForkPointDescriptor, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkPointDescriptor{}, err
	}
	return owner.resolveConversationForkPoint(ctx, sourceSessionID, selector)
}

func (s conversationForkStore) createOperatorConversationFork(ctx context.Context, req runtimerunfork.ConversationForkCreateRequest) (runtimerunfork.OperatorConversationForkSession, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	createdBy := strings.TrimSpace(req.CreatedBy)
	if createdBy == "" {
		return runtimerunfork.OperatorConversationForkSession{}, &operatorread.EntityReadParamError{Field: "created_by", Reason: "is required"}
	}
	source, err := s.loadConversationForkSource(ctx, req.SourceSessionID)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	descriptor, err := s.resolveConversationForkPoint(ctx, req.SourceSessionID, req.ForkPoint)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	expiresAt := now.Add(runtimerunfork.ConversationForkLifecycleTTL).UTC()
	sourceIdentity, err := agentIdentityFields(source.Identity)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	var created runtimerunfork.OperatorConversationForkSession
	err = s.runMutation(ctx, false, func(txctx context.Context, tx *sql.Tx) error {
		row := s.queryRow(txctx, tx, `
		INSERT INTO conversation_forks (
			source_session_id, source_run_id, source_agent_id,
			source_agent_name_owner, source_agent_name_source, source_agent_route_presence,
			source_flow_scope_key, source_flow_instance_id, source_flow_instance,
			fork_point_kind, fork_point_turn_index, fork_point_turn_id,
			fork_point_event_id, fork_point_at, fork_point_selected_at,
			created_by, created_at, expires_at
		)
		VALUES (
			?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?,
			?, ?, ?
		)
		RETURNING
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, source_agent_name_owner, source_agent_name_source, source_agent_route_presence,
			source_flow_scope_key, source_flow_instance_id, source_flow_instance,
			fork_point_kind, fork_point_turn_index,
			COALESCE(CAST(fork_point_turn_id AS TEXT), ''), COALESCE(CAST(fork_point_event_id AS TEXT), ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
	`, source.SessionID, nullableConversationForkID(source.RunID), sourceIdentity.AgentID,
			sourceIdentity.NameOwner, sourceIdentity.NameSource, sourceIdentity.RoutePresence,
			sourceIdentity.FlowScopeKey, sourceIdentity.FlowInstanceID, sourceIdentity.FlowInstancePath,
			descriptor.Kind, descriptor.TurnIndex, descriptor.TurnID,
			nullableConversationForkID(descriptor.EventID), descriptor.At, descriptor.SelectedAt, createdBy, now, expiresAt)
		created, err = scanConversationForkSession(row, now)
		return err
	})
	return created, err
}

func (s *RunForkPostgresOwner) ListOperatorConversationForks(ctx context.Context, opts runtimerunfork.ConversationForkListOptions) (runtimerunfork.ConversationForkListResult, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkListResult{}, err
	}
	return owner.listOperatorConversationForks(ctx, opts)
}

func (s *RunForkSQLiteOwner) ListOperatorConversationForks(ctx context.Context, opts runtimerunfork.ConversationForkListOptions) (runtimerunfork.ConversationForkListResult, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkListResult{}, err
	}
	return owner.listOperatorConversationForks(ctx, opts)
}

func (s conversationForkStore) listOperatorConversationForks(ctx context.Context, opts runtimerunfork.ConversationForkListOptions) (runtimerunfork.ConversationForkListResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunfork.ConversationForkListResult{}, err
	}
	opts, err := defaultConversationForkListOptions(opts)
	if err != nil {
		return runtimerunfork.ConversationForkListResult{}, err
	}
	args := make([]any, 0, 6)
	where := []string{"deleted_at IS NULL", "expires_at > ?"}
	args = append(args, opts.Now.UTC())
	if opts.SourceSessionID != "" {
		sessionID, err := normalizeUUIDParam(opts.SourceSessionID, "source_session_id")
		if err != nil {
			return runtimerunfork.ConversationForkListResult{}, err
		}
		args = append(args, sessionID)
		where = append(where, "source_session_id = ?")
	}
	if opts.Cursor != "" {
		cursor, err := decodeConversationForkCursor(opts.Cursor)
		if err != nil {
			return runtimerunfork.ConversationForkListResult{}, err
		}
		createdAt, err := time.Parse(time.RFC3339Nano, cursor.CreatedAt)
		if err != nil || strings.TrimSpace(cursor.ForkID) == "" {
			return runtimerunfork.ConversationForkListResult{}, runtimerunfork.ErrInvalidConversationForkCursor
		}
		forkID, err := normalizeUUIDParam(cursor.ForkID, "cursor.fork_id")
		if err != nil {
			return runtimerunfork.ConversationForkListResult{}, runtimerunfork.ErrInvalidConversationForkCursor
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
			source_agent_id, source_agent_name_owner, source_agent_name_source, source_agent_route_presence,
			source_flow_scope_key, source_flow_instance_id, source_flow_instance,
			fork_point_kind, fork_point_turn_index,
			COALESCE(CAST(fork_point_turn_id AS TEXT), ''), COALESCE(CAST(fork_point_event_id AS TEXT), ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
		FROM conversation_forks
		WHERE %s
		ORDER BY created_at DESC, fork_id ASC
		LIMIT ?
	`, strings.Join(where, " AND ")), args...)
	if err != nil {
		return runtimerunfork.ConversationForkListResult{}, fmt.Errorf("list conversation forks: %w", err)
	}
	defer rows.Close()
	forks := []runtimerunfork.OperatorConversationForkSession{}
	for rows.Next() {
		item, err := scanConversationForkSession(rows, opts.Now)
		if err != nil {
			return runtimerunfork.ConversationForkListResult{}, err
		}
		forks = append(forks, item)
	}
	if err := rows.Err(); err != nil {
		return runtimerunfork.ConversationForkListResult{}, fmt.Errorf("read conversation forks: %w", err)
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
		forks = []runtimerunfork.OperatorConversationForkSession{}
	}
	return runtimerunfork.ConversationForkListResult{Forks: forks, NextCursor: nextCursor}, nil
}

func (s *RunForkPostgresOwner) LoadOperatorConversationFork(ctx context.Context, forkID string) (runtimerunfork.OperatorConversationForkSession, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	return owner.loadOperatorConversationFork(ctx, forkID)
}

func (s *RunForkSQLiteOwner) LoadOperatorConversationFork(ctx context.Context, forkID string) (runtimerunfork.OperatorConversationForkSession, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	return owner.loadOperatorConversationFork(ctx, forkID)
}

func (s conversationForkStore) loadOperatorConversationFork(ctx context.Context, forkID string) (runtimerunfork.OperatorConversationForkSession, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	id, err := normalizeUUIDParam(forkID, "fork_id")
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	row := s.queryRow(ctx, s.db, `
		SELECT
			CAST(fork_id AS TEXT), CAST(source_session_id AS TEXT), COALESCE(CAST(source_run_id AS TEXT), ''),
			source_agent_id, source_agent_name_owner, source_agent_name_source, source_agent_route_presence,
			source_flow_scope_key, source_flow_instance_id, source_flow_instance,
			fork_point_kind, fork_point_turn_index,
			COALESCE(CAST(fork_point_turn_id AS TEXT), ''), COALESCE(CAST(fork_point_event_id AS TEXT), ''),
			fork_point_at, fork_point_selected_at, created_by, created_at, expires_at, deleted_at
		FROM conversation_forks
		WHERE fork_id = ?
	`, id)
	item, err := scanConversationForkSession(row, time.Now().UTC())
	if errors.Is(err, sql.ErrNoRows) {
		return runtimerunfork.OperatorConversationForkSession{}, runtimerunfork.ErrConversationForkNotFound
	}
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	turns, err := loadConversationForkTurns(ctx, s, s.db, item.ForkID)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	item.Turns = turns
	return item, nil
}

func (s *RunForkPostgresOwner) DeleteOperatorConversationFork(ctx context.Context, forkID string, now time.Time) (runtimerunfork.ConversationForkDeleteResult, error) {
	owner, err := postgresConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkDeleteResult{}, err
	}
	return owner.deleteOperatorConversationFork(ctx, forkID, now)
}

func (s *RunForkSQLiteOwner) DeleteOperatorConversationFork(ctx context.Context, forkID string, now time.Time) (runtimerunfork.ConversationForkDeleteResult, error) {
	owner, err := sqliteConversationForkStore(s)
	if err != nil {
		return runtimerunfork.ConversationForkDeleteResult{}, err
	}
	return owner.deleteOperatorConversationFork(ctx, forkID, now)
}

func (s conversationForkStore) deleteOperatorConversationFork(ctx context.Context, forkID string, now time.Time) (runtimerunfork.ConversationForkDeleteResult, error) {
	if err := s.requireCurrentSchema(); err != nil {
		return runtimerunfork.ConversationForkDeleteResult{}, err
	}
	id, err := normalizeUUIDParam(forkID, "fork_id")
	if err != nil {
		return runtimerunfork.ConversationForkDeleteResult{}, err
	}
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	var result runtimerunfork.ConversationForkDeleteResult
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
			result = runtimerunfork.ConversationForkDeleteResult{ForkID: id, Deleted: true}
			return nil
		}
		var existingDeleted conversationForkTimeValue
		if err := s.queryRow(txctx, tx, `SELECT deleted_at FROM conversation_forks WHERE fork_id = ?`, id).Scan(&existingDeleted); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return runtimerunfork.ErrConversationForkNotFound
			}
			return fmt.Errorf("load conversation fork delete state: %w", err)
		}
		if existingDeleted.Valid {
			result = runtimerunfork.ConversationForkDeleteResult{ForkID: id, AlreadyDeleted: true}
			return nil
		}
		return fmt.Errorf("conversation fork delete state changed concurrently")
	})
	return result, err
}

func defaultConversationForkListOptions(opts runtimerunfork.ConversationForkListOptions) (runtimerunfork.ConversationForkListOptions, error) {
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

func (s conversationForkStore) loadConversationForkSource(ctx context.Context, sourceSessionID string) (runtimerunfork.ConversationForkSource, error) {
	sessionID, err := normalizeUUIDParam(sourceSessionID, "source_session_id")
	if err != nil {
		return runtimerunfork.ConversationForkSource{}, err
	}
	if s.sources == nil {
		return runtimerunfork.ConversationForkSource{}, fmt.Errorf("conversation fork source owner is required")
	}
	return s.sources.LoadConversationForkSource(ctx, sessionID)
}

func (s conversationForkStore) resolveConversationForkPoint(ctx context.Context, sourceSessionID string, selector runtimerunfork.ConversationForkPointSelector) (runtimerunfork.ConversationForkPointDescriptor, error) {
	sessionID, err := normalizeUUIDParam(sourceSessionID, "source_session_id")
	if err != nil {
		return runtimerunfork.ConversationForkPointDescriptor{}, err
	}
	kind := strings.ToLower(strings.TrimSpace(selector.Kind))
	switch kind {
	case "turn":
		turnID := strings.TrimSpace(selector.TurnID)
		if turnID == "" || strings.TrimSpace(selector.EventID) != "" || selector.At != nil {
			return runtimerunfork.ConversationForkPointDescriptor{}, &operatorread.EntityReadParamError{Field: "fork_point", Reason: "turn selector requires only turn_id"}
		}
		return s.resolveConversationTurnCoordinateByID(ctx, sessionID, turnID)
	case "event":
		eventID, err := normalizeUUIDParam(selector.EventID, "fork_point.event_id")
		if err != nil {
			return runtimerunfork.ConversationForkPointDescriptor{}, err
		}
		if strings.TrimSpace(selector.TurnID) != "" || selector.At != nil {
			return runtimerunfork.ConversationForkPointDescriptor{}, &operatorread.EntityReadParamError{Field: "fork_point", Reason: "event selector requires only event_id"}
		}
		return s.resolveConversationTurnCoordinateByEvent(ctx, sessionID, eventID)
	case "time":
		if selector.At == nil || strings.TrimSpace(selector.TurnID) != "" || strings.TrimSpace(selector.EventID) != "" {
			return runtimerunfork.ConversationForkPointDescriptor{}, &operatorread.EntityReadParamError{Field: "fork_point", Reason: "time selector requires only at"}
		}
		return s.resolveConversationTurnCoordinateAt(ctx, sessionID, selector.At.UTC())
	default:
		return runtimerunfork.ConversationForkPointDescriptor{}, &operatorread.EntityReadParamError{Field: "fork_point.kind", Reason: "must be one of turn, event, time"}
	}
}

func scanConversationForkSession(scanner interface {
	Scan(dest ...any) error
}, now time.Time) (runtimerunfork.OperatorConversationForkSession, error) {
	var item runtimerunfork.OperatorConversationForkSession
	var turnIndex sql.NullInt64
	var turnID string
	var eventID string
	var at conversationForkTimeValue
	var selectedAt conversationForkTimeValue
	var createdAt conversationForkTimeValue
	var expiresAt conversationForkTimeValue
	var deletedAt conversationForkTimeValue
	var identityFields runtimeagentidentity.StorageFields
	if err := scanner.Scan(
		&item.ForkID,
		&item.SourceSessionID,
		&item.SourceRunID,
		&identityFields.AgentID,
		&identityFields.NameOwner,
		&identityFields.NameSource,
		&identityFields.RoutePresence,
		&identityFields.FlowScopeKey,
		&identityFields.FlowInstanceID,
		&identityFields.FlowInstancePath,
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
		return runtimerunfork.OperatorConversationForkSession{}, err
	}
	identityFields.RunID = item.SourceRunID
	identity, err := runtimeagentidentity.FromStorageFields(identityFields)
	if err != nil {
		return runtimerunfork.OperatorConversationForkSession{}, fmt.Errorf("decode conversation fork source identity: %w", err)
	}
	item.SourceIdentity = identity
	item.SourceAgentID = item.SourceIdentity.AgentID()
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
	item.Turns = []operatorread.OperatorConversationTurn{}
	return item, nil
}

func conversationForkState(item runtimerunfork.OperatorConversationForkSession, now time.Time) string {
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
		return "", &operatorread.EntityReadParamError{Field: field, Reason: "is required"}
	}
	parsed, err := uuid.Parse(value)
	if err != nil {
		return "", &operatorread.EntityReadParamError{Field: field, Reason: "must be a UUID"}
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
		return conversationForkCursor{}, runtimerunfork.ErrInvalidConversationForkCursor
	}
	var cursor conversationForkCursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return conversationForkCursor{}, runtimerunfork.ErrInvalidConversationForkCursor
	}
	if strings.TrimSpace(cursor.Kind) != "conversation.fork_list" {
		return conversationForkCursor{}, runtimerunfork.ErrInvalidConversationForkCursor
	}
	return cursor, nil
}
