package runforkpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	runtimerunfork "github.com/division-sh/swarm/internal/runtime/runfork"
)

type conversationCoordinateScanner interface {
	Scan(dest ...any) error
}

func (s conversationForkStore) resolveConversationTurnCoordinateByID(ctx context.Context, sessionID, turnID string) (runtimerunfork.ConversationForkPointDescriptor, error) {
	sessionID = strings.TrimSpace(sessionID)
	turnID = strings.TrimSpace(turnID)
	if turnID == "" {
		return runtimerunfork.ConversationForkPointDescriptor{}, operatorread.ErrTurnNotFound
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
		return runtimerunfork.ConversationForkPointDescriptor{}, operatorread.ErrTurnNotFound
	}
	if err != nil {
		return runtimerunfork.ConversationForkPointDescriptor{}, err
	}
	return item, nil
}

func (s conversationForkStore) resolveConversationTurnCoordinateByEvent(ctx context.Context, sessionID, eventID string) (runtimerunfork.ConversationForkPointDescriptor, error) {
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
		return runtimerunfork.ConversationForkPointDescriptor{}, fmt.Errorf("resolve conversation event coordinate: %w", err)
	}
	defer rows.Close()
	matches := make([]runtimerunfork.ConversationForkPointDescriptor, 0, 2)
	for rows.Next() {
		item, err := scanConversationTurnCoordinate(rows, "event", eventID, nil)
		if err != nil {
			return runtimerunfork.ConversationForkPointDescriptor{}, err
		}
		matches = append(matches, item)
	}
	if err := rows.Err(); err != nil {
		return runtimerunfork.ConversationForkPointDescriptor{}, err
	}
	if len(matches) == 0 {
		return runtimerunfork.ConversationForkPointDescriptor{}, operatorread.ErrEventNotFound
	}
	if len(matches) > 1 {
		return runtimerunfork.ConversationForkPointDescriptor{}, &operatorread.EntityReadParamError{Field: "fork_point.event_id", Reason: "event matches multiple source turns"}
	}
	return matches[0], nil
}

func (s conversationForkStore) resolveConversationTurnCoordinateAt(ctx context.Context, sessionID string, at time.Time) (runtimerunfork.ConversationForkPointDescriptor, error) {
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
		return runtimerunfork.ConversationForkPointDescriptor{}, &operatorread.EntityReadParamError{Field: "fork_point.at", Reason: "does not select a source turn"}
	}
	if err != nil {
		return runtimerunfork.ConversationForkPointDescriptor{}, err
	}
	return item, nil
}

func scanConversationTurnCoordinate(scanner conversationCoordinateScanner, kind, selectedEventID string, at *time.Time) (runtimerunfork.ConversationForkPointDescriptor, error) {
	var (
		item          runtimerunfork.ConversationForkPointDescriptor
		storedEventID string
		selectedAtRaw any
	)
	if err := scanner.Scan(&item.TurnIndex, &item.TurnID, &storedEventID, &selectedAtRaw); err != nil {
		return runtimerunfork.ConversationForkPointDescriptor{}, err
	}
	selectedAt, valid, err := sqliteTimeValue(selectedAtRaw)
	if err != nil {
		return runtimerunfork.ConversationForkPointDescriptor{}, fmt.Errorf("decode conversation selected_at: %w", err)
	}
	if !valid {
		return runtimerunfork.ConversationForkPointDescriptor{}, fmt.Errorf("decode conversation selected_at: value is required")
	}
	item.Kind = kind
	item.EventID = strings.TrimSpace(selectedEventID)
	if item.EventID == "" && kind != "turn" {
		item.EventID = strings.TrimSpace(storedEventID)
	}
	item.At = at
	item.SelectedAt = selectedAt.UTC()
	return item, nil
}
