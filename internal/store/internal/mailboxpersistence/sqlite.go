package mailboxpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	mailboxcontract "github.com/division-sh/swarm/internal/mailbox"

	"github.com/division-sh/swarm/internal/events"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

func (s *MailboxSQLiteOwner) InsertMailboxItem(ctx context.Context, item runtimetools.MailboxItem) (string, error) {
	if strings.TrimSpace(item.Type) == "" {
		return "", fmt.Errorf("mailbox item type is required")
	}
	if strings.TrimSpace(item.ID) == "" {
		item.ID = uuid.NewString()
	}
	if strings.TrimSpace(item.Priority) == "" {
		item.Priority = "normal"
	}
	if strings.TrimSpace(item.Status) == "" {
		item.Status = "pending"
	}
	if len(item.Context) == 0 {
		item.Context = []byte("{}")
	}
	if err := validateGenericMailboxNotice(item.Type, item.Context); err != nil {
		return "", err
	}
	if strings.TrimSpace(item.ReplyContextID) == "" {
		item.ReplyContextID = events.DeliveryContextFromContext(ctx).ReplyContextID()
	}
	scope := "global"
	if entityID := coalesceMailboxEntityID(item); entityID != "" {
		scope = "entity"
	}
	status, decision := mailboxStateForStoredStatus(item.Status, item.Decision)
	if err := s.backend.RunTransaction(ctx, "sqlite mailbox insert", func(txctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(txctx, `
			INSERT INTO mailbox (
				item_id, entity_id, flow_instance, scope, item_type, source_event_id,
				from_agent, severity, summary, payload, status, decision, decision_notes,
				notified, expires_at, reply_context_id, created_at
			)
			VALUES (?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?)
		`, item.ID, sqliteNullUUID(coalesceMailboxEntityID(item)), strings.Trim(strings.TrimSpace(item.FlowInstance), "/"), scope, item.Type, sqliteNullUUID(item.EventID),
			sqliteNullString(item.FromAgent), normalizeMailboxSeverity(item.Priority), sqliteNullString(item.Summary), string(item.Context),
			status, sqliteNullString(decision), sqliteNullString(item.DecisionNotes), item.Notified, sqliteNullTime(item.TimeoutAt), strings.TrimSpace(item.ReplyContextID), time.Now().UTC())
		return err
	}); err != nil {
		return "", fmt.Errorf("insert sqlite mailbox item: %w", err)
	}
	return item.ID, nil
}

func (s *MailboxSQLiteOwner) ListMailboxItems(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error) {
	if limit <= 0 {
		limit = 50
	}
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, sqliteMailboxSelectSQL(`status = ?`)+` ORDER BY created_at ASC LIMIT ?`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("query sqlite mailbox items: %w", err)
	}
	defer rows.Close()
	return scanSpecMailboxItems(rows)
}

func (s *MailboxSQLiteOwner) CountMailboxItems(ctx context.Context, status string) (int, error) {
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return 0, err
	}
	var n int
	if err := s.backend.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE status = ?`, status).Scan(&n); err != nil {
		return 0, fmt.Errorf("count sqlite mailbox items: %w", err)
	}
	return n, nil
}

func (s *MailboxSQLiteOwner) GetMailboxItem(ctx context.Context, id string) (runtimetools.MailboxItem, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return runtimetools.MailboxItem{}, fmt.Errorf("mailbox id is required")
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return runtimetools.MailboxItem{}, err
	}
	rows, err := s.backend.QueryContext(ctx, sqliteMailboxSelectSQL(`item_id = ?`), id)
	if err != nil {
		return runtimetools.MailboxItem{}, fmt.Errorf("get sqlite mailbox item: %w", err)
	}
	defer rows.Close()
	items, err := scanSpecMailboxItems(rows)
	if err != nil {
		return runtimetools.MailboxItem{}, err
	}
	if len(items) == 0 {
		return runtimetools.MailboxItem{}, fmt.Errorf("mailbox item not found: %s", id)
	}
	return items[0], nil
}

func (s *MailboxSQLiteOwner) ExpireMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if limit <= 0 {
		limit = 200
	}
	var items []runtimetools.MailboxItem
	if err := s.backend.RunTransaction(ctx, "sqlite mailbox expiry", func(txctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(txctx, sqliteMailboxSelectSQL(`status = 'pending' AND expires_at IS NOT NULL AND expires_at <= ?`)+` ORDER BY expires_at ASC LIMIT ?`, time.Now().UTC(), limit)
		if err != nil {
			return fmt.Errorf("query expiring sqlite mailbox items: %w", err)
		}
		items, err = scanSpecMailboxItems(rows)
		rows.Close()
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		for i, item := range items {
			if _, err := tx.ExecContext(txctx, `
				UPDATE mailbox
				SET status = 'expired',
				    decision = COALESCE(NULLIF(decision, ''), ''),
				    decision_notes = COALESCE(NULLIF(decision_notes, ''), 'Timed out without human decision'),
				    decided_at = COALESCE(decided_at, ?)
				WHERE item_id = ? AND status = 'pending'
			`, now, item.ID); err != nil {
				return fmt.Errorf("expire sqlite mailbox item: %w", err)
			}
			items[i].Status = "expired"
			if strings.TrimSpace(items[i].DecisionNotes) == "" {
				items[i].DecisionNotes = "Timed out without human decision"
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *MailboxSQLiteOwner) ListUnnotifiedCriticalMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if limit <= 0 {
		limit = 50
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return nil, err
	}
	rows, err := s.backend.QueryContext(ctx, sqliteMailboxSelectSQL(`status = 'pending' AND severity = 'critical' AND COALESCE(notified, false) = false`)+` ORDER BY created_at ASC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("query sqlite unnotified critical mailbox items: %w", err)
	}
	defer rows.Close()
	return scanSpecMailboxItems(rows)
}

func (s *MailboxSQLiteOwner) MarkMailboxItemNotified(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return fmt.Errorf("mailbox id is required")
	}
	if err := s.backend.RunTransaction(ctx, "sqlite mailbox notified", func(txctx context.Context, tx *sql.Tx) error {
		result, err := tx.ExecContext(txctx, `UPDATE mailbox SET notified = true WHERE item_id = ?`, id)
		if err != nil {
			return err
		}
		if rows, err := result.RowsAffected(); err != nil {
			return err
		} else if rows == 0 {
			return mailboxcontract.ErrV1NotFound
		}
		return nil
	}); err != nil {
		return fmt.Errorf("mark sqlite mailbox item notified: %w", err)
	}
	return nil
}

func sqliteMailboxSelectSQL(where string) string {
	return `
		SELECT item_id, COALESCE(source_event_id, ''), COALESCE(entity_id, ''), COALESCE(flow_instance, ''), COALESCE(from_agent, ''),
		       item_type, COALESCE(severity, 'normal'), status, COALESCE(notified, false),
		       COALESCE(payload, '{}'), COALESCE(summary, ''), expires_at,
		       COALESCE(decision, ''), COALESCE(decision_notes, ''), COALESCE(reply_context_id, '')
		FROM mailbox
		WHERE ` + where
}
