package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimetools "empireai/internal/runtime/tools"
	"github.com/google/uuid"
)

// MailboxStore preserves the mailbox persistence surface after the Empire
// Legacy Empire split removed; this store is the sole implementation.
type MailboxStore interface {
	runtimetools.MailboxPersistence
}

func (s *PostgresStore) InsertMailboxItem(ctx context.Context, item runtimetools.MailboxItem) (string, error) {
	if s == nil || s.DB == nil {
		return "", fmt.Errorf("postgres store is required")
	}
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

	const q = `
		INSERT INTO mailbox (
			id, event_id, vertical_id, from_agent, type, priority, status,
			context, summary, timeout_at, created_at
		)
		VALUES (
			$1::uuid,
			NULLIF($2,'')::uuid,
			NULLIF($3,'')::uuid,
			NULLIF($4,''),
			$5,
			$6,
			$7,
			$8::jsonb,
			NULLIF($9,''),
			$10,
			now()
		)
	`
	var timeout any
	if !item.TimeoutAt.IsZero() {
		timeout = item.TimeoutAt
	}
	_, err := s.DB.ExecContext(ctx, q,
		item.ID,
		item.EventID,
		coalesceMailboxEntityID(item),
		item.FromAgent,
		item.Type,
		item.Priority,
		item.Status,
		string(item.Context),
		item.Summary,
		timeout,
	)
	if err != nil {
		return "", fmt.Errorf("insert mailbox item: %w", err)
	}
	return item.ID, nil
}

func (s *PostgresStore) ListMailboxItems(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if limit <= 0 {
		limit = 50
	}
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return nil, err
	}
	const q = `
		SELECT
			id::text,
			COALESCE(event_id::text, ''),
			COALESCE(vertical_id::text, ''),
			COALESCE(from_agent, ''),
			type,
			COALESCE(priority, 'normal'),
			COALESCE(status, 'pending'),
			COALESCE(notified, false),
			COALESCE(context, '{}'::jsonb),
			COALESCE(summary, ''),
			timeout_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, '')
		FROM mailbox
		WHERE status = $1
		ORDER BY created_at ASC
		LIMIT $2
	`
	rows, err := s.DB.QueryContext(ctx, q, status, limit)
	if err != nil {
		return nil, fmt.Errorf("query mailbox items: %w", err)
	}
	defer rows.Close()

	out := make([]runtimetools.MailboxItem, 0)
	for rows.Next() {
		var it runtimetools.MailboxItem
		var timeout sql.NullTime
		if err := rows.Scan(
			&it.ID,
			&it.EventID,
			&it.EntityID,
			&it.FromAgent,
			&it.Type,
			&it.Priority,
			&it.Status,
			&it.Notified,
			&it.Context,
			&it.Summary,
			&timeout,
			&it.Decision,
			&it.DecisionNotes,
		); err != nil {
			return nil, fmt.Errorf("scan mailbox item: %w", err)
		}
		if timeout.Valid {
			it.TimeoutAt = timeout.Time
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate mailbox items: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) CountMailboxItems(ctx context.Context, status string) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("postgres store is required")
	}
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return 0, err
	}
	var n int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE status = $1`, status).Scan(&n); err != nil {
		return 0, fmt.Errorf("count mailbox items: %w", err)
	}
	return n, nil
}

func (s *PostgresStore) GetMailboxItem(ctx context.Context, id string) (runtimetools.MailboxItem, error) {
	if s == nil || s.DB == nil {
		return runtimetools.MailboxItem{}, fmt.Errorf("postgres store is required")
	}
	if strings.TrimSpace(id) == "" {
		return runtimetools.MailboxItem{}, fmt.Errorf("mailbox id is required")
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return runtimetools.MailboxItem{}, err
	}

	const q = `
		SELECT
			id::text,
			COALESCE(event_id::text, ''),
			COALESCE(vertical_id::text, ''),
			COALESCE(from_agent, ''),
			type,
			COALESCE(priority, 'normal'),
			COALESCE(status, 'pending'),
			COALESCE(notified, false),
			COALESCE(context, '{}'::jsonb),
			COALESCE(summary, ''),
			timeout_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, '')
		FROM mailbox
		WHERE id = $1::uuid
	`
	var it runtimetools.MailboxItem
	var timeout sql.NullTime
	if err := s.DB.QueryRowContext(ctx, q, id).Scan(
		&it.ID,
		&it.EventID,
		&it.EntityID,
		&it.FromAgent,
		&it.Type,
		&it.Priority,
		&it.Status,
		&it.Notified,
		&it.Context,
		&it.Summary,
		&timeout,
		&it.Decision,
		&it.DecisionNotes,
	); err != nil {
		if err == sql.ErrNoRows {
			return runtimetools.MailboxItem{}, fmt.Errorf("mailbox item not found: %s", id)
		}
		return runtimetools.MailboxItem{}, fmt.Errorf("get mailbox item: %w", err)
	}
	if timeout.Valid {
		it.TimeoutAt = timeout.Time
	}
	return it, nil
}

func (s *PostgresStore) DecideMailboxItem(ctx context.Context, id, status, decision, notes string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	if strings.TrimSpace(id) == "" || strings.TrimSpace(status) == "" || strings.TrimSpace(decision) == "" {
		return fmt.Errorf("mailbox id, status, and decision are required")
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return err
	}
	switch status {
	case "approved", "rejected", "more_data", "timed_out":
	default:
		return fmt.Errorf("invalid mailbox status: %s", status)
	}
	const q = `
		UPDATE mailbox
		SET status = $2,
		    decision = $3,
		    decision_notes = NULLIF($4,''),
		    decided_at = now()
		WHERE id = $1::uuid
		  AND status = 'pending'
	`
	res, err := s.DB.ExecContext(ctx, q, id, status, decision, notes)
	if err != nil {
		return fmt.Errorf("decide mailbox item: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("mailbox item is not pending or not found: %s", id)
	}
	return nil
}

func (s *PostgresStore) ExpireMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if limit <= 0 {
		limit = 200
	}
	const q = `
		WITH due AS (
			SELECT id
			FROM mailbox
			WHERE status = 'pending'
			  AND timeout_at IS NOT NULL
			  AND timeout_at <= now()
			ORDER BY timeout_at ASC
			LIMIT $1
		)
		UPDATE mailbox m
		SET status = 'timed_out',
		    decision = COALESCE(NULLIF(m.decision, ''), 'timed_out'),
		    decision_notes = COALESCE(NULLIF(m.decision_notes, ''), 'Timed out without human decision'),
		    decided_at = COALESCE(m.decided_at, now())
		FROM due
		WHERE m.id = due.id
		RETURNING
			m.id::text,
			COALESCE(m.event_id::text, ''),
			COALESCE(m.vertical_id::text, ''),
			COALESCE(m.from_agent, ''),
			m.type,
			COALESCE(m.priority, 'normal'),
			COALESCE(m.status, 'timed_out'),
			COALESCE(m.notified, false),
			COALESCE(m.context, '{}'::jsonb),
			COALESCE(m.summary, ''),
			m.timeout_at,
			COALESCE(m.decision, ''),
			COALESCE(m.decision_notes, '')
	`
	rows, err := s.DB.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("expire mailbox items: %w", err)
	}
	defer rows.Close()

	out := make([]runtimetools.MailboxItem, 0)
	for rows.Next() {
		var it runtimetools.MailboxItem
		var timeout sql.NullTime
		if err := rows.Scan(
			&it.ID,
			&it.EventID,
			&it.EntityID,
			&it.FromAgent,
			&it.Type,
			&it.Priority,
			&it.Status,
			&it.Notified,
			&it.Context,
			&it.Summary,
			&timeout,
			&it.Decision,
			&it.DecisionNotes,
		); err != nil {
			return nil, fmt.Errorf("scan expired mailbox item: %w", err)
		}
		if timeout.Valid {
			it.TimeoutAt = timeout.Time
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired mailbox items: %w", err)
	}
	return out, nil
}

func (s *PostgresStore) ListUnnotifiedCriticalMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	if limit <= 0 {
		limit = 50
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return nil, err
	}
	const q = `
		SELECT
			id::text,
			COALESCE(event_id::text, ''),
			COALESCE(vertical_id::text, ''),
			COALESCE(from_agent, ''),
			type,
			COALESCE(priority, 'normal'),
			COALESCE(status, 'pending'),
			COALESCE(notified, false),
			COALESCE(context, '{}'::jsonb),
			COALESCE(summary, ''),
			timeout_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, '')
		FROM mailbox
		WHERE status = 'pending'
		  AND priority = 'critical'
		  AND COALESCE(notified, false) = false
		ORDER BY created_at ASC
		LIMIT $1
	`
	rows, err := s.DB.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("query unnotified critical mailbox items: %w", err)
	}
	defer rows.Close()

	out := make([]runtimetools.MailboxItem, 0)
	for rows.Next() {
		var it runtimetools.MailboxItem
		var timeout sql.NullTime
		if err := rows.Scan(
			&it.ID,
			&it.EventID,
			&it.EntityID,
			&it.FromAgent,
			&it.Type,
			&it.Priority,
			&it.Status,
			&it.Notified,
			&it.Context,
			&it.Summary,
			&timeout,
			&it.Decision,
			&it.DecisionNotes,
		); err != nil {
			return nil, fmt.Errorf("scan critical mailbox item: %w", err)
		}
		if timeout.Valid {
			it.TimeoutAt = timeout.Time
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate critical mailbox items: %w", err)
	}
	return out, nil
}

func coalesceMailboxEntityID(item runtimetools.MailboxItem) string {
	return strings.TrimSpace(item.EntityID)
}

func (s *PostgresStore) MarkMailboxItemNotified(ctx context.Context, id string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("mailbox id is required")
	}
	const q = `
		UPDATE mailbox
		SET notified = true
		WHERE id = $1::uuid
	`
	if _, err := s.DB.ExecContext(ctx, q, id); err != nil {
		return fmt.Errorf("mark mailbox item notified: %w", err)
	}
	return nil
}
