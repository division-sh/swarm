package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimetools "swarm/internal/runtime/tools"
	"github.com/google/uuid"
)

// MailboxStore is the runtime mailbox persistence surface.
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
	if err := s.insertMailboxItemSpec(ctx, item); err == nil {
		return item.ID, nil
	} else if !shouldFallbackLegacyMailboxSchema(err) {
		return "", err
	}

	const q = `
		INSERT INTO mailbox (
			id, event_id, entity_id, from_agent, type, priority, status,
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
	if items, err := s.listMailboxItemsSpec(ctx, status, limit); err == nil {
		return items, nil
	} else if !shouldFallbackLegacyMailboxSchema(err) {
		return nil, err
	}

	const q = `
		SELECT
			id::text,
			COALESCE(event_id::text, ''),
			COALESCE(entity_id::text, ''),
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
	return scanLegacyMailboxItems(rows)
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
	if err := s.countMailboxItemsSpec(ctx, status, &n); err == nil {
		return n, nil
	} else if !shouldFallbackLegacyMailboxSchema(err) {
		return 0, err
	}
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
	if item, err := s.getMailboxItemSpec(ctx, id); err == nil {
		return item, nil
	} else if !shouldFallbackLegacyMailboxSchema(err) {
		return runtimetools.MailboxItem{}, err
	}

	const q = `
		SELECT
			id::text,
			COALESCE(event_id::text, ''),
			COALESCE(entity_id::text, ''),
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
	if err := s.decideMailboxItemSpec(ctx, id, status, decision, notes); err == nil {
		return nil
	} else if !shouldFallbackLegacyMailboxSchema(err) {
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
	if items, err := s.expireMailboxItemsSpec(ctx, limit); err == nil {
		return items, nil
	} else if !shouldFallbackLegacyMailboxSchema(err) {
		return nil, err
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
			COALESCE(m.entity_id::text, ''),
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
	return scanLegacyMailboxItems(rows)
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
	if items, err := s.listUnnotifiedCriticalMailboxItemsSpec(ctx, limit); err == nil {
		return items, nil
	} else if !shouldFallbackLegacyMailboxSchema(err) {
		return nil, err
	}

	const q = `
		SELECT
			id::text,
			COALESCE(event_id::text, ''),
			COALESCE(entity_id::text, ''),
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
	return scanLegacyMailboxItems(rows)
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
	if err := s.markMailboxItemNotifiedSpec(ctx, id); err == nil {
		return nil
	} else if !shouldFallbackLegacyMailboxSchema(err) {
		return err
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

func (s *PostgresStore) insertMailboxItemSpec(ctx context.Context, item runtimetools.MailboxItem) error {
	scope := "global"
	if entityID := coalesceMailboxEntityID(item); entityID != "" {
		scope = "entity"
	}
	status, decision := mailboxStateForStoredStatus(item.Status)
	var expiresAt any
	if !item.TimeoutAt.IsZero() {
		expiresAt = item.TimeoutAt
	}
	_, err := s.DB.ExecContext(ctx, `
		INSERT INTO mailbox (
			item_id, entity_id, flow_instance, scope, item_type, source_event_id,
			from_agent, severity, summary, payload, status, decision, decision_notes,
			notified, expires_at, created_at
		)
		VALUES (
			$1::uuid, NULLIF($2,'')::uuid, NULL, $3, $4, NULLIF($5,'')::uuid,
			NULLIF($6,''), $7, NULLIF($8,''), $9::jsonb, $10, NULLIF($11,''),
			NULLIF($12,''), $13, $14, now()
		)
	`, item.ID, coalesceMailboxEntityID(item), scope, item.Type, item.EventID, item.FromAgent, normalizeMailboxSeverity(item.Priority), item.Summary, string(item.Context), status, decision, item.DecisionNotes, item.Notified, expiresAt)
	if err != nil {
		return fmt.Errorf("insert mailbox item: %w", err)
	}
	return nil
}

func (s *PostgresStore) listMailboxItemsSpec(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error) {
	where, arg := mailboxListFilter(status)
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			item_id::text,
			COALESCE(source_event_id::text, ''),
			COALESCE(entity_id::text, ''),
			COALESCE(from_agent, ''),
			item_type,
			COALESCE(severity, 'normal'),
			status,
			COALESCE(notified, false),
			COALESCE(payload, '{}'::jsonb),
			COALESCE(summary, ''),
			expires_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, '')
		FROM mailbox
		WHERE `+where+`
		ORDER BY created_at ASC
		LIMIT $2
	`, arg, limit)
	if err != nil {
		return nil, fmt.Errorf("query mailbox items: %w", err)
	}
	defer rows.Close()
	return scanSpecMailboxItems(rows)
}

func (s *PostgresStore) countMailboxItemsSpec(ctx context.Context, status string, out *int) error {
	where, arg := mailboxListFilter(status)
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE `+where, arg).Scan(out); err != nil {
		return fmt.Errorf("count mailbox items: %w", err)
	}
	return nil
}

func (s *PostgresStore) getMailboxItemSpec(ctx context.Context, id string) (runtimetools.MailboxItem, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			item_id::text,
			COALESCE(source_event_id::text, ''),
			COALESCE(entity_id::text, ''),
			COALESCE(from_agent, ''),
			item_type,
			COALESCE(severity, 'normal'),
			status,
			COALESCE(notified, false),
			COALESCE(payload, '{}'::jsonb),
			COALESCE(summary, ''),
			expires_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, '')
		FROM mailbox
		WHERE item_id = $1::uuid
	`, id)
	if err != nil {
		return runtimetools.MailboxItem{}, fmt.Errorf("get mailbox item: %w", err)
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

func (s *PostgresStore) decideMailboxItemSpec(ctx context.Context, id, status, decision, notes string) error {
	rowStatus, rowDecision := mailboxDecisionState(status, decision)
	res, err := s.DB.ExecContext(ctx, `
		UPDATE mailbox
		SET status = $2,
		    decision = $3,
		    decision_notes = NULLIF($4,''),
		    decided_at = now()
		WHERE item_id = $1::uuid
		  AND status = 'pending'
	`, id, rowStatus, rowDecision, notes)
	if err != nil {
		return fmt.Errorf("decide mailbox item: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("mailbox item is not pending or not found: %s", id)
	}
	return nil
}

func (s *PostgresStore) expireMailboxItemsSpec(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	rows, err := s.DB.QueryContext(ctx, `
		WITH due AS (
			SELECT item_id
			FROM mailbox
			WHERE status = 'pending'
			  AND expires_at IS NOT NULL
			  AND expires_at <= now()
			ORDER BY expires_at ASC
			LIMIT $1
		)
		UPDATE mailbox m
		SET status = 'expired',
		    decision = COALESCE(NULLIF(m.decision, ''), 'timed_out'),
		    decision_notes = COALESCE(NULLIF(m.decision_notes, ''), 'Timed out without human decision'),
		    decided_at = COALESCE(m.decided_at, now())
		FROM due
		WHERE m.item_id = due.item_id
		RETURNING
			m.item_id::text,
			COALESCE(m.source_event_id::text, ''),
			COALESCE(m.entity_id::text, ''),
			COALESCE(m.from_agent, ''),
			m.item_type,
			COALESCE(m.severity, 'normal'),
			m.status,
			COALESCE(m.notified, false),
			COALESCE(m.payload, '{}'::jsonb),
			COALESCE(m.summary, ''),
			m.expires_at,
			COALESCE(m.decision, ''),
			COALESCE(m.decision_notes, '')
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("expire mailbox items: %w", err)
	}
	defer rows.Close()
	return scanSpecMailboxItems(rows)
}

func (s *PostgresStore) listUnnotifiedCriticalMailboxItemsSpec(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	rows, err := s.DB.QueryContext(ctx, `
		SELECT
			item_id::text,
			COALESCE(source_event_id::text, ''),
			COALESCE(entity_id::text, ''),
			COALESCE(from_agent, ''),
			item_type,
			COALESCE(severity, 'normal'),
			status,
			COALESCE(notified, false),
			COALESCE(payload, '{}'::jsonb),
			COALESCE(summary, ''),
			expires_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, '')
		FROM mailbox
		WHERE status = 'pending'
		  AND severity = 'critical'
		  AND COALESCE(notified, false) = false
		ORDER BY created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("query unnotified critical mailbox items: %w", err)
	}
	defer rows.Close()
	return scanSpecMailboxItems(rows)
}

func (s *PostgresStore) markMailboxItemNotifiedSpec(ctx context.Context, id string) error {
	if _, err := s.DB.ExecContext(ctx, `
		UPDATE mailbox
		SET notified = true
		WHERE item_id = $1::uuid
	`, id); err != nil {
		return fmt.Errorf("mark mailbox item notified: %w", err)
	}
	return nil
}

func scanSpecMailboxItems(rows *sql.Rows) ([]runtimetools.MailboxItem, error) {
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
		it.Priority = denormalizeMailboxSeverity(it.Priority)
		it.Status = denormalizeMailboxStatus(it.Status, it.Decision)
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

func scanLegacyMailboxItems(rows *sql.Rows) ([]runtimetools.MailboxItem, error) {
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

func normalizeMailboxSeverity(priority string) string {
	switch strings.TrimSpace(priority) {
	case "critical", "urgent":
		return strings.TrimSpace(priority)
	default:
		return "normal"
	}
}

func denormalizeMailboxSeverity(severity string) string {
	return normalizeMailboxSeverity(severity)
}

func mailboxStateForStoredStatus(status string) (rowStatus string, decision string) {
	switch strings.TrimSpace(status) {
	case "approved", "rejected", "more_data":
		return "decided", strings.TrimSpace(status)
	case "timed_out":
		return "expired", "timed_out"
	case "cancelled":
		return "cancelled", ""
	default:
		return "pending", ""
	}
}

func mailboxDecisionState(status, decision string) (rowStatus string, rowDecision string) {
	switch strings.TrimSpace(status) {
	case "timed_out":
		return "expired", "timed_out"
	case "approved", "rejected", "more_data":
		return "decided", strings.TrimSpace(status)
	default:
		return "decided", strings.TrimSpace(decision)
	}
}

func denormalizeMailboxStatus(status, decision string) string {
	switch strings.TrimSpace(status) {
	case "decided":
		if strings.TrimSpace(decision) != "" {
			return strings.TrimSpace(decision)
		}
	case "expired":
		if strings.TrimSpace(decision) != "" {
			return strings.TrimSpace(decision)
		}
		return "timed_out"
	}
	return strings.TrimSpace(status)
}

func mailboxListFilter(status string) (string, string) {
	status = strings.TrimSpace(status)
	switch status {
	case "approved", "rejected", "more_data":
		return `status = 'decided' AND decision = $1`, status
	case "timed_out":
		return `status = 'expired' AND decision = 'timed_out'`, status
	default:
		return `status = $1`, status
	}
}

func shouldFallbackLegacyMailboxSchema(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(msg, `item_id`) ||
		strings.Contains(msg, `item_type`) ||
		strings.Contains(msg, `source_event_id`) ||
		strings.Contains(msg, `severity`) ||
		strings.Contains(msg, `payload`) ||
		strings.Contains(msg, `expires_at`) ||
		strings.Contains(msg, `decision_notes`) ||
		strings.Contains(msg, `notified`)
}
