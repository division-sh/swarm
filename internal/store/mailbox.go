package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/google/uuid"
	runtimetools "swarm/internal/runtime/tools"
)

// MailboxStore is the runtime mailbox persistence surface.
type MailboxStore interface {
	runtimetools.MailboxPersistence
}

func (s *PostgresStore) InsertMailboxItem(ctx context.Context, item runtimetools.MailboxItem) (string, error) {
	if s == nil || s.DB == nil {
		return "", fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return "", err
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
	if caps.Mailbox == SchemaFlavorCanonical {
		if err := s.insertMailboxItemSpec(ctx, item); err != nil {
			return "", err
		}
		return item.ID, nil
	}
	return "", unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func (s *PostgresStore) ListMailboxItems(ctx context.Context, status string, limit int) ([]runtimetools.MailboxItem, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
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
	if caps.Mailbox == SchemaFlavorCanonical {
		return s.listMailboxItemsSpec(ctx, status, limit)
	}
	return nil, unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func (s *PostgresStore) CountMailboxItems(ctx context.Context, status string) (int, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(status) == "" {
		status = "pending"
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return 0, err
	}
	var n int
	if caps.Mailbox == SchemaFlavorCanonical {
		if err := s.countMailboxItemsSpec(ctx, status, &n); err != nil {
			return 0, err
		}
		return n, nil
	}
	return 0, unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func (s *PostgresStore) GetMailboxItem(ctx context.Context, id string) (runtimetools.MailboxItem, error) {
	if s == nil || s.DB == nil {
		return runtimetools.MailboxItem{}, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return runtimetools.MailboxItem{}, err
	}
	if strings.TrimSpace(id) == "" {
		return runtimetools.MailboxItem{}, fmt.Errorf("mailbox id is required")
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return runtimetools.MailboxItem{}, err
	}
	if caps.Mailbox == SchemaFlavorCanonical {
		return s.getMailboxItemSpec(ctx, id)
	}
	return runtimetools.MailboxItem{}, unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func (s *PostgresStore) DecideMailboxItem(ctx context.Context, id, status, decision, notes string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" || strings.TrimSpace(status) == "" || strings.TrimSpace(decision) == "" {
		return fmt.Errorf("mailbox id, status, and decision are required")
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return err
	}
	if caps.Mailbox == SchemaFlavorCanonical {
		return s.decideMailboxItemSpec(ctx, id, status, decision, notes)
	}
	return unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func (s *PostgresStore) ExpireMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 200
	}
	if caps.Mailbox == SchemaFlavorCanonical {
		return s.expireMailboxItemsSpec(ctx, limit)
	}
	return nil, unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func (s *PostgresStore) ListUnnotifiedCriticalMailboxItems(ctx context.Context, limit int) ([]runtimetools.MailboxItem, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 50
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return nil, err
	}
	if caps.Mailbox == SchemaFlavorCanonical {
		return s.listUnnotifiedCriticalMailboxItemsSpec(ctx, limit)
	}
	return nil, unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func coalesceMailboxEntityID(item runtimetools.MailboxItem) string {
	return strings.TrimSpace(item.EntityID)
}

func (s *PostgresStore) MarkMailboxItemNotified(ctx context.Context, id string) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return err
	}
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("mailbox id is required")
	}
	if caps.Mailbox == SchemaFlavorCanonical {
		return s.markMailboxItemNotifiedSpec(ctx, id)
	}
	return unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func (s *PostgresStore) insertMailboxItemSpec(ctx context.Context, item runtimetools.MailboxItem) error {
	scope := "global"
	if entityID := coalesceMailboxEntityID(item); entityID != "" {
		scope = "entity"
	}
	status, decision := mailboxStateForStoredStatus(item.Status, item.Decision)
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
		    decision = COALESCE(NULLIF(m.decision, ''), ''),
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

func mailboxStateForStoredStatus(status, decision string) (rowStatus string, rowDecision string) {
	switch strings.TrimSpace(status) {
	case "decided", "expired", "cancelled":
		return strings.TrimSpace(status), strings.TrimSpace(decision)
	default:
		return "pending", strings.TrimSpace(decision)
	}
}

func mailboxDecisionState(status, decision string) (rowStatus string, rowDecision string) {
	switch strings.TrimSpace(status) {
	case "decided", "expired", "cancelled":
		return strings.TrimSpace(status), strings.TrimSpace(decision)
	default:
		return "decided", strings.TrimSpace(coalesce(strings.TrimSpace(decision), strings.TrimSpace(status)))
	}
}

func denormalizeMailboxStatus(status, decision string) string {
	_, _ = decision, status
	return strings.TrimSpace(status)
}

func mailboxListFilter(status string) (string, string) {
	status = strings.TrimSpace(status)
	return `status = $1`, status
}
