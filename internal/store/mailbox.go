package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
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
	if err := validateGenericMailboxNotice(item.Type, item.Context); err != nil {
		return "", err
	}
	if strings.TrimSpace(item.ReplyContextID) == "" {
		item.ReplyContextID = events.DeliveryContextFromContext(ctx).ReplyContextID()
	}
	if caps.Mailbox == SchemaFlavorCanonical {
		if err := s.insertMailboxItemSpec(ctx, item); err != nil {
			return "", err
		}
		return item.ID, nil
	}
	return "", unsupportedSchemaCapability("mailbox", caps.Mailbox)
}

func validateGenericMailboxNotice(itemType string, raw []byte) error {
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("mailbox context must be a JSON object: %w", err)
	}
	return decisioncard.ValidateNoticeShape(itemType, payload)
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
			notified, expires_at, reply_context_id, created_at
		)
		VALUES (
			$1::uuid, NULLIF($2,'')::uuid, NULLIF($3,''), $4, $5, NULLIF($6,'')::uuid,
			NULLIF($7,''), $8, NULLIF($9,''), $10::jsonb, $11, NULLIF($12,''),
			NULLIF($13,''), $14, $15, NULLIF($16,''), now()
		)
	`, item.ID, coalesceMailboxEntityID(item), strings.Trim(strings.TrimSpace(item.FlowInstance), "/"), scope, item.Type, item.EventID, item.FromAgent, normalizeMailboxSeverity(item.Priority), item.Summary, string(item.Context), status, decision, item.DecisionNotes, item.Notified, expiresAt, strings.TrimSpace(item.ReplyContextID))
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
			COALESCE(flow_instance, ''),
			COALESCE(from_agent, ''),
			item_type,
			COALESCE(severity, 'normal'),
			status,
			COALESCE(notified, false),
			COALESCE(payload, '{}'::jsonb),
			COALESCE(summary, ''),
			expires_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, ''),
			COALESCE(reply_context_id, '')
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
			COALESCE(flow_instance, ''),
			COALESCE(from_agent, ''),
			item_type,
			COALESCE(severity, 'normal'),
			status,
			COALESCE(notified, false),
			COALESCE(payload, '{}'::jsonb),
			COALESCE(summary, ''),
			expires_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, ''),
			COALESCE(reply_context_id, '')
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
			COALESCE(m.flow_instance, ''),
			COALESCE(m.from_agent, ''),
			m.item_type,
			COALESCE(m.severity, 'normal'),
			m.status,
			COALESCE(m.notified, false),
			COALESCE(m.payload, '{}'::jsonb),
			COALESCE(m.summary, ''),
			m.expires_at,
			COALESCE(m.decision, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.reply_context_id, '')
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
			COALESCE(flow_instance, ''),
			COALESCE(from_agent, ''),
			item_type,
			COALESCE(severity, 'normal'),
			status,
			COALESCE(notified, false),
			COALESCE(payload, '{}'::jsonb),
			COALESCE(summary, ''),
			expires_at,
			COALESCE(decision, ''),
			COALESCE(decision_notes, ''),
			COALESCE(reply_context_id, '')
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
	db := interface {
		ExecContext(context.Context, string, ...any) (sql.Result, error)
	}(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		db = tx
	}
	result, err := db.ExecContext(ctx, `
		UPDATE mailbox
		SET notified = true
		WHERE item_id = $1::uuid
	`, id)
	if err != nil {
		return fmt.Errorf("mark mailbox item notified: %w", err)
	}
	if rows, err := result.RowsAffected(); err != nil {
		return err
	} else if rows == 0 {
		return ErrMailboxV1NotFound
	}
	return nil
}

func scanSpecMailboxItems(rows *sql.Rows) ([]runtimetools.MailboxItem, error) {
	out := make([]runtimetools.MailboxItem, 0)
	for rows.Next() {
		var it runtimetools.MailboxItem
		var contextRaw any
		var timeoutRaw any
		if err := rows.Scan(
			&it.ID,
			&it.EventID,
			&it.EntityID,
			&it.FlowInstance,
			&it.FromAgent,
			&it.Type,
			&it.Priority,
			&it.Status,
			&it.Notified,
			&contextRaw,
			&it.Summary,
			&timeoutRaw,
			&it.Decision,
			&it.DecisionNotes,
			&it.ReplyContextID,
		); err != nil {
			return nil, fmt.Errorf("scan mailbox item: %w", err)
		}
		it.Context = jsonRawMessageValue(contextRaw)
		it.Priority = denormalizeMailboxSeverity(it.Priority)
		it.Status = denormalizeMailboxStatus(it.Status, it.Decision)
		if timeout, ok, err := sqliteTimeValue(timeoutRaw); err != nil {
			return nil, fmt.Errorf("scan mailbox timeout: %w", err)
		} else if ok {
			it.TimeoutAt = timeout
		}
		out = append(out, it)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate mailbox items: %w", err)
	}
	return out, nil
}

func jsonRawMessageValue(raw any) json.RawMessage {
	switch v := raw.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), v...)
	case []byte:
		return json.RawMessage(append([]byte(nil), v...))
	case string:
		return json.RawMessage([]byte(v))
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil
		}
		return encoded
	}
}

func normalizeMailboxSeverity(priority string) string {
	switch strings.TrimSpace(strings.ToLower(priority)) {
	case "critical":
		return "critical"
	case "urgent", "high":
		return "urgent"
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

func denormalizeMailboxStatus(status, decision string) string {
	_, _ = decision, status
	return strings.TrimSpace(status)
}

func mailboxListFilter(status string) (string, string) {
	status = strings.TrimSpace(status)
	return `status = $1`, status
}
