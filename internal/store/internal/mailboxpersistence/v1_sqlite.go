package mailboxpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	mailboxcontract "github.com/division-sh/swarm/internal/mailbox"
)

func (s *MailboxSQLiteOwner) ListV1MailboxItems(ctx context.Context, opts mailboxcontract.V1ListOptions) ([]mailboxcontract.V1Item, string, error) {
	if s == nil || s.backend == nil {
		return nil, "", fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return nil, "", err
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return nil, "", err
	}
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 200 {
		opts.Limit = 200
	}
	cursor, err := mailboxcontract.DecodeV1Cursor(opts.Cursor)
	if err != nil {
		return nil, "", err
	}
	where, args := sqliteMailboxV1ListWhere(opts, cursor)
	args = append(args, opts.Limit+1)
	rows, err := s.backend.QueryContext(ctx, `
		SELECT
			m.item_id,
			m.item_type,
			m.status,
			COALESCE(m.decision, ''),
			COALESCE(m.severity, 'normal'),
			COALESCE(m.source_event_id, ''),
			COALESCE(m.flow_instance, ''),
			COALESCE(m.entity_id, ''),
			COALESCE(m.payload, '{}'),
			m.expires_at,
			m.deferred_until,
			m.created_at,
			m.decided_at,
			COALESCE(m.decided_by, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.from_agent, ''),
			COALESCE(e.run_id, ''),
			COALESCE(e.execution_mode, ''),
			COALESCE(m.reply_context_id, '')
		FROM mailbox m
		LEFT JOIN events e ON e.event_id = m.source_event_id
		`+where+`
		ORDER BY m.created_at ASC, m.item_id ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list sqlite v1 mailbox items: %w", err)
	}
	defer rows.Close()
	rowItems, err := scanSQLiteMailboxV1Rows(rows)
	if err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(rowItems) > opts.Limit {
		next := rowItems[opts.Limit-1]
		nextCursor = mailboxcontract.EncodeV1Cursor(next.CreatedAtTime, next.ID)
		rowItems = rowItems[:opts.Limit]
	}
	items := make([]mailboxcontract.V1Item, 0, len(rowItems))
	for _, row := range rowItems {
		items = append(items, row.projectItem())
	}
	return items, nextCursor, nil
}

func (s *MailboxSQLiteOwner) GetV1MailboxItem(ctx context.Context, id string) (mailboxcontract.V1ItemDetail, error) {
	if s == nil || s.backend == nil {
		return mailboxcontract.V1ItemDetail{}, fmt.Errorf("sqlite runtime store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return mailboxcontract.V1ItemDetail{}, err
	}
	if strings.TrimSpace(id) == "" {
		return mailboxcontract.V1ItemDetail{}, mailboxcontract.ErrV1NotFound
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return mailboxcontract.V1ItemDetail{}, err
	}
	row, err := s.loadSQLiteMailboxV1Row(ctx, id)
	if err != nil {
		return mailboxcontract.V1ItemDetail{}, err
	}
	return row.projectDetail(), nil
}

func sqliteMailboxV1ListWhere(opts mailboxcontract.V1ListOptions, cursor mailboxcontract.V1Cursor) (string, []any) {
	clauses := []string{"1=1"}
	args := []any{}
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, clause)
	}
	switch strings.TrimSpace(strings.ToLower(opts.Status)) {
	case "":
	case "pending":
		clauses = append(clauses, "m.status = 'pending' AND m.deferred_until IS NULL")
	case "decided":
		clauses = append(clauses, "m.status = 'decided'")
	case "expired":
		clauses = append(clauses, "m.status = 'expired'")
	case "deferred":
		clauses = append(clauses, "m.status = 'pending' AND m.deferred_until IS NOT NULL")
	default:
		clauses = append(clauses, "false")
	}
	if runID := strings.TrimSpace(opts.RunID); runID != "" {
		add("e.run_id = ?", runID)
	}
	if entityID := strings.TrimSpace(opts.EntityID); entityID != "" {
		add("m.entity_id = ?", entityID)
	}
	if itemType := strings.TrimSpace(opts.Type); itemType != "" {
		add("m.item_type = ?", itemType)
	}
	if priority := mailboxV1SeverityForPriority(opts.Priority); priority != "" {
		add("COALESCE(m.severity, 'normal') = ?", priority)
	}
	if !cursor.CreatedAt.IsZero() {
		args = append(args, cursor.CreatedAt.UTC(), cursor.CreatedAt.UTC(), strings.TrimSpace(cursor.MailboxID))
		clauses = append(clauses, "(m.created_at > ? OR (m.created_at = ? AND m.item_id > ?))")
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func (s *MailboxSQLiteOwner) loadSQLiteMailboxV1Row(ctx context.Context, id string) (mailboxV1Row, error) {
	return s.loadSQLiteMailboxV1RowTx(ctx, nil, id)
}

func (s *MailboxSQLiteOwner) loadSQLiteMailboxV1RowTx(ctx context.Context, tx *sql.Tx, id string) (mailboxV1Row, error) {
	var q mailboxV1RowQueryer = s.backend
	if tx != nil {
		q = tx
	}
	rows, err := q.QueryContext(ctx, `
		SELECT
			m.item_id,
			m.item_type,
			m.status,
			COALESCE(m.decision, ''),
			COALESCE(m.severity, 'normal'),
			COALESCE(m.source_event_id, ''),
			COALESCE(m.flow_instance, ''),
			COALESCE(m.entity_id, ''),
			COALESCE(m.payload, '{}'),
			m.expires_at,
			m.deferred_until,
			m.created_at,
			m.decided_at,
			COALESCE(m.decided_by, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.from_agent, ''),
			COALESCE(e.run_id, ''),
			COALESCE(e.execution_mode, ''),
			COALESCE(m.reply_context_id, '')
		FROM mailbox m
		LEFT JOIN events e ON e.event_id = m.source_event_id
		WHERE m.item_id = ?
	`, strings.TrimSpace(id))
	if err != nil {
		return mailboxV1Row{}, fmt.Errorf("load sqlite v1 mailbox item: %w", err)
	}
	defer rows.Close()
	items, err := scanSQLiteMailboxV1Rows(rows)
	if err != nil {
		return mailboxV1Row{}, err
	}
	if len(items) == 0 {
		return mailboxV1Row{}, mailboxcontract.ErrV1NotFound
	}
	return items[0], nil
}

func scanSQLiteMailboxV1Rows(rows *sql.Rows) ([]mailboxV1Row, error) {
	out := make([]mailboxV1Row, 0)
	for rows.Next() {
		var row mailboxV1Row
		var payloadRaw any
		var expiresAtRaw any
		var deferredUntilRaw any
		var createdAtRaw any
		var decidedAtRaw any
		if err := rows.Scan(
			&row.ID,
			&row.Type,
			&row.Status,
			&row.Decision,
			&row.Priority,
			&row.SourceEventID,
			&row.FlowInstance,
			&row.EntityID,
			&payloadRaw,
			&expiresAtRaw,
			&deferredUntilRaw,
			&createdAtRaw,
			&decidedAtRaw,
			&row.DecidedBy,
			&row.DecisionNotes,
			&row.FromAgent,
			&row.RunID,
			&row.ExecutionMode,
			&row.ReplyContextID,
		); err != nil {
			return nil, fmt.Errorf("scan sqlite v1 mailbox item: %w", err)
		}
		row.RawPayload = jsonRawMessageValue(payloadRaw)
		row.Payload = map[string]any{}
		if len(row.RawPayload) > 0 {
			_ = json.Unmarshal(row.RawPayload, &row.Payload)
		}
		if row.Payload == nil {
			row.Payload = map[string]any{}
		}
		if err := validateMailboxV1ExecutionMode(row.ExecutionMode); err != nil {
			return nil, err
		}
		if at, ok, err := sqliteTimeValue(createdAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite v1 mailbox created_at: %w", err)
		} else if ok {
			row.CreatedAtTime = at
		}
		if at, ok, err := sqliteTimeValue(expiresAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite v1 mailbox expires_at: %w", err)
		} else if ok {
			row.ExpiresAt = sql.NullTime{Time: at, Valid: true}
		}
		if at, ok, err := sqliteTimeValue(deferredUntilRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite v1 mailbox deferred_until: %w", err)
		} else if ok {
			row.DeferredUntil = sql.NullTime{Time: at, Valid: true}
		}
		if at, ok, err := sqliteTimeValue(decidedAtRaw); err != nil {
			return nil, fmt.Errorf("scan sqlite v1 mailbox decided_at: %w", err)
		} else if ok {
			row.DecidedAt = sql.NullTime{Time: at, Valid: true}
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sqlite v1 mailbox items: %w", err)
	}
	return out, nil
}
