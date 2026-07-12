package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

func (s *SQLiteRuntimeStore) ListV1MailboxItems(ctx context.Context, opts MailboxV1ListOptions) ([]MailboxV1Item, string, error) {
	if s == nil || s.DB == nil {
		return nil, "", fmt.Errorf("sqlite runtime store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return nil, "", err
	}
	if caps.Mailbox != SchemaFlavorCanonical {
		return nil, "", unsupportedSchemaCapability("mailbox", caps.Mailbox)
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
	cursor, err := decodeMailboxV1Cursor(opts.Cursor)
	if err != nil {
		return nil, "", err
	}
	where, args := sqliteMailboxV1ListWhere(opts, cursor)
	args = append(args, opts.Limit+1)
	rows, err := s.DB.QueryContext(ctx, `
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
		nextCursor = encodeMailboxV1Cursor(next.CreatedAtTime, next.ID)
		rowItems = rowItems[:opts.Limit]
	}
	items := make([]MailboxV1Item, 0, len(rowItems))
	for _, row := range rowItems {
		items = append(items, row.projectItem())
	}
	return items, nextCursor, nil
}

func (s *SQLiteRuntimeStore) GetV1MailboxItem(ctx context.Context, id string) (MailboxV1ItemDetail, error) {
	if s == nil || s.DB == nil {
		return MailboxV1ItemDetail{}, fmt.Errorf("sqlite runtime store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return MailboxV1ItemDetail{}, err
	}
	if caps.Mailbox != SchemaFlavorCanonical {
		return MailboxV1ItemDetail{}, unsupportedSchemaCapability("mailbox", caps.Mailbox)
	}
	if strings.TrimSpace(id) == "" {
		return MailboxV1ItemDetail{}, ErrMailboxV1NotFound
	}
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return MailboxV1ItemDetail{}, err
	}
	row, err := s.loadSQLiteMailboxV1Row(ctx, id)
	if err != nil {
		return MailboxV1ItemDetail{}, err
	}
	return row.projectDetail(), nil
}

func sqliteMailboxV1ListWhere(opts MailboxV1ListOptions, cursor mailboxV1Cursor) (string, []any) {
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

func (s *SQLiteRuntimeStore) loadSQLiteMailboxV1Row(ctx context.Context, id string) (mailboxV1Row, error) {
	return s.loadSQLiteMailboxV1RowTx(ctx, nil, id)
}

func (s *SQLiteRuntimeStore) loadSQLiteMailboxV1RowTx(ctx context.Context, tx *sql.Tx, id string) (mailboxV1Row, error) {
	var q mailboxV1RowQueryer = s.DB
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
		return mailboxV1Row{}, ErrMailboxV1NotFound
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

func sqliteStoreAPIIdempotency(ctx context.Context, q execQueryer, req APIIdempotencyRequest, completion APIIdempotencyCompletion) error {
	_, err := q.ExecContext(ctx, `
		INSERT INTO api_idempotency (
			method, actor_token_id, idempotency_key, request_hash,
			resource_id, response, created_at, expires_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, req.Method, req.ActorTokenID, req.IdempotencyKey, req.RequestHash, strings.TrimSpace(completion.ResourceID), string(completion.Response), req.Now.UTC(), req.Now.Add(req.TTL).UTC())
	if err != nil {
		return fmt.Errorf("store sqlite api idempotency response: %w", err)
	}
	return nil
}
