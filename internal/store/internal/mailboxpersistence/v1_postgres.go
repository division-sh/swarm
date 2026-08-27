package mailboxpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	mailboxcontract "github.com/division-sh/swarm/internal/mailbox"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func (s *MailboxPostgresOwner) ListV1MailboxItems(ctx context.Context, opts mailboxcontract.V1ListOptions) ([]mailboxcontract.V1Item, string, error) {
	if s == nil || s.backend == nil {
		return nil, "", fmt.Errorf("postgres store is required")
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
	where, args := mailboxV1ListWhere(opts, cursor)
	args = append(args, opts.Limit+1)
	rows, err := s.backend.QueryContext(ctx, `
		SELECT
			m.item_id::text,
			m.item_type,
			m.status,
			COALESCE(m.decision, ''),
			COALESCE(m.severity, 'normal'),
			COALESCE(m.source_event_id::text, ''),
			COALESCE(m.flow_instance, ''),
			COALESCE(m.entity_id::text, ''),
			COALESCE(m.payload, '{}'::jsonb),
			m.expires_at,
			m.deferred_until,
			m.created_at,
			m.decided_at,
			COALESCE(m.decided_by, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.from_agent, ''),
			COALESCE(e.run_id::text, ''),
			COALESCE(e.execution_mode, ''),
			COALESCE(m.reply_context_id, '')
		FROM mailbox m
		LEFT JOIN events e ON e.event_id = m.source_event_id
		`+where+`
		ORDER BY m.created_at ASC, m.item_id ASC
		LIMIT $`+fmt.Sprint(len(args))+`
	`, args...)
	if err != nil {
		return nil, "", fmt.Errorf("list v1 mailbox items: %w", err)
	}
	defer rows.Close()
	rowItems, err := scanMailboxV1Rows(rows)
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

func (s *MailboxPostgresOwner) GetV1MailboxItem(ctx context.Context, id string) (mailboxcontract.V1ItemDetail, error) {
	if s == nil || s.backend == nil {
		return mailboxcontract.V1ItemDetail{}, fmt.Errorf("postgres store is required")
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
	row, err := s.loadMailboxV1Row(ctx, id)
	if err != nil {
		return mailboxcontract.V1ItemDetail{}, err
	}
	return row.projectDetail(), nil
}

func mailboxV1ListWhere(opts mailboxcontract.V1ListOptions, cursor mailboxcontract.V1Cursor) (string, []any) {
	clauses := []string{"1=1"}
	args := []any{}
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
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
		add("e.run_id = $%d::uuid", runID)
	}
	if entityID := strings.TrimSpace(opts.EntityID); entityID != "" {
		add("m.entity_id = $%d::uuid", entityID)
	}
	if itemType := strings.TrimSpace(opts.Type); itemType != "" {
		add("m.item_type = $%d", itemType)
	}
	if priority := mailboxV1SeverityForPriority(opts.Priority); priority != "" {
		add("COALESCE(m.severity, 'normal') = $%d", priority)
	}
	if !cursor.CreatedAt.IsZero() {
		args = append(args, cursor.CreatedAt.UTC(), strings.TrimSpace(cursor.MailboxID))
		clauses = append(clauses, fmt.Sprintf("(m.created_at, m.item_id) > ($%d, $%d::uuid)", len(args)-1, len(args)))
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

type mailboxV1Row struct {
	ID             string
	Type           string
	Status         string
	Decision       string
	Priority       string
	SourceEventID  string
	FlowInstance   string
	EntityID       string
	Payload        map[string]any
	RawPayload     json.RawMessage
	ExpiresAt      sql.NullTime
	DeferredUntil  sql.NullTime
	CreatedAtTime  time.Time
	DecidedAt      sql.NullTime
	DecidedBy      string
	DecisionNotes  string
	FromAgent      string
	RunID          string
	ExecutionMode  string
	ReplyContextID string
}

type mailboxV1RowQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (s *MailboxPostgresOwner) loadMailboxV1Row(ctx context.Context, id string) (mailboxV1Row, error) {
	return s.loadMailboxV1RowTx(ctx, nil, id, false)
}

func (s *MailboxPostgresOwner) loadMailboxV1RowTx(ctx context.Context, tx *sql.Tx, id string, forUpdate bool) (mailboxV1Row, error) {
	var q mailboxV1RowQueryer = s.backend
	if tx != nil {
		q = tx
	}
	lockClause := ""
	if forUpdate {
		lockClause = " FOR UPDATE OF m"
	}
	rows, err := q.QueryContext(ctx, `
		SELECT
			m.item_id::text,
			m.item_type,
			m.status,
			COALESCE(m.decision, ''),
			COALESCE(m.severity, 'normal'),
			COALESCE(m.source_event_id::text, ''),
			COALESCE(m.flow_instance, ''),
			COALESCE(m.entity_id::text, ''),
			COALESCE(m.payload, '{}'::jsonb),
			m.expires_at,
			m.deferred_until,
			m.created_at,
			m.decided_at,
			COALESCE(m.decided_by, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.from_agent, ''),
			COALESCE(e.run_id::text, ''),
			COALESCE(e.execution_mode, ''),
			COALESCE(m.reply_context_id, '')
		FROM mailbox m
		LEFT JOIN events e ON e.event_id = m.source_event_id
		WHERE m.item_id = $1::uuid
		`+lockClause+`
	`, strings.TrimSpace(id))
	if err != nil {
		return mailboxV1Row{}, fmt.Errorf("load v1 mailbox item: %w", err)
	}
	defer rows.Close()
	items, err := scanMailboxV1Rows(rows)
	if err != nil {
		return mailboxV1Row{}, err
	}
	if len(items) == 0 {
		return mailboxV1Row{}, mailboxcontract.ErrV1NotFound
	}
	return items[0], nil
}

func scanMailboxV1Rows(rows *sql.Rows) ([]mailboxV1Row, error) {
	out := make([]mailboxV1Row, 0)
	for rows.Next() {
		var row mailboxV1Row
		if err := rows.Scan(
			&row.ID,
			&row.Type,
			&row.Status,
			&row.Decision,
			&row.Priority,
			&row.SourceEventID,
			&row.FlowInstance,
			&row.EntityID,
			&row.RawPayload,
			&row.ExpiresAt,
			&row.DeferredUntil,
			&row.CreatedAtTime,
			&row.DecidedAt,
			&row.DecidedBy,
			&row.DecisionNotes,
			&row.FromAgent,
			&row.RunID,
			&row.ExecutionMode,
			&row.ReplyContextID,
		); err != nil {
			return nil, fmt.Errorf("scan v1 mailbox item: %w", err)
		}
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
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate v1 mailbox items: %w", err)
	}
	return out, nil
}

func validateMailboxV1ExecutionMode(raw string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if _, ok := executionmode.Parse(raw); !ok {
		return fmt.Errorf("mailbox source event has invalid execution_mode %q", raw)
	}
	return nil
}

func (r mailboxV1Row) projectItem() mailboxcontract.V1Item {
	item := mailboxcontract.V1Item{
		MailboxID:      strings.TrimSpace(r.ID),
		Type:           strings.TrimSpace(r.Type),
		Status:         mailboxV1APIStatus(r.Status, r.Decision, r.DeferredUntil),
		Priority:       mailboxV1PriorityForSeverity(r.Priority),
		SourceEventID:  strings.TrimSpace(r.SourceEventID),
		ExecutionMode:  strings.TrimSpace(r.ExecutionMode),
		SourceRunID:    strings.TrimSpace(r.RunID),
		SourceFlow:     mailboxV1SourceFlow(r.FlowInstance),
		SourceEntityID: strings.TrimSpace(r.EntityID),
		Payload:        cloneMailboxV1Payload(r.Payload),
		CreatedAt:      r.CreatedAtTime.UTC().Format(time.RFC3339Nano),
		Decision:       mailboxV1Decision(r.Status, r.Decision),
	}
	if r.DecidedAt.Valid {
		item.DecidedAt = r.DecidedAt.Time.UTC().Format(time.RFC3339Nano)
	}
	if r.DeferredUntil.Valid {
		item.DeferredUntil = r.DeferredUntil.Time.UTC().Format(time.RFC3339Nano)
	}
	return item
}

func (r mailboxV1Row) projectDetail() mailboxcontract.V1ItemDetail {
	history := []mailboxcontract.V1HistoryEntry{{
		Action:       "created",
		ActorTokenID: strings.TrimSpace(coalesce(r.FromAgent, "system")),
		TS:           r.CreatedAtTime.UTC().Format(time.RFC3339Nano),
	}}
	if r.DecidedAt.Valid {
		action := mailboxV1Decision(r.Status, r.Decision)
		if action == "" && r.DeferredUntil.Valid {
			action = "deferred"
		}
		entry := mailboxcontract.V1HistoryEntry{
			Action:       action,
			ActorTokenID: strings.TrimSpace(coalesce(r.DecidedBy, "unknown")),
			TS:           r.DecidedAt.Time.UTC().Format(time.RFC3339Nano),
		}
		if strings.TrimSpace(r.DecisionNotes) != "" {
			entry.Reason = strings.TrimSpace(r.DecisionNotes)
		}
		history = append(history, entry)
	}
	return mailboxcontract.V1ItemDetail{
		Item:    r.projectItem(),
		Payload: cloneMailboxV1Payload(r.Payload),
		History: history,
	}
}

func mailboxV1APIStatus(status, decision string, deferredUntil sql.NullTime) string {
	status = strings.TrimSpace(status)
	decision = strings.TrimSpace(decision)
	switch {
	case status == "pending" && deferredUntil.Valid:
		return "deferred"
	case status == "expired" || status == "cancelled":
		return "expired"
	case status == "decided":
		return "decided"
	default:
		return "pending"
	}
}

func mailboxV1Decision(status, decision string) string {
	status = strings.TrimSpace(status)
	decision = strings.TrimSpace(decision)
	switch decision {
	case "approve":
		return "approved"
	case "reject":
		return "rejected"
	case "approved", "rejected", "expired":
		return decision
	}
	if status == "expired" || status == "cancelled" {
		return "expired"
	}
	return ""
}

func mailboxV1PriorityForSeverity(severity string) string {
	switch strings.TrimSpace(severity) {
	case "critical":
		return "critical"
	case "urgent", "high":
		return "high"
	default:
		return "normal"
	}
}

func mailboxV1SeverityForPriority(priority string) string {
	switch strings.TrimSpace(strings.ToLower(priority)) {
	case "":
		return ""
	case "critical":
		return "critical"
	case "high", "urgent":
		return "urgent"
	default:
		return "normal"
	}
}

func mailboxV1SourceFlow(flowInstance string) string {
	flowInstance = strings.TrimSpace(flowInstance)
	if flowInstance == "" {
		return "global"
	}
	return flowInstance
}

func cloneMailboxV1Payload(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		if strings.TrimSpace(k) == attemptgeneration.PayloadKey {
			continue
		}
		out[k] = v
	}
	return out
}
