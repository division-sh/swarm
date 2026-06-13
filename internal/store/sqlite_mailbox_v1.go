package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/google/uuid"
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
			m.created_at,
			m.decided_at,
			COALESCE(m.decided_by, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.from_agent, ''),
			COALESCE(e.run_id, '')
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

func (s *SQLiteRuntimeStore) DecideV1MailboxItem(ctx context.Context, input MailboxV1DecisionRequest) (MailboxV1DecisionOutcome, error) {
	if s == nil || s.DB == nil {
		return MailboxV1DecisionOutcome{}, fmt.Errorf("sqlite runtime store is required")
	}
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return MailboxV1DecisionOutcome{}, err
	}
	if caps.Mailbox != SchemaFlavorCanonical {
		return MailboxV1DecisionOutcome{}, unsupportedSchemaCapability("mailbox", caps.Mailbox)
	}
	if input.Now.IsZero() {
		input.Now = time.Now().UTC()
	}
	input.Now = input.Now.UTC()
	if _, err := s.ExpireMailboxItems(ctx, 200); err != nil {
		return MailboxV1DecisionOutcome{}, err
	}

	action := strings.TrimSpace(strings.ToLower(input.Action))
	switch action {
	case "approve", "approved":
		action = "approved"
	case "reject", "rejected":
		action = "rejected"
	case "defer", "deferred":
		action = "deferred"
	default:
		return MailboxV1DecisionOutcome{}, fmt.Errorf("unsupported mailbox decision action %q", input.Action)
	}
	idempotencyReq, hasIdempotency, err := prepareMailboxV1IdempotencyRequest(input)
	if err != nil {
		return MailboxV1DecisionOutcome{}, err
	}
	var outcome MailboxV1DecisionOutcome
	if err := s.runRuntimeMutation(ctx, "sqlite v1 mailbox decision", func(txctx context.Context, tx *sql.Tx) error {
		if hasIdempotency {
			if _, err := tx.ExecContext(txctx, `DELETE FROM api_idempotency WHERE expires_at <= ?`, idempotencyReq.Now.UTC()); err != nil {
				return fmt.Errorf("purge expired sqlite api idempotency: %w", err)
			}
			existing, ok, err := sqliteLoadAPIIdempotency(txctx, tx, idempotencyReq)
			if err != nil {
				return err
			}
			if ok {
				if existing.RequestHash != idempotencyReq.RequestHash {
					return &APIIdempotencyConflictError{
						OriginalRequestHash:    existing.RequestHash,
						ConflictingRequestHash: idempotencyReq.RequestHash,
						Method:                 idempotencyReq.Method,
						ResourceID:             existing.ResourceID,
					}
				}
				var result MailboxV1DecisionResult
				if err := json.Unmarshal(existing.Response, &result); err != nil {
					return fmt.Errorf("decode sqlite api idempotency mailbox response: %w", err)
				}
				if err := normalizeSQLiteMailboxV1DecisionReplayResult(txctx, tx, &result); err != nil {
					return err
				}
				outcome = MailboxV1DecisionOutcome{Result: result, Replayed: true}
				return nil
			}
		}
		if action == "deferred" && !input.DeferUntil.After(input.Now) {
			return &MailboxV1InvalidDeferUntilError{Reason: "in_past"}
		}

		row, err := s.loadSQLiteMailboxV1RowTx(txctx, tx, input.MailboxID)
		if err != nil {
			return err
		}
		if row.Status != "pending" {
			return &MailboxV1AlreadyDecidedError{
				MailboxID:        row.ID,
				ExistingDecision: row.existingDecision(),
				DecidedAt:        row.decisionTime(input.Now),
			}
		}
		if action == "approved" && strings.TrimSpace(input.ApprovalEventType) == "" {
			return &MailboxV1ApprovalRouteError{MailboxID: row.ID, ItemType: row.Type}
		}

		decisionID := uuid.NewString()
		outcome = MailboxV1DecisionOutcome{
			Result: MailboxV1DecisionResult{
				OK:                true,
				MailboxDecisionID: decisionID,
				Status:            mailboxV1APIStatus("decided", action),
			},
		}
		var expiresAt any
		notes := strings.TrimSpace(input.Reason)
		if action == "deferred" {
			expiresAt = input.DeferUntil.UTC()
			if notes == "" {
				notes = "Deferred until " + input.DeferUntil.UTC().Format(time.RFC3339Nano)
			}
		}
		if action == "approved" {
			eventID := uuid.NewString()
			evt, err := row.approvalEvent(eventID, decisionID, strings.TrimSpace(input.ApprovalEventType), input.ActorTokenID, input.DecisionPayload, input.Now)
			if err != nil {
				return err
			}
			subscribers := append([]string(nil), input.ApprovalEventSubscribers...)
			if subscribers == nil {
				subscribers = []string{}
			}
			subscriberSource := strings.TrimSpace(input.ApprovalEventSubscriberSource)
			if subscriberSource == "" {
				subscriberSource = "unavailable"
			}
			outcome.Result.DownstreamEventID = eventID
			outcome.Result.DownstreamEventName = strings.TrimSpace(input.ApprovalEventType)
			outcome.Result.DownstreamSubscribers = &subscribers
			outcome.Result.DownstreamSubscriberSource = subscriberSource
			outcome.ApprovalEvent = &evt
		}
		res, err := tx.ExecContext(txctx, `
			UPDATE mailbox
			SET status = 'decided',
			    decision = ?,
			    decision_notes = NULLIF(?, ''),
			    decided_by = NULLIF(?, ''),
			    decided_at = ?,
			    expires_at = COALESCE(?, expires_at)
			WHERE item_id = ?
			  AND status = 'pending'
		`, action, notes, strings.TrimSpace(input.ActorTokenID), input.Now, expiresAt, row.ID)
		if err != nil {
			return fmt.Errorf("decide sqlite v1 mailbox item: %w", err)
		}
		if rows, _ := res.RowsAffected(); rows == 0 {
			latest, latestErr := s.loadSQLiteMailboxV1RowTx(txctx, tx, row.ID)
			if latestErr != nil {
				return latestErr
			}
			return &MailboxV1AlreadyDecidedError{
				MailboxID:        latest.ID,
				ExistingDecision: latest.existingDecision(),
				DecidedAt:        latest.decisionTime(input.Now),
			}
		}
		if outcome.ApprovalEvent != nil {
			publish := input.ApprovalEventPublish
			if publish == nil {
				publish = func(ctx context.Context, evt events.Event) error {
					return s.appendSQLiteMailboxV1ApprovalEventTx(ctx, tx, evt)
				}
			}
			if err := publish(txctx, *outcome.ApprovalEvent); err != nil {
				return fmt.Errorf("publish sqlite v1 mailbox approval event: %w", err)
			}
		}
		if hasIdempotency {
			raw, err := json.Marshal(outcome.Result)
			if err != nil {
				return err
			}
			if err := sqliteStoreAPIIdempotency(txctx, tx, idempotencyReq, APIIdempotencyCompletion{
				ResourceID: row.ID,
				Response:   raw,
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return MailboxV1DecisionOutcome{}, err
	}
	return outcome, nil
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
		clauses = append(clauses, "m.status = 'pending'")
	case "decided":
		clauses = append(clauses, "m.status = 'decided' AND COALESCE(m.decision, '') <> 'deferred'")
	case "expired":
		clauses = append(clauses, "m.status = 'expired'")
	case "deferred":
		clauses = append(clauses, "m.status = 'decided' AND COALESCE(m.decision, '') = 'deferred'")
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
			m.created_at,
			m.decided_at,
			COALESCE(m.decided_by, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.from_agent, ''),
			COALESCE(e.run_id, '')
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
			&createdAtRaw,
			&decidedAtRaw,
			&row.DecidedBy,
			&row.DecisionNotes,
			&row.FromAgent,
			&row.RunID,
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

func (s *SQLiteRuntimeStore) appendSQLiteMailboxV1ApprovalEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	return s.AppendEventTx(ctx, tx, evt)
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

func normalizeSQLiteMailboxV1DecisionReplayResult(ctx context.Context, q execQueryer, result *MailboxV1DecisionResult) error {
	if result == nil || strings.TrimSpace(result.DownstreamEventID) == "" {
		return nil
	}
	if strings.TrimSpace(result.DownstreamEventName) == "" {
		eventName, err := loadSQLiteMailboxV1DecisionReplayEventName(ctx, q, result.DownstreamEventID)
		if err != nil {
			return err
		}
		result.DownstreamEventName = eventName
	}
	if result.DownstreamSubscribers == nil {
		subscribers := []string{}
		result.DownstreamSubscribers = &subscribers
	}
	if strings.TrimSpace(result.DownstreamSubscriberSource) == "" {
		result.DownstreamSubscriberSource = "unavailable"
	}
	return nil
}

func loadSQLiteMailboxV1DecisionReplayEventName(ctx context.Context, q execQueryer, eventID string) (string, error) {
	var eventName string
	err := q.QueryRowContext(ctx, `
		SELECT event_name
		FROM events
		WHERE event_id = ?
	`, strings.TrimSpace(eventID)).Scan(&eventName)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", fmt.Errorf("load sqlite mailbox idempotency replay event name: event %s not found", strings.TrimSpace(eventID))
	case err != nil:
		return "", fmt.Errorf("load sqlite mailbox idempotency replay event name: %w", err)
	}
	eventName = strings.TrimSpace(eventName)
	if eventName == "" {
		return "", fmt.Errorf("load sqlite mailbox idempotency replay event name: event %s has empty event_name", strings.TrimSpace(eventID))
	}
	return eventName, nil
}
