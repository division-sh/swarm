package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/google/uuid"
)

var ErrMailboxV1NotFound = errors.New("mailbox item not found")
var ErrMailboxV1InvalidCursor = errors.New("invalid mailbox cursor")
var ErrMailboxV1ApprovalRouteUnconfigured = errors.New("mailbox approval event route is not configured")

type MailboxV1ListOptions struct {
	Status   string
	RunID    string
	EntityID string
	Type     string
	Priority string
	Limit    int
	Cursor   string
}

type MailboxV1Item struct {
	MailboxID      string         `json:"mailbox_id"`
	Type           string         `json:"type"`
	Status         string         `json:"status"`
	Priority       string         `json:"priority"`
	SourceEventID  string         `json:"source_event_id"`
	SourceRunID    string         `json:"-"`
	SourceFlow     string         `json:"source_flow"`
	SourceEntityID string         `json:"source_entity_id,omitempty"`
	Payload        map[string]any `json:"payload"`
	CreatedAt      string         `json:"created_at"`
	DecidedAt      string         `json:"decided_at,omitempty"`
	Decision       string         `json:"decision,omitempty"`
}

type MailboxV1HistoryEntry struct {
	Action          string         `json:"action"`
	ActorTokenID    string         `json:"actor_token_id"`
	TS              string         `json:"ts"`
	DecisionPayload map[string]any `json:"decision_payload,omitempty"`
	Reason          string         `json:"reason,omitempty"`
}

type MailboxV1ItemDetail struct {
	Item          MailboxV1Item           `json:"item"`
	Payload       map[string]any          `json:"payload"`
	History       []MailboxV1HistoryEntry `json:"history"`
	DecisionSheet *MailboxV1DecisionSheet `json:"decision_sheet,omitempty"`
}

type MailboxV1DecisionSheet struct {
	EntityContext     MailboxV1EntityContext     `json:"entity_context"`
	DownstreamPreview MailboxV1DownstreamPreview `json:"downstream_preview"`
}

type MailboxV1EntityContext struct {
	Available bool                `json:"available"`
	Reason    string              `json:"reason,omitempty"`
	Entity    *OperatorEntityFull `json:"entity,omitempty"`
}

type MailboxV1DownstreamPreview struct {
	Available        bool     `json:"available"`
	Reason           string   `json:"reason,omitempty"`
	EventName        string   `json:"event_name,omitempty"`
	Subscribers      []string `json:"subscribers"`
	SubscriberSource string   `json:"subscriber_source"`
}

type MailboxV1DecisionRequest struct {
	MailboxID                     string
	Action                        string
	ActorTokenID                  string
	Reason                        string
	DecisionPayload               json.RawMessage
	DeferUntil                    time.Time
	Now                           time.Time
	ApprovalEventType             string
	ApprovalEventSubscribers      []string
	ApprovalEventSubscriberSource string
	ApprovalEventPublish          func(context.Context, events.Event) error
	Idempotency                   *APIIdempotencyRequest
}

type MailboxV1DecisionResult struct {
	OK                         bool      `json:"ok"`
	MailboxDecisionID          string    `json:"mailbox_decision_id"`
	DownstreamEventID          string    `json:"downstream_event_id,omitempty"`
	DownstreamEventName        string    `json:"downstream_event_name,omitempty"`
	DownstreamSubscribers      *[]string `json:"downstream_subscribers,omitempty"`
	DownstreamSubscriberSource string    `json:"downstream_subscriber_source,omitempty"`
	Status                     string    `json:"status"`
	IdempotencyReplayed        bool      `json:"idempotency_replayed"`
}

type MailboxV1DecisionOutcome struct {
	Result        MailboxV1DecisionResult
	ApprovalEvent *events.Event
	Replayed      bool
}

type MailboxV1AlreadyDecidedError struct {
	MailboxID        string
	ExistingDecision string
	DecidedAt        time.Time
}

func (e *MailboxV1AlreadyDecidedError) Error() string {
	return "mailbox item is already decided"
}

type MailboxV1InvalidDeferUntilError struct {
	Reason   string
	MaxUntil *time.Time
}

func (e *MailboxV1InvalidDeferUntilError) Error() string {
	return "invalid mailbox defer timestamp"
}

type MailboxV1ApprovalRouteError struct {
	MailboxID string
	ItemType  string
}

func (e *MailboxV1ApprovalRouteError) Error() string {
	return "mailbox approval event route is not configured"
}

func (e *MailboxV1ApprovalRouteError) Is(target error) bool {
	return target == ErrMailboxV1ApprovalRouteUnconfigured
}

func (s *PostgresStore) ListV1MailboxItems(ctx context.Context, opts MailboxV1ListOptions) ([]MailboxV1Item, string, error) {
	if s == nil || s.DB == nil {
		return nil, "", fmt.Errorf("postgres store is required")
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
	where, args := mailboxV1ListWhere(opts, cursor)
	args = append(args, opts.Limit+1)
	rows, err := s.DB.QueryContext(ctx, `
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
			m.created_at,
			m.decided_at,
			COALESCE(m.decided_by, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.from_agent, ''),
			COALESCE(e.run_id::text, '')
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
		nextCursor = encodeMailboxV1Cursor(next.CreatedAtTime, next.ID)
		rowItems = rowItems[:opts.Limit]
	}
	items := make([]MailboxV1Item, 0, len(rowItems))
	for _, row := range rowItems {
		items = append(items, row.projectItem())
	}
	return items, nextCursor, nil
}

func (s *PostgresStore) GetV1MailboxItem(ctx context.Context, id string) (MailboxV1ItemDetail, error) {
	if s == nil || s.DB == nil {
		return MailboxV1ItemDetail{}, fmt.Errorf("postgres store is required")
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
	row, err := s.loadMailboxV1Row(ctx, id)
	if err != nil {
		return MailboxV1ItemDetail{}, err
	}
	return row.projectDetail(), nil
}

func (s *PostgresStore) DecideV1MailboxItem(ctx context.Context, input MailboxV1DecisionRequest) (MailboxV1DecisionOutcome, error) {
	if s == nil || s.DB == nil {
		return MailboxV1DecisionOutcome{}, fmt.Errorf("postgres store is required")
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

	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return MailboxV1DecisionOutcome{}, fmt.Errorf("begin v1 mailbox decision tx: %w", err)
	}
	postCommitActions := make([]func(), 0, 4)
	txctx := runtimepipeline.WithPipelineSQLTxContext(ctx, tx)
	txctx = runtimepipeline.WithPipelinePostCommitActions(txctx, &postCommitActions)
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	idempotencyReq, hasIdempotency, err := prepareMailboxV1IdempotencyRequest(input)
	if err != nil {
		return MailboxV1DecisionOutcome{}, err
	}
	if hasIdempotency {
		if _, err := tx.ExecContext(txctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, apiIdempotencyLockKey(idempotencyReq.Method, idempotencyReq.ActorTokenID, idempotencyReq.IdempotencyKey)); err != nil {
			return MailboxV1DecisionOutcome{}, fmt.Errorf("lock api idempotency key: %w", err)
		}
		if err := purgeExpiredAPIIdempotency(txctx, tx, idempotencyReq.Now); err != nil {
			return MailboxV1DecisionOutcome{}, err
		}
		existing, ok, err := loadAPIIdempotency(txctx, tx, idempotencyReq)
		if err != nil {
			return MailboxV1DecisionOutcome{}, err
		}
		if ok {
			if existing.RequestHash != idempotencyReq.RequestHash {
				return MailboxV1DecisionOutcome{}, &APIIdempotencyConflictError{
					OriginalRequestHash:    existing.RequestHash,
					ConflictingRequestHash: idempotencyReq.RequestHash,
					Method:                 idempotencyReq.Method,
					ResourceID:             existing.ResourceID,
				}
			}
			var result MailboxV1DecisionResult
			if err := json.Unmarshal(existing.Response, &result); err != nil {
				return MailboxV1DecisionOutcome{}, fmt.Errorf("decode api idempotency mailbox response: %w", err)
			}
			if err := normalizeMailboxV1DecisionReplayResult(txctx, tx, &result); err != nil {
				return MailboxV1DecisionOutcome{}, err
			}
			if err := tx.Commit(); err != nil {
				return MailboxV1DecisionOutcome{}, fmt.Errorf("commit v1 mailbox idempotency replay tx: %w", err)
			}
			committed = true
			return MailboxV1DecisionOutcome{Result: result, Replayed: true}, nil
		}
	}
	if action == "deferred" && !input.DeferUntil.After(input.Now) {
		return MailboxV1DecisionOutcome{}, &MailboxV1InvalidDeferUntilError{Reason: "in_past"}
	}

	row, err := s.loadMailboxV1RowTx(txctx, tx, input.MailboxID, true)
	if err != nil {
		return MailboxV1DecisionOutcome{}, err
	}
	if row.Status != "pending" {
		return MailboxV1DecisionOutcome{}, &MailboxV1AlreadyDecidedError{
			MailboxID:        row.ID,
			ExistingDecision: row.existingDecision(),
			DecidedAt:        row.decisionTime(input.Now),
		}
	}
	if action == "approved" && strings.TrimSpace(input.ApprovalEventType) == "" {
		return MailboxV1DecisionOutcome{}, &MailboxV1ApprovalRouteError{MailboxID: row.ID, ItemType: row.Type}
	}

	decisionID := uuid.NewString()
	outcome := MailboxV1DecisionOutcome{
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
			return MailboxV1DecisionOutcome{}, err
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
		    decision = $2,
		    decision_notes = NULLIF($3, ''),
		    decided_by = NULLIF($4, ''),
		    decided_at = $5,
		    expires_at = COALESCE($6::timestamptz, expires_at)
		WHERE item_id = $1::uuid
		  AND status = 'pending'
	`, row.ID, action, notes, strings.TrimSpace(input.ActorTokenID), input.Now, expiresAt)
	if err != nil {
		return MailboxV1DecisionOutcome{}, fmt.Errorf("decide v1 mailbox item: %w", err)
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		latest, latestErr := s.loadMailboxV1RowTx(txctx, tx, row.ID, false)
		if latestErr != nil {
			return MailboxV1DecisionOutcome{}, latestErr
		}
		return MailboxV1DecisionOutcome{}, &MailboxV1AlreadyDecidedError{
			MailboxID:        latest.ID,
			ExistingDecision: latest.existingDecision(),
			DecidedAt:        latest.decisionTime(input.Now),
		}
	}
	if outcome.ApprovalEvent != nil {
		publish := input.ApprovalEventPublish
		if publish == nil {
			publish = func(ctx context.Context, evt events.Event) error {
				return s.appendMailboxV1ApprovalEventTx(ctx, tx, evt)
			}
		}
		if err := publish(txctx, *outcome.ApprovalEvent); err != nil {
			return MailboxV1DecisionOutcome{}, fmt.Errorf("publish v1 mailbox approval event: %w", err)
		}
	}
	if hasIdempotency {
		raw, err := json.Marshal(outcome.Result)
		if err != nil {
			return MailboxV1DecisionOutcome{}, err
		}
		if err := storeAPIIdempotency(txctx, tx, idempotencyReq, APIIdempotencyCompletion{
			ResourceID: row.ID,
			Response:   raw,
		}); err != nil {
			return MailboxV1DecisionOutcome{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return MailboxV1DecisionOutcome{}, fmt.Errorf("commit v1 mailbox decision tx: %w", err)
	}
	committed = true
	runtimepipeline.FlushPipelinePostCommitActions(postCommitActions)
	return outcome, nil
}

func prepareMailboxV1IdempotencyRequest(input MailboxV1DecisionRequest) (APIIdempotencyRequest, bool, error) {
	if input.Idempotency == nil || strings.TrimSpace(input.Idempotency.IdempotencyKey) == "" {
		return APIIdempotencyRequest{}, false, nil
	}
	req := *input.Idempotency
	req.Method = strings.TrimSpace(req.Method)
	req.ActorTokenID = strings.TrimSpace(req.ActorTokenID)
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.RequestHash = strings.TrimSpace(req.RequestHash)
	req.ResourceID = strings.TrimSpace(req.ResourceID)
	if req.ResourceID == "" {
		req.ResourceID = strings.TrimSpace(input.MailboxID)
	}
	if req.Method == "" || req.ActorTokenID == "" || req.RequestHash == "" {
		return APIIdempotencyRequest{}, false, fmt.Errorf("method, actor token id, and request hash are required")
	}
	if req.TTL <= 0 {
		req.TTL = 24 * time.Hour
	}
	if req.Now.IsZero() {
		req.Now = input.Now
	}
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	req.Now = req.Now.UTC()
	return req, true, nil
}

func normalizeMailboxV1DecisionReplayResult(ctx context.Context, q execQueryer, result *MailboxV1DecisionResult) error {
	if result == nil || strings.TrimSpace(result.DownstreamEventID) == "" {
		return nil
	}
	if strings.TrimSpace(result.DownstreamEventName) == "" {
		eventName, err := loadMailboxV1DecisionReplayEventName(ctx, q, result.DownstreamEventID)
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

func loadMailboxV1DecisionReplayEventName(ctx context.Context, q execQueryer, eventID string) (string, error) {
	var eventName string
	err := q.QueryRowContext(ctx, `
		SELECT event_name
		FROM events
		WHERE event_id = $1::uuid
	`, strings.TrimSpace(eventID)).Scan(&eventName)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", fmt.Errorf("load mailbox idempotency replay event name: event %s not found", strings.TrimSpace(eventID))
	case err != nil:
		return "", fmt.Errorf("load mailbox idempotency replay event name: %w", err)
	}
	eventName = strings.TrimSpace(eventName)
	if eventName == "" {
		return "", fmt.Errorf("load mailbox idempotency replay event name: event %s has empty event_name", strings.TrimSpace(eventID))
	}
	return eventName, nil
}

func (s *PostgresStore) appendMailboxV1ApprovalEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	if err := s.AppendEventTx(ctx, tx, evt); err != nil {
		return err
	}
	if err := s.UpsertCommittedReplayScopeTx(ctx, tx, evt.ID(), runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		return err
	}
	if err := s.UpsertPipelineReceiptTx(ctx, tx, evt.ID(), "processed", ""); err != nil {
		return err
	}
	return nil
}

type mailboxV1Cursor struct {
	CreatedAt time.Time `json:"created_at"`
	MailboxID string    `json:"mailbox_id"`
}

func decodeMailboxV1Cursor(raw string) (mailboxV1Cursor, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return mailboxV1Cursor{}, nil
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return mailboxV1Cursor{}, ErrMailboxV1InvalidCursor
	}
	var cursor mailboxV1Cursor
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return mailboxV1Cursor{}, ErrMailboxV1InvalidCursor
	}
	if cursor.CreatedAt.IsZero() || strings.TrimSpace(cursor.MailboxID) == "" {
		return mailboxV1Cursor{}, ErrMailboxV1InvalidCursor
	}
	return cursor, nil
}

func encodeMailboxV1Cursor(createdAt time.Time, mailboxID string) string {
	raw, _ := json.Marshal(mailboxV1Cursor{CreatedAt: createdAt.UTC(), MailboxID: strings.TrimSpace(mailboxID)})
	return base64.RawURLEncoding.EncodeToString(raw)
}

func mailboxV1ListWhere(opts MailboxV1ListOptions, cursor mailboxV1Cursor) (string, []any) {
	clauses := []string{"1=1"}
	args := []any{}
	add := func(clause string, value any) {
		args = append(args, value)
		clauses = append(clauses, fmt.Sprintf(clause, len(args)))
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
	ID            string
	Type          string
	Status        string
	Decision      string
	Priority      string
	SourceEventID string
	FlowInstance  string
	EntityID      string
	Payload       map[string]any
	RawPayload    json.RawMessage
	CreatedAtTime time.Time
	DecidedAt     sql.NullTime
	DecidedBy     string
	DecisionNotes string
	FromAgent     string
	RunID         string
}

type mailboxV1RowQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (s *PostgresStore) loadMailboxV1Row(ctx context.Context, id string) (mailboxV1Row, error) {
	return s.loadMailboxV1RowTx(ctx, nil, id, false)
}

func (s *PostgresStore) loadMailboxV1RowTx(ctx context.Context, tx *sql.Tx, id string, forUpdate bool) (mailboxV1Row, error) {
	var q mailboxV1RowQueryer = s.DB
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
			m.created_at,
			m.decided_at,
			COALESCE(m.decided_by, ''),
			COALESCE(m.decision_notes, ''),
			COALESCE(m.from_agent, ''),
			COALESCE(e.run_id::text, '')
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
		return mailboxV1Row{}, ErrMailboxV1NotFound
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
			&row.CreatedAtTime,
			&row.DecidedAt,
			&row.DecidedBy,
			&row.DecisionNotes,
			&row.FromAgent,
			&row.RunID,
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
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate v1 mailbox items: %w", err)
	}
	return out, nil
}

func (r mailboxV1Row) projectItem() MailboxV1Item {
	item := MailboxV1Item{
		MailboxID:      strings.TrimSpace(r.ID),
		Type:           strings.TrimSpace(r.Type),
		Status:         mailboxV1APIStatus(r.Status, r.Decision),
		Priority:       mailboxV1PriorityForSeverity(r.Priority),
		SourceEventID:  strings.TrimSpace(r.SourceEventID),
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
	return item
}

func (r mailboxV1Row) projectDetail() MailboxV1ItemDetail {
	history := []MailboxV1HistoryEntry{{
		Action:       "created",
		ActorTokenID: strings.TrimSpace(coalesce(r.FromAgent, "system")),
		TS:           r.CreatedAtTime.UTC().Format(time.RFC3339Nano),
	}}
	if r.DecidedAt.Valid {
		entry := MailboxV1HistoryEntry{
			Action:       mailboxV1Decision(r.Status, r.Decision),
			ActorTokenID: strings.TrimSpace(coalesce(r.DecidedBy, "unknown")),
			TS:           r.DecidedAt.Time.UTC().Format(time.RFC3339Nano),
		}
		if strings.TrimSpace(r.DecisionNotes) != "" {
			entry.Reason = strings.TrimSpace(r.DecisionNotes)
		}
		history = append(history, entry)
	}
	return MailboxV1ItemDetail{
		Item:    r.projectItem(),
		Payload: cloneMailboxV1Payload(r.Payload),
		History: history,
	}
}

func (r mailboxV1Row) existingDecision() string {
	if decision := mailboxV1Decision(r.Status, r.Decision); decision != "" {
		return decision
	}
	if strings.TrimSpace(r.Status) == "expired" {
		return "expired"
	}
	return "expired"
}

func (r mailboxV1Row) decisionTime(fallback time.Time) time.Time {
	if r.DecidedAt.Valid {
		return r.DecidedAt.Time.UTC()
	}
	if !fallback.IsZero() {
		return fallback.UTC()
	}
	return time.Now().UTC()
}

func (r mailboxV1Row) approvalEvent(eventID, decisionID, eventType, actorTokenID string, decisionPayload json.RawMessage, now time.Time) (events.Event, error) {
	payloadMap := map[string]any{}
	if len(decisionPayload) > 0 {
		if err := json.Unmarshal(decisionPayload, &payloadMap); err != nil {
			return events.EmptyEvent(), fmt.Errorf("decode decision payload: %w", err)
		}
	}
	if payloadMap == nil {
		payloadMap = map[string]any{}
	}
	eventPayload := map[string]any{
		"mailbox_id":          strings.TrimSpace(r.ID),
		"mailbox_decision_id": strings.TrimSpace(decisionID),
		"decision":            "approved",
		"decision_payload":    payloadMap,
		"item_type":           strings.TrimSpace(r.Type),
		"mailbox_payload":     cloneMailboxV1Payload(r.Payload),
		"source_event_id":     strings.TrimSpace(r.SourceEventID),
		"source_flow":         mailboxV1SourceFlow(r.FlowInstance),
		"source_entity_id":    strings.TrimSpace(r.EntityID),
		"decided_by":          strings.TrimSpace(actorTokenID),
		"decided_at":          now.UTC().Format(time.RFC3339Nano),
	}
	raw, err := json.Marshal(eventPayload)
	if err != nil {
		return events.EmptyEvent(), err
	}
	envelope := events.EventEnvelope{
		EntityID:     strings.TrimSpace(r.EntityID),
		FlowInstance: strings.TrimSpace(r.FlowInstance),
	}
	evt := events.NewChildEventWithLineage(
		strings.TrimSpace(eventID),
		events.EventType(strings.TrimSpace(eventType)),
		"mailbox_human",
		"",
		raw,
		0,
		events.EventLineage{
			RunID:         strings.TrimSpace(r.RunID),
			ParentEventID: strings.TrimSpace(r.SourceEventID),
		},
		envelope,
		now.UTC(),
	)
	return evt, nil
}

func mailboxV1APIStatus(status, decision string) string {
	status = strings.TrimSpace(status)
	decision = strings.TrimSpace(decision)
	switch {
	case status == "decided" && decision == "deferred":
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
	case "approved", "rejected", "deferred", "expired":
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
		out[k] = v
	}
	return out
}
