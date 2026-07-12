package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type decisionCardSQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

var _ decisioncard.Store = (*PostgresStore)(nil)
var _ decisioncard.Store = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) CreateDecisionCard(ctx context.Context, card decisioncard.Card) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return insertDecisionCard(ctx, tx, card, true)
	}
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		return insertDecisionCard(txctx, tx, card, true)
	})
}

func (s *SQLiteRuntimeStore) CreateDecisionCard(ctx context.Context, card decisioncard.Card) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return insertDecisionCard(ctx, tx, card, false)
	}
	return s.runRuntimeMutation(ctx, "sqlite create decision card", func(txctx context.Context, tx *sql.Tx) error {
		return insertDecisionCard(txctx, tx, card, false)
	})
}

func insertDecisionCard(ctx context.Context, db decisionCardSQL, card decisioncard.Card, postgres bool) error {
	if err := card.Validate(); err != nil {
		return err
	}
	snapshot, err := json.Marshal(card.Snapshot)
	if err != nil {
		return err
	}
	cadence, err := json.Marshal(card.EffectiveCadence)
	if err != nil {
		return err
	}
	provenance, err := json.Marshal(card.Provenance)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO decision_cards (
			card_id, run_id, flow_instance, flow_id, entity_id, stage,
			stage_activation_id, decision_id, status, snapshot,
			card_content_hash, decision_schema_hash, bundle_hash, workflow_version,
			effective_cadence, provenance, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT (card_id) DO NOTHING`
	if postgres {
		query = strings.ReplaceAll(query, "?", "$%d")
		query = numberPostgresPlaceholders(query)
	}
	res, err := db.ExecContext(ctx, query,
		card.CardID, card.RunID, card.FlowInstance, nullString(card.FlowID), card.EntityID, card.Stage,
		card.StageActivationID, card.DecisionID, card.Status, string(snapshot),
		card.CardContentHash, card.DecisionSchemaHash, card.BundleHash, nullString(card.WorkflowVersion),
		string(cadence), string(provenance), card.CreatedAt.UTC(), card.UpdatedAt.UTC(),
	)
	if err != nil {
		return fmt.Errorf("create decision card: %w", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		existing, loadErr := loadDecisionCard(ctx, db, card.CardID, postgres, false)
		if loadErr == nil && existing.CardContentHash == card.CardContentHash && existing.StageActivationID == card.StageActivationID {
			return nil
		}
		if loadErr != nil {
			return loadErr
		}
		return fmt.Errorf("decision card identity collision: %s", card.CardID)
	}
	_, err = appendDecisionCardChange(ctx, db, card.RunID, card.CardID, decisioncard.ChangeCreated, map[string]any{
		"status": card.Status, "stage_activation_id": card.StageActivationID,
	}, card.CreatedAt, postgres)
	return err
}

func (s *PostgresStore) GetDecisionCard(ctx context.Context, id string) (decisioncard.Card, error) {
	db := decisionCardSQL(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		db = tx
	}
	return loadDecisionCard(ctx, db, id, true, false)
}

func (s *SQLiteRuntimeStore) GetDecisionCard(ctx context.Context, id string) (decisioncard.Card, error) {
	db := decisionCardSQL(s.DB)
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		db = tx
	}
	return loadDecisionCard(ctx, db, id, false, false)
}

const decisionCardSelect = `SELECT
	card_id, run_id, flow_instance, COALESCE(flow_id, ''), entity_id, stage,
	stage_activation_id, decision_id, status, snapshot, card_content_hash,
	decision_schema_hash, bundle_hash, COALESCE(workflow_version, ''),
	effective_cadence, provenance, COALESCE(verdict, ''), COALESCE(fields, '{}'),
	COALESCE(decided_by, ''), decided_at, deferred_until,
	COALESCE(CAST(decision_event_id AS TEXT), ''), COALESCE(delivery_receipt_id, ''),
	COALESCE(delivery_render_hash, ''), COALESCE(superseded_reason, ''),
	created_at, updated_at
FROM decision_cards`

func loadDecisionCard(ctx context.Context, db decisionCardSQL, id string, postgres, forUpdate bool) (decisioncard.Card, error) {
	if strings.TrimSpace(id) == "" {
		return decisioncard.Card{}, decisioncard.ErrNotFound
	}
	query := decisionCardSelect + ` WHERE card_id = ?`
	if postgres {
		query = strings.Replace(query, "?", "$1", 1)
		if forUpdate {
			query += ` FOR UPDATE`
		}
	}
	return scanDecisionCard(db.QueryRowContext(ctx, query, strings.TrimSpace(id)))
}

func scanDecisionCard(row *sql.Row) (decisioncard.Card, error) {
	var card decisioncard.Card
	var snapshot, cadence, provenance, fields []byte
	var decidedAt, deferredUntil, createdAt, updatedAt any
	err := row.Scan(
		&card.CardID, &card.RunID, &card.FlowInstance, &card.FlowID, &card.EntityID, &card.Stage,
		&card.StageActivationID, &card.DecisionID, &card.Status, &snapshot, &card.CardContentHash,
		&card.DecisionSchemaHash, &card.BundleHash, &card.WorkflowVersion,
		&cadence, &provenance, &card.Verdict, &fields, &card.DecidedBy, &decidedAt, &deferredUntil,
		&card.DecisionEventID, &card.DeliveryReceiptID, &card.DeliveryRenderHash, &card.SupersededReason,
		&createdAt, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return decisioncard.Card{}, decisioncard.ErrNotFound
	}
	if err != nil {
		return decisioncard.Card{}, err
	}
	if err := json.Unmarshal(snapshot, &card.Snapshot); err != nil {
		return decisioncard.Card{}, fmt.Errorf("decode decision card snapshot: %w", err)
	}
	if err := json.Unmarshal(cadence, &card.EffectiveCadence); err != nil {
		return decisioncard.Card{}, fmt.Errorf("decode decision card cadence: %w", err)
	}
	if err := json.Unmarshal(provenance, &card.Provenance); err != nil {
		return decisioncard.Card{}, fmt.Errorf("decode decision card provenance: %w", err)
	}
	if len(fields) > 0 {
		if err := json.Unmarshal(fields, &card.Fields); err != nil {
			return decisioncard.Card{}, fmt.Errorf("decode decision card fields: %w", err)
		}
	}
	if at, ok, parseErr := sqliteTimeValue(decidedAt); parseErr != nil {
		return decisioncard.Card{}, parseErr
	} else if ok {
		card.DecidedAt = at
	}
	if at, ok, parseErr := sqliteTimeValue(deferredUntil); parseErr != nil {
		return decisioncard.Card{}, parseErr
	} else if ok {
		card.DeferredUntil = at
	}
	if at, ok, parseErr := sqliteTimeValue(createdAt); parseErr != nil || !ok {
		return decisioncard.Card{}, fmt.Errorf("decode decision card created_at: %w", parseErr)
	} else {
		card.CreatedAt = at
	}
	if at, ok, parseErr := sqliteTimeValue(updatedAt); parseErr != nil || !ok {
		return decisioncard.Card{}, fmt.Errorf("decode decision card updated_at: %w", parseErr)
	} else {
		card.UpdatedAt = at
	}
	return card, card.Validate()
}

func (s *PostgresStore) ListDecisionCards(ctx context.Context, opts decisioncard.ListOptions) ([]decisioncard.ListItem, string, error) {
	return listDecisionCards(ctx, s.DB, opts, true)
}

func (s *SQLiteRuntimeStore) ListDecisionCards(ctx context.Context, opts decisioncard.ListOptions) ([]decisioncard.ListItem, string, error) {
	return listDecisionCards(ctx, s.DB, opts, false)
}

func listDecisionCards(ctx context.Context, db decisionCardSQL, opts decisioncard.ListOptions, postgres bool) ([]decisioncard.ListItem, string, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 200 {
		opts.Limit = 200
	}
	cursor, err := decodeMailboxV1Cursor(opts.Cursor)
	if err != nil {
		return nil, "", decisioncard.ErrInvalidCursor
	}
	clauses := []string{"1=1"}
	args := []any{}
	add := func(column string, value any) {
		args = append(args, value)
		placeholder := "?"
		if postgres {
			placeholder = "$" + strconv.Itoa(len(args))
		}
		clauses = append(clauses, column+" = "+placeholder)
	}
	if value := strings.TrimSpace(opts.Status); value != "" {
		if value == "deferred" {
			clauses = append(clauses, "status = 'pending' AND deferred_until IS NOT NULL")
		} else if value == "pending" {
			clauses = append(clauses, "status = 'pending' AND deferred_until IS NULL")
		} else {
			add("status", value)
		}
	}
	if value := strings.TrimSpace(opts.RunID); value != "" {
		add("run_id", value)
	}
	if value := strings.TrimSpace(opts.EntityID); value != "" {
		add("entity_id", value)
	}
	if !cursor.CreatedAt.IsZero() {
		if postgres {
			args = append(args, cursor.CreatedAt.UTC(), cursor.MailboxID)
			first := "$" + strconv.Itoa(len(args)-1)
			second := "$" + strconv.Itoa(len(args))
			clauses = append(clauses, "(created_at > "+first+" OR (created_at = "+first+" AND card_id > "+second+"))")
		} else {
			args = append(args, cursor.CreatedAt.UTC(), cursor.CreatedAt.UTC(), cursor.MailboxID)
			clauses = append(clauses, "(created_at > ? OR (created_at = ? AND card_id > ?))")
		}
	}
	args = append(args, opts.Limit+1)
	limit := "?"
	if postgres {
		limit = "$" + strconv.Itoa(len(args))
	}
	query := `SELECT card_id, run_id, flow_instance, entity_id, stage, decision_id,
		COALESCE(snapshot->>'title', ''), status, deferred_until, created_at, updated_at
		FROM decision_cards WHERE ` + strings.Join(clauses, " AND ") + ` ORDER BY created_at, card_id LIMIT ` + limit
	if !postgres {
		query = strings.Replace(query, "snapshot->>'title'", "json_extract(snapshot, '$.title')", 1)
	}
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	results := []decisioncard.ListItem{}
	for rows.Next() {
		var row decisioncard.ListItem
		var deferred, created, updated any
		if err := rows.Scan(&row.CardID, &row.RunID, &row.FlowInstance, &row.EntityID, &row.Stage,
			&row.DecisionID, &row.Title, &row.Status, &deferred, &created, &updated); err != nil {
			return nil, "", err
		}
		row.Kind = decisioncard.KindDecisionCard
		if at, ok, err := sqliteTimeValue(deferred); err != nil {
			return nil, "", err
		} else if ok {
			row.DeferredUntil = at
		}
		if at, ok, err := sqliteTimeValue(created); err != nil || !ok {
			return nil, "", fmt.Errorf("decode decision card list created_at: %w", err)
		} else {
			row.CreatedAt = at
		}
		if at, ok, err := sqliteTimeValue(updated); err != nil || !ok {
			return nil, "", fmt.Errorf("decode decision card list updated_at: %w", err)
		} else {
			row.UpdatedAt = at
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(results) > opts.Limit {
		next = encodeMailboxV1Cursor(results[opts.Limit-1].CreatedAt, results[opts.Limit-1].CardID)
		results = results[:opts.Limit]
	}
	return results, next, nil
}

func (s *PostgresStore) DecideDecisionCard(ctx context.Context, req decisioncard.DecideRequest) (decisioncard.DecisionOutcome, error) {
	var out decisioncard.DecisionOutcome
	err := runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = decideDecisionCard(txctx, tx, req, true)
		return err
	})
	return out, err
}

func (s *SQLiteRuntimeStore) DecideDecisionCard(ctx context.Context, req decisioncard.DecideRequest) (decisioncard.DecisionOutcome, error) {
	var out decisioncard.DecisionOutcome
	err := s.runDecisionCardMutation(ctx, "sqlite decide decision card", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = decideDecisionCard(txctx, tx, req, false)
		return err
	})
	return out, err
}

func decideDecisionCard(ctx context.Context, tx *sql.Tx, req decisioncard.DecideRequest, postgres bool) (decisioncard.DecisionOutcome, error) {
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	card, err := loadDecisionCard(ctx, tx, req.CardID, postgres, true)
	if err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	if card.Status != decisioncard.StatusPending {
		return decisioncard.DecisionOutcome{}, decisioncard.ErrAlreadyTerminal
	}
	if strings.TrimSpace(req.ObservedContentHash) == "" || strings.TrimSpace(req.ObservedContentHash) != card.CardContentHash {
		return decisioncard.DecisionOutcome{}, decisioncard.ErrStaleContent
	}
	if err := decisioncard.ValidateDecision(card, req.Verdict, req.Fields); err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	if strings.TrimSpace(req.DecisionEventID) == "" {
		return decisioncard.DecisionOutcome{}, fmt.Errorf("decision event id is required")
	}
	if strings.TrimSpace(req.InputDraftID) != "" {
		draft, err := loadDecisionCardDraft(ctx, tx, req.InputDraftID, postgres)
		if err != nil {
			return decisioncard.DecisionOutcome{}, err
		}
		if draft.CardID != card.CardID || draft.ActorTokenID != strings.TrimSpace(req.ActorTokenID) || draft.Verdict != strings.TrimSpace(req.Verdict) || draft.Status != decisioncard.DraftStatusActive || !draft.ExpiresAt.After(now) {
			return decisioncard.DecisionOutcome{}, decisioncard.ErrDraftNotAuthority
		}
		if err := updateDecisionCardDraftStatus(ctx, tx, draft.InputDraftID, decisioncard.DraftStatusConsumed, now, postgres); err != nil {
			return decisioncard.DecisionOutcome{}, err
		}
		if _, err := appendDecisionCardChange(ctx, tx, draft.RunID, draft.CardID, decisioncard.ChangeDraftConsumed, map[string]any{"input_draft_id": draft.InputDraftID}, now, postgres); err != nil {
			return decisioncard.DecisionOutcome{}, err
		}
	}
	if _, err := transitionDecisionCardDrafts(ctx, tx, draftTransitionFilter{cardID: card.CardID, excludeID: strings.TrimSpace(req.InputDraftID)}, now, false, postgres); err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	fields, err := json.Marshal(req.Fields)
	if err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	query := `UPDATE decision_cards SET status = ?, verdict = ?, fields = ?, decided_by = ?, decided_at = ?, deferred_until = NULL,
		decision_event_id = ?, delivery_receipt_id = NULLIF(?, ''), delivery_render_hash = NULLIF(?, ''), updated_at = ?
		WHERE card_id = ? AND status = 'pending'`
	args := []any{decisioncard.StatusDecided, strings.TrimSpace(req.Verdict), string(fields), strings.TrimSpace(req.ActorTokenID), now,
		strings.TrimSpace(req.DecisionEventID), strings.TrimSpace(req.DeliveryReceiptID), strings.TrimSpace(req.DeliveryRenderHash), now, card.CardID}
	if postgres {
		query = numberPostgresPlaceholders(strings.ReplaceAll(query, "?", "$%d"))
	}
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return decisioncard.DecisionOutcome{}, decisioncard.ErrAlreadyTerminal
	}
	card.Status = decisioncard.StatusDecided
	card.Verdict = strings.TrimSpace(req.Verdict)
	card.Fields = req.Fields
	card.DecidedBy = strings.TrimSpace(req.ActorTokenID)
	card.DecidedAt = now
	card.DeferredUntil = time.Time{}
	card.DecisionEventID = strings.TrimSpace(req.DecisionEventID)
	card.DeliveryReceiptID = strings.TrimSpace(req.DeliveryReceiptID)
	card.DeliveryRenderHash = strings.TrimSpace(req.DeliveryRenderHash)
	card.UpdatedAt = now
	changeID, err := appendDecisionCardChange(ctx, tx, card.RunID, card.CardID, decisioncard.ChangeDecided, map[string]any{
		"verdict": card.Verdict, "decision_event_id": card.DecisionEventID,
	}, now, postgres)
	if err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	if err := insertDecisionRouteObligation(ctx, tx, card, now, postgres); err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	return decisioncard.DecisionOutcome{Card: card, ChangeID: changeID}, nil
}

func (s *PostgresStore) DeferDecisionCard(ctx context.Context, req decisioncard.DeferRequest) (decisioncard.DecisionOutcome, error) {
	var out decisioncard.DecisionOutcome
	err := runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = deferDecisionCard(txctx, tx, req, true)
		return err
	})
	return out, err
}

func (s *SQLiteRuntimeStore) DeferDecisionCard(ctx context.Context, req decisioncard.DeferRequest) (decisioncard.DecisionOutcome, error) {
	var out decisioncard.DecisionOutcome
	err := s.runDecisionCardMutation(ctx, "sqlite defer decision card", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = deferDecisionCard(txctx, tx, req, false)
		return err
	})
	return out, err
}

func deferDecisionCard(ctx context.Context, tx *sql.Tx, req decisioncard.DeferRequest, postgres bool) (decisioncard.DecisionOutcome, error) {
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if !req.Until.After(now) {
		return decisioncard.DecisionOutcome{}, decisioncard.ErrInvalidDeferUntil
	}
	card, err := loadDecisionCard(ctx, tx, req.CardID, postgres, true)
	if err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	if card.Status != decisioncard.StatusPending {
		return decisioncard.DecisionOutcome{}, decisioncard.ErrAlreadyTerminal
	}
	query := `UPDATE decision_cards SET deferred_until = ?, updated_at = ? WHERE card_id = ? AND status = 'pending'`
	if postgres {
		query = `UPDATE decision_cards SET deferred_until = $1, updated_at = $2 WHERE card_id = $3 AND status = 'pending'`
	}
	if _, err := tx.ExecContext(ctx, query, req.Until.UTC(), now, card.CardID); err != nil {
		return decisioncard.DecisionOutcome{}, err
	}
	card.DeferredUntil = req.Until.UTC()
	card.UpdatedAt = now
	changeID, err := appendDecisionCardChange(ctx, tx, card.RunID, card.CardID, decisioncard.ChangeDeferred, map[string]any{"until": card.DeferredUntil}, now, postgres)
	return decisioncard.DecisionOutcome{Card: card, ChangeID: changeID}, err
}

func (s *PostgresStore) BeginDecisionCardInput(ctx context.Context, req decisioncard.BeginInputRequest) (decisioncard.InputDraft, error) {
	var draft decisioncard.InputDraft
	err := runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		draft, err = beginDecisionCardInput(txctx, tx, req, true)
		return err
	})
	return draft, err
}

func (s *SQLiteRuntimeStore) BeginDecisionCardInput(ctx context.Context, req decisioncard.BeginInputRequest) (decisioncard.InputDraft, error) {
	var draft decisioncard.InputDraft
	err := s.runDecisionCardMutation(ctx, "sqlite begin decision card input", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		draft, err = beginDecisionCardInput(txctx, tx, req, false)
		return err
	})
	return draft, err
}

func beginDecisionCardInput(ctx context.Context, tx *sql.Tx, req decisioncard.BeginInputRequest, postgres bool) (decisioncard.InputDraft, error) {
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if req.TTL <= 0 {
		req.TTL = 15 * time.Minute
	}
	card, err := loadDecisionCard(ctx, tx, req.CardID, postgres, true)
	if err != nil {
		return decisioncard.InputDraft{}, err
	}
	if card.Status != decisioncard.StatusPending {
		return decisioncard.InputDraft{}, decisioncard.ErrAlreadyTerminal
	}
	outcome, ok := card.Snapshot.Outcomes[strings.TrimSpace(req.Verdict)]
	if !ok {
		return decisioncard.InputDraft{}, decisioncard.ErrInvalidVerdict
	}
	requiresInput := false
	for _, field := range outcome.Input {
		requiresInput = requiresInput || field.Required
	}
	if !requiresInput {
		return decisioncard.InputDraft{}, fmt.Errorf("verdict %s does not require an input draft", req.Verdict)
	}
	actor := strings.TrimSpace(req.ActorTokenID)
	if actor == "" {
		return decisioncard.InputDraft{}, fmt.Errorf("actor token id is required")
	}
	if _, err := transitionDecisionCardDrafts(ctx, tx, draftTransitionFilter{cardID: card.CardID, actor: actor}, now, false, postgres); err != nil {
		return decisioncard.InputDraft{}, err
	}
	draft := decisioncard.InputDraft{
		InputDraftID: uuid.NewString(), RunID: card.RunID, CardID: card.CardID, ActorTokenID: actor,
		Verdict: strings.TrimSpace(req.Verdict), DeliveryReceiptID: strings.TrimSpace(req.DeliveryReceiptID), Status: decisioncard.DraftStatusActive,
		ExpiresAt: now.Add(req.TTL), CreatedAt: now, UpdatedAt: now,
	}
	query := `INSERT INTO decision_card_input_drafts (input_draft_id, run_id, card_id, actor_token_id, verdict, delivery_receipt_id, status, expires_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	if postgres {
		query = numberPostgresPlaceholders(strings.ReplaceAll(query, "?", "$%d"))
	}
	if _, err := tx.ExecContext(ctx, query, draft.InputDraftID, draft.RunID, draft.CardID, draft.ActorTokenID, draft.Verdict, nullString(draft.DeliveryReceiptID), draft.Status, draft.ExpiresAt, draft.CreatedAt, draft.UpdatedAt); err != nil {
		return decisioncard.InputDraft{}, err
	}
	_, err = appendDecisionCardChange(ctx, tx, card.RunID, card.CardID, decisioncard.ChangeDraftStarted, map[string]any{
		"input_draft_id": draft.InputDraftID, "verdict": draft.Verdict, "actor_token_id": draft.ActorTokenID, "expires_at": draft.ExpiresAt,
	}, now, postgres)
	return draft, err
}

func (s *PostgresStore) CancelDecisionCardInput(ctx context.Context, req decisioncard.CancelInputRequest) (decisioncard.InputDraft, error) {
	var draft decisioncard.InputDraft
	err := runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		draft, err = cancelDecisionCardInput(txctx, tx, req, true)
		return err
	})
	return draft, err
}

func (s *SQLiteRuntimeStore) CancelDecisionCardInput(ctx context.Context, req decisioncard.CancelInputRequest) (decisioncard.InputDraft, error) {
	var draft decisioncard.InputDraft
	err := s.runDecisionCardMutation(ctx, "sqlite cancel decision card input", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		draft, err = cancelDecisionCardInput(txctx, tx, req, false)
		return err
	})
	return draft, err
}

func cancelDecisionCardInput(ctx context.Context, tx *sql.Tx, req decisioncard.CancelInputRequest, postgres bool) (decisioncard.InputDraft, error) {
	now := req.Now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	draft, err := loadDecisionCardDraft(ctx, tx, req.InputDraftID, postgres)
	if err != nil {
		return decisioncard.InputDraft{}, err
	}
	if draft.CardID != strings.TrimSpace(req.CardID) || draft.ActorTokenID != strings.TrimSpace(req.ActorTokenID) || draft.Status != decisioncard.DraftStatusActive || !draft.ExpiresAt.After(now) {
		return decisioncard.InputDraft{}, decisioncard.ErrDraftNotAuthority
	}
	if err := updateDecisionCardDraftStatus(ctx, tx, draft.InputDraftID, decisioncard.DraftStatusCancelled, now, postgres); err != nil {
		return decisioncard.InputDraft{}, err
	}
	draft.Status = decisioncard.DraftStatusCancelled
	draft.UpdatedAt = now
	_, err = appendDecisionCardChange(ctx, tx, draft.RunID, draft.CardID, decisioncard.ChangeDraftCancelled, map[string]any{"input_draft_id": draft.InputDraftID}, now, postgres)
	return draft, err
}

func loadDecisionCardDraft(ctx context.Context, db decisionCardSQL, id string, postgres bool) (decisioncard.InputDraft, error) {
	query := `SELECT input_draft_id, run_id, card_id, actor_token_id, verdict, COALESCE(delivery_receipt_id, ''), status, expires_at, created_at, updated_at FROM decision_card_input_drafts WHERE input_draft_id = ?`
	if postgres {
		query = strings.Replace(query, "?", "$1", 1) + ` FOR UPDATE`
	}
	var draft decisioncard.InputDraft
	var expires, created, updated any
	err := db.QueryRowContext(ctx, query, strings.TrimSpace(id)).Scan(&draft.InputDraftID, &draft.RunID, &draft.CardID, &draft.ActorTokenID, &draft.Verdict, &draft.DeliveryReceiptID, &draft.Status, &expires, &created, &updated)
	if errors.Is(err, sql.ErrNoRows) {
		return decisioncard.InputDraft{}, decisioncard.ErrDraftNotFound
	}
	if err != nil {
		return draft, err
	}
	for _, item := range []struct {
		raw    any
		target *time.Time
	}{{expires, &draft.ExpiresAt}, {created, &draft.CreatedAt}, {updated, &draft.UpdatedAt}} {
		at, ok, parseErr := sqliteTimeValue(item.raw)
		if parseErr != nil || !ok {
			return decisioncard.InputDraft{}, fmt.Errorf("decode decision card draft timestamp: %w", parseErr)
		}
		*item.target = at
	}
	return draft, nil
}

func updateDecisionCardDraftStatus(ctx context.Context, db decisionCardSQL, id, status string, now time.Time, postgres bool) error {
	query := `UPDATE decision_card_input_drafts SET status = ?, updated_at = ? WHERE input_draft_id = ? AND status = 'active'`
	if postgres {
		query = `UPDATE decision_card_input_drafts SET status = $1, updated_at = $2 WHERE input_draft_id = $3 AND status = 'active'`
	}
	res, err := db.ExecContext(ctx, query, status, now, id)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows != 1 {
		return decisioncard.ErrDraftNotAuthority
	}
	return nil
}

type draftTransitionFilter struct {
	runID       string
	cardID      string
	actor       string
	excludeID   string
	onlyExpired bool
}

func transitionDecisionCardDrafts(ctx context.Context, tx *sql.Tx, filter draftTransitionFilter, now time.Time, onlyExpired, postgres bool) (int, error) {
	filter.onlyExpired = onlyExpired
	clauses := []string{"status = 'active'"}
	args := []any{}
	add := func(column, value string, negate bool) {
		args = append(args, value)
		op := " = "
		if negate {
			op = " <> "
		}
		placeholder := "?"
		if postgres {
			placeholder = "$" + strconv.Itoa(len(args))
		}
		clauses = append(clauses, column+op+placeholder)
	}
	if value := strings.TrimSpace(filter.runID); value != "" {
		add("run_id", value, false)
	}
	if value := strings.TrimSpace(filter.cardID); value != "" {
		add("card_id", value, false)
	}
	if value := strings.TrimSpace(filter.actor); value != "" {
		add("actor_token_id", value, false)
	}
	if value := strings.TrimSpace(filter.excludeID); value != "" {
		add("input_draft_id", value, true)
	}
	if onlyExpired {
		args = append(args, now)
		placeholder := "?"
		if postgres {
			placeholder = "$" + strconv.Itoa(len(args))
		}
		clauses = append(clauses, "expires_at <= "+placeholder)
	}
	query := `SELECT input_draft_id, run_id, card_id, expires_at FROM decision_card_input_drafts WHERE ` + strings.Join(clauses, " AND ") + ` ORDER BY input_draft_id`
	if postgres {
		query += ` FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	type draftRow struct {
		id, runID, cardID string
		expiresAt         time.Time
	}
	drafts := []draftRow{}
	for rows.Next() {
		var row draftRow
		var rawExpires any
		if err := rows.Scan(&row.id, &row.runID, &row.cardID, &rawExpires); err != nil {
			rows.Close()
			return 0, err
		}
		expiresAt, ok, err := sqliteTimeValue(rawExpires)
		if err != nil || !ok {
			rows.Close()
			return 0, fmt.Errorf("decode decision card draft expiry: %w", err)
		}
		row.expiresAt = expiresAt
		drafts = append(drafts, row)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, draft := range drafts {
		status := decisioncard.DraftStatusCancelled
		changeType := decisioncard.ChangeDraftCancelled
		if !draft.expiresAt.After(now) {
			status = decisioncard.DraftStatusExpired
			changeType = decisioncard.ChangeDraftExpired
		}
		if err := updateDecisionCardDraftStatus(ctx, tx, draft.id, status, now, postgres); err != nil {
			return 0, err
		}
		if _, err := appendDecisionCardChange(ctx, tx, draft.runID, draft.cardID, changeType, map[string]any{"input_draft_id": draft.id}, now, postgres); err != nil {
			return 0, err
		}
	}
	return len(drafts), nil
}

func (s *PostgresStore) SupersedeDecisionCardsForStage(ctx context.Context, runID, entityID, activationID, reason string, now time.Time) error {
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		return supersedeDecisionCardsForStage(txctx, tx, runID, entityID, activationID, reason, now, true)
	})
}

func (s *SQLiteRuntimeStore) SupersedeDecisionCardsForStage(ctx context.Context, runID, entityID, activationID, reason string, now time.Time) error {
	return s.runDecisionCardMutation(ctx, "sqlite supersede decision card", func(txctx context.Context, tx *sql.Tx) error {
		return supersedeDecisionCardsForStage(txctx, tx, runID, entityID, activationID, reason, now, false)
	})
}

func supersedeDecisionCardsForStage(ctx context.Context, tx *sql.Tx, runID, entityID, activationID, reason string, now time.Time, postgres bool) error {
	now = now.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}
	card, err := loadDecisionCardByActivation(ctx, tx, runID, entityID, activationID, postgres)
	if errors.Is(err, decisioncard.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if card.Status != decisioncard.StatusPending {
		return nil
	}
	if _, err := transitionDecisionCardDrafts(ctx, tx, draftTransitionFilter{cardID: card.CardID}, now, false, postgres); err != nil {
		return err
	}
	query := `UPDATE decision_cards SET status = ?, superseded_reason = ?, updated_at = ? WHERE card_id = ? AND status = 'pending'`
	if postgres {
		query = `UPDATE decision_cards SET status = $1, superseded_reason = $2, updated_at = $3 WHERE card_id = $4 AND status = 'pending'`
	}
	if _, err := tx.ExecContext(ctx, query, decisioncard.StatusSuperseded, strings.TrimSpace(reason), now, card.CardID); err != nil {
		return err
	}
	_, err = appendDecisionCardChange(ctx, tx, card.RunID, card.CardID, decisioncard.ChangeSuperseded, map[string]any{"reason": strings.TrimSpace(reason)}, now, postgres)
	return err
}

func (s *PostgresStore) SupersedeDecisionCardsForRun(ctx context.Context, runID, reason string, now time.Time) error {
	return runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		return supersedeDecisionCardsForRun(txctx, tx, runID, reason, now, true)
	})
}

func (s *SQLiteRuntimeStore) SupersedeDecisionCardsForRun(ctx context.Context, runID, reason string, now time.Time) error {
	return s.runDecisionCardMutation(ctx, "sqlite supersede run decision cards", func(txctx context.Context, tx *sql.Tx) error {
		return supersedeDecisionCardsForRun(txctx, tx, runID, reason, now, false)
	})
}

func supersedeDecisionCardsForRun(ctx context.Context, tx *sql.Tx, runID, reason string, now time.Time, postgres bool) error {
	runID = strings.TrimSpace(runID)
	reason = strings.TrimSpace(reason)
	now = now.UTC()
	if runID == "" {
		return fmt.Errorf("run id is required to supersede decision cards")
	}
	if reason == "" {
		return fmt.Errorf("supersession reason is required")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := supersedeRunGateActivations(ctx, tx, runID, reason, now, postgres); err != nil {
		return err
	}
	query := `SELECT card_id FROM decision_cards WHERE run_id = ? AND status = 'pending' ORDER BY card_id`
	if postgres {
		query = `SELECT card_id FROM decision_cards WHERE run_id = $1 AND status = 'pending' ORDER BY card_id FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return err
	}
	var cardIDs []string
	for rows.Next() {
		var cardID string
		if err := rows.Scan(&cardID); err != nil {
			rows.Close()
			return err
		}
		cardIDs = append(cardIDs, strings.TrimSpace(cardID))
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, cardID := range cardIDs {
		card, err := loadDecisionCard(ctx, tx, cardID, postgres, true)
		if err != nil {
			return err
		}
		update := `UPDATE decision_cards SET status = ?, superseded_reason = ?, updated_at = ? WHERE card_id = ? AND status = 'pending'`
		if postgres {
			update = `UPDATE decision_cards SET status = $1, superseded_reason = $2, updated_at = $3 WHERE card_id = $4 AND status = 'pending'`
		}
		if _, err := tx.ExecContext(ctx, update, decisioncard.StatusSuperseded, reason, now, cardID); err != nil {
			return err
		}
		if _, err := appendDecisionCardChange(ctx, tx, runID, cardID, decisioncard.ChangeSuperseded, map[string]any{"reason": reason}, now, postgres); err != nil {
			return err
		}
		payload, err := json.Marshal(map[string]any{
			"card_id": card.CardID, "stage_activation_id": card.StageActivationID, "reason": reason,
		})
		if err != nil {
			return err
		}
		evt := events.NewRuntimeControlEvent(uuid.NewString(), events.EventType("mailbox.card_superseded"), "platform", "", payload, 0, card.RunID, "",
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, card.EntityID), card.FlowInstance), now)
		if err := insertDecisionCardLifecycleOutbox(ctx, tx, card, evt, postgres); err != nil {
			return fmt.Errorf("queue run decision card supersession event: %w", err)
		}
	}
	_, err = transitionDecisionCardDrafts(ctx, tx, draftTransitionFilter{runID: runID}, now, false, postgres)
	return err
}

func (s *PostgresStore) ExpireDecisionCardInputDrafts(ctx context.Context, now time.Time) (int, error) {
	count := 0
	err := runPostgresDecisionCardMutation(ctx, s.DB, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = transitionDecisionCardDrafts(txctx, tx, draftTransitionFilter{}, now.UTC(), true, true)
		return err
	})
	return count, err
}

func (s *SQLiteRuntimeStore) ExpireDecisionCardInputDrafts(ctx context.Context, now time.Time) (int, error) {
	count := 0
	err := s.runDecisionCardMutation(ctx, "sqlite expire decision card input drafts", func(txctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = transitionDecisionCardDrafts(txctx, tx, draftTransitionFilter{}, now.UTC(), true, false)
		return err
	})
	return count, err
}

func supersedeRunGateActivations(ctx context.Context, tx *sql.Tx, runID, reason string, now time.Time, postgres bool) error {
	query := `SELECT entity_id, accumulator FROM entity_state WHERE run_id = ? ORDER BY entity_id`
	if postgres {
		query = `SELECT entity_id::text, accumulator FROM entity_state WHERE run_id = $1::uuid ORDER BY entity_id FOR UPDATE`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return fmt.Errorf("load run gate activations: %w", err)
	}
	type update struct {
		entityID    string
		accumulator map[string]any
	}
	updates := []update{}
	for rows.Next() {
		var entityID string
		var raw any
		if err := rows.Scan(&entityID, &raw); err != nil {
			rows.Close()
			return err
		}
		accumulator, err := toolDecodeJSONMap(raw)
		if err != nil {
			rows.Close()
			return fmt.Errorf("decode run gate activations for entity %s: %w", entityID, err)
		}
		carrier, err := runtimeengine.StateCarrierFromPersisted(nil, accumulator)
		if err != nil {
			rows.Close()
			return err
		}
		activations, err := gateruntime.List(carrier.StateBuckets)
		if err != nil {
			rows.Close()
			return err
		}
		changed := false
		for _, activation := range activations {
			if activation.Status == gateruntime.StatusDecisionCommitted {
				rows.Close()
				return fmt.Errorf("run %s cannot terminate while decision card %s has a committed verdict awaiting its frozen route", runID, activation.CardID)
			}
			if activation.Supersede(reason, now) {
				if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
					rows.Close()
					return err
				}
				changed = true
			}
		}
		if changed {
			updates = append(updates, update{entityID: strings.TrimSpace(entityID), accumulator: carrier.PersistedStateBuckets()})
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range updates {
		raw, err := json.Marshal(item.accumulator)
		if err != nil {
			return err
		}
		query := `UPDATE entity_state SET accumulator = ?, updated_at = ? WHERE run_id = ? AND entity_id = ?`
		args := []any{string(raw), now, runID, item.entityID}
		if postgres {
			query = `UPDATE entity_state SET accumulator = $1::jsonb, updated_at = $2 WHERE run_id = $3::uuid AND entity_id = $4::uuid`
		}
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("persist run gate activation supersession: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return fmt.Errorf("persist run gate activation supersession for entity %s affected %d rows: %w", item.entityID, affected, err)
		}
	}
	return nil
}

func loadDecisionCardByActivation(ctx context.Context, db decisionCardSQL, runID, entityID, activationID string, postgres bool) (decisioncard.Card, error) {
	query := decisionCardSelect + ` WHERE entity_id = ? AND stage_activation_id = ?`
	args := []any{entityID, activationID}
	if strings.TrimSpace(runID) != "" {
		query += ` AND run_id = ?`
		args = append(args, runID)
	}
	if postgres {
		query = numberPostgresPlaceholders(strings.ReplaceAll(query, "?", "$%d")) + ` FOR UPDATE`
	}
	return scanDecisionCard(db.QueryRowContext(ctx, query, args...))
}

func (s *PostgresStore) ListDecisionCardChanges(ctx context.Context, opts decisioncard.SubscriptionOptions) ([]decisioncard.Change, error) {
	return listDecisionCardChanges(ctx, s.DB, opts, true)
}

func (s *SQLiteRuntimeStore) ListDecisionCardChanges(ctx context.Context, opts decisioncard.SubscriptionOptions) ([]decisioncard.Change, error) {
	return listDecisionCardChanges(ctx, s.DB, opts, false)
}

func listDecisionCardChanges(ctx context.Context, db decisionCardSQL, opts decisioncard.SubscriptionOptions, postgres bool) ([]decisioncard.Change, error) {
	if opts.Limit <= 0 || opts.Limit > 200 {
		opts.Limit = 200
	}
	query := `SELECT change_id, card_id, run_id, change_type, payload, created_at FROM decision_card_changes WHERE change_id > ? ORDER BY change_id LIMIT ?`
	if postgres {
		query = `SELECT change_id, card_id, run_id, change_type, payload, created_at FROM decision_card_changes WHERE change_id > $1 ORDER BY change_id LIMIT $2`
	}
	rows, err := db.QueryContext(ctx, query, opts.After, opts.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []decisioncard.Change{}
	for rows.Next() {
		var change decisioncard.Change
		var payload []byte
		var created any
		if err := rows.Scan(&change.Sequence, &change.CardID, &change.RunID, &change.ChangeType, &payload, &created); err != nil {
			return nil, err
		}
		if at, ok, err := sqliteTimeValue(created); err != nil || !ok {
			return nil, fmt.Errorf("decode decision card change created_at: %w", err)
		} else {
			change.CreatedAt = at
		}
		if err := json.Unmarshal(payload, &change.Payload); err != nil {
			return nil, err
		}
		out = append(out, change)
	}
	return out, rows.Err()
}

func appendDecisionCardChange(ctx context.Context, db decisionCardSQL, runID, cardID, changeType string, payload map[string]any, now time.Time, postgres bool) (int64, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	query := `INSERT INTO decision_card_changes (run_id, card_id, change_type, payload, created_at) VALUES (?, ?, ?, ?, ?) RETURNING change_id`
	if postgres {
		query = `INSERT INTO decision_card_changes (run_id, card_id, change_type, payload, created_at) VALUES ($1, $2, $3, $4, $5) RETURNING change_id`
	}
	var id int64
	if err := db.QueryRowContext(ctx, query, runID, cardID, changeType, string(raw), now.UTC()).Scan(&id); err != nil {
		return 0, fmt.Errorf("append decision card change: %w", err)
	}
	return id, nil
}

func runPostgresDecisionCardMutation(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return fn(ctx, tx)
	}
	conn, borrowed := runtimepipeline.PipelineSQLConnFromContext(ctx)
	if !borrowed {
		var err error
		conn, err = db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	txctx := runtimepipeline.WithPipelineSQLConnContext(ctx, conn)
	txctx = runtimepipeline.WithPipelineSQLTxContext(txctx, tx)
	if err := fn(txctx, tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteRuntimeStore) runDecisionCardMutation(ctx context.Context, label string, fn func(context.Context, *sql.Tx) error) error {
	if tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx); ok && tx != nil {
		return fn(ctx, tx)
	}
	return s.runRuntimeMutation(ctx, label, fn)
}

func numberPostgresPlaceholders(query string) string {
	for index := 1; strings.Contains(query, "$%d"); index++ {
		query = strings.Replace(query, "$%d", "$"+strconv.Itoa(index), 1)
	}
	return query
}

func nullString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
