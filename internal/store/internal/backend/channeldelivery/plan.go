package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/google/uuid"
)

type Plan struct {
	DeliveryID             string
	SourceKind             string
	SourceID               string
	RequestActivationID    string
	SummaryCount           int64
	PrincipalID            string
	InterfaceKey           string
	BindingRevision        int64
	CurrentBindingRevision int64
	DeliveryEpoch          int64
	ExternalAccountRef     string
	ConversationRef        string
	ConversationScope      operatorchannel.ConversationScope
	State                  string
	CurrentRenderID        string
	CurrentReceiptID       string
	RecoveryPending        bool
	Bounds                 packs.PresentationBounds
	ActionPageIndex        int
}

const (
	PlanNotice       = "notice"
	PlanCard         = "card"
	PlanSummary      = "summary"
	PlanResponse     = "response"
	PlanStatePlanned = "planned"
)

func (p Plan) validateSource() error {
	switch p.SourceKind {
	case PlanNotice, PlanCard, PlanResponse:
		if p.SummaryCount != 0 {
			return fmt.Errorf("non-summary delivery plan has summary count")
		}
		if (p.SourceKind == PlanResponse && uuid.Validate(p.RequestActivationID) != nil) ||
			(p.SourceKind != PlanResponse && p.RequestActivationID != "") {
			return fmt.Errorf("channel response activation ownership is invalid")
		}
	case PlanSummary:
		if p.SummaryCount < 1 {
			return fmt.Errorf("summary delivery plan has no notices")
		}
		if p.RequestActivationID != "" {
			return fmt.Errorf("summary delivery has response activation")
		}
	default:
		return fmt.Errorf("stored channel delivery source kind is invalid")
	}
	return nil
}

func LoadPlan(ctx context.Context, db queryer, deliveryID string, postgres bool) (Plan, bool, error) {
	if db == nil || uuid.Validate(deliveryID) != nil {
		return Plan{}, false, fmt.Errorf("channel delivery plan requires a store and delivery id")
	}
	query := `SELECT delivery_id, source_kind, source_id, COALESCE(request_activation_id, ''), COALESCE(summary_count, 0), principal_id, interface_key, binding_revision, delivery_epoch,
		external_account_reference, conversation_reference, conversation_scope, state,
		COALESCE(current_render_id, ''), COALESCE(current_receipt_operation_id, ''), action_capacity, text_capacity, label_capacity, action_page_index, ` + AcceptedEffectPredicate(false) + `
		FROM channel_delivery_plans p WHERE delivery_id = ?`
	if postgres {
		query = `SELECT delivery_id::text, source_kind, source_id::text, COALESCE(request_activation_id::text, ''), COALESCE(summary_count, 0), principal_id::text, interface_key, binding_revision, delivery_epoch,
			external_account_reference, conversation_reference, conversation_scope, state,
			COALESCE(current_render_id::text, ''), COALESCE(current_receipt_operation_id::text, ''), action_capacity, text_capacity, label_capacity, action_page_index, ` + AcceptedEffectPredicate(true) + `
			FROM channel_delivery_plans p WHERE delivery_id = $1::uuid`
	}
	var plan Plan
	var scope string
	err := db.QueryRowContext(ctx, query, deliveryID).Scan(&plan.DeliveryID, &plan.SourceKind, &plan.SourceID, &plan.RequestActivationID, &plan.SummaryCount,
		&plan.PrincipalID, &plan.InterfaceKey, &plan.BindingRevision, &plan.DeliveryEpoch, &plan.ExternalAccountRef,
		&plan.ConversationRef, &scope, &plan.State, &plan.CurrentRenderID, &plan.CurrentReceiptID,
		&plan.Bounds.Actions, &plan.Bounds.TextRunes, &plan.Bounds.LabelRunes, &plan.ActionPageIndex, &plan.RecoveryPending)
	if errors.Is(err, sql.ErrNoRows) {
		return Plan{}, false, nil
	}
	if err != nil {
		return Plan{}, false, err
	}
	plan.ConversationScope = operatorchannel.ConversationScope(scope)
	if err := plan.Validate(); err != nil {
		return Plan{}, false, err
	}
	return plan, true, nil
}

func (p Plan) Validate() error {
	if uuid.Validate(p.DeliveryID) != nil || uuid.Validate(p.SourceID) != nil || uuid.Validate(p.PrincipalID) != nil ||
		p.InterfaceKey == "" || p.BindingRevision < 1 || p.DeliveryEpoch < 1 || p.ExternalAccountRef == "" || p.ConversationRef == "" ||
		!p.ConversationScope.Valid() {
		return fmt.Errorf("stored channel delivery plan identity is invalid")
	}
	if p.CurrentBindingRevision != 0 && p.CurrentBindingRevision < p.BindingRevision {
		return fmt.Errorf("selected binding revision predates channel delivery plan")
	}
	if p.ActionPageIndex < 0 || (p.Bounds == (packs.PresentationBounds{}) && p.ActionPageIndex != 0) ||
		(p.Bounds != (packs.PresentationBounds{}) && p.Bounds.Validate() != nil) {
		return fmt.Errorf("stored channel action page is invalid")
	}
	if err := p.validateSource(); err != nil {
		return err
	}
	switch p.State {
	case PlanStatePlanned, "rendered", "sent", "uncertain", "retired":
	default:
		return fmt.Errorf("stored channel delivery plan state is invalid")
	}
	if (p.CurrentRenderID != "" && uuid.Validate(p.CurrentRenderID) != nil) ||
		(p.CurrentReceiptID != "" && uuid.Validate(p.CurrentReceiptID) != nil) ||
		(p.State == "rendered" && p.CurrentRenderID == "") ||
		(p.State == "sent" && p.CurrentReceiptID == "") {
		return fmt.Errorf("stored channel delivery plan pointers are invalid")
	}
	return nil
}

func SetPresentationBoundsTx(ctx context.Context, tx *sql.Tx, deliveryID string, bounds packs.PresentationBounds, postgres bool) error {
	if tx == nil || uuid.Validate(deliveryID) != nil || bounds.Validate() != nil {
		return fmt.Errorf("channel action capacity requires exact delivery and positive selected bound")
	}
	query := `UPDATE channel_delivery_plans SET action_capacity=?, text_capacity=?, label_capacity=?
		WHERE delivery_id=? AND ((action_capacity=0 AND text_capacity=0 AND label_capacity=0)
		OR (action_capacity=? AND text_capacity=? AND label_capacity=?))`
	if postgres {
		query = `UPDATE channel_delivery_plans SET action_capacity=$1, text_capacity=$2, label_capacity=$3
			WHERE delivery_id=$4::uuid AND ((action_capacity=0 AND text_capacity=0 AND label_capacity=0)
			OR (action_capacity=$5 AND text_capacity=$6 AND label_capacity=$7))`
	}
	result, err := tx.ExecContext(ctx, query, bounds.Actions, bounds.TextRunes, bounds.LabelRunes, deliveryID,
		bounds.Actions, bounds.TextRunes, bounds.LabelRunes)
	if err != nil {
		return err
	}
	if rows, err := result.RowsAffected(); err != nil || rows != 1 {
		return fmt.Errorf("channel delivery action capacity changed from its selected plan: %w", err)
	}
	return nil
}

// PlanFirstSummaryTx runs only after inserting the first default in the same
// principal-fenced confirmation transaction. It counts only older pending
// notices; the cut is transaction order, never a timestamp comparison.
func PlanFirstSummaryTx(ctx context.Context, tx *sql.Tx, selected Default, postgres bool) error {
	if tx == nil || selected.State != StateCurrent || selected.DeliveryEpoch != 1 ||
		uuid.Validate(selected.FirstOperationID) != nil {
		return fmt.Errorf("first channel summary requires exact new default")
	}
	var count int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM mailbox WHERE status = 'pending'`).Scan(&count); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	query := `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, summary_count, principal_id,
		interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
		conversation_scope, state, created_at) VALUES (?, 'summary', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?)`
	if postgres {
		query = `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, summary_count, principal_id,
			interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
			conversation_scope, state, created_at) VALUES ($1, 'summary', $2, $3, $4, $5, $6, $7, $8, $9, $10, 'planned', $11)`
	}
	_, err := tx.ExecContext(ctx, query, uuid.NewString(), selected.FirstOperationID, count, selected.PrincipalID,
		selected.InterfaceKey, selected.BindingRevision, selected.DeliveryEpoch, selected.ExternalAccountRef,
		selected.ConversationRef, string(selected.ConversationScope), time.Now().UTC())
	return err
}

// PlanNoticeTx shares the mailbox insertion transaction. The principal fence
// establishes whether a notice committed before or after the first default.
func PlanNoticeTx(ctx context.Context, tx *sql.Tx, noticeID string, postgres bool) (bool, error) {
	if tx == nil || uuid.Validate(noticeID) != nil {
		return false, fmt.Errorf("channel notice plan requires a transaction and notice id")
	}
	principalID, found, err := LockCurrentPrincipalTx(ctx, tx, postgres)
	if err != nil || !found {
		return false, err
	}
	selected, found, err := LoadDefault(ctx, tx, postgres)
	if err != nil || !found || selected.State != StateCurrent {
		return false, err
	}
	if selected.PrincipalID != principalID {
		return false, fmt.Errorf("channel delivery default principal contradicts selected store")
	}
	created, err := insertPlanTx(ctx, tx, PlanNotice, noticeID, selected, postgres)
	if err != nil {
		return false, fmt.Errorf("plan channel notice: %w", err)
	}
	return created, nil
}

// PlanOpenCardTx reads canonical card status inside the same selected mutation
// as destination admission. A stale list result cannot turn a terminal card
// into a new delivery.
func PlanOpenCardTx(ctx context.Context, tx *sql.Tx, cardID string, postgres bool) (bool, error) {
	if tx == nil || uuid.Validate(cardID) != nil {
		return false, fmt.Errorf("channel card plan requires a transaction and card id")
	}
	principalID, found, err := LockCurrentPrincipalTx(ctx, tx, postgres)
	if err != nil || !found {
		return false, err
	}
	selected, found, err := LoadDefault(ctx, tx, postgres)
	if err != nil || !found || selected.State != StateCurrent {
		return false, err
	}
	if selected.PrincipalID != principalID {
		return false, fmt.Errorf("channel delivery default principal contradicts selected store")
	}
	query := `SELECT status FROM decision_cards WHERE card_id = ?`
	if postgres {
		query = `SELECT status FROM decision_cards WHERE card_id = $1::uuid FOR UPDATE`
	}
	var status string
	err = tx.QueryRowContext(ctx, query, cardID).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if status != "pending" {
		return false, nil
	}
	return insertPlanTx(ctx, tx, PlanCard, cardID, selected, postgres)
}

func insertPlanTx(ctx context.Context, tx *sql.Tx, kind, sourceID string, selected Default, postgres bool) (bool, error) {
	query := `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, principal_id,
		interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
		conversation_scope, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?)
		ON CONFLICT (source_kind, source_id, interface_key, delivery_epoch, resend_generation) DO NOTHING`
	if postgres {
		query = `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, principal_id,
			interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
			conversation_scope, state, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'planned', $11)
			ON CONFLICT (source_kind, source_id, interface_key, delivery_epoch, resend_generation) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query, uuid.NewString(), kind, sourceID, selected.PrincipalID, selected.InterfaceKey,
		selected.BindingRevision, selected.DeliveryEpoch, selected.ExternalAccountRef, selected.ConversationRef,
		string(selected.ConversationScope), time.Now().UTC())
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows == 1 {
		return true, nil
	}
	if rows != 0 {
		return false, fmt.Errorf("channel delivery plan insert affected %d rows", rows)
	}
	query = `SELECT principal_id, external_account_reference, conversation_reference, conversation_scope
		FROM channel_delivery_plans WHERE source_kind = ? AND source_id = ? AND interface_key = ? AND delivery_epoch = ? AND resend_generation = 0`
	if postgres {
		query = `SELECT principal_id::text, external_account_reference, conversation_reference, conversation_scope
			FROM channel_delivery_plans WHERE source_kind = $1 AND source_id = $2::uuid AND interface_key = $3 AND delivery_epoch = $4 AND resend_generation = 0`
	}
	var principalID, account, conversation, scope string
	if err := tx.QueryRowContext(ctx, query, kind, sourceID, selected.InterfaceKey, selected.DeliveryEpoch).
		Scan(&principalID, &account, &conversation, &scope); err != nil {
		return false, err
	}
	if principalID != selected.PrincipalID || account != selected.ExternalAccountRef ||
		conversation != selected.ConversationRef || scope != string(selected.ConversationScope) {
		return false, fmt.Errorf("existing channel delivery plan contradicts current delivery epoch")
	}
	return false, nil
}
