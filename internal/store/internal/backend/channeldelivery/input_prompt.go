package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

// PlanInputPromptForActionTx belongs to the accepted input mutation transaction,
// not an edit fallback. Process death cannot separate progress from its prompt.
func PlanInputPromptForActionTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, cardID, draftID string, postgres bool) error {
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, true)
	if err != nil || state != "settled" {
		return errors.Join(err, fmt.Errorf("input prompt requires a settled requested action"))
	}
	resolved, found, err := ResolveActionFactForMutationTx(ctx, tx, action.ActionFact, postgres)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("input prompt request is no longer current")
	}
	query := `SELECT disposition FROM operator_channel_action_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT disposition FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
	}
	var disposition string
	if err := tx.QueryRowContext(ctx, query, action.PublicationID).Scan(&disposition); err != nil {
		return err
	}
	if !((disposition == "input_started" && resolved.Action.Kind == "verdict" && resolved.TargetCardID() == cardID) ||
		(disposition == "applied" && resolved.Action.Kind == "skip_input" && resolved.TargetCardID() == cardID && resolved.Action.DraftID == draftID) ||
		(disposition == "applied" && resolved.Action.Kind == "select_draft" && resolved.Action.CardID == cardID && resolved.Action.DraftID == draftID)) {
		return fmt.Errorf("input prompt source is not the exact accepted input control")
	}
	if draftID == "" {
		if disposition != "input_started" {
			return fmt.Errorf("input prompt continuation requires its exact draft identity")
		}
		card, err := decisionpersistence.LoadDecisionCardInTx(ctx, tx, cardID, postgres)
		if err != nil {
			return err
		}
		prompt, err := loadCurrentCardPromptTx(ctx, tx, Plan{SourceID: cardID, PrincipalID: resolved.PrincipalID}, card, postgres)
		if err != nil {
			return err
		}
		if prompt.DraftID == "" || prompt.Verdict != resolved.Action.Verdict {
			return fmt.Errorf("accepted input start does not own the current draft")
		}
		draftID = prompt.DraftID
	}
	selected, found, err := LockDefaultTx(ctx, tx, postgres)
	if err != nil {
		return err
	}
	if !found || selected.State != StateCurrent || selected.PrincipalID != resolved.PrincipalID ||
		selected.InterfaceKey != action.Interface.Key() || selected.BindingRevision != resolved.BindingRevision ||
		selected.ExternalAccountRef != action.ExternalAccountRef || selected.ConversationRef != action.ConversationRef ||
		selected.ConversationScope != action.ConversationScope {
		return fmt.Errorf("input prompt destination is no longer current")
	}
	audience := render.Audience{PrincipalID: resolved.PrincipalID, InterfaceKey: selected.InterfaceKey,
		DeliveryEpoch: selected.DeliveryEpoch, ExternalAccountRef: selected.ExternalAccountRef,
		ConversationRef: selected.ConversationRef, ConversationScope: selected.ConversationScope}
	return planInputPromptTx(ctx, tx, action.PublicationID, cardID, draftID,
		resolved.ActivationID, resolved.BindingRevision, audience, postgres)
}

func PlanInputPromptForTextTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, cardID, draftID string, postgres bool) error {
	state, disposition, err := requireExactTextIntentTx(ctx, tx, text, postgres, true)
	if err != nil || state != "settled" || disposition != "input_progressed" {
		return errors.Join(err, fmt.Errorf("input prompt requires exact settled field progress"))
	}
	activationID, revision, audience, err := currentBoundTextAudienceTx(ctx, tx, text, postgres, true)
	if err != nil {
		return err
	}
	return planInputPromptTx(ctx, tx, text.PublicationID, cardID, draftID, activationID, revision, audience, postgres)
}

func planInputPromptTx(ctx context.Context, tx *sql.Tx, publicationID, cardID, draftID, activationID string,
	bindingRevision int64, audience render.Audience, postgres bool) error {
	card, err := decisionpersistence.LoadDecisionCardInTx(ctx, tx, cardID, postgres)
	if err != nil {
		return err
	}
	if err := runstate.RequireNormalControlTx(ctx, tx, card.RunID, runfork.ControlMailboxDecide); err != nil {
		return err
	}
	query := `SELECT draft.delivery_receipt_id, receipt.delivery_id FROM decision_card_input_drafts draft
		JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=draft.delivery_receipt_id AND receipt.state='sent'
		WHERE draft.input_draft_id=? AND draft.card_id=? AND draft.principal_id=? AND draft.status='active'`
	if postgres {
		query = `SELECT draft.delivery_receipt_id, receipt.delivery_id::text FROM decision_card_input_drafts draft
			JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=NULLIF(draft.delivery_receipt_id,'')::uuid AND receipt.state='sent'
			WHERE draft.input_draft_id=$1::uuid AND draft.card_id=$2::uuid AND draft.principal_id=$3 AND draft.status='active'`
	}
	var receiptID, deliveryID string
	if err := tx.QueryRowContext(ctx, query, draftID, cardID, audience.PrincipalID).Scan(&receiptID, &deliveryID); err != nil {
		return err
	}
	parent, found, err := LoadPlan(ctx, tx, deliveryID, postgres)
	if err != nil {
		return err
	}
	if !found || parent.SourceKind != PlanCard || parent.SourceID != cardID || parent.State == "retired" ||
		parent.PrincipalID != audience.PrincipalID || parent.InterfaceKey != audience.InterfaceKey ||
		parent.DeliveryEpoch != audience.DeliveryEpoch || parent.BindingRevision != bindingRevision ||
		parent.ExternalAccountRef != audience.ExternalAccountRef || parent.ConversationRef != audience.ConversationRef ||
		parent.ConversationScope != audience.ConversationScope {
		return fmt.Errorf("input prompt parent receipt contradicts current card audience")
	}
	prompt, err := loadCurrentCardPromptTx(ctx, tx, parent, card, postgres)
	if err != nil {
		return err
	}
	if prompt.DraftID != draftID {
		return fmt.Errorf("input prompt does not name the current requested draft")
	}
	frozen, err := render.FreezeInputCardPrompt(publicationID, card, prompt, receiptID, audience)
	if err != nil {
		return err
	}
	frozen, err = render.WithPresentation(frozen, parent.Bounds, 0)
	if err != nil {
		return err
	}
	_, err = insertResponsePlanTx(ctx, tx, frozen, activationID, bindingRevision, time.Now().UTC(), postgres)
	return err
}

func inputPromptCurrent(ctx context.Context, q intentQueryer, frozen render.Frozen, postgres, lock bool) (bool, error) {
	if frozen.InputCard == nil || frozen.Prompt == nil {
		return false, fmt.Errorf("input prompt currentness requires exact frozen evidence")
	}
	query := `SELECT draft.next_field_index, draft.verdict, draft.expires_at, draft.run_id
		FROM decision_card_input_drafts draft JOIN decision_cards card ON card.card_id=draft.card_id
		JOIN runs run ON run.run_id=draft.run_id AND run.status IN (` + runstate.ActiveStateSQLValues + `)
		WHERE draft.input_draft_id=? AND draft.card_id=? AND draft.principal_id=?
		AND draft.status='active' AND card.status='pending' AND card.card_content_hash=? AND draft.delivery_receipt_id=?`
	if postgres {
		query = `SELECT draft.next_field_index, draft.verdict, draft.expires_at, draft.run_id::text
			FROM decision_card_input_drafts draft JOIN decision_cards card ON card.card_id=draft.card_id
			JOIN runs run ON run.run_id=draft.run_id AND run.status IN (` + runstate.ActiveStateSQLValues + `)
			WHERE draft.input_draft_id=$1::uuid AND draft.card_id=$2::uuid AND draft.principal_id=$3
			AND draft.status='active' AND card.status='pending' AND card.card_content_hash=$4 AND draft.delivery_receipt_id=$5`
		if lock {
			query += ` FOR UPDATE OF draft, card, run`
		}
	}
	var index int
	var verdict, runID string
	var rawTime any
	err := q.QueryRowContext(ctx, query, frozen.Prompt.DraftID, frozen.InputCard.CardID, frozen.Audience.PrincipalID,
		frozen.InputCard.CardContentHash, frozen.InputCard.ParentReceiptOperationID).Scan(&index, &verdict, &rawTime, &runID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	expiresAt, err := decodeActionTime(rawTime)
	if err != nil {
		return false, err
	}
	if index != frozen.Prompt.NextFieldIndex || verdict != frozen.Prompt.Verdict ||
		!expiresAt.Equal(frozen.Prompt.ExpiresAt) || !expiresAt.After(time.Now()) {
		return false, nil
	}
	binding, err := runstate.LoadSelectedContractBinding(ctx, q, runID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err := runfork.RequireNormalControlForBinding(runID, runfork.ControlMailboxDecide, binding, err == nil); err != nil {
		return false, err
	}
	return true, nil
}
