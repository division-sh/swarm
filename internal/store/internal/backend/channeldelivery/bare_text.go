package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	runstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	"github.com/google/uuid"
)

// ListCurrentInputDraftsTx is the single selected-store interpretation of a
// channel draft's current conversation and sent receipt. A quote additionally
// matches the provider-neutral opaque delivery reference without fallback.
func ListCurrentInputDraftsTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, at time.Time,
	afterDraftID string, limit int, requireIntent, postgres bool) ([]render.InputDraftCandidate, string, error) {
	if tx == nil || at.IsZero() || text.EntryReference != "" || limit < 1 || limit > 500 ||
		(afterDraftID != "" && uuid.Validate(afterDraftID) != nil) {
		return nil, "", fmt.Errorf("channel input draft scan requires an exact transaction, text and bounded cursor")
	}
	if err := text.Validate(); err != nil {
		return nil, "", err
	}
	if requireIntent {
		if err := RequireTextIntentTx(ctx, tx, text, postgres); err != nil {
			return nil, "", err
		}
	}
	bound, current, err := ResolveCurrentTextTx(ctx, tx, text, postgres)
	if err != nil || !current {
		return nil, "", err
	}
	cursor := afterDraftID
	if cursor == "" {
		cursor = "00000000-0000-0000-0000-000000000000"
	}
	query := `SELECT draft.input_draft_id, draft.card_id, draft.verdict, draft.delivery_receipt_id
		FROM decision_card_input_drafts draft
		JOIN decision_cards card ON card.card_id=draft.card_id AND card.status='pending'
		JOIN runs run ON run.run_id=draft.run_id AND run.status IN (` + runstate.ActiveStateSQLValues + `)
		JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=draft.delivery_receipt_id AND receipt.state='sent'
		JOIN channel_delivery_plans plan ON plan.delivery_id=receipt.delivery_id
			AND plan.source_kind='card' AND plan.source_id=draft.card_id AND plan.state<>'retired'
		JOIN channel_delivery_defaults selected ON selected.singleton_id=1 AND selected.state='current'
			AND selected.interface_key=plan.interface_key AND selected.delivery_epoch=plan.delivery_epoch
			AND selected.binding_revision=plan.binding_revision AND selected.principal_id=plan.principal_id
		WHERE draft.principal_id=? AND draft.status='active' AND draft.expires_at>?
			AND plan.interface_key=? AND plan.binding_revision=?
			AND plan.external_account_reference=? AND plan.conversation_reference=? AND plan.conversation_scope=?
			AND draft.input_draft_id>?
		ORDER BY draft.input_draft_id LIMIT ?`
	if postgres {
		query = `SELECT draft.input_draft_id::text, draft.card_id::text, draft.verdict, draft.delivery_receipt_id
			FROM decision_card_input_drafts draft
			JOIN decision_cards card ON card.card_id=draft.card_id AND card.status='pending'
			JOIN runs run ON run.run_id=draft.run_id AND run.status IN (` + runstate.ActiveStateSQLValues + `)
			JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=NULLIF(draft.delivery_receipt_id, '')::uuid AND receipt.state='sent'
			JOIN channel_delivery_plans plan ON plan.delivery_id=receipt.delivery_id
				AND plan.source_kind='card' AND plan.source_id=draft.card_id AND plan.state<>'retired'
			JOIN channel_delivery_defaults selected ON selected.singleton_id=1 AND selected.state='current'
				AND selected.interface_key=plan.interface_key AND selected.delivery_epoch=plan.delivery_epoch
				AND selected.binding_revision=plan.binding_revision AND selected.principal_id=plan.principal_id
			WHERE draft.principal_id=$1 AND draft.status='active' AND draft.expires_at>$2
				AND plan.interface_key=$3 AND plan.binding_revision=$4
				AND plan.external_account_reference=$5 AND plan.conversation_reference=$6 AND plan.conversation_scope=$7
				AND draft.input_draft_id>$8::uuid
			ORDER BY draft.input_draft_id LIMIT $9`
	}
	rows, err := tx.QueryContext(ctx, query, bound.PrincipalID, at.UTC(), bound.InterfaceKey, bound.BindingRevision,
		text.ExternalAccountRef, text.ConversationRef, string(text.ConversationScope), cursor, limit)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	selected := make([]render.InputDraftCandidate, 0)
	seen := 0
	last := ""
	for rows.Next() {
		var candidate render.InputDraftCandidate
		if err := rows.Scan(&candidate.DraftID, &candidate.CardID, &candidate.Verdict,
			&candidate.ReceiptOperationID); err != nil {
			return nil, "", err
		}
		seen++
		last = candidate.DraftID
		selected = append(selected, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if err := rows.Close(); err != nil {
		return nil, "", err
	}
	if text.ReplyToReference != "" {
		matched := selected[:0]
		for _, candidate := range selected {
			current, err := quotedCardReceiptMatchesTx(ctx, tx, text, bound, candidate.CardID, postgres)
			if err != nil {
				return nil, "", err
			}
			if current {
				matched = append(matched, candidate)
			}
		}
		selected = matched
	}
	if seen < limit {
		last = ""
	}
	return selected, last, nil
}

func quotedCardReceiptMatchesTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText,
	bound render.ResolvedText, cardID string, postgres bool) (bool, error) {
	query := `SELECT receipt.provider_reference FROM channel_delivery_receipts receipt
		JOIN channel_delivery_plans plan ON plan.delivery_id=receipt.delivery_id
		JOIN channel_delivery_defaults selected ON selected.singleton_id=1 AND selected.state='current'
			AND selected.interface_key=plan.interface_key AND selected.delivery_epoch=plan.delivery_epoch
			AND selected.binding_revision=plan.binding_revision AND selected.principal_id=plan.principal_id
		WHERE receipt.state='sent' AND plan.state<>'retired' AND plan.source_kind='card' AND plan.source_id=?
		AND plan.principal_id=? AND plan.interface_key=? AND plan.binding_revision=?
		AND plan.external_account_reference=? AND plan.conversation_reference=? AND plan.conversation_scope=?`
	if postgres {
		query = `SELECT receipt.provider_reference FROM channel_delivery_receipts receipt
			JOIN channel_delivery_plans plan ON plan.delivery_id=receipt.delivery_id
			JOIN channel_delivery_defaults selected ON selected.singleton_id=1 AND selected.state='current'
				AND selected.interface_key=plan.interface_key AND selected.delivery_epoch=plan.delivery_epoch
				AND selected.binding_revision=plan.binding_revision AND selected.principal_id=plan.principal_id
			WHERE receipt.state='sent' AND plan.state<>'retired' AND plan.source_kind='card' AND plan.source_id=$1::uuid
			AND plan.principal_id=$2::uuid AND plan.interface_key=$3 AND plan.binding_revision=$4
			AND plan.external_account_reference=$5 AND plan.conversation_reference=$6 AND plan.conversation_scope=$7`
	}
	rows, err := tx.QueryContext(ctx, query, cardID, bound.PrincipalID, bound.InterfaceKey, bound.BindingRevision,
		text.ExternalAccountRef, text.ConversationRef, string(text.ConversationScope))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	matched := false
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return false, err
		}
		var receipt map[string]any
		if err := json.Unmarshal(raw, &receipt); err != nil {
			return false, fmt.Errorf("decode channel card receipt: %w", err)
		}
		reference, ok := receipt["delivery_reference"]
		if !ok {
			return false, fmt.Errorf("channel card receipt lacks delivery reference")
		}
		encoded, valid, err := operatorchannel.OpaqueReference(reference)
		if err != nil || !valid {
			return false, fmt.Errorf("channel card receipt has invalid delivery reference: %w", err)
		}
		matched = matched || encoded == text.ReplyToReference
	}
	return matched, rows.Err()
}

func HasCurrentBareInputDraftTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, at time.Time, postgres bool) (bool, error) {
	if text.ReplyToReference != "" {
		return false, fmt.Errorf("bare channel input draft classification requires unquoted text")
	}
	candidates, _, err := ListCurrentInputDraftsTx(ctx, tx, text, at, "", 1, false, postgres)
	return len(candidates) != 0, err
}

// RequireCurrentInputDraftTx is the exact selector for an inbound answer. A
// bare answer is unambiguous only with one current prompt; a quote must match
// exactly one retained receipt. Callers may not choose a different draft.
func RequireCurrentInputDraftTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText,
	at time.Time, draftID string, lock, postgres bool) (render.InputDraftCandidate, render.ResolvedText, error) {
	if uuid.Validate(draftID) != nil {
		return render.InputDraftCandidate{}, render.ResolvedText{}, fmt.Errorf("channel answer requires an exact draft id")
	}
	if err := RequireTextIntentTx(ctx, tx, text, postgres); err != nil {
		return render.InputDraftCandidate{}, render.ResolvedText{}, err
	}
	resolved, current, err := ResolveCurrentTextTx(ctx, tx, text, postgres)
	if err != nil {
		return render.InputDraftCandidate{}, render.ResolvedText{}, err
	}
	if !current {
		return render.InputDraftCandidate{}, render.ResolvedText{}, fmt.Errorf("channel answer has no current principal")
	}
	if lock {
		if err := LockPrincipalTx(ctx, tx, resolved.PrincipalID, postgres); err != nil {
			return render.InputDraftCandidate{}, render.ResolvedText{}, err
		}
	}
	var selected render.InputDraftCandidate
	cursor := ""
	for {
		candidates, next, err := ListCurrentInputDraftsTx(ctx, tx, text, at, cursor, 200, true, postgres)
		if err != nil {
			return render.InputDraftCandidate{}, render.ResolvedText{}, err
		}
		for _, candidate := range candidates {
			if selected.DraftID != "" {
				return render.InputDraftCandidate{}, render.ResolvedText{}, fmt.Errorf("channel answer selects multiple current drafts")
			}
			selected = candidate
		}
		if next == "" {
			break
		}
		if next == cursor {
			return render.InputDraftCandidate{}, render.ResolvedText{}, fmt.Errorf("channel draft cursor did not advance")
		}
		cursor = next
	}
	if selected.DraftID != draftID {
		return render.InputDraftCandidate{}, render.ResolvedText{}, fmt.Errorf("channel answer draft is no longer current")
	}
	return selected, resolved, nil
}
