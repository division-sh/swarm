package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

var ErrInputPromptObsoleted = errors.New("exact input prompt was obsoleted by its terminal card")

// ResponseSourceCurrent admits a response from one settled responsibility.
// A reply transfer retains text evidence, not a second response authority.
func ResponseSourceCurrent(ctx context.Context, q intentQueryer, frozen render.Frozen, postgres, lock, proveObsolescence bool) (bool, error) {
	publicationID := frozen.SourceID
	if q == nil || uuid.Validate(publicationID) != nil {
		return false, fmt.Errorf("channel response requires an exact source occurrence")
	}
	if err := frozen.Validate(); err != nil {
		return false, err
	}
	currentPurpose := func() (bool, error) {
		if frozen.InputCard != nil {
			current, err := inputPromptCurrent(ctx, q, frozen, postgres, lock)
			if current || err != nil || !proveObsolescence {
				return current, err
			}
			return false, requireObsoleteInputPromptEvidence(ctx, q, frozen, postgres)
		}
		return true, nil
	}
	query := `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition,'')
		FROM operator_channel_action_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition,'')
			FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var action operatorchannel.InboundAction
	action.PublicationID = publicationID
	var key, state, disposition string
	var raw []byte
	err := q.QueryRowContext(ctx, query, publicationID).Scan(&action.Provider, &action.ProviderEventID,
		&key, &raw, &action.ProviderAuthorization, &state, &disposition)
	if err == nil {
		current, err := responseActionRowCurrent(ctx, q, action, raw, key, state, disposition, frozen.InputCard != nil, postgres, lock)
		if err != nil || !current {
			return false, err
		}
		return currentPurpose()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition,'')
		FROM operator_channel_text_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT provider, provider_event_id, interface_key, fact, provider_authorization, state, COALESCE(disposition,'')
			FROM operator_channel_text_intents WHERE publication_id=$1::uuid`
		if lock {
			query += ` FOR UPDATE`
		}
	}
	var text operatorchannel.InboundText
	text.PublicationID = publicationID
	err = q.QueryRowContext(ctx, query, publicationID).Scan(&text.Provider, &text.ProviderEventID,
		&key, &raw, &text.ProviderAuthorization, &state, &disposition)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(raw, &text.TextFact); err != nil {
		return false, err
	}
	if err := text.Validate(); err != nil {
		return false, err
	}
	if key != text.Interface.Key() || state != "settled" ||
		(disposition != "entry" && disposition != "teaching" && disposition != "chooser" &&
			!(frozen.InputCard != nil && disposition == "input_progressed")) {
		return false, nil
	}
	return currentPurpose()
}

func requireObsoleteInputPromptEvidence(ctx context.Context, q intentQueryer, frozen render.Frozen, postgres bool) error {
	query := `SELECT draft.next_field_index, draft.verdict, draft.expires_at
		FROM decision_card_input_drafts draft
		JOIN decision_cards card ON card.card_id=draft.card_id AND card.run_id=draft.run_id
		JOIN runs run ON run.run_id=draft.run_id
		JOIN channel_delivery_receipts receipt ON CAST(receipt.effect_operation_id AS TEXT)=draft.delivery_receipt_id
		JOIN channel_delivery_plans parent ON parent.delivery_id=receipt.delivery_id
		WHERE draft.input_draft_id=$1 AND draft.card_id=$2 AND draft.principal_id=$3
		AND card.card_content_hash=$4 AND draft.delivery_receipt_id=$5
	AND draft.status='cancelled' AND receipt.state='sent'
	AND (card.status='decided' OR (
		card.status='superseded' AND card.superseded_reason='run_cancelled' AND run.status='cancelled'
		AND EXISTS (SELECT 1 FROM run_control_state control
			WHERE control.run_id=run.run_id AND control.control_status='stopped' AND control.stopped_at IS NOT NULL)
	))
		AND parent.source_kind='card' AND parent.source_id=card.card_id
		AND CAST(parent.principal_id AS TEXT)=draft.principal_id AND parent.interface_key=$6
		AND parent.delivery_epoch=$7 AND parent.external_account=$8 AND parent.conversation_reference=$9
		AND parent.conversation_scope=$10`
	if postgres {
		query += " FOR UPDATE OF draft, card"
	}
	var index int
	var verdict string
	var expiry any
	a := frozen.Audience
	err := q.QueryRowContext(ctx, query, frozen.Prompt.DraftID, frozen.InputCard.CardID, a.PrincipalID,
		frozen.InputCard.CardContentHash, frozen.InputCard.ParentReceiptOperationID, a.InterfaceKey,
		a.DeliveryEpoch, a.ExternalAccountRef, a.ConversationRef, a.ConversationScope).Scan(&index, &verdict, &expiry)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read obsolete channel input prompt evidence: %w", err)
	}
	expires, err := decodeActionTime(expiry)
	if err == nil && index == frozen.Prompt.NextFieldIndex && verdict == frozen.Prompt.Verdict && expires.Equal(frozen.Prompt.ExpiresAt) {
		return ErrInputPromptObsoleted
	}
	return err
}

func responseActionRowCurrent(ctx context.Context, q intentQueryer, action operatorchannel.InboundAction,
	raw []byte, key, state, disposition string, inputPrompt, postgres, lock bool) (bool, error) {
	if err := json.Unmarshal(raw, &action.ActionFact); err != nil {
		return false, err
	}
	if err := action.Validate(); err != nil {
		return false, err
	}
	if key != action.Interface.Key() || state != "settled" ||
		(disposition != "navigation" && !(inputPrompt && (disposition == "input_started" || disposition == "applied"))) {
		return false, nil
	}
	if _, err := RequireActionIntentTx(ctx, q, action, postgres, lock); err != nil {
		return false, err
	}
	if action.Kind == operatorchannel.ActionSourceReply {
		return true, nil
	}
	query := `SELECT COUNT(*) FROM operator_channel_text_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT COUNT(*) FROM operator_channel_text_intents WHERE publication_id=$1::uuid`
	}
	var count int
	if err := q.QueryRowContext(ctx, query, action.PublicationID).Scan(&count); err != nil {
		return false, err
	}
	return count == 0, nil
}
