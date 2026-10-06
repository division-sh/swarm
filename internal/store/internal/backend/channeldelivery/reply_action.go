package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

// AdmitReplyActionTx transfers one exact verified text occurrence to the action
// owner. The original text remains evidence; it is never a second pending owner.
func AdmitReplyActionTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) (render.PendingAction, bool, error) {
	state, disposition, err := requireExactTextIntentTx(ctx, tx, text, postgres, false)
	if err != nil {
		return render.PendingAction{}, false, err
	}
	if text.EntryReference != "" || text.ReplyToReference == "" {
		return render.PendingAction{}, false, nil
	}
	if state == "settled" {
		if disposition != "control" {
			return render.PendingAction{}, false, nil
		}
		return loadReplyActionTx(ctx, tx, text, postgres)
	}
	if _, _, _, err := currentTextResponseAudienceTx(ctx, tx, text, postgres); err != nil {
		return render.PendingAction{}, false, err
	}
	state, _, err = requireExactTextIntentTx(ctx, tx, text, postgres, true)
	if err != nil || state != "pending" {
		return render.PendingAction{}, false, fmt.Errorf("reply text changed before control admission: %w", err)
	}
	selected, err := findReplyControlTx(ctx, tx, text, postgres)
	if err != nil || selected.Token == "" {
		return render.PendingAction{}, false, err
	}
	query := `SELECT recorded_at FROM operator_channel_text_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT recorded_at FROM operator_channel_text_intents WHERE publication_id=$1::uuid`
	}
	var rawTime any
	if err := tx.QueryRowContext(ctx, query, text.PublicationID).Scan(&rawTime); err != nil {
		return render.PendingAction{}, false, err
	}
	receivedAt, err := decodeActionTime(rawTime)
	if err != nil {
		return render.PendingAction{}, false, err
	}
	action := operatorchannel.InboundAction{ActionFact: selected, Provider: text.Provider,
		ProviderEventID: text.ProviderEventID, PublicationID: text.PublicationID,
		ProviderAuthorization: text.ProviderAuthorization}
	if err := InsertActionIntentTx(ctx, tx, action, receivedAt, postgres); err != nil {
		return render.PendingAction{}, false, err
	}
	if err := SettleTextIntentTx(ctx, tx, text, "control", postgres); err != nil {
		return render.PendingAction{}, false, err
	}
	return render.PendingAction{PublicationID: text.PublicationID, Fact: action, ReceivedAt: receivedAt}, true, nil
}

func findReplyControlTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) (operatorchannel.ActionFact, error) {
	var selected operatorchannel.ActionFact
	cursor := "00000000-0000-0000-0000-000000000000"
	for {
		page, err := loadReplyControlPageTx(ctx, tx, text, cursor, postgres)
		if err != nil {
			return operatorchannel.ActionFact{}, err
		}
		for _, item := range page {
			if !render.MatchesTextControl(item.label, text.Text) {
				continue
			}
			fact := operatorchannel.ActionFact{
				Kind: operatorchannel.ActionSourceReply, TextSource: text.TextFact,
				Interface: text.Interface, ExternalAccountRef: text.ExternalAccountRef,
				ConversationRef: text.ConversationRef, ConversationScope: text.ConversationScope,
				MessageReference: text.ReplyToReference, Token: item.token,
			}
			resolved, found, err := ResolveActionFactForMutationTx(ctx, tx, fact, postgres)
			if err != nil {
				return operatorchannel.ActionFact{}, err
			}
			if !found || !resolved.CurrentRender {
				continue
			}
			if selected.Token != "" {
				return operatorchannel.ActionFact{}, fmt.Errorf("quoted channel action matches multiple current controls")
			}
			selected = fact
		}
		if len(page) < 200 {
			break
		}
		cursor = page[len(page)-1].token
	}
	return selected, nil
}

type replyControl struct{ token, label string }

func loadReplyControlPageTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, cursor string, postgres bool) ([]replyControl, error) {
	query := `SELECT action.action_token, action.label FROM channel_delivery_actions action
			JOIN channel_delivery_renders render ON render.render_id=action.render_id
			JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
			JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=plan.current_receipt_operation_id
			WHERE plan.interface_key=? AND plan.external_account_reference=?
			AND plan.conversation_reference=? AND plan.conversation_scope=?
			AND receipt.state='sent' AND receipt.render_id=render.render_id
			AND plan.current_render_id=render.render_id AND action.action_token>?
			ORDER BY action.action_token LIMIT 200`
	if postgres {
		query = `SELECT action.action_token::text, action.label FROM channel_delivery_actions action
				JOIN channel_delivery_renders render ON render.render_id=action.render_id
				JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
				JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=plan.current_receipt_operation_id
				WHERE plan.interface_key=$1 AND plan.external_account_reference=$2
				AND plan.conversation_reference=$3 AND plan.conversation_scope=$4
				AND receipt.state='sent' AND receipt.render_id=render.render_id
				AND plan.current_render_id=render.render_id AND action.action_token>$5::uuid
				ORDER BY action.action_token LIMIT 200`
	}
	rows, err := tx.QueryContext(ctx, query, text.Interface.Key(), text.ExternalAccountRef,
		text.ConversationRef, string(text.ConversationScope), cursor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	page := make([]replyControl, 0, 200)
	for rows.Next() {
		var item replyControl
		if err := rows.Scan(&item.token, &item.label); err != nil {
			return nil, err
		}
		page = append(page, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return page, nil
}

func loadReplyActionTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) (render.PendingAction, bool, error) {
	query := `SELECT fact, recorded_at FROM operator_channel_action_intents WHERE publication_id=?`
	if postgres {
		query = `SELECT fact, recorded_at FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
	}
	var raw []byte
	var rawTime any
	if err := tx.QueryRowContext(ctx, query, text.PublicationID).Scan(&raw, &rawTime); err != nil {
		return render.PendingAction{}, false, fmt.Errorf("load transferred reply action: %w", err)
	}
	action := operatorchannel.InboundAction{Provider: text.Provider, ProviderEventID: text.ProviderEventID,
		PublicationID: text.PublicationID, ProviderAuthorization: text.ProviderAuthorization}
	if err := json.Unmarshal(raw, &action.ActionFact); err != nil {
		return render.PendingAction{}, false, err
	}
	if action.Kind != operatorchannel.ActionSourceReply || action.TextSource != text.TextFact {
		return render.PendingAction{}, false, fmt.Errorf("transferred reply action contradicts original text")
	}
	if _, err := RequireActionIntentTx(ctx, tx, action, postgres, false); err != nil {
		return render.PendingAction{}, false, err
	}
	receivedAt, err := decodeActionTime(rawTime)
	if err != nil {
		return render.PendingAction{}, false, err
	}
	return render.PendingAction{PublicationID: text.PublicationID, Fact: action, ReceivedAt: receivedAt}, true, nil
}
