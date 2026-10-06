package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

// Terminal refusal consumes immutable choice/receipt evidence, not executable
// draft or activation authority that may have retired after mutation admission.
func loadUnsupportedChooserTextTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres bool) (render.PendingText, bool, error) {
	query := `SELECT action.action_token,action.action_kind,action.verdict,action.label,action.action_position,
		render.render_input,render.render_hash,receipt.provider_reference
		FROM channel_delivery_actions action
		JOIN channel_delivery_renders render ON render.render_id=action.render_id
		JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
		JOIN channel_delivery_receipts receipt ON receipt.render_id=render.render_id AND receipt.delivery_id=plan.delivery_id
		WHERE action.action_token=? AND action.action_kind='select_draft' AND plan.source_kind='response'
		AND plan.interface_key=? AND plan.external_account_reference=?
		AND plan.conversation_reference=? AND plan.conversation_scope=? AND receipt.state='sent' LIMIT 2`
	if postgres {
		query = `SELECT action.action_token::text,action.action_kind,action.verdict,action.label,action.action_position,
			render.render_input,render.render_hash,receipt.provider_reference
			FROM channel_delivery_actions action
			JOIN channel_delivery_renders render ON render.render_id=action.render_id
			JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
			JOIN channel_delivery_receipts receipt ON receipt.render_id=render.render_id AND receipt.delivery_id=plan.delivery_id
			WHERE action.action_token=$1::uuid AND action.action_kind='select_draft' AND plan.source_kind='response'
			AND plan.interface_key=$2 AND plan.external_account_reference=$3
			AND plan.conversation_reference=$4 AND plan.conversation_scope=$5 AND receipt.state='sent' LIMIT 2
			FOR UPDATE OF action,render,plan,receipt`
	}
	rows, err := tx.QueryContext(ctx, query, fact.Token, fact.Interface.Key(), fact.ExternalAccountRef, fact.ConversationRef, string(fact.ConversationScope))
	if err != nil {
		return render.PendingText{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return render.PendingText{}, false, rows.Err()
	}
	var action render.Action
	var verdict sql.NullString
	var position int
	var raw, receipt []byte
	var hash string
	if err := rows.Scan(&action.Token, &action.Kind, &verdict, &action.Label, &position, &raw, &hash, &receipt); err != nil {
		return render.PendingText{}, false, err
	}
	if rows.Next() {
		return render.PendingText{}, false, fmt.Errorf("unsupported chooser has ambiguous delivery evidence")
	}
	if err := rows.Err(); err != nil {
		return render.PendingText{}, false, err
	}
	if err := rows.Close(); err != nil {
		return render.PendingText{}, false, err
	}
	action.Verdict = verdict.String
	action, _, err = hydrateFrozenChannelAction(action, raw, hash, position)
	if err != nil {
		return render.PendingText{}, false, err
	}
	messageRef, err := receiptMessageReference(receipt)
	if err != nil {
		return render.PendingText{}, false, err
	}
	if messageRef != fact.MessageReference {
		return render.PendingText{}, false, fmt.Errorf("unsupported chooser has a different message reference")
	}
	text, err := LoadChooserTextIntentTx(ctx, tx, action.TextPublicationID, postgres, true)
	if err != nil {
		return render.PendingText{}, false, err
	}
	if text.Fact.Interface.Key() != fact.Interface.Key() || text.Fact.ExternalAccountRef != fact.ExternalAccountRef ||
		text.Fact.ConversationRef != fact.ConversationRef || text.Fact.ConversationScope != fact.ConversationScope || text.Fact.ReplyToReference != "" {
		return render.PendingText{}, false, fmt.Errorf("unsupported chooser answer has a different audience")
	}
	return text, true, nil
}
