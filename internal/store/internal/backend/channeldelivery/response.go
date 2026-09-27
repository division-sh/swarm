package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

// PlanNativeInboxResponseTx commits a requested readback and its immutable
// render together with the verified inbound intent disposition. Delivery is a
// later managed effect; the entry alone grants no resend or mailbox mutation.
func PlanNativeInboxResponseTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText,
	expected render.ResolvedNativeEntry, fullText string, postgres bool) (string, error) {
	if tx == nil || text.EntryReference == "" || expected.PrincipalID == "" {
		return "", fmt.Errorf("native inbox response requires verified entry")
	}
	if err := LockPrincipalTx(ctx, tx, expected.PrincipalID, postgres); err != nil {
		return "", err
	}
	entry, found, err := ResolveCurrentNativeInboxEntryTx(ctx, tx, text, postgres)
	if err != nil {
		return "", err
	}
	if !found || entry != expected {
		return "", fmt.Errorf("native inbox entry changed before response admission")
	}
	selected, found, err := LoadDefault(ctx, tx, postgres)
	if err != nil {
		return "", err
	}
	if !found || selected.State != StateCurrent || selected.PrincipalID != entry.PrincipalID {
		return "", fmt.Errorf("native inbox response has no current delivery epoch")
	}
	audience := render.Audience{
		PrincipalID: entry.PrincipalID, InterfaceKey: entry.InterfaceKey,
		DeliveryEpoch: selected.DeliveryEpoch, ExternalAccountRef: text.ExternalAccountRef,
		ConversationRef: text.ConversationRef, ConversationScope: text.ConversationScope,
	}
	frozen, err := render.FreezeResponse(text.PublicationID, fullText, audience)
	if err != nil {
		return "", err
	}
	deliveryID, renderID := uuid.NewString(), uuid.NewString()
	now := time.Now().UTC()
	query := `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, entry_activation_id, principal_id,
		interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
		conversation_scope, state, current_render_id, created_at)
		VALUES (?, 'response', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'rendered', ?, ?)`
	if postgres {
		query = `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, entry_activation_id, principal_id,
			interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
			conversation_scope, state, current_render_id, created_at)
			VALUES ($1::uuid, 'response', $2::uuid, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, $10, 'rendered', $11::uuid, $12)`
	}
	if _, err := tx.ExecContext(ctx, query, deliveryID, text.PublicationID, entry.ActivationID, entry.PrincipalID,
		entry.InterfaceKey, entry.BindingRevision, selected.DeliveryEpoch, text.ExternalAccountRef,
		text.ConversationRef, string(text.ConversationScope), renderID, now); err != nil {
		return "", fmt.Errorf("plan native inbox response: %w", err)
	}
	query = `INSERT INTO channel_delivery_renders (render_id, delivery_id, source_revision,
		projection_version, render_input, render_hash, created_at) VALUES (?, ?, 1, ?, ?, ?, ?)`
	if postgres {
		query = `INSERT INTO channel_delivery_renders (render_id, delivery_id, source_revision,
			projection_version, render_input, render_hash, created_at) VALUES ($1::uuid, $2::uuid, 1, $3, $4::jsonb, $5, $6)`
	}
	if _, err := tx.ExecContext(ctx, query, renderID, deliveryID, render.ProjectionVersion,
		string(frozen.Input), frozen.Hash, now); err != nil {
		return "", fmt.Errorf("freeze native inbox response: %w", err)
	}
	query = `UPDATE operator_channel_text_intents SET state='settled', disposition='entry', settled_at=?
		WHERE publication_id=? AND state='pending'`
	if postgres {
		query = `UPDATE operator_channel_text_intents SET state='settled', disposition='entry', settled_at=$1
			WHERE publication_id=$2::uuid AND state='pending'`
	}
	result, err := tx.ExecContext(ctx, query, now, text.PublicationID)
	if err != nil {
		return "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows != 1 {
		return "", fmt.Errorf("native inbox intent was not settled with its response")
	}
	return deliveryID, nil
}
