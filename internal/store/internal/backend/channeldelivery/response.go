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
	now := time.Now().UTC()
	deliveryID, err := insertResponsePlanTx(ctx, tx, frozen, entry.ActivationID, entry.BindingRevision, now, postgres)
	if err != nil {
		return "", err
	}
	query := `UPDATE operator_channel_text_intents SET state='settled', disposition='entry', settled_at=?
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

func insertResponsePlanTx(ctx context.Context, tx *sql.Tx, frozen render.Frozen, activationID string, bindingRevision int64, now time.Time, postgres bool) (string, error) {
	if tx == nil || frozen.Validate() != nil || frozen.SourceKind != PlanResponse ||
		uuid.Validate(activationID) != nil || bindingRevision < 1 {
		return "", fmt.Errorf("channel response plan is incomplete")
	}
	deliveryID, renderID := uuid.NewString(), uuid.NewString()
	query := `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, request_activation_id, principal_id,
		interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
		conversation_scope, state, current_render_id, created_at)
		VALUES (?, 'response', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'rendered', ?, ?)`
	if postgres {
		query = `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, request_activation_id, principal_id,
			interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
			conversation_scope, state, current_render_id, created_at)
			VALUES ($1::uuid, 'response', $2::uuid, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, $10, 'rendered', $11::uuid, $12)`
	}
	if _, err := tx.ExecContext(ctx, query, deliveryID, frozen.SourceID, activationID, frozen.Audience.PrincipalID,
		frozen.Audience.InterfaceKey, bindingRevision, frozen.Audience.DeliveryEpoch, frozen.Audience.ExternalAccountRef,
		frozen.Audience.ConversationRef, string(frozen.Audience.ConversationScope), renderID, now); err != nil {
		return "", fmt.Errorf("plan channel response: %w", err)
	}
	query = `INSERT INTO channel_delivery_renders (render_id, delivery_id, source_revision,
		projection_version, render_input, render_hash, created_at) VALUES (?, ?, 1, ?, ?, ?, ?)`
	if postgres {
		query = `INSERT INTO channel_delivery_renders (render_id, delivery_id, source_revision,
			projection_version, render_input, render_hash, created_at) VALUES ($1::uuid, $2::uuid, 1, $3, $4::jsonb, $5, $6)`
	}
	if _, err := tx.ExecContext(ctx, query, renderID, deliveryID, render.ProjectionVersion,
		string(frozen.Input), frozen.Hash, now); err != nil {
		return "", fmt.Errorf("freeze channel response: %w", err)
	}
	return deliveryID, nil
}

// PlanActionResponseTx commits one requested navigation page and settles its
// verified callback in the same selected-store transaction. View-full content
// is derived only from the immutable source render referenced by the tap.
func PlanActionResponseTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	expected render.ResolvedAction, inboxText string, postgres bool) (string, error) {
	if tx == nil || expected.PrincipalID == "" {
		return "", fmt.Errorf("channel navigation response requires a verified action")
	}
	if err := LockPrincipalTx(ctx, tx, expected.PrincipalID, postgres); err != nil {
		return "", err
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, true)
	if err != nil {
		return "", err
	}
	if state != "pending" {
		return "", fmt.Errorf("channel navigation action is already settled")
	}
	resolved, found, err := ResolveActionFactForMutationTx(ctx, tx, action.ActionFact, postgres)
	if err != nil {
		return "", err
	}
	if !found || !resolved.CurrentRender || resolved != expected {
		return "", fmt.Errorf("channel navigation action is no longer current")
	}
	selected, found, err := LoadDefault(ctx, tx, postgres)
	if err != nil {
		return "", err
	}
	if !found || selected.State != StateCurrent || selected.PrincipalID != resolved.PrincipalID ||
		selected.BindingRevision != resolved.BindingRevision || selected.InterfaceKey != action.Interface.Key() ||
		selected.ExternalAccountRef != action.ExternalAccountRef || selected.ConversationRef != action.ConversationRef ||
		selected.ConversationScope != action.ConversationScope {
		return "", fmt.Errorf("channel navigation destination is no longer current")
	}
	audience := render.Audience{
		PrincipalID: resolved.PrincipalID, InterfaceKey: selected.InterfaceKey,
		DeliveryEpoch: selected.DeliveryEpoch, ExternalAccountRef: selected.ExternalAccountRef,
		ConversationRef: selected.ConversationRef, ConversationScope: selected.ConversationScope,
	}
	var frozen render.Frozen
	switch resolved.Action.Kind {
	case "open_inbox":
		if resolved.SourceKind != PlanSummary || inboxText == "" {
			return "", fmt.Errorf("open inbox requires a summary action and canonical list")
		}
		frozen, err = render.FreezeResponse(action.PublicationID, inboxText, audience)
	case "view_full", "next_page":
		if inboxText != "" {
			return "", fmt.Errorf("view-full content cannot be supplied by the caller")
		}
		stored, current, loadErr := LoadRender(ctx, tx, resolved.RenderID, postgres)
		if loadErr != nil {
			return "", loadErr
		}
		if !current || stored.Frozen.Hash != resolved.RenderHash {
			return "", fmt.Errorf("view-full action lacks its immutable render")
		}
		source, sourceRenderID, index := stored.Frozen, stored.RenderID, 0
		if resolved.Action.Kind == "next_page" {
			if source.Page == nil || source.Page.Index+1 >= source.Page.Count {
				return "", fmt.Errorf("next page action has no retained continuation")
			}
			sourceRenderID, index = source.Page.SourceRenderID, source.Page.Index+1
			original, originalFound, originalErr := LoadRender(ctx, tx, sourceRenderID, postgres)
			if originalErr != nil {
				return "", originalErr
			}
			if !originalFound || original.Frozen.Hash != source.Page.SourceRenderHash {
				return "", fmt.Errorf("next page original render changed")
			}
			source = original.Frozen
		} else if source.Page != nil {
			return "", fmt.Errorf("view-full action cannot restart a continuation page")
		}
		if source.Audience != audience {
			return "", fmt.Errorf("view-full page audience changed")
		}
		frozen, err = render.FreezeResponsePage(action.PublicationID, source, sourceRenderID, index, audience)
	default:
		return "", fmt.Errorf("unsupported channel navigation action %q", resolved.Action.Kind)
	}
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	deliveryID, err := insertResponsePlanTx(ctx, tx, frozen, resolved.ActivationID, resolved.BindingRevision, now, postgres)
	if err != nil {
		return "", err
	}
	query := `UPDATE operator_channel_action_intents SET state='settled', disposition='navigation', settled_at=?
		WHERE publication_id=? AND state='pending'`
	if postgres {
		query = `UPDATE operator_channel_action_intents SET state='settled', disposition='navigation', settled_at=$1
			WHERE publication_id=$2::uuid AND state='pending'`
	}
	result, err := tx.ExecContext(ctx, query, now, action.PublicationID)
	if err != nil {
		return "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows != 1 {
		return "", fmt.Errorf("channel navigation intent was not settled with its response")
	}
	return deliveryID, nil
}
