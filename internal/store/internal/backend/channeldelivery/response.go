package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store/internal/backend/decisionpersistence"
	"github.com/google/uuid"
)

// PlanInboxResponseTx commits a requested readback and its immutable
// render together with the verified inbound intent disposition. Delivery is a
// later managed effect; the entry alone grants no resend or mailbox mutation.
func PlanInboxResponseTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText,
	expected render.ResolvedInboxEntry, fullText string, postgres bool) (string, error) {
	if tx == nil || text.EntryReference == "" || expected.PrincipalID == "" {
		return "", fmt.Errorf("native inbox response requires verified entry")
	}
	if err := LockPrincipalTx(ctx, tx, expected.PrincipalID, postgres); err != nil {
		return "", err
	}
	entry, err := requireInboxResponseEntryTx(ctx, tx, text, expected, postgres)
	if err != nil {
		return "", err
	}
	selected, found, err := LockDefaultTx(ctx, tx, postgres)
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
	fullText, err = appendUncertaintyReadbackTx(ctx, tx, audience, fullText, postgres)
	if err != nil {
		return "", err
	}
	recovery, err := ListActionableUncertainTx(ctx, tx, selected, postgres)
	if err != nil {
		return "", err
	}
	var frozen render.Frozen
	if len(recovery) == 0 {
		frozen, err = render.FreezeResponse(text.PublicationID, fullText, audience)
	} else {
		frozen, err = render.FreezeRecoveryInbox(text.PublicationID, fullText, recovery, audience)
	}
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	deliveryID, err := insertResponsePlanTx(ctx, tx, frozen, entry.ActivationID, entry.BindingRevision, now, postgres)
	if err != nil {
		return "", err
	}
	if err := SettleTextIntentTx(ctx, tx, text, "entry", postgres); err != nil {
		return "", err
	}
	return deliveryID, nil
}

func requireInboxResponseEntryTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText,
	expected render.ResolvedInboxEntry, postgres bool) (render.ResolvedInboxEntry, error) {
	entry, found, err := ResolveCurrentInboxEntryTx(ctx, tx, text, postgres)
	if err == nil && found && entry.Kind == render.InboxEntryNative {
		qualification, qualificationErr := ReadNativeInboxQualificationTx(ctx, tx, entry.ActivationID, postgres)
		if qualificationErr != nil {
			return render.ResolvedInboxEntry{}, qualificationErr
		}
		if qualification.State != channelnative.QualificationQualified || qualification.SettingID != entry.SettingID || qualification.SettingGeneration != entry.SettingGeneration {
			return render.ResolvedInboxEntry{}, fmt.Errorf("native inbox response requires exact current client qualification")
		}
	}
	if err != nil {
		return render.ResolvedInboxEntry{}, err
	}
	if !found || entry != expected {
		return render.ResolvedInboxEntry{}, fmt.Errorf("native inbox entry changed before response admission")
	}
	if entry.Kind != render.InboxEntryNative && entry.Kind != render.InboxEntryTextReply {
		return render.ResolvedInboxEntry{}, fmt.Errorf("inbox entry kind is not admitted")
	}
	return entry, nil
}

// PlanTextResponseTx records a non-sensitive response to a verified ordinary
// text occurrence. It does not grant card, draft, or resend authority.
func PlanTextResponseTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText,
	fullText, disposition string, postgres bool) (string, error) {
	if tx == nil || text.EntryReference != "" || (disposition != "teaching" && disposition != "chooser") {
		return "", fmt.Errorf("channel text response requires verified ordinary text and a response disposition")
	}
	activationID, bindingRevision, audience, err := currentTextResponseAudienceTx(ctx, tx, text, postgres)
	if err != nil {
		return "", err
	}
	frozen, err := render.FreezeResponse(text.PublicationID, fullText, audience)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	deliveryID, err := insertResponsePlanTx(ctx, tx, frozen, activationID, bindingRevision, now, postgres)
	if err != nil {
		return "", err
	}
	if err := SettleTextIntentTx(ctx, tx, text, disposition, postgres); err != nil {
		return "", err
	}
	return deliveryID, nil
}

func PlanDraftChooserTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, at time.Time, postgres bool) (string, error) {
	if tx == nil || at.IsZero() || text.EntryReference != "" {
		return "", fmt.Errorf("draft chooser requires a verified ordinary text occurrence")
	}
	activationID, bindingRevision, audience, err := currentTextResponseAudienceTx(ctx, tx, text, postgres)
	if err != nil {
		return "", err
	}
	choices := make([]render.DraftChoice, 0)
	cursor := ""
	for {
		candidates, next, err := ListCurrentInputDraftsTx(ctx, tx, text, at, cursor, 200, true, postgres)
		if err != nil {
			return "", err
		}
		for _, candidate := range candidates {
			card, err := decisionpersistence.LoadDecisionCardInTx(ctx, tx, candidate.CardID, postgres)
			if err != nil {
				return "", err
			}
			if card.Status != decisioncard.StatusPending {
				return "", fmt.Errorf("draft chooser card is no longer pending")
			}
			label := card.Snapshot.Title
			if label == "" {
				label = card.Snapshot.Decision
			}
			label = render.DraftChoiceLabel(label, candidate.CardID)
			choices = append(choices, render.DraftChoice{DraftID: candidate.DraftID, CardID: candidate.CardID, Label: label})
		}
		if next == "" {
			break
		}
		if next == cursor {
			return "", fmt.Errorf("channel draft chooser cursor did not advance")
		}
		cursor = next
	}
	if len(choices) < 2 {
		return "", fmt.Errorf("draft chooser no longer has multiple current prompts")
	}
	frozen, err := render.FreezeDraftChooser(text.PublicationID, text.PublicationID, choices, audience)
	if err != nil {
		return "", err
	}
	deliveryID, err := insertResponsePlanTx(ctx, tx, frozen, activationID, bindingRevision, time.Now().UTC(), postgres)
	if err != nil {
		return "", err
	}
	if err := SettleTextIntentTx(ctx, tx, text, "chooser", postgres); err != nil {
		return "", err
	}
	return deliveryID, nil
}

func currentTextResponseAudienceTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres bool) (string, int64, render.Audience, error) {
	if err := RequireTextIntentTx(ctx, tx, text, postgres); err != nil {
		return "", 0, render.Audience{}, err
	}
	return currentBoundTextAudienceTx(ctx, tx, text, postgres, true)
}

func currentBoundTextAudienceTx(ctx context.Context, tx *sql.Tx, text operatorchannel.InboundText, postgres, lock bool) (string, int64, render.Audience, error) {
	resolved, current, err := ResolveCurrentTextTx(ctx, tx, text, postgres)
	if err != nil {
		return "", 0, render.Audience{}, err
	}
	if !current {
		return "", 0, render.Audience{}, fmt.Errorf("channel text response has no current binding")
	}
	if lock {
		if err := LockPrincipalTx(ctx, tx, resolved.PrincipalID, postgres); err != nil {
			return "", 0, render.Audience{}, err
		}
	}
	selected, found, err := loadDefault(ctx, tx, postgres, lock)
	if err != nil {
		return "", 0, render.Audience{}, err
	}
	if !found || selected.State != StateCurrent || selected.PrincipalID != resolved.PrincipalID ||
		selected.InterfaceKey != resolved.InterfaceKey || selected.BindingRevision != resolved.BindingRevision ||
		selected.ExternalAccountRef != text.ExternalAccountRef || selected.ConversationRef != text.ConversationRef ||
		selected.ConversationScope != text.ConversationScope {
		return "", 0, render.Audience{}, fmt.Errorf("channel text response destination is no longer current")
	}
	activationID, found, err := CurrentActivationID(ctx, tx, postgres)
	if err != nil {
		return "", 0, render.Audience{}, err
	}
	if !found {
		return "", 0, render.Audience{}, fmt.Errorf("channel text response has no current activation")
	}
	audience := render.Audience{
		PrincipalID: selected.PrincipalID, InterfaceKey: selected.InterfaceKey,
		DeliveryEpoch: selected.DeliveryEpoch, ExternalAccountRef: selected.ExternalAccountRef,
		ConversationRef: selected.ConversationRef, ConversationScope: selected.ConversationScope,
	}
	return activationID, selected.BindingRevision, audience, nil
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

func freezeActionContinuationTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, resolved render.ResolvedAction, inboxText string, audience render.Audience, postgres bool) (render.Frozen, error) {
	if inboxText != "" {
		return render.Frozen{}, fmt.Errorf("view-full content cannot be supplied by the caller")
	}
	stored, current, loadErr := LoadRender(ctx, tx, resolved.RenderID, postgres)
	if loadErr != nil {
		return render.Frozen{}, loadErr
	}
	if !current || stored.Frozen.Hash != resolved.RenderHash {
		return render.Frozen{}, fmt.Errorf("view-full action lacks its immutable render")
	}
	source, sourceRenderID, index := stored.Frozen, stored.RenderID, 0
	if resolved.Action.Kind == "next_page" {
		if source.Page == nil || source.Page.Index+1 >= source.Page.Count {
			return render.Frozen{}, fmt.Errorf("next page action has no retained continuation")
		}
		sourceRenderID, index = source.Page.SourceRenderID, source.Page.Index+1
		original, originalFound, originalErr := LoadRender(ctx, tx, sourceRenderID, postgres)
		if originalErr != nil {
			return render.Frozen{}, originalErr
		}
		if !originalFound || original.Frozen.Hash != source.Page.SourceRenderHash {
			return render.Frozen{}, fmt.Errorf("next page original render changed")
		}
		source = original.Frozen
	} else if source.Page != nil {
		return render.Frozen{}, fmt.Errorf("view-full action cannot restart a continuation page")
	}
	if source.Audience != audience {
		return render.Frozen{}, fmt.Errorf("view-full page audience changed")
	}
	return render.FreezeResponsePage(action.PublicationID, source, sourceRenderID, index, audience)
}

func freezeActionResponseTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction, resolved render.ResolvedAction, inboxText string, audience render.Audience, postgres bool) (render.Frozen, error) {
	var frozen render.Frozen
	var err error
	switch resolved.Action.Kind {
	case "open_inbox":
		if resolved.SourceKind != PlanSummary || inboxText == "" {
			return render.Frozen{}, fmt.Errorf("open inbox requires a summary action and canonical list")
		}
		inboxText, err = appendUncertaintyReadbackTx(ctx, tx, audience, inboxText, postgres)
		if err != nil {
			return render.Frozen{}, err
		}
		frozen, err = render.FreezeResponse(action.PublicationID, inboxText, audience)
	case "view_full", "next_page":
		frozen, err = freezeActionContinuationTx(ctx, tx, action, resolved, inboxText, audience, postgres)
	case "select_draft":
		if inboxText != "" {
			return render.Frozen{}, fmt.Errorf("draft choice teaching cannot receive caller content")
		}
		if _, _, _, err = RequireChosenInputDraftTx(ctx, tx, action, time.Now().UTC(), true, postgres); err != nil {
			return render.Frozen{}, err
		}
		frozen, err = render.FreezeResponse(action.PublicationID,
			"That answer does not match the requested field. Reply with a value of the required type, or use the card controls.", audience)
	default:
		return render.Frozen{}, fmt.Errorf("unsupported channel navigation action %q", resolved.Action.Kind)
	}
	if err != nil {
		return render.Frozen{}, err
	}
	return frozen, nil
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
	selected, found, err := LockDefaultTx(ctx, tx, postgres)
	if err != nil {
		return "", err
	}
	if !found || selected.State != StateCurrent || selected.PrincipalID != resolved.PrincipalID ||
		(resolved.SourceKind != PlanResponse && (selected.BindingRevision != resolved.BindingRevision ||
			selected.InterfaceKey != action.Interface.Key() || selected.ExternalAccountRef != action.ExternalAccountRef ||
			selected.ConversationRef != action.ConversationRef || selected.ConversationScope != action.ConversationScope)) {
		return "", fmt.Errorf("channel navigation destination is no longer current")
	}
	audience := render.Audience{
		PrincipalID: resolved.PrincipalID, InterfaceKey: action.Interface.Key(),
		DeliveryEpoch: selected.DeliveryEpoch, ExternalAccountRef: action.ExternalAccountRef,
		ConversationRef: action.ConversationRef, ConversationScope: action.ConversationScope,
	}
	frozen, err := freezeActionResponseTx(ctx, tx, action, resolved, inboxText, audience, postgres)
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
