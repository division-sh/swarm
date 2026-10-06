package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

// ResolveActionFactTx is a read-only admission projection. The same exact
// joins must be rechecked in the card/notice mutation transaction before
// replay and before the domain write.
func ResolveActionFactTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres bool) (render.ResolvedAction, bool, error) {
	return resolveActionFactTx(ctx, tx, fact, postgres, false)
}

// ResolveActionFact reads the same admission projection outside a write
// transaction for effect-currentness checks. Prelaunch still uses the locked
// transaction form.
func ResolveActionFact(ctx context.Context, q interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, fact operatorchannel.ActionFact, postgres bool) (render.ResolvedAction, bool, error) {
	return resolveActionFactTx(ctx, q, fact, postgres, false)
}

func ResolveActionFactForMutationTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres bool) (render.ResolvedAction, bool, error) {
	return resolveActionFactTx(ctx, tx, fact, postgres, true)
}

// RequireCardActionTx is the channel-owned pre-mutation projection. Call it
// before idempotency replay and again in the card write transaction.
func RequireCardActionTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, demand render.CardActionDemand, postgres, lock bool) error {
	if uuid.Validate(demand.CardID) != nil || uuid.Validate(demand.PrincipalID) != nil {
		return fmt.Errorf("channel card action demand is incomplete")
	}
	switch demand.Method {
	case "mailbox.cancel_input":
		if uuid.Validate(demand.DraftID) != nil {
			return fmt.Errorf("channel card action demand is incomplete")
		}
	case "mailbox.decide", "mailbox.begin_input":
		if uuid.Validate(demand.ReceiptOperationID) != nil || demand.Verdict == "" {
			return fmt.Errorf("channel card action demand is incomplete")
		}
		if demand.Method == "mailbox.decide" && demand.RenderHash == "" {
			return fmt.Errorf("channel card action demand is incomplete")
		}
	default:
		return fmt.Errorf("channel card action demand is incomplete")
	}
	var resolved render.ResolvedAction
	var found bool
	var err error
	if lock {
		if err := LockPrincipalTx(ctx, tx, demand.PrincipalID, postgres); err != nil {
			return err
		}
		resolved, found, err = ResolveActionFactForMutationTx(ctx, tx, fact, postgres)
	} else {
		resolved, found, err = ResolveActionFactTx(ctx, tx, fact, postgres)
	}
	if err != nil {
		return err
	}
	if !found || !resolved.CurrentRender || resolved.SourceKind != "card" ||
		resolved.SourceID != demand.CardID || resolved.PrincipalID != demand.PrincipalID ||
		(demand.Method == "mailbox.cancel_input" && (resolved.Action.Kind != "cancel_input" || resolved.Action.DraftID != demand.DraftID)) ||
		(demand.Method != "mailbox.cancel_input" && (resolved.Action.Kind != "verdict" || resolved.Action.Verdict != demand.Verdict ||
			resolved.ReceiptOperationID != demand.ReceiptOperationID)) ||
		(demand.Method == "mailbox.decide" && resolved.RenderHash != demand.RenderHash) {
		return fmt.Errorf("channel action is not current card authority")
	}
	return nil
}

// RequireNoticeActionTx is the selected-store admission for a notice callback.
// It must run before completion replay and again in the notice write transaction.
func RequireNoticeActionTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	principalID, noticeID string, postgres bool) (string, error) {
	if uuid.Validate(principalID) != nil || uuid.Validate(noticeID) != nil {
		return "", fmt.Errorf("channel notice acknowledgment identity is incomplete")
	}
	if err := LockPrincipalTx(ctx, tx, principalID, postgres); err != nil {
		return "", err
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, true)
	if err != nil {
		return "", err
	}
	resolved, found, err := ResolveActionFactForMutationTx(ctx, tx, action.ActionFact, postgres)
	if err != nil {
		return "", err
	}
	if !found || !resolved.CurrentRender || resolved.SourceKind != PlanNotice ||
		resolved.SourceID != noticeID || resolved.PrincipalID != principalID ||
		resolved.Action.Kind != "acknowledge_notice" || resolved.Action.Token != action.Token {
		return "", fmt.Errorf("channel action is not current notice authority")
	}
	if state == "settled" {
		query := `SELECT disposition FROM operator_channel_action_intents WHERE publication_id=?`
		if postgres {
			query = `SELECT disposition FROM operator_channel_action_intents WHERE publication_id=$1::uuid`
		}
		var disposition string
		if err := tx.QueryRowContext(ctx, query, action.PublicationID).Scan(&disposition); err != nil {
			return "", err
		}
		if disposition != string(render.ActionApplied) {
			return "", fmt.Errorf("channel notice action settled with a different disposition")
		}
	}
	return state, nil
}

func hydrateFrozenChannelAction(action render.Action, renderRaw []byte, renderHash string, position int) (render.Action, error) {
	renderRaw, err := canonicaljson.Canonicalize(renderRaw)
	if err != nil {
		return render.Action{}, fmt.Errorf("canonicalize exact channel action render: %w", err)
	}
	frozen, err := render.Decode(renderRaw, renderHash)
	if err != nil {
		return render.Action{}, fmt.Errorf("decode exact channel action render: %w", err)
	}
	expected, err := actionsForFrozen(frozen)
	if err != nil || position < 1 || position > len(expected) {
		return render.Action{}, fmt.Errorf("channel action position contradicts immutable render: %w", err)
	}
	if choice := expected[position-1]; choice.Kind != action.Kind ||
		choice.Verdict != action.Verdict || choice.Label != action.Label {
		return render.Action{}, fmt.Errorf("channel action contradicts immutable render projection")
	} else {
		action.DraftID = choice.DraftID
		action.CardID = choice.CardID
		action.TextPublicationID = choice.TextPublicationID
		action.RecoveryDeliveryID = choice.RecoveryDeliveryID
	}
	return action, nil
}

func resolveActionFactTx(ctx context.Context, tx interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, fact operatorchannel.ActionFact, postgres, mutation bool) (render.ResolvedAction, bool, error) {
	if tx == nil {
		return render.ResolvedAction{}, false, fmt.Errorf("channel action requires a selected transaction")
	}
	if err := fact.Validate(); err != nil {
		return render.ResolvedAction{}, false, err
	}
	if uuid.Validate(fact.Token) != nil {
		return render.ResolvedAction{}, false, nil
	}
	query := `SELECT action.action_token, action.action_kind, action.verdict, action.label, action.action_position,
		plan.delivery_id, render.render_id, render.render_hash, render.render_input, receipt.effect_operation_id,
		plan.source_kind, plan.source_id, plan.principal_id, binding.binding_revision,
		activation.activation_id, activation.activation_revision,
		plan.current_render_id=render.render_id, receipt.provider_reference
		FROM channel_delivery_actions action
		JOIN channel_delivery_renders render ON render.render_id=action.render_id
		JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
		JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=plan.current_receipt_operation_id
		JOIN channel_delivery_defaults selected ON selected.singleton_id=1
		JOIN operator_channel_bindings binding ON binding.interface_key=plan.interface_key
		JOIN channel_onboarding_operations onboarding ON onboarding.identity_operation_id=binding.operation_id
		JOIN connected_channel_activations activation ON activation.operation_id=onboarding.operation_id
		WHERE action.action_token=? AND plan.interface_key=?
		AND plan.external_account_reference=? AND plan.conversation_reference=? AND plan.conversation_scope=?
		AND selected.state='current' AND selected.principal_id=plan.principal_id
		AND selected.delivery_epoch=plan.delivery_epoch
		AND binding.status='current' AND binding.principal_id=plan.principal_id
		AND binding.binding_revision>=plan.binding_revision
		AND binding.external_account_reference=plan.external_account_reference
		AND binding.conversation_reference=plan.conversation_reference
		AND binding.conversation_scope=plan.conversation_scope
		AND ((plan.source_kind='response' AND activation.activation_id=plan.request_activation_id)
		 OR (plan.source_kind<>'response' AND selected.interface_key=plan.interface_key
			AND selected.binding_revision=binding.binding_revision
			AND selected.external_account_reference=plan.external_account_reference
			AND selected.conversation_reference=plan.conversation_reference
			AND selected.conversation_scope=plan.conversation_scope))
		AND onboarding.phase='succeeded' AND activation.status='current'
		AND activation.principal_id=plan.principal_id AND activation.interface_key=plan.interface_key
		AND activation.binding_revision=binding.binding_revision
		AND activation.conversation_reference=plan.conversation_reference
		AND receipt.state='sent' AND receipt.delivery_id=plan.delivery_id
		AND receipt.render_id=render.render_id
		LIMIT 2`
	if postgres {
		query = `SELECT action.action_token::text, action.action_kind, action.verdict, action.label, action.action_position,
			plan.delivery_id::text, render.render_id::text, render.render_hash, render.render_input, receipt.effect_operation_id::text,
			plan.source_kind, plan.source_id::text, plan.principal_id::text, binding.binding_revision,
			activation.activation_id::text, activation.activation_revision,
			plan.current_render_id=render.render_id, receipt.provider_reference
			FROM channel_delivery_actions action
			JOIN channel_delivery_renders render ON render.render_id=action.render_id
			JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
			JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=plan.current_receipt_operation_id
			JOIN channel_delivery_defaults selected ON selected.singleton_id=1
			JOIN operator_channel_bindings binding ON binding.interface_key=plan.interface_key
			JOIN channel_onboarding_operations onboarding ON onboarding.identity_operation_id=binding.operation_id
			JOIN connected_channel_activations activation ON activation.operation_id=onboarding.operation_id
			WHERE action.action_token=$1::uuid AND plan.interface_key=$2
			AND plan.external_account_reference=$3 AND plan.conversation_reference=$4 AND plan.conversation_scope=$5
			AND selected.state='current' AND selected.principal_id=plan.principal_id
			AND selected.delivery_epoch=plan.delivery_epoch
			AND binding.status='current' AND binding.principal_id=plan.principal_id
			AND binding.binding_revision>=plan.binding_revision
			AND binding.external_account_reference=plan.external_account_reference
			AND binding.conversation_reference=plan.conversation_reference
			AND binding.conversation_scope=plan.conversation_scope
			AND ((plan.source_kind='response' AND activation.activation_id=plan.request_activation_id)
			 OR (plan.source_kind<>'response' AND selected.interface_key=plan.interface_key
				AND selected.binding_revision=binding.binding_revision
				AND selected.external_account_reference=plan.external_account_reference
				AND selected.conversation_reference=plan.conversation_reference
				AND selected.conversation_scope=plan.conversation_scope))
			AND onboarding.phase='succeeded' AND activation.status='current'
			AND activation.principal_id=plan.principal_id AND activation.interface_key=plan.interface_key
			AND activation.binding_revision=binding.binding_revision
			AND activation.conversation_reference=plan.conversation_reference
			AND receipt.state='sent' AND receipt.delivery_id=plan.delivery_id
			AND receipt.render_id=render.render_id
			LIMIT 2`
		if mutation {
			query += ` FOR UPDATE OF action, render, plan, receipt, selected, binding, onboarding, activation`
		}
	}
	rows, err := tx.QueryContext(ctx, query, fact.Token, fact.Interface.Key(), fact.ExternalAccountRef,
		fact.ConversationRef, string(fact.ConversationScope))
	if err != nil {
		return render.ResolvedAction{}, false, err
	}
	defer rows.Close()
	var resolved render.ResolvedAction
	var referenceRaw []byte
	var renderRaw []byte
	var position int
	var verdict sql.NullString
	if !rows.Next() {
		return render.ResolvedAction{}, false, rows.Err()
	}
	if err := rows.Scan(&resolved.Action.Token, &resolved.Action.Kind, &verdict, &resolved.Action.Label, &position,
		&resolved.DeliveryID, &resolved.RenderID, &resolved.RenderHash, &renderRaw, &resolved.ReceiptOperationID,
		&resolved.SourceKind, &resolved.SourceID, &resolved.PrincipalID, &resolved.BindingRevision,
		&resolved.ActivationID, &resolved.ActivationRevision, &resolved.CurrentRender, &referenceRaw); err != nil {
		return render.ResolvedAction{}, false, err
	}
	if rows.Next() {
		return render.ResolvedAction{}, false, fmt.Errorf("channel action resolves to multiple current activations")
	}
	if err := rows.Err(); err != nil {
		return render.ResolvedAction{}, false, err
	}
	if verdict.Valid {
		resolved.Action.Verdict = verdict.String
	}
	resolved.Action, err = hydrateFrozenChannelAction(resolved.Action, renderRaw, resolved.RenderHash, position)
	if err != nil {
		return render.ResolvedAction{}, false, err
	}
	if resolved.Action.Token != fact.Token || resolved.Action.Label == "" ||
		resolved.ActivationID == "" || resolved.BindingRevision < 1 || resolved.ActivationRevision < 1 {
		return render.ResolvedAction{}, false, fmt.Errorf("stored channel action identity is contradictory")
	}
	messageRef, err := receiptMessageReference(referenceRaw)
	if err != nil {
		return render.ResolvedAction{}, false, err
	}
	if messageRef != fact.MessageReference {
		return render.ResolvedAction{}, false, nil
	}
	return resolved, true, nil
}

func receiptMessageReference(raw []byte) (string, error) {
	var receipt map[string]any
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return "", fmt.Errorf("decode exact channel receipt: %w", err)
	}
	value, ok := receipt["delivery_reference"]
	if !ok {
		return "", errors.New("channel receipt lacks delivery reference")
	}
	messageRef, valid, err := operatorchannel.OpaqueReference(value)
	if err != nil || !valid {
		return "", fmt.Errorf("channel receipt has invalid delivery reference: %w", err)
	}
	return messageRef, nil
}
