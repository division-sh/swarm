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

// ResolveActionFactTx is a read-only admission projection. The same exact
// joins must be rechecked in the card/notice mutation transaction before
// replay and before the domain write.
func ResolveActionFactTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres bool) (render.ResolvedAction, bool, error) {
	return resolveActionFactTx(ctx, tx, fact, postgres, false)
}

func ResolveActionFactForMutationTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres bool) (render.ResolvedAction, bool, error) {
	return resolveActionFactTx(ctx, tx, fact, postgres, true)
}

// RequireCardActionTx is the channel-owned pre-mutation projection. Call it
// before idempotency replay and again in the card write transaction.
func RequireCardActionTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, demand render.CardActionDemand, postgres, lock bool) error {
	if uuid.Validate(demand.CardID) != nil || uuid.Validate(demand.PrincipalID) != nil ||
		uuid.Validate(demand.ReceiptOperationID) != nil || demand.Verdict == "" ||
		(demand.Method != "mailbox.decide" && demand.Method != "mailbox.begin_input") ||
		(demand.Method == "mailbox.decide" && demand.RenderHash == "") {
		return fmt.Errorf("channel card action demand is incomplete")
	}
	var resolved render.ResolvedAction
	var found bool
	var err error
	if lock {
		resolved, found, err = ResolveActionFactForMutationTx(ctx, tx, fact, postgres)
	} else {
		resolved, found, err = ResolveActionFactTx(ctx, tx, fact, postgres)
	}
	if err != nil {
		return err
	}
	if !found || !resolved.CurrentRender || resolved.SourceKind != "card" ||
		resolved.SourceID != demand.CardID || resolved.PrincipalID != demand.PrincipalID ||
		resolved.Action.Kind != "verdict" || resolved.Action.Verdict != demand.Verdict ||
		resolved.ReceiptOperationID != demand.ReceiptOperationID ||
		(demand.Method == "mailbox.decide" && resolved.RenderHash != demand.RenderHash) {
		return fmt.Errorf("channel action is not current card authority")
	}
	return nil
}

func resolveActionFactTx(ctx context.Context, tx *sql.Tx, fact operatorchannel.ActionFact, postgres, mutation bool) (render.ResolvedAction, bool, error) {
	if tx == nil {
		return render.ResolvedAction{}, false, fmt.Errorf("channel action requires a selected transaction")
	}
	if err := fact.Validate(); err != nil {
		return render.ResolvedAction{}, false, err
	}
	if uuid.Validate(fact.Token) != nil {
		return render.ResolvedAction{}, false, nil
	}
	query := `SELECT action.action_token, action.action_kind, action.verdict, action.label,
		plan.delivery_id, render.render_id, render.render_hash, receipt.effect_operation_id,
		plan.source_kind, plan.source_id, plan.principal_id, selected.binding_revision,
		activation.activation_id, activation.activation_revision,
		plan.current_render_id=render.render_id, receipt.provider_reference
		FROM channel_delivery_actions action
		JOIN channel_delivery_renders render ON render.render_id=action.render_id
		JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
		JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=plan.current_receipt_operation_id
		JOIN channel_delivery_defaults selected ON selected.singleton_id=1
		JOIN operator_channel_bindings binding ON binding.interface_key=selected.interface_key
		JOIN channel_onboarding_operations onboarding ON onboarding.identity_operation_id=binding.operation_id
		JOIN connected_channel_activations activation ON activation.operation_id=onboarding.operation_id
		WHERE action.action_token=? AND plan.interface_key=?
		AND plan.external_account_reference=? AND plan.conversation_reference=? AND plan.conversation_scope=?
		AND selected.state='current' AND selected.principal_id=plan.principal_id
		AND selected.interface_key=plan.interface_key AND selected.delivery_epoch=plan.delivery_epoch
		AND selected.external_account_reference=plan.external_account_reference
		AND selected.conversation_reference=plan.conversation_reference
		AND selected.conversation_scope=plan.conversation_scope
		AND binding.status='current' AND binding.principal_id=selected.principal_id
		AND binding.binding_revision=selected.binding_revision
		AND binding.external_account_reference=selected.external_account_reference
		AND binding.conversation_reference=selected.conversation_reference
		AND binding.conversation_scope=selected.conversation_scope
		AND onboarding.phase='succeeded' AND activation.status='current'
		AND activation.principal_id=selected.principal_id AND activation.interface_key=selected.interface_key
		AND activation.binding_revision=selected.binding_revision
		AND activation.conversation_reference=selected.conversation_reference
		AND receipt.state='sent' AND receipt.delivery_id=plan.delivery_id
		AND receipt.render_id=render.render_id
		LIMIT 2`
	if postgres {
		query = `SELECT action.action_token::text, action.action_kind, action.verdict, action.label,
			plan.delivery_id::text, render.render_id::text, render.render_hash, receipt.effect_operation_id::text,
			plan.source_kind, plan.source_id::text, plan.principal_id::text, selected.binding_revision,
			activation.activation_id::text, activation.activation_revision,
			plan.current_render_id=render.render_id, receipt.provider_reference
			FROM channel_delivery_actions action
			JOIN channel_delivery_renders render ON render.render_id=action.render_id
			JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
			JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=plan.current_receipt_operation_id
			JOIN channel_delivery_defaults selected ON selected.singleton_id=1
			JOIN operator_channel_bindings binding ON binding.interface_key=selected.interface_key
			JOIN channel_onboarding_operations onboarding ON onboarding.identity_operation_id=binding.operation_id
			JOIN connected_channel_activations activation ON activation.operation_id=onboarding.operation_id
			WHERE action.action_token=$1::uuid AND plan.interface_key=$2
			AND plan.external_account_reference=$3 AND plan.conversation_reference=$4 AND plan.conversation_scope=$5
			AND selected.state='current' AND selected.principal_id=plan.principal_id
			AND selected.interface_key=plan.interface_key AND selected.delivery_epoch=plan.delivery_epoch
			AND selected.external_account_reference=plan.external_account_reference
			AND selected.conversation_reference=plan.conversation_reference
			AND selected.conversation_scope=plan.conversation_scope
			AND binding.status='current' AND binding.principal_id=selected.principal_id
			AND binding.binding_revision=selected.binding_revision
			AND binding.external_account_reference=selected.external_account_reference
			AND binding.conversation_reference=selected.conversation_reference
			AND binding.conversation_scope=selected.conversation_scope
			AND onboarding.phase='succeeded' AND activation.status='current'
			AND activation.principal_id=selected.principal_id AND activation.interface_key=selected.interface_key
			AND activation.binding_revision=selected.binding_revision
			AND activation.conversation_reference=selected.conversation_reference
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
	var verdict sql.NullString
	if !rows.Next() {
		return render.ResolvedAction{}, false, rows.Err()
	}
	if err := rows.Scan(&resolved.Action.Token, &resolved.Action.Kind, &verdict, &resolved.Action.Label,
		&resolved.DeliveryID, &resolved.RenderID, &resolved.RenderHash, &resolved.ReceiptOperationID,
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
	if resolved.Action.Token != fact.Token || resolved.Action.Label == "" ||
		resolved.ActivationID == "" || resolved.BindingRevision < 1 || resolved.ActivationRevision < 1 {
		return render.ResolvedAction{}, false, fmt.Errorf("stored channel action identity is contradictory")
	}
	var receipt map[string]any
	if err := json.Unmarshal(referenceRaw, &receipt); err != nil {
		return render.ResolvedAction{}, false, fmt.Errorf("decode exact channel receipt: %w", err)
	}
	value, ok := receipt["delivery_reference"]
	if !ok {
		return render.ResolvedAction{}, false, errors.New("channel receipt lacks delivery reference")
	}
	messageRef, valid, err := operatorchannel.OpaqueReference(value)
	if err != nil || !valid {
		return render.ResolvedAction{}, false, fmt.Errorf("channel receipt has invalid delivery reference: %w", err)
	}
	if messageRef != fact.MessageReference {
		return render.ResolvedAction{}, false, nil
	}
	return resolved, true, nil
}
