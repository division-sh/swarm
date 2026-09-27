package effectpersistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

// A delivery send owns no run or agent. The principal lock serializes default
// changes with authorization and launch; the joined rows fence a replaced
// binding or activation even when the immutable plan still exists.
func requireChannelDeliveryAuthorityTx(ctx context.Context, tx *sql.Tx, authority runtimeeffects.Authority, postgres bool) error {
	delivery := authority.ChannelDelivery
	if err := channeldelivery.LockPrincipalTx(ctx, tx, delivery.PrincipalID, postgres); err != nil {
		return invalidExternalAuthority(authority, "principal_not_current")
	}
	current, err := channelDeliveryAuthorityCurrent(ctx, tx, authority, postgres, true)
	if err != nil {
		return err
	}
	if !current {
		return invalidExternalAuthority(authority, "stale")
	}
	plan, found, err := channeldelivery.LoadPlan(ctx, tx, delivery.DeliveryID, postgres)
	if err != nil {
		return err
	}
	if !found {
		return invalidExternalAuthority(authority, "source_missing")
	}
	frozen, err := channeldelivery.FreezeCurrentSourceTx(ctx, tx, plan, postgres)
	if err != nil || frozen.Hash != delivery.RenderHash {
		return invalidExternalAuthority(authority, "source_changed")
	}
	return nil
}

func channelDeliveryAuthorityCurrent(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority, postgres, lock bool) (bool, error) {
	if !authority.Valid() || authority.Kind != runtimeeffects.AuthorityChannelDelivery {
		return false, nil
	}
	d := authority.ChannelDelivery
	plan, found, err := channeldelivery.LoadPlan(ctx, q, d.DeliveryID, postgres)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	if plan.SourceKind == channeldelivery.PlanResponse {
		return channelResponseAuthorityCurrent(ctx, q, authority, postgres, lock)
	}
	query := `SELECT p.delivery_id
		FROM channel_delivery_plans p
		JOIN channel_delivery_renders r ON r.delivery_id=p.delivery_id
		JOIN channel_delivery_defaults selected ON selected.singleton_id=1
		JOIN operator_channel_bindings binding ON binding.interface_key=selected.interface_key
		JOIN connected_channel_activations activation ON activation.activation_id=?
		JOIN channel_onboarding_operations onboarding ON onboarding.operation_id=activation.operation_id
		WHERE p.delivery_id=? AND r.render_id=? AND p.current_render_id=r.render_id AND r.render_hash=?
		  AND p.state='rendered'
		  AND p.principal_id=? AND p.interface_key=? AND p.delivery_epoch=?
		  AND p.external_account_reference=? AND p.conversation_reference=?
		  AND selected.state='current' AND selected.principal_id=p.principal_id
		  AND selected.interface_key=p.interface_key AND selected.delivery_epoch=p.delivery_epoch
		  AND selected.binding_revision=? AND selected.external_account_reference=p.external_account_reference
		  AND selected.conversation_reference=p.conversation_reference
		  AND selected.conversation_scope=p.conversation_scope
		  AND binding.status='current' AND binding.principal_id=selected.principal_id
		  AND binding.binding_revision=selected.binding_revision
		  AND binding.external_account_reference=selected.external_account_reference
		  AND binding.conversation_reference=selected.conversation_reference
		  AND binding.conversation_scope=selected.conversation_scope
		  AND onboarding.identity_operation_id=binding.operation_id
		  AND activation.status='current' AND activation.activation_revision=?
		  AND onboarding.phase='succeeded'
		  AND activation.principal_id=selected.principal_id AND activation.interface_key=selected.interface_key
		  AND activation.binding_revision=selected.binding_revision
		  AND activation.conversation_reference=selected.conversation_reference
		  AND activation.bundle_hash=? AND activation.bundle_identity=?
		  AND activation.pack_inventory_generation=? AND activation.runtime_instance_id=?
		  AND activation.context_publication_generation=? AND activation.plan_generation=?
		  AND activation.target_generation=?
		  AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations other
		       WHERE other.authority_kind='channel_delivery'
		         AND json_extract(other.authority_evidence, '$.delivery_id')=p.delivery_id
		         AND other.operation_id<>?
		         AND other.state IN ('authorized','launched','response_observed'))
		  AND p.source_kind IN ('summary','notice','card')`
	args := []any{d.ActivationID, d.DeliveryID, d.RenderID, d.RenderHash, d.PrincipalID,
		d.InterfaceKey, d.DeliveryEpoch, d.ExternalAccountRef, d.ConversationRef,
		d.BindingRevision, d.ActivationRevision, d.BundleHash, d.BundleIdentity,
		d.PackInventoryGeneration, d.RuntimeInstanceID, d.ContextPublicationGeneration,
		d.PlanGeneration.Diagnostic(), d.TargetGeneration, d.EffectOperationID}
	if postgres {
		query = `SELECT p.delivery_id::text
			FROM channel_delivery_plans p
			JOIN channel_delivery_renders r ON r.delivery_id=p.delivery_id
			JOIN channel_delivery_defaults selected ON selected.singleton_id=1
			JOIN operator_channel_bindings binding ON binding.interface_key=selected.interface_key
			JOIN connected_channel_activations activation ON activation.activation_id=$1::uuid
			JOIN channel_onboarding_operations onboarding ON onboarding.operation_id=activation.operation_id
			WHERE p.delivery_id=$2::uuid AND r.render_id=$3::uuid AND p.current_render_id=r.render_id AND r.render_hash=$4
			  AND p.state='rendered'
			  AND p.principal_id=$5::uuid AND p.interface_key=$6 AND p.delivery_epoch=$7
			  AND p.external_account_reference=$8 AND p.conversation_reference=$9
			  AND selected.state='current' AND selected.principal_id=p.principal_id
			  AND selected.interface_key=p.interface_key AND selected.delivery_epoch=p.delivery_epoch
			  AND selected.binding_revision=$10 AND selected.external_account_reference=p.external_account_reference
			  AND selected.conversation_reference=p.conversation_reference
			  AND selected.conversation_scope=p.conversation_scope
			  AND binding.status='current' AND binding.principal_id=selected.principal_id
			  AND binding.binding_revision=selected.binding_revision
			  AND binding.external_account_reference=selected.external_account_reference
			  AND binding.conversation_reference=selected.conversation_reference
			  AND binding.conversation_scope=selected.conversation_scope
			  AND onboarding.identity_operation_id=binding.operation_id
			  AND activation.status='current' AND activation.activation_revision=$11
			  AND onboarding.phase='succeeded'
			  AND activation.principal_id=selected.principal_id AND activation.interface_key=selected.interface_key
			  AND activation.binding_revision=selected.binding_revision
			  AND activation.conversation_reference=selected.conversation_reference
			  AND activation.bundle_hash=$12 AND activation.bundle_identity=$13
			  AND activation.pack_inventory_generation=$14 AND activation.runtime_instance_id=$15::uuid
			  AND activation.context_publication_generation=$16 AND activation.plan_generation=$17
			  AND activation.target_generation=$18
			  AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations other
			       WHERE other.authority_kind='channel_delivery'
			         AND other.authority_evidence->>'delivery_id'=p.delivery_id::text
			         AND other.operation_id<>$19::uuid
			         AND other.state IN ('authorized','launched','response_observed'))
			  AND p.source_kind IN ('summary','notice','card')`
	}
	if d.PreviousReceiptOperationID == "" {
		query += ` AND p.current_receipt_operation_id IS NULL
			AND (p.source_kind='summary' OR (p.source_kind='notice' AND EXISTS
			(SELECT 1 FROM mailbox notice WHERE notice.item_id=p.source_id AND notice.status='pending'))
			OR (p.source_kind='card' AND EXISTS
			(SELECT 1 FROM decision_cards card WHERE card.card_id=p.source_id AND card.status='pending')))`
	} else if postgres {
		query += ` AND p.current_receipt_operation_id=$20::uuid
			AND EXISTS (SELECT 1 FROM channel_delivery_receipts previous
			WHERE previous.effect_operation_id=$20::uuid AND previous.delivery_id=p.delivery_id AND previous.state='sent')`
		args = append(args, d.PreviousReceiptOperationID)
	} else {
		query += ` AND p.current_receipt_operation_id=?
			AND EXISTS (SELECT 1 FROM channel_delivery_receipts previous
			WHERE previous.effect_operation_id=? AND previous.delivery_id=p.delivery_id AND previous.state='sent')`
		args = append(args, d.PreviousReceiptOperationID, d.PreviousReceiptOperationID)
	}
	if postgres {
		if lock {
			query += ` FOR UPDATE OF p, selected, binding, activation`
		}
	}
	var id string
	err = q.QueryRowContext(ctx, query, args...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check exact channel delivery authority: %w", err)
	}
	return id == d.DeliveryID, nil
}

func channelResponseAuthorityCurrent(ctx context.Context, q schemaQueryer, authority runtimeeffects.Authority, postgres, lock bool) (bool, error) {
	d := authority.ChannelDelivery
	if d.PreviousReceiptOperationID != "" {
		return false, nil
	}
	query := `SELECT p.delivery_id FROM channel_delivery_plans p
		JOIN channel_delivery_renders r ON r.delivery_id=p.delivery_id
		LEFT JOIN operator_channel_text_intents intent ON intent.publication_id=p.source_id
		LEFT JOIN operator_channel_action_intents action ON action.publication_id=p.source_id
		JOIN channel_delivery_defaults selected ON selected.singleton_id=1
		JOIN operator_channel_bindings binding ON binding.interface_key=p.interface_key
		JOIN connected_channel_activations activation ON activation.activation_id=?
		JOIN channel_onboarding_operations onboarding ON onboarding.operation_id=activation.operation_id
		WHERE p.delivery_id=? AND p.source_kind='response' AND p.state='rendered'
		AND p.current_render_id=r.render_id AND r.render_id=? AND r.render_hash=?
		AND p.current_receipt_operation_id IS NULL
		AND ((intent.state='settled' AND intent.disposition='entry' AND action.publication_id IS NULL)
		  OR (action.state='settled' AND action.disposition='navigation' AND intent.publication_id IS NULL))
		AND p.principal_id=? AND p.interface_key=? AND p.delivery_epoch=?
		AND p.external_account_reference=? AND p.conversation_reference=?
		AND selected.state='current' AND selected.principal_id=p.principal_id
		AND selected.delivery_epoch=p.delivery_epoch
		AND binding.status='current' AND binding.principal_id=p.principal_id
		AND binding.binding_revision>=p.binding_revision AND binding.binding_revision=?
		AND binding.external_account_reference=p.external_account_reference
		AND binding.conversation_reference=p.conversation_reference
		AND binding.conversation_scope=p.conversation_scope
		AND onboarding.identity_operation_id=binding.operation_id
		AND activation.status='current' AND activation.activation_id=p.request_activation_id
		AND activation.activation_revision=?
		AND onboarding.phase='succeeded' AND activation.principal_id=binding.principal_id
		AND activation.interface_key=binding.interface_key
		AND activation.binding_revision=binding.binding_revision
		AND activation.conversation_reference=binding.conversation_reference
		AND activation.bundle_hash=? AND activation.bundle_identity=?
		AND activation.pack_inventory_generation=? AND activation.runtime_instance_id=?
		AND activation.context_publication_generation=? AND activation.plan_generation=?
		AND activation.target_generation=?
		AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations other
			WHERE other.authority_kind='channel_delivery'
			AND json_extract(other.authority_evidence, '$.delivery_id')=p.delivery_id
			AND other.operation_id<>? AND other.state IN ('authorized','launched','response_observed'))`
	args := []any{d.ActivationID, d.DeliveryID, d.RenderID, d.RenderHash,
		d.PrincipalID, d.InterfaceKey, d.DeliveryEpoch, d.ExternalAccountRef, d.ConversationRef,
		d.BindingRevision, d.ActivationRevision, d.BundleHash, d.BundleIdentity,
		d.PackInventoryGeneration, d.RuntimeInstanceID, d.ContextPublicationGeneration,
		d.PlanGeneration.Diagnostic(), d.TargetGeneration, d.EffectOperationID}
	if postgres {
		query = `SELECT p.delivery_id::text FROM channel_delivery_plans p
			JOIN channel_delivery_renders r ON r.delivery_id=p.delivery_id
			LEFT JOIN operator_channel_text_intents intent ON intent.publication_id=p.source_id
			LEFT JOIN operator_channel_action_intents action ON action.publication_id=p.source_id
			JOIN channel_delivery_defaults selected ON selected.singleton_id=1
			JOIN operator_channel_bindings binding ON binding.interface_key=p.interface_key
			JOIN connected_channel_activations activation ON activation.activation_id=$1::uuid
			JOIN channel_onboarding_operations onboarding ON onboarding.operation_id=activation.operation_id
			WHERE p.delivery_id=$2::uuid AND p.source_kind='response' AND p.state='rendered'
			AND p.current_render_id=r.render_id AND r.render_id=$3::uuid AND r.render_hash=$4
			AND p.current_receipt_operation_id IS NULL
			AND ((intent.state='settled' AND intent.disposition='entry' AND action.publication_id IS NULL)
			  OR (action.state='settled' AND action.disposition='navigation' AND intent.publication_id IS NULL))
			AND p.principal_id=$5::uuid AND p.interface_key=$6 AND p.delivery_epoch=$7
			AND p.external_account_reference=$8 AND p.conversation_reference=$9
			AND selected.state='current' AND selected.principal_id=p.principal_id
			AND selected.delivery_epoch=p.delivery_epoch
			AND binding.status='current' AND binding.principal_id=p.principal_id
			AND binding.binding_revision>=p.binding_revision AND binding.binding_revision=$10
			AND binding.external_account_reference=p.external_account_reference
			AND binding.conversation_reference=p.conversation_reference
			AND binding.conversation_scope=p.conversation_scope
			AND onboarding.identity_operation_id=binding.operation_id
			AND activation.status='current' AND activation.activation_id=p.request_activation_id
			AND activation.activation_revision=$11
			AND onboarding.phase='succeeded' AND activation.principal_id=binding.principal_id
			AND activation.interface_key=binding.interface_key
			AND activation.binding_revision=binding.binding_revision
			AND activation.conversation_reference=binding.conversation_reference
			AND activation.bundle_hash=$12 AND activation.bundle_identity=$13
			AND activation.pack_inventory_generation=$14 AND activation.runtime_instance_id=$15::uuid
			AND activation.context_publication_generation=$16 AND activation.plan_generation=$17
			AND activation.target_generation=$18
			AND NOT EXISTS (SELECT 1 FROM runtime_external_effect_operations other
				WHERE other.authority_kind='channel_delivery'
				AND other.authority_evidence->>'delivery_id'=p.delivery_id::text
				AND other.operation_id<>$19::uuid AND other.state IN ('authorized','launched','response_observed'))`
		if lock {
			query += ` FOR UPDATE OF p, selected, binding, activation`
		}
	}
	var id string
	err := q.QueryRowContext(ctx, query, args...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check exact channel response authority: %w", err)
	}
	return id == d.DeliveryID, nil
}
