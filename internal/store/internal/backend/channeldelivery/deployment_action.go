package channeldelivery

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

func resolveDeploymentAction(ctx context.Context, q actionQueryer, fact operatorchannel.ActionFact, postgres, mutation bool, locked *actionParent) (render.ResolvedAction, bool, error) {
	parent, found, err := deploymentBindingParent(ctx, q, fact.Interface.Key())
	if err != nil || !found {
		return render.ResolvedAction{}, false, err
	}
	if mutation && (locked == nil || locked.transport != parent || locked.bindingRevision < 1) {
		return render.ResolvedAction{}, false, fmt.Errorf("deployment action has no exact locked parent")
	}
	raw, err := canonicaljson.Bytes(parent)
	if err != nil {
		return render.ResolvedAction{}, false, err
	}
	query := `SELECT CAST(action.action_token AS TEXT), action.action_kind, action.verdict, action.label, action.action_position,
		CAST(plan.delivery_id AS TEXT), CAST(r.render_id AS TEXT), r.render_hash, r.render_input, CAST(receipt.effect_operation_id AS TEXT),
		plan.source_kind, CAST(plan.source_id AS TEXT), CAST(plan.principal_id AS TEXT), b.binding_revision,
		CAST(o.operation_id AS TEXT), o.operation_revision, plan.current_render_id=r.render_id, receipt.provider_reference
		FROM channel_delivery_actions action
		JOIN channel_delivery_renders r ON r.render_id=action.render_id
		JOIN channel_delivery_plans plan ON plan.delivery_id=r.delivery_id
		JOIN channel_delivery_receipts receipt ON receipt.effect_operation_id=plan.current_receipt_operation_id
		JOIN channel_delivery_defaults selected ON selected.singleton_id=1 AND selected.state='current'
		JOIN operator_channel_bindings b ON b.interface_key=plan.interface_key
		JOIN deployment_channel_operations o ON o.identity_operation_id=b.operation_id
		JOIN operator_channel_operations child ON child.operation_id=b.operation_id
		WHERE ` + deploymentTransportConditions + ` AND action.action_token=$1 AND plan.interface_key=$2
		AND plan.external_account=$3 AND plan.conversation_reference=$4 AND plan.conversation_scope=$5
		AND plan.principal_id=b.principal_id AND b.binding_revision>=plan.binding_revision
		AND b.external_account=plan.external_account AND b.conversation_reference=plan.conversation_reference
		AND b.conversation_scope=plan.conversation_scope
		AND selected.principal_id=plan.principal_id AND selected.delivery_epoch=plan.delivery_epoch
		AND ((plan.source_kind='response' AND plan.request_activation_id IS NULL
		 AND plan.request_transport_parent=$6)
		 OR (plan.source_kind<>'response' AND selected.interface_key=plan.interface_key
		 AND selected.binding_revision=b.binding_revision AND selected.external_account=plan.external_account
		 AND selected.conversation_reference=plan.conversation_reference AND selected.conversation_scope=plan.conversation_scope))
		AND receipt.state='sent' AND receipt.delivery_id=plan.delivery_id AND receipt.render_id=r.render_id
		AND o.operation_id=$7 AND o.operation_revision=$8 AND child.operation_id=$9 AND child.operation_revision=$10`
	if postgres {
		query = strings.Replace(query, "plan.request_transport_parent=$6", "plan.request_transport_parent=$6::jsonb", 1)
	}
	if mutation {
		query += ` AND b.binding_revision=$11`
	}
	query += ` LIMIT 2`
	if postgres && mutation {
		query += ` FOR UPDATE OF action,r,plan,receipt,selected,b`
	}
	args := []any{fact.Token, fact.Interface.Key(), fact.ExternalAccountRef, fact.ConversationRef, string(fact.ConversationScope),
		string(raw), parent.ID, parent.Revision, parent.BindingOperationID, parent.BindingOperationRevision}
	if mutation {
		args = append(args, locked.bindingRevision)
	}
	return scanResolvedAction(ctx, q, query, args, fact, postgres, mutation, parent)
}
