package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

func LoadCurrentPlan(ctx context.Context, tx *sql.Tx, deliveryID string, postgres bool) (Plan, bool, error) {
	if tx == nil {
		return Plan{}, false, fmt.Errorf("current channel plan requires a transaction")
	}
	plan, found, err := LoadPlan(ctx, tx, deliveryID, postgres)
	if err != nil || !found || plan.State == "retired" {
		return Plan{}, false, err
	}
	if plan.SourceKind == PlanResponse {
		query := `SELECT b.binding_revision FROM operator_channel_bindings b
			JOIN channel_delivery_defaults d ON d.singleton_id=1 AND d.state='current'
			WHERE b.interface_key=? AND b.status='current' AND b.principal_id=?
			AND b.binding_revision>=? AND b.external_account_reference=?
			AND b.conversation_reference=? AND b.conversation_scope=?
			AND d.principal_id=b.principal_id AND d.delivery_epoch=?
			AND EXISTS (SELECT 1 FROM connected_channel_activations a
				JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
				WHERE a.activation_id=? AND a.interface_key=b.interface_key AND a.status='current'
				AND a.principal_id=b.principal_id AND a.binding_revision=b.binding_revision
				AND a.conversation_reference=b.conversation_reference
				AND o.identity_operation_id=b.operation_id AND o.phase='succeeded')`
		if postgres {
			query = `SELECT b.binding_revision FROM operator_channel_bindings b
				JOIN channel_delivery_defaults d ON d.singleton_id=1 AND d.state='current'
				WHERE b.interface_key=$1 AND b.status='current' AND b.principal_id=$2::uuid
				AND b.binding_revision>=$3 AND b.external_account_reference=$4
				AND b.conversation_reference=$5 AND b.conversation_scope=$6
				AND d.principal_id=b.principal_id AND d.delivery_epoch=$7
				AND EXISTS (SELECT 1 FROM connected_channel_activations a
					JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
					WHERE a.activation_id=$8::uuid AND a.interface_key=b.interface_key AND a.status='current'
					AND a.principal_id=b.principal_id AND a.binding_revision=b.binding_revision
					AND a.conversation_reference=b.conversation_reference
					AND o.identity_operation_id=b.operation_id AND o.phase='succeeded')`
		}
		err = tx.QueryRowContext(ctx, query, plan.InterfaceKey, plan.PrincipalID, plan.BindingRevision,
			plan.ExternalAccountRef, plan.ConversationRef, string(plan.ConversationScope), plan.DeliveryEpoch, plan.EntryActivationID).
			Scan(&plan.CurrentBindingRevision)
		if errors.Is(err, sql.ErrNoRows) {
			return Plan{}, false, nil
		}
		if err != nil {
			return Plan{}, false, err
		}
		return plan, true, plan.Validate()
	}
	query := `SELECT binding_revision FROM channel_delivery_defaults WHERE singleton_id=1
		AND state='current' AND principal_id=? AND interface_key=? AND delivery_epoch=?
		AND external_account_reference=? AND conversation_reference=? AND conversation_scope=?
		AND binding_revision>=?`
	if postgres {
		query = `SELECT binding_revision FROM channel_delivery_defaults WHERE singleton_id=1
			AND state='current' AND principal_id=$1::uuid AND interface_key=$2 AND delivery_epoch=$3
			AND external_account_reference=$4 AND conversation_reference=$5 AND conversation_scope=$6
			AND binding_revision>=$7`
	}
	err = tx.QueryRowContext(ctx, query, plan.PrincipalID, plan.InterfaceKey, plan.DeliveryEpoch,
		plan.ExternalAccountRef, plan.ConversationRef, string(plan.ConversationScope), plan.BindingRevision).
		Scan(&plan.CurrentBindingRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return Plan{}, false, nil
	}
	if err != nil {
		return Plan{}, false, err
	}
	return plan, true, plan.Validate()
}

// ListCurrentPlans reads one bounded page of selected responsibilities. A
// historical or retired destination remains durable but is not executable.
func ListCurrentPlans(ctx context.Context, tx *sql.Tx, afterDeliveryID string, limit int, postgres bool) ([]Plan, error) {
	if tx == nil || limit < 1 || limit > 500 || (afterDeliveryID != "" && uuid.Validate(afterDeliveryID) != nil) {
		return nil, fmt.Errorf("channel delivery scan requires a transaction, valid cursor and bounded limit")
	}
	query := `SELECT p.delivery_id, p.source_kind, p.source_id, COALESCE(p.entry_activation_id, ''), COALESCE(p.summary_count, 0), p.principal_id,
		p.interface_key, p.binding_revision, CASE WHEN p.source_kind='response' THEN b.binding_revision ELSE d.binding_revision END,
		p.delivery_epoch, p.external_account_reference,
		p.conversation_reference, p.conversation_scope, p.state,
		COALESCE(p.current_render_id, ''), COALESCE(p.current_receipt_operation_id, '')
		FROM channel_delivery_plans p JOIN channel_delivery_defaults d ON d.singleton_id=1
		LEFT JOIN operator_channel_bindings b ON b.interface_key=p.interface_key
		WHERE d.state='current' AND p.principal_id=d.principal_id AND p.delivery_epoch=d.delivery_epoch
		AND ((p.source_kind='response' AND b.status='current' AND b.principal_id=p.principal_id
			AND b.binding_revision>=p.binding_revision AND b.external_account_reference=p.external_account_reference
			AND b.conversation_reference=p.conversation_reference AND b.conversation_scope=p.conversation_scope
			AND EXISTS (SELECT 1 FROM connected_channel_activations a
				JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
				WHERE a.activation_id=p.entry_activation_id AND a.interface_key=b.interface_key AND a.status='current'
				AND a.principal_id=b.principal_id AND a.binding_revision=b.binding_revision
				AND a.conversation_reference=b.conversation_reference
				AND o.identity_operation_id=b.operation_id AND o.phase='succeeded'))
		OR (p.source_kind<>'response' AND p.interface_key=d.interface_key
			AND d.binding_revision>=p.binding_revision
			AND p.external_account_reference=d.external_account_reference
			AND p.conversation_reference=d.conversation_reference AND p.conversation_scope=d.conversation_scope))
		AND p.state <> 'retired' AND p.delivery_id > ? ORDER BY p.delivery_id LIMIT ?`
	if postgres {
		query = `SELECT p.delivery_id::text, p.source_kind, p.source_id::text, COALESCE(p.entry_activation_id::text, ''), COALESCE(p.summary_count, 0), p.principal_id::text,
			p.interface_key, p.binding_revision, CASE WHEN p.source_kind='response' THEN b.binding_revision ELSE d.binding_revision END,
			p.delivery_epoch, p.external_account_reference,
			p.conversation_reference, p.conversation_scope, p.state,
			COALESCE(p.current_render_id::text, ''), COALESCE(p.current_receipt_operation_id::text, '')
			FROM channel_delivery_plans p JOIN channel_delivery_defaults d ON d.singleton_id=1
			LEFT JOIN operator_channel_bindings b ON b.interface_key=p.interface_key
			WHERE d.state='current' AND p.principal_id=d.principal_id AND p.delivery_epoch=d.delivery_epoch
			AND ((p.source_kind='response' AND b.status='current' AND b.principal_id=p.principal_id
				AND b.binding_revision>=p.binding_revision AND b.external_account_reference=p.external_account_reference
				AND b.conversation_reference=p.conversation_reference AND b.conversation_scope=p.conversation_scope
				AND EXISTS (SELECT 1 FROM connected_channel_activations a
					JOIN channel_onboarding_operations o ON o.operation_id=a.operation_id
					WHERE a.activation_id=p.entry_activation_id AND a.interface_key=b.interface_key AND a.status='current'
					AND a.principal_id=b.principal_id AND a.binding_revision=b.binding_revision
					AND a.conversation_reference=b.conversation_reference
					AND o.identity_operation_id=b.operation_id AND o.phase='succeeded'))
			OR (p.source_kind<>'response' AND p.interface_key=d.interface_key
				AND d.binding_revision>=p.binding_revision
				AND p.external_account_reference=d.external_account_reference
				AND p.conversation_reference=d.conversation_reference AND p.conversation_scope=d.conversation_scope))
			AND p.state <> 'retired' AND p.delivery_id > $1::uuid ORDER BY p.delivery_id LIMIT $2`
	}
	cursor := afterDeliveryID
	if cursor == "" {
		cursor = "00000000-0000-0000-0000-000000000000"
	}
	rows, err := tx.QueryContext(ctx, query, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	plans := make([]Plan, 0, limit)
	for rows.Next() {
		var p Plan
		var scope string
		if err := rows.Scan(&p.DeliveryID, &p.SourceKind, &p.SourceID, &p.EntryActivationID, &p.SummaryCount,
			&p.PrincipalID, &p.InterfaceKey, &p.BindingRevision, &p.CurrentBindingRevision, &p.DeliveryEpoch,
			&p.ExternalAccountRef, &p.ConversationRef, &scope, &p.State,
			&p.CurrentRenderID, &p.CurrentReceiptID); err != nil {
			return nil, err
		}
		p.ConversationScope = operatorchannel.ConversationScope(scope)
		if err := p.Validate(); err != nil {
			return nil, err
		}
		plans = append(plans, p)
	}
	return plans, rows.Err()
}
