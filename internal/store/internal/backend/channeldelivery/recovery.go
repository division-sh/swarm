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

func ListActionableUncertainTx(ctx context.Context, tx *sql.Tx, selected Default, postgres bool) ([]render.RecoveryChoice, error) {
	if tx == nil || selected.State != StateCurrent {
		return nil, fmt.Errorf("uncertain channel readback requires current default")
	}
	query := `SELECT p.delivery_id, p.source_kind, p.source_id FROM channel_delivery_plans p
		WHERE p.state='uncertain' AND p.source_kind IN ('notice','card')
		AND p.principal_id=? AND p.interface_key=? AND p.delivery_epoch=?
		AND p.external_account_reference=? AND p.conversation_reference=? AND p.conversation_scope=?
		AND NOT EXISTS (SELECT 1 FROM channel_delivery_plans child WHERE child.resend_of_delivery_id=p.delivery_id)
		AND ((p.source_kind='notice' AND EXISTS (SELECT 1 FROM mailbox n WHERE n.item_id=p.source_id AND n.status='pending'))
		 OR (p.source_kind='card' AND EXISTS (SELECT 1 FROM decision_cards c WHERE c.card_id=p.source_id AND c.status='pending')))
		ORDER BY p.created_at, p.delivery_id`
	if postgres {
		query = `SELECT p.delivery_id::text, p.source_kind, p.source_id::text FROM channel_delivery_plans p
			WHERE p.state='uncertain' AND p.source_kind IN ('notice','card')
			AND p.principal_id=$1::uuid AND p.interface_key=$2 AND p.delivery_epoch=$3
			AND p.external_account_reference=$4 AND p.conversation_reference=$5 AND p.conversation_scope=$6
			AND NOT EXISTS (SELECT 1 FROM channel_delivery_plans child WHERE child.resend_of_delivery_id=p.delivery_id)
			AND ((p.source_kind='notice' AND EXISTS (SELECT 1 FROM mailbox n WHERE n.item_id=p.source_id AND n.status='pending'))
			 OR (p.source_kind='card' AND EXISTS (SELECT 1 FROM decision_cards c WHERE c.card_id=p.source_id AND c.status='pending')))
			ORDER BY p.created_at, p.delivery_id`
	}
	rows, err := tx.QueryContext(ctx, query, selected.PrincipalID, selected.InterfaceKey, selected.DeliveryEpoch,
		selected.ExternalAccountRef, selected.ConversationRef, string(selected.ConversationScope))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	choices := make([]render.RecoveryChoice, 0)
	for rows.Next() {
		var deliveryID, kind, sourceID string
		if err := rows.Scan(&deliveryID, &kind, &sourceID); err != nil {
			return nil, err
		}
		if uuid.Validate(deliveryID) != nil || uuid.Validate(sourceID) != nil {
			return nil, fmt.Errorf("uncertain channel source identity is invalid")
		}
		choices = append(choices, render.RecoveryChoice{DeliveryID: deliveryID,
			Label: "Resend " + kind + " [" + sourceID[:8] + "]"})
	}
	return choices, rows.Err()
}

func PlanManualResendTx(ctx context.Context, tx *sql.Tx, action operatorchannel.InboundAction,
	expected render.ResolvedAction, postgres bool) (string, error) {
	if tx == nil || expected.Action.Kind != "resend" || uuid.Validate(expected.Action.RecoveryDeliveryID) != nil {
		return "", fmt.Errorf("manual resend requires exact uncertain delivery action")
	}
	if err := LockPrincipalTx(ctx, tx, expected.PrincipalID, postgres); err != nil {
		return "", err
	}
	state, err := RequireActionIntentTx(ctx, tx, action, postgres, true)
	if err != nil {
		return "", err
	}
	if state != "pending" {
		return "", fmt.Errorf("manual resend action was already consumed")
	}
	resolved, found, err := ResolveActionFactForMutationTx(ctx, tx, action.ActionFact, postgres)
	if err != nil {
		return "", err
	}
	if !found || !resolved.CurrentRender || resolved != expected || resolved.SourceKind != PlanResponse {
		return "", fmt.Errorf("manual resend action is not current")
	}
	selected, found, err := LoadDefault(ctx, tx, postgres)
	if err != nil {
		return "", err
	}
	if !found || selected.State != StateCurrent || selected.PrincipalID != resolved.PrincipalID {
		return "", fmt.Errorf("manual resend destination is no longer current")
	}
	query := `SELECT source_kind, source_id, resend_generation FROM channel_delivery_plans p
		WHERE delivery_id=? AND state='uncertain' AND source_kind IN ('notice','card')
		AND principal_id=? AND interface_key=? AND delivery_epoch=?
		AND external_account_reference=? AND conversation_reference=? AND conversation_scope=?
		AND NOT EXISTS (SELECT 1 FROM channel_delivery_plans child WHERE child.resend_of_delivery_id=p.delivery_id)
		AND ((source_kind='notice' AND EXISTS (SELECT 1 FROM mailbox n WHERE n.item_id=p.source_id AND n.status='pending'))
		 OR (source_kind='card' AND EXISTS (SELECT 1 FROM decision_cards c WHERE c.card_id=p.source_id AND c.status='pending')))`
	if postgres {
		query = `SELECT source_kind, source_id::text, resend_generation FROM channel_delivery_plans p
			WHERE delivery_id=$1::uuid AND state='uncertain' AND source_kind IN ('notice','card')
			AND principal_id=$2::uuid AND interface_key=$3 AND delivery_epoch=$4
			AND external_account_reference=$5 AND conversation_reference=$6 AND conversation_scope=$7
			AND NOT EXISTS (SELECT 1 FROM channel_delivery_plans child WHERE child.resend_of_delivery_id=p.delivery_id)
			AND ((source_kind='notice' AND EXISTS (SELECT 1 FROM mailbox n WHERE n.item_id=p.source_id AND n.status='pending'))
			 OR (source_kind='card' AND EXISTS (SELECT 1 FROM decision_cards c WHERE c.card_id=p.source_id AND c.status='pending')))
			FOR UPDATE OF p`
	}
	var kind, sourceID string
	var generation int64
	if err := tx.QueryRowContext(ctx, query, resolved.Action.RecoveryDeliveryID, selected.PrincipalID,
		selected.InterfaceKey, selected.DeliveryEpoch, selected.ExternalAccountRef, selected.ConversationRef,
		string(selected.ConversationScope)).Scan(&kind, &sourceID, &generation); err != nil {
		return "", fmt.Errorf("manual resend predecessor is no longer actionable: %w", err)
	}
	newID := uuid.NewString()
	query = `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, principal_id,
		interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
		conversation_scope, state, resend_generation, resend_of_delivery_id, resend_action_publication_id, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?, ?, ?, ?)`
	if postgres {
		query = `INSERT INTO channel_delivery_plans (delivery_id, source_kind, source_id, principal_id,
			interface_key, binding_revision, delivery_epoch, external_account_reference, conversation_reference,
			conversation_scope, state, resend_generation, resend_of_delivery_id, resend_action_publication_id, created_at)
			VALUES ($1::uuid, $2, $3::uuid, $4::uuid, $5, $6, $7, $8, $9, $10, 'planned', $11, $12::uuid, $13::uuid, $14)`
	}
	if _, err := tx.ExecContext(ctx, query, newID, kind, sourceID, selected.PrincipalID, selected.InterfaceKey,
		selected.BindingRevision, selected.DeliveryEpoch, selected.ExternalAccountRef, selected.ConversationRef,
		string(selected.ConversationScope), generation+1, resolved.Action.RecoveryDeliveryID,
		action.PublicationID, time.Now().UTC()); err != nil {
		return "", fmt.Errorf("plan linked manual resend: %w", err)
	}
	if err := SettleAppliedActionIntentTx(ctx, tx, action, render.ActionApplied, postgres); err != nil {
		return "", err
	}
	return newID, nil
}
