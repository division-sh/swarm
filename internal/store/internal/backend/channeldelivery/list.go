package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/google/uuid"
)

// ListCurrentPlans reads one bounded page of selected responsibilities. A
// historical or retired destination remains durable but is not executable.
func ListCurrentPlans(ctx context.Context, tx *sql.Tx, afterDeliveryID string, limit int, postgres bool) ([]Plan, error) {
	if tx == nil || limit < 1 || limit > 500 || (afterDeliveryID != "" && uuid.Validate(afterDeliveryID) != nil) {
		return nil, fmt.Errorf("channel delivery scan requires a transaction, valid cursor and bounded limit")
	}
	query := `SELECT p.delivery_id, p.source_kind, p.source_id, COALESCE(p.summary_count, 0), p.principal_id,
		p.interface_key, p.binding_revision, p.delivery_epoch, p.external_account_reference,
		p.conversation_reference, p.conversation_scope, p.state,
		COALESCE(p.current_render_id, ''), COALESCE(p.current_receipt_operation_id, '')
		FROM channel_delivery_plans p JOIN channel_delivery_defaults d ON d.singleton_id=1
		WHERE d.state='current' AND p.principal_id=d.principal_id AND p.interface_key=d.interface_key
		AND p.delivery_epoch=d.delivery_epoch AND p.external_account_reference=d.external_account_reference
		AND p.conversation_reference=d.conversation_reference AND p.conversation_scope=d.conversation_scope
		AND p.state <> 'retired' AND p.delivery_id > ? ORDER BY p.delivery_id LIMIT ?`
	if postgres {
		query = `SELECT p.delivery_id::text, p.source_kind, p.source_id::text, COALESCE(p.summary_count, 0), p.principal_id::text,
			p.interface_key, p.binding_revision, p.delivery_epoch, p.external_account_reference,
			p.conversation_reference, p.conversation_scope, p.state,
			COALESCE(p.current_render_id::text, ''), COALESCE(p.current_receipt_operation_id::text, '')
			FROM channel_delivery_plans p JOIN channel_delivery_defaults d ON d.singleton_id=1
			WHERE d.state='current' AND p.principal_id=d.principal_id AND p.interface_key=d.interface_key
			AND p.delivery_epoch=d.delivery_epoch AND p.external_account_reference=d.external_account_reference
			AND p.conversation_reference=d.conversation_reference AND p.conversation_scope=d.conversation_scope
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
		if err := rows.Scan(&p.DeliveryID, &p.SourceKind, &p.SourceID, &p.SummaryCount,
			&p.PrincipalID, &p.InterfaceKey, &p.BindingRevision, &p.DeliveryEpoch,
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
