package channeldelivery

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/google/uuid"
)

// ReadCurrentSentReceiptTx returns the exact provider message reference for
// editing. A historical receipt cannot become the edit predecessor.
func ReadCurrentSentReceiptTx(ctx context.Context, tx *sql.Tx, deliveryID, operationID string, postgres bool) (render.SentReceipt, bool, error) {
	if tx == nil || uuid.Validate(deliveryID) != nil || uuid.Validate(operationID) != nil {
		return render.SentReceipt{}, false, fmt.Errorf("channel receipt read requires exact ids and selected transaction")
	}
	plan, found, err := LoadDestinationCurrentPlan(ctx, tx, deliveryID, postgres)
	if err != nil || !found || plan.CurrentReceiptID != operationID ||
		(plan.State != "sent" && plan.State != "rendered") {
		return render.SentReceipt{}, false, err
	}
	query := `SELECT r.render_id, r.provider_reference FROM channel_delivery_receipts r
		JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id
		WHERE r.effect_operation_id=? AND r.delivery_id=? AND r.state='sent'
		AND p.current_receipt_operation_id=r.effect_operation_id`
	if postgres {
		query = `SELECT r.render_id::text, r.provider_reference FROM channel_delivery_receipts r
			JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id
			WHERE r.effect_operation_id=$1::uuid AND r.delivery_id=$2::uuid AND r.state='sent'
			AND p.current_receipt_operation_id=r.effect_operation_id`
	}
	var result render.SentReceipt
	var raw []byte
	err = tx.QueryRowContext(ctx, query, operationID, deliveryID).Scan(&result.RenderID, &raw)
	if err == sql.ErrNoRows {
		return render.SentReceipt{}, false, nil
	}
	if err != nil {
		return render.SentReceipt{}, false, err
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return render.SentReceipt{}, false, err
	}
	reference, present := object["delivery_reference"]
	if _, valid, err := operatorchannel.OpaqueReference(reference); err != nil || !present || !valid {
		return render.SentReceipt{}, false, fmt.Errorf("current channel receipt lacks a valid delivery reference: %w", err)
	}
	result.OperationID, result.DeliveryID, result.DeliveryReference = operationID, deliveryID, reference
	return result, true, nil
}
