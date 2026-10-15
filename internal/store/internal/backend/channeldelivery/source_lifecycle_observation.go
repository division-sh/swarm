package channeldelivery

import (
	"context"
	"database/sql"
)

type SourceSuccessorPlanStorage struct{ DeliveryID, State, RenderID, ReceiptID string }

func ReadSourceSuccessorPlanStorageTx(ctx context.Context, tx *sql.Tx, card string) (SourceSuccessorPlanStorage, error) {
	var result SourceSuccessorPlanStorage
	err := tx.QueryRowContext(ctx, `SELECT delivery_id,state,COALESCE(CAST(current_render_id AS TEXT),''),COALESCE(CAST(current_receipt_operation_id AS TEXT),'') FROM channel_delivery_plans WHERE source_kind='card' AND source_id=$1 AND resend_generation=0`, card).
		Scan(&result.DeliveryID, &result.State, &result.RenderID, &result.ReceiptID)
	return result, err
}

func ReadSourceNativeConsumerStateStorageTx(ctx context.Context, tx *sql.Tx, activation string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM channel_native_setting_consumers WHERE activation_id=$1`, activation).Scan(&state)
	return state, err
}
