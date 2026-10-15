package channeldelivery

import (
	"context"
	"database/sql"
)

type NativeProcessSettingStorage struct {
	State    string
	Readback sql.NullString
}

func ReadNativeProcessSettingStorageTx(ctx context.Context, tx *sql.Tx, setting string) (NativeProcessSettingStorage, error) {
	var result NativeProcessSettingStorage
	err := tx.QueryRowContext(ctx, `SELECT state,readback_hash FROM channel_native_settings WHERE setting_id=$1`, setting).Scan(&result.State, &result.Readback)
	return result, err
}

type NativeProcessAttemptStorage struct{ OperationID, SettingID, State string }

func ReadNativeProcessAttemptStorageTx(ctx context.Context, tx *sql.Tx) (NativeProcessAttemptStorage, error) {
	var result NativeProcessAttemptStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(o.operation_id AS TEXT),CAST(s.setting_id AS TEXT),a.state
			FROM channel_native_settings s JOIN runtime_external_effect_operations o ON o.operation_id=s.install_operation_id
			JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id WHERE o.authority_kind='channel_native_setting'`).Scan(&result.OperationID, &result.SettingID, &result.State)
	return result, err
}

type DeliveryProcessAttemptStorage struct{ OperationID, RenderID, State string }

func ReadDeliveryProcessAttemptStorageTx(ctx context.Context, tx *sql.Tx, card string, postgres bool) (DeliveryProcessAttemptStorage, error) {
	evidence := "json_extract(o.authority_evidence,'$.delivery_id')=p.delivery_id"
	if postgres {
		evidence = "o.authority_evidence->>'delivery_id'=CAST(p.delivery_id AS TEXT)"
	}
	query := `SELECT CAST(o.operation_id AS TEXT),p.current_render_id,a.state
		FROM channel_delivery_plans p JOIN runtime_external_effect_operations o ON ` + evidence + `
		JOIN runtime_external_effect_attempts a ON a.operation_id=o.operation_id
		WHERE p.source_id=$1 AND o.authority_kind='channel_delivery'`
	var result DeliveryProcessAttemptStorage
	err := tx.QueryRowContext(ctx, query, card).Scan(&result.OperationID, &result.RenderID, &result.State)
	return result, err
}

type ProcessActionCardStorage struct{ RunID, Anchor string }

func ReadProcessActionCardStorageTx(ctx context.Context, tx *sql.Tx, token string) (ProcessActionCardStorage, error) {
	var result ProcessActionCardStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(card.run_id AS TEXT),CAST(card.anchor AS TEXT)
		FROM channel_delivery_actions action
		JOIN channel_delivery_renders render ON render.render_id=action.render_id
		JOIN channel_delivery_plans plan ON plan.delivery_id=render.delivery_id
		JOIN decision_cards card ON card.card_id=plan.source_id
		WHERE action.action_token=$1`, token).Scan(&result.RunID, &result.Anchor)
	return result, err
}

type ProcessRecoveredReceiptStorage struct{ OperationID, PlanState, ReceiptState, CurrentOperationID string }

func ReadProcessRecoveredReceiptStorageTx(ctx context.Context, tx *sql.Tx, card, operation string) (ProcessRecoveredReceiptStorage, error) {
	var result ProcessRecoveredReceiptStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(r.effect_operation_id AS TEXT),p.state,r.state,COALESCE(CAST(p.current_receipt_operation_id AS TEXT),'')
		FROM channel_delivery_plans p JOIN channel_delivery_receipts r ON r.delivery_id=p.delivery_id AND r.effect_operation_id=$2
		WHERE p.source_id=$1`, card, operation).Scan(&result.OperationID, &result.PlanState, &result.ReceiptState, &result.CurrentOperationID)
	return result, err
}

type ProcessRetainedReceiptStorage struct {
	PlanState, CurrentOperationID string
	Receipts                      int
}

func ReadProcessRetainedReceiptStorageTx(ctx context.Context, tx *sql.Tx, card, operation string) (ProcessRetainedReceiptStorage, error) {
	var result ProcessRetainedReceiptStorage
	err := tx.QueryRowContext(ctx, `SELECT p.state,COALESCE(CAST(p.current_receipt_operation_id AS TEXT),''),
		(SELECT COUNT(*) FROM channel_delivery_receipts WHERE effect_operation_id=$2)
		FROM channel_delivery_plans p WHERE p.source_id=$1`, card, operation).Scan(&result.PlanState, &result.CurrentOperationID, &result.Receipts)
	return result, err
}

func ReadNativeHistoryStorageTx(ctx context.Context, tx *sql.Tx) ([8]string, error) {
	var row [8]string
	err := tx.QueryRowContext(ctx, `SELECT s.setting_id,CAST(s.generation AS TEXT),s.install_operation_id,s.state,
		COALESCE(s.readback_hash,''),CAST(s.desired_commands AS TEXT),c.activation_id,c.state
		FROM channel_native_settings s JOIN channel_native_setting_consumers c ON c.setting_id=s.setting_id
		ORDER BY c.updated_at LIMIT 1`).Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7])
	return row, err
}

type SourceHistoryStorage struct{ Render, Receipt string }

func ReadSourceHistoryStorageTx(ctx context.Context, tx *sql.Tx, card string) (SourceHistoryStorage, error) {
	var result SourceHistoryStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(r.render_input AS TEXT),CAST(c.provider_reference AS TEXT)
		FROM channel_delivery_plans p JOIN channel_delivery_receipts c ON c.delivery_id=p.delivery_id
		JOIN channel_delivery_renders r ON r.render_id=c.render_id
		WHERE p.source_id=$1 AND c.state='sent' ORDER BY c.settled_at LIMIT 1`, card).Scan(&result.Render, &result.Receipt)
	return result, err
}

type PlanCardStateStorage struct{ State, Status string }

func ReadPlanCardStateStorageTx(ctx context.Context, tx *sql.Tx, card string) (PlanCardStateStorage, error) {
	var result PlanCardStateStorage
	err := tx.QueryRowContext(ctx, `SELECT p.state,c.status FROM channel_delivery_plans p
		JOIN decision_cards c ON c.card_id=p.source_id WHERE p.source_id=$1`, card).Scan(&result.State, &result.Status)
	return result, err
}

type ActionChoiceStorage struct {
	Input    []byte
	Hash     string
	Position int
}

func ReadActionChoiceStorageTx(ctx context.Context, tx *sql.Tx, token string) (ActionChoiceStorage, error) {
	var result ActionChoiceStorage
	err := tx.QueryRowContext(ctx, `SELECT r.render_input,r.render_hash,a.action_position
		FROM channel_delivery_actions a JOIN channel_delivery_renders r ON r.render_id=a.render_id
		WHERE a.action_token=$1`, token).Scan(&result.Input, &result.Hash, &result.Position)
	return result, err
}

type UncertainNoticePlanCountsStorage struct{ Total, Uncertain, Sent int }

func ReadUncertainNoticePlanCountsStorageTx(ctx context.Context, tx *sql.Tx, id string) (UncertainNoticePlanCountsStorage, error) {
	var result UncertainNoticePlanCountsStorage
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),
		SUM(CASE WHEN state='uncertain' THEN 1 ELSE 0 END),
		SUM(CASE WHEN state='sent' THEN 1 ELSE 0 END)
		FROM channel_delivery_plans WHERE source_kind='notice' AND source_id=$1`, id).Scan(&result.Total, &result.Uncertain, &result.Sent)
	return result, err
}

func ReadFirstReceiptedDeliveryStorageTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var result string
	err := tx.QueryRowContext(ctx, `SELECT CAST(delivery_id AS TEXT) FROM channel_delivery_plans
		WHERE current_receipt_operation_id IS NOT NULL ORDER BY created_at LIMIT 1`).Scan(&result)
	return result, err
}

func CountRejectedIntentPlansStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var result int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_delivery_plans p JOIN operator_channel_text_intents i
		ON p.source_id=i.publication_id WHERE i.disposition='entry_rejected'`).Scan(&result)
	return result, err
}
