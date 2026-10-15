package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"

	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
)

// This is physical receipt evidence, not destination/edit admission authority.
func ReadLatestCardSentReceiptStorageTx(ctx context.Context, tx *sql.Tx, card string) (string, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT CAST(r.provider_reference AS TEXT) FROM channel_delivery_receipts r
		JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id AND p.current_receipt_operation_id=r.effect_operation_id
		WHERE p.source_kind='card' AND p.source_id=$1 AND p.state='sent' AND r.state='sent'
		ORDER BY r.settled_at DESC LIMIT 1`, card).Scan(&raw)
	return raw, err
}

type IntentResponseStorage struct{ DeliveryID, Reference string }

func ReadIntentSentResponseStorageTx(ctx context.Context, tx *sql.Tx, kind render.IntentKind, eventID string) (IntentResponseStorage, error) {
	var table string
	switch kind {
	case render.IntentText:
		table = "operator_channel_text_intents"
	case render.IntentAction:
		table = "operator_channel_action_intents"
	default:
		return IntentResponseStorage{}, fmt.Errorf("unsupported response intent kind %q", kind)
	}
	var result IntentResponseStorage
	err := tx.QueryRowContext(ctx, `SELECT p.delivery_id, CAST(r.provider_reference AS TEXT)
		FROM channel_delivery_plans p JOIN `+table+` i ON i.publication_id=p.source_id
		JOIN channel_delivery_receipts r ON r.effect_operation_id=p.current_receipt_operation_id AND r.delivery_id=p.delivery_id
		WHERE p.source_kind='response' AND i.provider_event_id=$1 AND p.state='sent' AND r.state='sent'`, eventID).Scan(&result.DeliveryID, &result.Reference)
	return result, err
}

type RenderStorage struct{ Input, Hash string }

func ReadAcknowledgedCardRenderStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (RenderStorage, error) {
	var result RenderStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(r.render_input AS TEXT),r.render_hash
		FROM channel_delivery_plans p JOIN channel_delivery_receipts receipt
		ON receipt.effect_operation_id=p.current_receipt_operation_id AND receipt.render_id=p.current_render_id
		JOIN channel_delivery_renders r ON r.render_id=p.current_render_id
		WHERE p.source_kind='card' AND p.source_id=$1 AND p.state='sent' AND receipt.state='sent'`, cardID).Scan(&result.Input, &result.Hash)
	return result, err
}

func CountTextIntentDispositionStorageTx(ctx context.Context, tx *sql.Tx, disposition string) (int, error) {
	var count int
	if disposition == "" {
		err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='pending'`).Scan(&count)
		return count, err
	}
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='settled' AND disposition=$1`, disposition).Scan(&count)
	return count, err
}

func CountUnsettledResponsibilitiesStorageTx(ctx context.Context, tx *sql.Tx, postgres bool) (int, error) {
	query := `SELECT COUNT(*) FROM channel_delivery_plans p WHERE
		(p.state IN ('planned','rendered') AND ` + SendEligibilityPredicate + `)
		OR ` + AcceptedEffectPredicate(postgres)
	var count int
	err := tx.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

func ReadDeliveryStateStorageTx(ctx context.Context, tx *sql.Tx, deliveryID string) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM channel_delivery_plans WHERE delivery_id=$1`, deliveryID).Scan(&state)
	return state, err
}

func CountSentFreshCardCopiesStorageTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_delivery_plans WHERE source_kind='card' AND resend_generation>0 AND state='sent'`).Scan(&count)
	return count, err
}

type CardCopyCountsStorage struct{ Total, Uncertain int }

func ReadCardCopyCountsStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (CardCopyCountsStorage, error) {
	var result CardCopyCountsStorage
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),SUM(CASE WHEN state='uncertain' THEN 1 ELSE 0 END)
		FROM channel_delivery_plans WHERE source_kind='card' AND source_id=$1`, cardID).Scan(&result.Total, &result.Uncertain)
	return result, err
}

func ReadRenderStorageTx(ctx context.Context, tx *sql.Tx, renderID string) (RenderStorage, error) {
	var result RenderStorage
	err := tx.QueryRowContext(ctx, `SELECT CAST(render_input AS TEXT),render_hash FROM channel_delivery_renders WHERE render_id=$1`, renderID).Scan(&result.Input, &result.Hash)
	return result, err
}

type CardRenderStorage struct{ RenderID, Input, Hash, Snapshot string }

func ReadCardRenderStorageTx(ctx context.Context, tx *sql.Tx, cardID string) (CardRenderStorage, error) {
	var result CardRenderStorage
	err := tx.QueryRowContext(ctx, `SELECT p.current_render_id,CAST(r.render_input AS TEXT),r.render_hash,CAST(c.snapshot AS TEXT)
		FROM decision_cards c JOIN channel_delivery_plans p ON p.source_id=c.card_id
		JOIN channel_delivery_renders r ON r.render_id=p.current_render_id WHERE c.card_id=$1`, cardID).Scan(&result.RenderID, &result.Input, &result.Hash, &result.Snapshot)
	return result, err
}

func CountUncertainSourcePlansStorageTx(ctx context.Context, tx *sql.Tx, sourceID string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_delivery_plans WHERE source_id=$1 AND state='uncertain'`, sourceID).Scan(&count)
	return count, err
}
