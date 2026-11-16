package channeldelivery

import (
	"context"
	"database/sql"
	"errors"
)

type ChannelResetSettlement struct {
	PlanID, SourceID, SourceKind, ReceiptState, OperationState string
	SourcePresent, ExactReceipt, UnsafeOperations              bool
}

func (r ChannelResetSettlement) SettledOriginal(originals map[string]string) bool {
	return r.SourceID != "" && originals[r.PlanID] == r.SourceID && r.SourceKind == "card" && !r.SourcePresent &&
		r.ExactReceipt && r.ReceiptState == "sent" && r.OperationState == "settled" && !r.UnsafeOperations
}

func CountUnsettledChannelAfterResetForTest(ctx context.Context, backend any, postgres bool, originals map[string]string) (int, error) {
	identity := func(alias, field string) string {
		if postgres {
			return alias + ".authority_evidence->>'" + field + "'"
		}
		return "json_extract(" + alias + ".authority_evidence,'$." + field + "')"
	}
	var count int
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		query := `SELECT p.delivery_id,p.source_id,p.source_kind,COALESCE(r.state,''),COALESCE(o.state,''),
		EXISTS(SELECT 1 FROM decision_cards c WHERE c.card_id=p.source_id),
		COALESCE(r.delivery_id=p.delivery_id AND o.authority_kind='channel_delivery' AND o.effect_kind='channel_delivery'
		AND ` + identity("o", "delivery_id") + `=CAST(p.delivery_id AS TEXT) AND ` + identity("o", "render_id") + `=CAST(r.render_id AS TEXT),FALSE),
		EXISTS(SELECT 1 FROM runtime_external_effect_operations q WHERE q.authority_kind='channel_delivery'
		AND ` + identity("q", "delivery_id") + `=CAST(p.delivery_id AS TEXT) AND q.state<>'settled')
		FROM channel_delivery_plans p LEFT JOIN channel_delivery_receipts r ON r.effect_operation_id=p.current_receipt_operation_id
		LEFT JOIN runtime_external_effect_operations o ON o.operation_id=r.effect_operation_id
		WHERE (p.state IN ('planned','rendered') AND ` + SendEligibilityPredicate + `) OR ` + AcceptedEffectPredicate(postgres)
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var row ChannelResetSettlement
			if err := rows.Scan(&row.PlanID, &row.SourceID, &row.SourceKind, &row.ReceiptState, &row.OperationState, &row.SourcePresent, &row.ExactReceipt, &row.UnsafeOperations); err != nil {
				return err
			}
			if !row.SettledOriginal(originals) {
				count++
			}
		}
		return errors.Join(rows.Err(), rows.Close())
	})
	return count, err
}
