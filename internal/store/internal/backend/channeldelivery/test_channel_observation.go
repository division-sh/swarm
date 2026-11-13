package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func readChannelObservation(ctx context.Context, backend any, read func(context.Context, *sql.Tx) error) error {
	switch b := backend.(type) {
	case *postgresbackend.Backend:
		return b.RunReadTransaction(ctx, read)
	case *sqlitebackend.Backend:
		return b.RunReadTransaction(ctx, read)
	default:
		return fmt.Errorf("channel observation requires its native backend")
	}
}

func readChannelNativeConsumerEvidenceForTestTx(ctx context.Context, tx *sql.Tx, operation string, currentOnly bool) ([]byte, error) {
	var raw []byte
	query := `SELECT parent_evidence FROM channel_native_setting_consumers WHERE activation_id=$1 AND parent_kind='deployment_registration'`
	if currentOnly {
		query += ` AND state='current'`
	}
	if err := tx.QueryRowContext(ctx, query, operation).Scan(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func readChannelNativeHistoryForTestTx(ctx context.Context, tx *sql.Tx) ([8]string, error) {
	var row [8]string
	err := tx.QueryRowContext(ctx, `SELECT s.setting_id,CAST(s.generation AS TEXT),s.install_operation_id,s.state,
		COALESCE(s.readback_hash,''),CAST(s.desired_commands AS TEXT),c.activation_id,c.state
		FROM channel_native_settings s JOIN channel_native_setting_consumers c ON c.setting_id=s.setting_id
		ORDER BY c.updated_at LIMIT 1`).Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7])
	return row, err
}

func readChannelFrozenCardHistoryForTestTx(ctx context.Context, tx *sql.Tx, card string) (string, error) {
	var render, receipt string
	err := tx.QueryRowContext(ctx, `SELECT CAST(r.render_input AS TEXT),CAST(c.provider_reference AS TEXT)
		FROM channel_delivery_plans p JOIN channel_delivery_receipts c ON c.delivery_id=p.delivery_id
		JOIN channel_delivery_renders r ON r.render_id=c.render_id
		WHERE p.source_id=$1 AND c.state='sent' ORDER BY c.settled_at LIMIT 1`, card).Scan(&render, &receipt)
	return render + "\n" + receipt, err
}

func readChannelHistoricalReceiptForTestTx(ctx context.Context, tx *sql.Tx, card string) (string, error) {
	var receipt string
	err := tx.QueryRowContext(ctx, `SELECT CAST(r.provider_reference AS TEXT) FROM channel_delivery_receipts r
		JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id
		WHERE p.source_id=$1 AND r.state='sent' ORDER BY r.settled_at LIMIT 1`, card).Scan(&receipt)
	return receipt, err
}

func readChannelGenerationZeroSentPlanForTestTx(ctx context.Context, tx *sql.Tx, card string) (string, error) {
	var plan string
	err := tx.QueryRowContext(ctx, `SELECT p.delivery_id FROM channel_delivery_plans p
		JOIN channel_delivery_receipts r ON r.effect_operation_id=p.current_receipt_operation_id
		WHERE p.source_kind='card' AND p.source_id=$1 AND p.resend_generation=0
		AND r.state='sent' AND r.delivery_id=p.delivery_id AND r.render_id=p.current_render_id`, card).Scan(&plan)
	return plan, err
}

func readChannelControlCountsForTestTx(ctx context.Context, tx *sql.Tx, token string) (int, int, int, error) {
	var total, nonmutating, originalRefused int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),
		COALESCE(SUM(CASE WHEN state='settled' AND disposition IN ('stale','rejected') THEN 1 ELSE 0 END),0),
		COALESCE(SUM(CASE WHEN state='settled' AND disposition='rejected' AND CAST(fact AS TEXT) NOT LIKE '%restored-source-%' THEN 1 ELSE 0 END),0)
		FROM operator_channel_action_intents WHERE CAST(fact AS TEXT) LIKE $1`, "%"+token+"%").Scan(&total, &nonmutating, &originalRefused)
	return total, nonmutating, originalRefused, err
}

func countChannelEntryIntentsForTestTx(ctx context.Context, tx *sql.Tx) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='settled' AND disposition='entry'`).Scan(&count)
	return count, err
}

func countChannelRejectedEntriesForTestTx(ctx context.Context, tx *sql.Tx, event string) (int, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_text_intents WHERE provider_event_id=$1 AND state='settled' AND disposition='entry_rejected'`, event).Scan(&count)
	return count, err
}

func readChannelProviderActionForTestTx(ctx context.Context, tx *sql.Tx, event string) (int, string, string, error) {
	var count int
	var state, disposition string
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(MIN(state),''),COALESCE(MIN(disposition),'') FROM operator_channel_action_intents WHERE provider_event_id=$1`, event).Scan(&count, &state, &disposition)
	return count, state, disposition, err
}

func readChannelDeliveryDefaultStateForTestTx(ctx context.Context, tx *sql.Tx) (string, error) {
	var state string
	err := tx.QueryRowContext(ctx, `SELECT state FROM channel_delivery_defaults WHERE singleton_id=1`).Scan(&state)
	return state, err
}

func countUnsettledChannelResponsibilitiesForTestTx(ctx context.Context, tx *sql.Tx, postgres bool) (int, error) {
	var count int
	query := `SELECT COUNT(*) FROM channel_delivery_plans p WHERE
		(p.state IN ('planned','rendered') AND ` + SendEligibilityPredicate + `)
		OR ` + AcceptedEffectPredicate(postgres)
	err := tx.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

func ReadChannelNativeConsumerEvidenceForTest(ctx context.Context, backend any, operation string, currentOnly bool) ([]byte, error) {
	var out []byte
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readChannelNativeConsumerEvidenceForTestTx(ctx, tx, operation, currentOnly)
		return err
	})
	return out, err
}

func ReadChannelNativeHistoryForTest(ctx context.Context, backend any) ([8]string, error) {
	var out [8]string
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readChannelNativeHistoryForTestTx(ctx, tx)
		return err
	})
	return out, err
}

func ReadChannelFrozenCardHistoryForTest(ctx context.Context, backend any, card string) (string, error) {
	out := "\n"
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readChannelFrozenCardHistoryForTestTx(ctx, tx, card)
		return err
	})
	return out, err
}

func ReadChannelHistoricalReceiptForTest(ctx context.Context, backend any, card string) (string, error) {
	var out string
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readChannelHistoricalReceiptForTestTx(ctx, tx, card)
		return err
	})
	return out, err
}

func ReadChannelGenerationZeroSentPlanForTest(ctx context.Context, backend any, card string) (string, error) {
	var out string
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readChannelGenerationZeroSentPlanForTestTx(ctx, tx, card)
		return err
	})
	return out, err
}

func ReadChannelControlCountsForTest(ctx context.Context, backend any, token string) (int, int, int, error) {
	var total, nonmutating, originalRefused int
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		total, nonmutating, originalRefused, err = readChannelControlCountsForTestTx(ctx, tx, token)
		return err
	})
	return total, nonmutating, originalRefused, err
}

func CountChannelEntryIntentsForTest(ctx context.Context, backend any) (int, error) {
	var out int
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = countChannelEntryIntentsForTestTx(ctx, tx)
		return err
	})
	return out, err
}

func CountChannelRejectedEntriesForTest(ctx context.Context, backend any, event string) (int, error) {
	var out int
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = countChannelRejectedEntriesForTestTx(ctx, tx, event)
		return err
	})
	return out, err
}

func ReadChannelProviderActionForTest(ctx context.Context, backend any, event string) (int, string, string, error) {
	var count int
	var state, disposition string
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, state, disposition, err = readChannelProviderActionForTestTx(ctx, tx, event)
		return err
	})
	return count, state, disposition, err
}

func ReadChannelDeliveryDefaultStateForTest(ctx context.Context, backend any) (string, error) {
	var out string
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = readChannelDeliveryDefaultStateForTestTx(ctx, tx)
		return err
	})
	return out, err
}

func CountUnsettledChannelResponsibilitiesForTest(ctx context.Context, backend any, postgres bool) (int, error) {
	var out int
	err := readChannelObservation(ctx, backend, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = countUnsettledChannelResponsibilitiesForTestTx(ctx, tx, postgres)
		return err
	})
	return out, err
}
