package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
)

type ChannelControlCounts struct{ Total, Nonmutating, OriginalRefused int }

func inspectChannelObservation(ctx context.Context, selected any, read func(context.Context, *sql.Tx) error) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	inspect := func(ctx context.Context) error { return readServedDeliveryObservation(ctx, selected, read) }
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.InspectSnapshot(ctx, inspect)
	case *SQLiteRuntimeStore:
		return owner.InspectSnapshot(ctx, inspect)
	}
	return fmt.Errorf("channel observation requires its original native owner")
}

func ReadChannelNativeConsumerForTest(ctx context.Context, selected any, operation string, currentOnly bool) (channelnative.TransportParent, error) {
	var raw []byte
	query := `SELECT parent_evidence FROM channel_native_setting_consumers WHERE activation_id=$1 AND parent_kind='deployment_registration'`
	if currentOnly {
		query += ` AND state='current'`
	}
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, query, operation).Scan(&raw)
	})
	if err != nil {
		return channelnative.TransportParent{}, err
	}
	return channelnative.DecodeTransportParent(raw)
}

func ReadChannelNativeHistoryForTest(ctx context.Context, selected any) ([8]string, error) {
	var row [8]string
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT s.setting_id,CAST(s.generation AS TEXT),s.install_operation_id,s.state,
			COALESCE(s.readback_hash,''),CAST(s.desired_commands AS TEXT),c.activation_id,c.state
			FROM channel_native_settings s JOIN channel_native_setting_consumers c ON c.setting_id=s.setting_id
			ORDER BY c.updated_at LIMIT 1`).Scan(&row[0], &row[1], &row[2], &row[3], &row[4], &row[5], &row[6], &row[7])
	})
	return row, err
}

func ReadChannelFrozenCardHistoryForTest(ctx context.Context, selected any, card string) (string, error) {
	var render, receipt string
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT CAST(r.render_input AS TEXT),CAST(c.provider_reference AS TEXT)
			FROM channel_delivery_plans p JOIN channel_delivery_receipts c ON c.delivery_id=p.delivery_id
			JOIN channel_delivery_renders r ON r.render_id=c.render_id
			WHERE p.source_id=$1 AND c.state='sent' ORDER BY c.settled_at LIMIT 1`, card).Scan(&render, &receipt)
	})
	return render + "\n" + receipt, err
}

func ReadChannelHistoricalReceiptForTest(ctx context.Context, selected any, card string) (string, error) {
	var receipt string
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT CAST(r.provider_reference AS TEXT) FROM channel_delivery_receipts r
			JOIN channel_delivery_plans p ON p.delivery_id=r.delivery_id
			WHERE p.source_id=$1 AND r.state='sent' ORDER BY r.settled_at LIMIT 1`, card).Scan(&receipt)
	})
	return receipt, err
}

func ReadChannelGenerationZeroSentPlanForTest(ctx context.Context, selected any, card string) (string, error) {
	var plan string
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT p.delivery_id FROM channel_delivery_plans p
			JOIN channel_delivery_receipts r ON r.effect_operation_id=p.current_receipt_operation_id
			WHERE p.source_kind='card' AND p.source_id=$1 AND p.resend_generation=0
			AND r.state='sent' AND r.delivery_id=p.delivery_id AND r.render_id=p.current_render_id`, card).Scan(&plan)
	})
	return plan, err
}

func ReadChannelControlCountsForTest(ctx context.Context, selected any, token string) (ChannelControlCounts, error) {
	var result ChannelControlCounts
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*),
			COALESCE(SUM(CASE WHEN state='settled' AND disposition IN ('stale','rejected') THEN 1 ELSE 0 END),0),
			COALESCE(SUM(CASE WHEN state='settled' AND disposition='rejected' AND CAST(fact AS TEXT) NOT LIKE '%restored-source-%' THEN 1 ELSE 0 END),0)
			FROM operator_channel_action_intents WHERE CAST(fact AS TEXT) LIKE $1`, "%"+token+"%").Scan(&result.Total, &result.Nonmutating, &result.OriginalRefused)
	})
	return result, err
}

func CountChannelSourceCardsForTest(ctx context.Context, selected any, run, source string) (int, error) {
	var count int
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM decision_cards WHERE run_id=$1 AND bundle_hash=$2`, run, source).Scan(&count)
	})
	return count, err
}

func CountChannelEntryIntentsForTest(ctx context.Context, selected any) (int, error) {
	var count int
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM operator_channel_text_intents WHERE state='settled' AND disposition='entry'`).Scan(&count)
	})
	return count, err
}

func CountChannelObservedEventsForTest(ctx context.Context, selected any, run, event string) (int, error) {
	if err := validateSelectedForkStorageIdentity(run); err != nil {
		return 0, err
	}
	if event == "" {
		return 0, fmt.Errorf("lifecycle event cardinality requires an exact event name")
	}
	var count int
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2`, run, event).Scan(&count)
	})
	return count, err
}

func ReadChannelRegistrationIndexForTest(ctx context.Context, selected any) (string, error) {
	query := `SELECT sql FROM sqlite_master WHERE name='idx_deployment_channel_account'`
	if _, postgres := selected.(*PostgresStore); postgres {
		query = `SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema() AND indexname='idx_deployment_channel_account'`
	}
	var definition string
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, query).Scan(&definition)
	})
	return definition, err
}

func ReadChannelRegistrationCountsForTest(ctx context.Context, selected any) (int, int, int, error) {
	var total, current, retired int
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*),SUM(CASE WHEN account_confirmed AND phase<>'retired' THEN 1 ELSE 0 END),
			SUM(CASE WHEN phase='retired' AND logout_completed THEN 1 ELSE 0 END)
			FROM deployment_channel_operations WHERE provider='telegram'`).Scan(&total, &current, &retired)
	})
	return total, current, retired, err
}

func ReadChannelDeliveryDefaultStateForTest(ctx context.Context, selected any) (string, error) {
	var state string
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT state FROM channel_delivery_defaults WHERE singleton_id=1`).Scan(&state)
	})
	return state, err
}

func CountUnsettledChannelResponsibilitiesForTest(ctx context.Context, selected any) (int, error) {
	_, postgres := selected.(*PostgresStore)
	var count int
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		query := `SELECT COUNT(*) FROM channel_delivery_plans p WHERE
			(p.state IN ('planned','rendered') AND ` + channeldelivery.SendEligibilityPredicate + `)
			OR ` + channeldelivery.AcceptedEffectPredicate(postgres)
		return tx.QueryRowContext(ctx, query).Scan(&count)
	})
	return count, err
}

func ReadChannelPreservedHistoryForTest(ctx context.Context, selected any, successors []string) (string, error) {
	queries := []string{
		`SELECT * FROM operator_principals ORDER BY principal_id`,
		`SELECT * FROM operator_channel_operations ORDER BY operation_id`,
		`SELECT * FROM operator_channel_bindings ORDER BY interface_key`,
		`SELECT principal_id,interface_key,binding_revision,delivery_epoch,external_account,conversation_reference,conversation_scope,state,first_operation_id FROM channel_delivery_defaults`,
		`SELECT setting_id,provider,resource_slot_id,conversation_reference,scope_kind,member_reference,language_code,pack_id,pack_version,pack_manifest_hash,entry_contract_hash,entry_command,desired_commands,principal_id,generation,install_operation_id,readback_hash,created_at FROM channel_native_settings ORDER BY setting_id`,
		`SELECT setting_id,activation_id,activation_revision,interface_key,binding_revision,context_publication_generation FROM channel_native_setting_consumers ORDER BY activation_id`,
		`SELECT CAST(delivery_id AS TEXT),source_kind,CAST(source_id AS TEXT),request_activation_id,summary_count,principal_id,interface_key,binding_revision,delivery_epoch,external_account,conversation_reference,conversation_scope,action_capacity,text_capacity,label_capacity,resend_generation,resend_of_delivery_id,resend_action_publication_id,created_at FROM channel_delivery_plans ORDER BY delivery_id`,
		`SELECT * FROM channel_delivery_renders ORDER BY render_id`,
		`SELECT * FROM channel_delivery_receipts ORDER BY effect_operation_id`,
		`SELECT * FROM channel_delivery_actions ORDER BY action_token`,
		`SELECT * FROM operator_channel_claim_receipts ORDER BY publication_id`,
		`SELECT * FROM operator_channel_action_intents ORDER BY publication_id`,
		`SELECT * FROM operator_channel_text_intents ORDER BY publication_id`,
		`SELECT * FROM runtime_external_effect_operations WHERE effect_kind IN ('channel_delivery','channel_native_setting','channel_action_ack') ORDER BY operation_id`,
		`SELECT a.* FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.effect_kind IN ('channel_delivery','channel_native_setting','channel_action_ack') ORDER BY a.attempt_id`,
	}
	var snapshots [][][]any
	for index, query := range queries {
		var args []any
		if len(successors) != 0 && successors[0] != "" {
			predicate := map[int]string{7: `delivery_id<>$1`, 8: `delivery_id<>$1`,
				9:  `render_id NOT IN (SELECT render_id FROM channel_delivery_renders WHERE delivery_id=$1)`,
				13: `operation_id NOT IN (SELECT effect_operation_id FROM channel_delivery_receipts WHERE delivery_id=$1)`,
				14: `a.operation_id NOT IN (SELECT effect_operation_id FROM channel_delivery_receipts WHERE delivery_id=$1)`,
			}[index]
			if predicate != "" {
				var predicates []string
				for i, id := range successors {
					predicates = append(predicates, strings.ReplaceAll(predicate, "$1", fmt.Sprintf("$%d", i+1)))
					args = append(args, id)
				}
				join := " WHERE "
				if index >= 13 {
					join = " AND "
				}
				query = strings.Replace(query, " ORDER BY", join+strings.Join(predicates, " AND ")+" ORDER BY", 1)
			}
		}
		var snapshot [][]any
		// Each original query retains its own read cut; this is not one batch snapshot.
		if err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			defer rows.Close()
			columns, err := rows.Columns()
			if err != nil {
				return err
			}
			for rows.Next() {
				values := make([]any, len(columns))
				dest := make([]any, len(columns))
				for i := range values {
					dest[i] = &values[i]
				}
				if err := rows.Scan(dest...); err != nil {
					return err
				}
				snapshot = append(snapshot, values)
			}
			if err := rows.Err(); err != nil {
				return err
			}
			return rows.Close()
		}); err != nil {
			return "", fmt.Errorf("history query %s: %w", query, err)
		}
		snapshots = append(snapshots, snapshot)
	}
	body, err := json.Marshal(snapshots)
	return string(body), err
}
