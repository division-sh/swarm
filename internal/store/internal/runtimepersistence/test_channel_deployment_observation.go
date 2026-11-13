package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/store/internal/backend/channeldelivery"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
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

func inspectChannelOwnedObservation(ctx context.Context, selected any, read func(context.Context) error) error {
	if err := validateChannelObservationOwner(selected); err != nil {
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		return owner.InspectSnapshot(ctx, read)
	case *SQLiteRuntimeStore:
		return owner.InspectSnapshot(ctx, read)
	}
	return fmt.Errorf("channel observation requires its original native owner")
}

func inspectChannelBackendObservation(ctx context.Context, selected any, read func(context.Context, any) error) error {
	return inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		switch owner := selected.(type) {
		case *PostgresStore:
			return read(ctx, owner.backend)
		case *SQLiteRuntimeStore:
			return read(ctx, owner.backend)
		}
		return fmt.Errorf("channel observation requires its original native owner")
	})
}

func ReadChannelNativeConsumerForTest(ctx context.Context, selected any, operation string, currentOnly bool) (channelnative.TransportParent, error) {
	var raw []byte
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		raw, err = channeldelivery.ReadChannelNativeConsumerEvidenceForTest(ctx, backend, operation, currentOnly)
		return err
	})
	if err != nil {
		return channelnative.TransportParent{}, err
	}
	return channelnative.DecodeTransportParent(raw)
}

func ReadChannelNativeHistoryForTest(ctx context.Context, selected any) ([8]string, error) {
	var row [8]string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		row, err = channeldelivery.ReadChannelNativeHistoryForTest(ctx, backend)
		return err
	})
	return row, err
}

func ReadChannelFrozenCardHistoryForTest(ctx context.Context, selected any, card string) (string, error) {
	out := "\n"
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		out, err = channeldelivery.ReadChannelFrozenCardHistoryForTest(ctx, backend, card)
		return err
	})
	return out, err
}

func ReadChannelHistoricalReceiptForTest(ctx context.Context, selected any, card string) (string, error) {
	var receipt string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		receipt, err = channeldelivery.ReadChannelHistoricalReceiptForTest(ctx, backend, card)
		return err
	})
	return receipt, err
}

func ReadChannelGenerationZeroSentPlanForTest(ctx context.Context, selected any, card string) (string, error) {
	var plan string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		plan, err = channeldelivery.ReadChannelGenerationZeroSentPlanForTest(ctx, backend, card)
		return err
	})
	return plan, err
}

func ReadChannelControlCountsForTest(ctx context.Context, selected any, token string) (ChannelControlCounts, error) {
	var result ChannelControlCounts
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		result.Total, result.Nonmutating, result.OriginalRefused, err = channeldelivery.ReadChannelControlCountsForTest(ctx, backend, token)
		return err
	})
	return result, err
}

func CountChannelSourceCardsForTest(ctx context.Context, selected any, run, source string) (int, error) {
	var count int
	err := inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			count, err = owner.decisionPostgresOwner.CountChannelSourceCardsForTest(ctx, run, source)
		case *SQLiteRuntimeStore:
			count, err = owner.decisionSQLiteOwner.CountChannelSourceCardsForTest(ctx, run, source)
		}
		return err
	})
	return count, err
}

func CountChannelEntryIntentsForTest(ctx context.Context, selected any) (int, error) {
	var count int
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, err = channeldelivery.CountChannelEntryIntentsForTest(ctx, backend)
		return err
	})
	return count, err
}

func CountChannelRejectedEntriesForTest(ctx context.Context, selected any, event string) (int, error) {
	var count int
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, err = channeldelivery.CountChannelRejectedEntriesForTest(ctx, backend, event)
		return err
	})
	return count, err
}

func ReadChannelProviderActionForTest(ctx context.Context, selected any, event string) (int, string, string, error) {
	var count int
	var state, disposition string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, state, disposition, err = channeldelivery.ReadChannelProviderActionForTest(ctx, backend, event)
		return err
	})
	return count, state, disposition, err
}

func CountChannelObservedEventsForTest(ctx context.Context, selected any, run, event string) (int, error) {
	if err := validateSelectedForkStorageIdentity(run); err != nil {
		return 0, err
	}
	if event == "" {
		return 0, fmt.Errorf("lifecycle event cardinality requires an exact event name")
	}
	var count int
	err := inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			count, err = owner.eventPostgresOwner.CountChannelObservedEventsForTest(ctx, run, event)
		case *SQLiteRuntimeStore:
			count, err = owner.eventSQLiteOwner.CountChannelObservedEventsForTest(ctx, run, event)
		}
		return err
	})
	return count, err
}

func ReadChannelRegistrationIndexForTest(ctx context.Context, selected any) (string, error) {
	dialect := schemastore.SchemaDialectSQLite
	if _, postgres := selected.(*PostgresStore); postgres {
		dialect = schemastore.SchemaDialectPostgres
	}
	var definition string
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		definition, err = schemastore.DeploymentChannelAccountIndexForTest(ctx, tx, dialect)
		return err
	})
	return definition, err
}

func ReadChannelRegistrationCountsForTest(ctx context.Context, selected any) (int, int, int, error) {
	var total, current, retired int
	err := inspectChannelOwnedObservation(ctx, selected, func(ctx context.Context) error {
		var err error
		switch owner := selected.(type) {
		case *PostgresStore:
			total, current, retired, err = owner.channelOnboardingPostgresOwner.ReadChannelRegistrationCountsForTest(ctx)
		case *SQLiteRuntimeStore:
			total, current, retired, err = owner.channelOnboardingSQLiteOwner.ReadChannelRegistrationCountsForTest(ctx)
		}
		return err
	})
	return total, current, retired, err
}

func ReadChannelDeliveryDefaultStateForTest(ctx context.Context, selected any) (string, error) {
	var state string
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		state, err = channeldelivery.ReadChannelDeliveryDefaultStateForTest(ctx, backend)
		return err
	})
	return state, err
}

func CountUnsettledChannelResponsibilitiesForTest(ctx context.Context, selected any) (int, error) {
	_, postgres := selected.(*PostgresStore)
	var count int
	err := inspectChannelBackendObservation(ctx, selected, func(ctx context.Context, backend any) error {
		var err error
		count, err = channeldelivery.CountUnsettledChannelResponsibilitiesForTest(ctx, backend, postgres)
		return err
	})
	return count, err
}

type ChannelResetSettlement struct {
	PlanID, SourceID, SourceKind, ReceiptState, OperationState string
	SourcePresent, ExactReceipt, UnsafeOperations              bool
}

func (r ChannelResetSettlement) SettledOriginal(originals map[string]string) bool {
	return r.SourceID != "" && originals[r.PlanID] == r.SourceID && r.SourceKind == "card" && !r.SourcePresent &&
		r.ExactReceipt && r.ReceiptState == "sent" && r.OperationState == "settled" && !r.UnsafeOperations
}

func CountUnsettledChannelAfterResetForTest(ctx context.Context, selected any, originals map[string]string) (int, error) {
	_, postgres := selected.(*PostgresStore)
	identity := func(alias, field string) string {
		if postgres {
			return alias + ".authority_evidence->>'" + field + "'"
		}
		return "json_extract(" + alias + ".authority_evidence,'$." + field + "')"
	}
	var count int
	err := inspectChannelObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		query := `SELECT p.delivery_id,p.source_id,p.source_kind,COALESCE(r.state,''),COALESCE(o.state,''),
		EXISTS(SELECT 1 FROM decision_cards c WHERE c.card_id=p.source_id),
		COALESCE(r.delivery_id=p.delivery_id AND o.authority_kind='channel_delivery' AND o.effect_kind='channel_delivery'
		AND ` + identity("o", "delivery_id") + `=CAST(p.delivery_id AS TEXT) AND ` + identity("o", "render_id") + `=CAST(r.render_id AS TEXT),FALSE),
		EXISTS(SELECT 1 FROM runtime_external_effect_operations q WHERE q.authority_kind='channel_delivery'
		AND ` + identity("q", "delivery_id") + `=CAST(p.delivery_id AS TEXT) AND q.state<>'settled')
		FROM channel_delivery_plans p LEFT JOIN channel_delivery_receipts r ON r.effect_operation_id=p.current_receipt_operation_id
		LEFT JOIN runtime_external_effect_operations o ON o.operation_id=r.effect_operation_id
		WHERE (p.state IN ('planned','rendered') AND ` + channeldelivery.SendEligibilityPredicate + `) OR ` + channeldelivery.AcceptedEffectPredicate(postgres)
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
	filters := map[int]struct{ prefix, predicate string }{
		7:  {" WHERE ", `delivery_id<>$1`},
		8:  {" WHERE ", `delivery_id<>$1`},
		9:  {" WHERE ", `render_id NOT IN (SELECT render_id FROM channel_delivery_renders WHERE delivery_id=$1)`},
		13: {" AND ", `operation_id NOT IN (SELECT effect_operation_id FROM channel_delivery_receipts WHERE delivery_id=$1)`},
		14: {" AND ", `a.operation_id NOT IN (SELECT effect_operation_id FROM channel_delivery_receipts WHERE delivery_id=$1)`},
	}
	var snapshots [][][]any
	for index, query := range queries {
		var args []any
		filter := filters[index]
		if len(successors) != 0 && successors[0] != "" && filter.predicate != "" {
			var predicates []string
			for i, id := range successors {
				predicates = append(predicates, strings.ReplaceAll(filter.predicate, "$1", fmt.Sprintf("$%d", i+1)))
				args = append(args, id)
			}
			query = strings.Replace(query, " ORDER BY", filter.prefix+strings.Join(predicates, " AND ")+" ORDER BY", 1)
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
