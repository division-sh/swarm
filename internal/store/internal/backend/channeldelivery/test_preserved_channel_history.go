package channeldelivery

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func ReadPreservedChannelDeliveryHistoryForTest(ctx context.Context, backend any, section int, successors []string) ([][]any, error) {
	var query string
	switch section {
	case 3:
		query = `SELECT principal_id,interface_key,binding_revision,delivery_epoch,external_account,conversation_reference,conversation_scope,state,first_operation_id FROM channel_delivery_defaults`
	case 4:
		query = `SELECT setting_id,provider,resource_slot_id,conversation_reference,scope_kind,member_reference,language_code,pack_id,pack_version,pack_manifest_hash,entry_contract_hash,entry_command,desired_commands,principal_id,generation,install_operation_id,readback_hash,created_at FROM channel_native_settings ORDER BY setting_id`
	case 5:
		query = `SELECT setting_id,activation_id,activation_revision,interface_key,binding_revision,context_publication_generation FROM channel_native_setting_consumers ORDER BY activation_id`
	case 6:
		query = `SELECT CAST(delivery_id AS TEXT),source_kind,CAST(source_id AS TEXT),request_activation_id,summary_count,principal_id,interface_key,binding_revision,delivery_epoch,external_account,conversation_reference,conversation_scope,action_capacity,text_capacity,label_capacity,resend_generation,resend_of_delivery_id,resend_action_publication_id,created_at FROM channel_delivery_plans ORDER BY delivery_id`
	case 7:
		query = `SELECT * FROM channel_delivery_renders ORDER BY render_id`
	case 8:
		query = `SELECT * FROM channel_delivery_receipts ORDER BY effect_operation_id`
	case 9:
		query = `SELECT * FROM channel_delivery_actions ORDER BY action_token`
	case 11:
		query = `SELECT * FROM operator_channel_action_intents ORDER BY publication_id`
	case 12:
		query = `SELECT * FROM operator_channel_text_intents ORDER BY publication_id`
	default:
		return nil, fmt.Errorf("unsupported preserved channeldelivery history section %d", section)
	}
	var args []any
	filters := map[int]string{
		7: `delivery_id<>$1`,
		8: `delivery_id<>$1`,
		9: `render_id NOT IN (SELECT render_id FROM channel_delivery_renders WHERE delivery_id=$1)`,
	}
	if predicate := filters[section]; len(successors) != 0 && successors[0] != "" && predicate != "" {
		var predicates []string
		for i, id := range successors {
			predicates = append(predicates, strings.ReplaceAll(predicate, "$1", fmt.Sprintf("$%d", i+1)))
			args = append(args, id)
		}
		query = strings.Replace(query, " ORDER BY", " WHERE "+strings.Join(predicates, " AND ")+" ORDER BY", 1)
	}
	var snapshot [][]any
	read := func(ctx context.Context, tx *sql.Tx) error {
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
	}
	var err error
	switch b := backend.(type) {
	case *postgresbackend.Backend:
		if b == nil || !b.Valid() {
			err = fmt.Errorf("channel observation requires an initialized postgres read owner")
		} else {
			err = b.InspectSnapshot(ctx, func(ctx context.Context) error { return b.RunReadTransaction(ctx, read) })
		}
	case *sqlitebackend.Backend:
		if b == nil || !b.Valid() {
			err = fmt.Errorf("channel observation requires an initialized sqlite read owner")
		} else {
			err = b.InspectSnapshot(ctx, func(ctx context.Context) error { return b.RunReadTransaction(ctx, read) })
		}
	default:
		err = fmt.Errorf("channel observation requires its native backend")
	}
	if err != nil {
		return nil, fmt.Errorf("history query %s: %w", query, err)
	}
	return snapshot, nil
}
