package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type conformanceStorageColumns struct{ table, columns string }

// These fixed witness lists are not a public table/column selection API and
// do not replace authoritative schema admission or the workload assertions.
func CheckConversationStorageColumnsForTest(ctx context.Context, selected any) error {
	return checkConformanceStorageColumns(ctx, selected, []conformanceStorageColumns{
		{"agent_turns", "turn_id turn_blocks"},
		{"agent_conversation_audits", "session_id"},
	})
}

func CheckRuntimeLogStorageColumnsForTest(ctx context.Context, selected any) error {
	return checkConformanceStorageColumns(ctx, selected, []conformanceStorageColumns{
		{"events", "event_id event_name payload scope created_at"},
	})
}

func CheckMutationStorageColumnsForTest(ctx context.Context, selected any) error {
	return checkConformanceStorageColumns(ctx, selected, []conformanceStorageColumns{
		{"entity_state", "entity_id current_state fields bookkeeping gates accumulator"},
		{"entity_mutations", "entity_id domain path old_value new_value writer_type writer_id handler_step created_at"},
	})
}

func CheckDeliveryLifecycleStorageColumnsForTest(ctx context.Context, selected any) error {
	return checkConformanceStorageColumns(ctx, selected, []conformanceStorageColumns{
		{"event_deliveries", "delivery_id event_id route_identity subscriber_type subscriber_id status retry_count max_retries claim_version current_attempt_version current_attempt_open settled_at"},
		{"event_delivery_attempts", "delivery_id claim_version claim_token started_at lease_expires_at current_delivery_id active_session_id session_delivery_id session_run_id session_subscriber_type session_agent_id open_marker closure_kind outcome side_effects duration_ms completed_at"},
	})
}

func checkConformanceStorageColumns(ctx context.Context, selected any, inventory []conformanceStorageColumns) error {
	sqlite := false
	var read func(context.Context, func(context.Context, *sql.Tx) error) error
	switch owner := selected.(type) {
	case *PostgresStore:
		if owner == nil || owner.backend == nil {
			return fmt.Errorf("column evidence requires an initialized postgres owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return err
		}
		read = owner.backend.RunReadTransaction
	case *SQLiteRuntimeStore:
		if owner == nil || owner.backend == nil {
			return fmt.Errorf("column evidence requires an initialized sqlite owner")
		}
		if err := owner.requireCurrentSchema(); err != nil {
			return err
		}
		read = owner.backend.RunReadTransaction
		sqlite = true
	default:
		return fmt.Errorf("column evidence requires the original selected owner, got %T", selected)
	}
	return read(ctx, func(ctx context.Context, tx *sql.Tx) error {
		for _, item := range inventory {
			for _, column := range strings.Fields(item.columns) {
				query := `SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name=$1 AND column_name=$2)`
				if sqlite {
					query = `SELECT EXISTS(SELECT 1 FROM pragma_table_info($1, 'main') WHERE name=$2)`
				}
				var exists bool
				if err := tx.QueryRowContext(ctx, query, item.table, column).Scan(&exists); err != nil {
					return fmt.Errorf("inspect column %s.%s: %w", item.table, column, err)
				}
				if !exists {
					return fmt.Errorf("missing required canonical column %s.%s", item.table, column)
				}
			}
		}
		return nil
	})
}
