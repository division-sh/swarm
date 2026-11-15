package effectpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func ReadPreservedChannelEffectHistoryForTest(ctx context.Context, backend any, section int, successors []string) ([][]any, error) {
	var query string
	switch section {
	case 13:
		query = `SELECT * FROM runtime_external_effect_operations WHERE effect_kind IN ('channel_delivery','channel_native_setting','channel_action_ack') ORDER BY operation_id`
	case 14:
		query = `SELECT a.* FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.effect_kind IN ('channel_delivery','channel_native_setting','channel_action_ack') ORDER BY a.attempt_id`
	default:
		return nil, fmt.Errorf("unsupported preserved effectpersistence history section %d", section)
	}
	var args []any
	filters := map[int]string{
		13: `operation_id NOT IN (SELECT effect_operation_id FROM channel_delivery_receipts WHERE delivery_id=$1)`,
		14: `a.operation_id NOT IN (SELECT effect_operation_id FROM channel_delivery_receipts WHERE delivery_id=$1)`,
	}
	if len(successors) != 0 && successors[0] != "" {
		var predicates []string
		for i, id := range successors {
			predicates = append(predicates, strings.ReplaceAll(filters[section], "$1", fmt.Sprintf("$%d", i+1)))
			args = append(args, id)
		}
		query = strings.Replace(query, " ORDER BY", " AND "+strings.Join(predicates, " AND ")+" ORDER BY", 1)
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
