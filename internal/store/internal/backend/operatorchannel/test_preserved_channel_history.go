package operatorchannel

import (
	"context"
	"database/sql"
	"fmt"

	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func ReadPreservedOperatorChannelHistoryForTest(ctx context.Context, backend any, section int) ([][]any, error) {
	var query string
	switch section {
	case 0:
		query = `SELECT * FROM operator_principals ORDER BY principal_id`
	case 1:
		query = `SELECT * FROM operator_channel_operations ORDER BY operation_id`
	case 2:
		query = `SELECT * FROM operator_channel_bindings ORDER BY interface_key`
	case 10:
		query = `SELECT * FROM operator_channel_claim_receipts ORDER BY publication_id`
	default:
		return nil, fmt.Errorf("unsupported preserved operatorchannel history section %d", section)
	}
	var args []any
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
