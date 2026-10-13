package pipelinepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
)

// The two companion witnesses intentionally preserve independent table cuts,
// full physical columns and the original byte-to-text JSON representation.
func ReadForkReceiverCompanionTableStorageTx(ctx context.Context, tx *sql.Tx, table, runID string, paths ...string) ([]string, []string, error) {
	if table != "flow_instances" && table != "flow_instance_runtime_readiness" {
		return nil, nil, fmt.Errorf("fork receiver companion table is not owned")
	}
	query, args := `SELECT * FROM `+table+` WHERE run_id=$1`, []any{runID}
	if len(paths) != 0 {
		query += ` AND instance_path=$2`
		args = append(args, paths[0])
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	columnJSON, err := json.Marshal(columns)
	if err != nil {
		return nil, nil, err
	}
	out := []string{}
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, nil, err
		}
		for i, value := range values {
			if raw, ok := value.([]byte); ok {
				values[i] = string(raw)
			}
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, nil, err
		}
		out = append(out, string(encoded))
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	sort.Strings(out)
	return []string{string(columnJSON)}, out, nil
}
