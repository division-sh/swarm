package mailboxpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
)

// Preserve the independent physical notice witness, not the public mailbox
// projection: every original field, NULL, clock and duplicate row survives.
func ReadForkReceiverNoticeStorageTx(ctx context.Context, tx *sql.Tx, runID string) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT m.* FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := []string{}
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		for i, value := range values {
			if raw, ok := value.([]byte); ok {
				values[i] = string(raw)
			}
		}
		raw, err := json.Marshal(values)
		if err != nil {
			return nil, err
		}
		out = append(out, string(raw))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}
