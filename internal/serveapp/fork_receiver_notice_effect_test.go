package serveapp

import (
	"encoding/json"
	"sort"
	"testing"
)

func readForkReceiverNoticeDomain(t *testing.T, rt servedControlProofRuntime, runID string) []string {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT m.* FROM mailbox m JOIN events e ON e.event_id=m.source_event_id WHERE e.run_id=$1`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for rows.Next() {
		values, pointers := make([]any, len(columns)), make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			t.Fatal(err)
		}
		for i, value := range values {
			if raw, ok := value.([]byte); ok {
				values[i] = string(raw)
			}
		}
		raw, err := json.Marshal(values)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, string(raw))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(out)
	return out
}
