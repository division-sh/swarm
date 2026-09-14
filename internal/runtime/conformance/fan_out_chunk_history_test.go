package conformance

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
)

// Count actual committed cursor changes, not claim/release or unrelated history
// transactions. This is read only after the original workload has settled.
func assertReporterFanOutSingleChunkHistory(t *testing.T, ctx context.Context, db *sql.DB, runID string) {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT fact_key,fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='fan_out_obligations' ORDER BY revision,fact_key`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	observed := make(map[string][]int)
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			t.Fatal(err)
		}
		var fact struct {
			Kind        string `json:"fact_kind"`
			Cursor      int    `json:"cursor"`
			Cardinality int    `json:"cardinality"`
		}
		if err := json.Unmarshal(raw, &fact); err != nil {
			t.Fatal(err)
		}
		if fact.Kind != "intent" {
			continue
		}
		if fact.Cardinality != 25 {
			t.Fatalf("reporter intent %s cardinality=%d, want25", key, fact.Cardinality)
		}
		observed[key] = append(observed[key], fact.Cursor)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 20 {
		t.Fatalf("reporter history has %d intents, want20", len(observed))
	}
	for key, cursors := range observed {
		if len(cursors) != 2 || cursors[0] != 0 || cursors[1] != 25 {
			t.Fatalf("intent %s committed cursor history=%v, want one25-item chunk after creation", key, cursors)
		}
	}
	t.Log("durable history proves20 successful25-item chunk commits for500 publications")
}
