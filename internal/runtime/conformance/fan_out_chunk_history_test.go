package conformance

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/store/storetest"
)

// Count actual committed cursor changes, not claim/release or unrelated history
// transactions. This is read only after the original workload has settled.
func assertReporterFanOutSingleChunkHistory(t *testing.T, ctx context.Context, selected notifyAllChildrenStore, runID string) {
	t.Helper()
	rows, err := storetest.ReadFanOutObligationHistoryStorage(ctx, notifyAllChildrenNativeObservationOwner(selected), runID)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(map[string][]int)
	for _, row := range rows {
		key, raw := row.Key, row.Fact
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
