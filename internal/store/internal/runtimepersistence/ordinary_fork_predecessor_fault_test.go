package runtimepersistence

import (
	"encoding/json"
	"reflect"
	"testing"
)

func proveOrdinaryForkOriginRevisionDelta(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, runID string) {
	t.Helper()
	proveOrdinaryForkFaultTableDelta(t, before, after, "runs", 0)
	prior, err := DecodeSelectedForkSnapshotRowsForTest(before["runs"])
	if err != nil {
		t.Fatal(err)
	}
	current, err := DecodeSelectedForkSnapshotRowsForTest(after["runs"])
	if err != nil {
		t.Fatal(err)
	}
	changed := 0
	for index, row := range prior {
		id, err := row["run_id"].Text()
		if err != nil {
			t.Fatal(err)
		}
		for column, cell := range row {
			replacement := current[index][column]
			if id == runID && column == "forked_from_revision" {
				var cut int64
				if err := json.Unmarshal(replacement.Value, &cut); err != nil || replacement.Type != "int64" || cut != 1<<62 || reflect.DeepEqual(cell, replacement) {
					t.Fatalf("origin fault lost its exact sole delta: %+v %v", replacement, err)
				}
				changed++
			} else if !reflect.DeepEqual(cell, replacement) {
				t.Fatalf("origin fault changed unrelated run/column %s/%s", id, column)
			}
		}
	}
	if changed != 1 {
		t.Fatalf("origin fault changed %d revision coordinates, want one", changed)
	}
}

func proveOrdinaryForkFaultTableDelta(t *testing.T, before, after map[string]SelectedForkStorageTableSnapshot, table string, delta int) {
	t.Helper()
	if len(before) != len(after) || len(after[table].Rows)-len(before[table].Rows) != delta {
		t.Fatalf("fault changed wrong table cardinality: %s before=%+v after=%+v", table, before[table], after[table])
	}
	for name, rows := range before {
		if name != table && !reflect.DeepEqual(rows, after[name]) {
			t.Fatalf("fault changed unrelated table %s", name)
		}
	}
}
