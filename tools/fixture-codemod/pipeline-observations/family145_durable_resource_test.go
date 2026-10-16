package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func durableResourceCallerEscape(raw []byte) error {
	return family145DescriptorReadinessEscape(raw)
}

func assertDurableResourceHistoryPreservesOriginals(t *testing.T, prior, actual []byte, recipes bool) {
	t.Helper()
	var before, after []map[string]any
	if json.Unmarshal(prior, &before) != nil || json.Unmarshal(actual, &after) != nil {
		t.Fatal("invalid finite history")
	}
	added := 51
	if recipes {
		added = 49
	}
	if len(after) != len(before)+added {
		t.Fatalf("history append count=%d, want%d", len(after)-len(before), added)
	}
	for i, row := range before {
		for key, value := range row {
			if !reflect.DeepEqual(value, after[i][key]) {
				t.Fatalf("historical row%d %s was overwritten", i, key)
			}
		}
		if !recipes && !reflect.DeepEqual(row, after[i]) {
			t.Fatalf("historical transition%d was edited", i)
		}
	}
}

func assertDurableResourceHistoryHostiles(t *testing.T, root string, actual []byte, recipes bool) {
	t.Helper()
	for _, cut := range []string{"lost", "order", "before", "foreign"} {
		var rows []map[string]any
		if err := json.Unmarshal(actual, &rows); err != nil {
			t.Fatal(err)
		}
		switch cut {
		case "lost":
			rows = rows[:len(rows)-1]
		case "order":
			rows[len(rows)-1], rows[len(rows)-2] = rows[len(rows)-2], rows[len(rows)-1]
		case "before":
			rows[len(rows)-1]["Before"] = "foreign original"
		case "foreign":
			rows[len(rows)-1]["After"] = strings.Repeat("foreign authority", 2)
		}
		bad, err := json.Marshal(rows)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := selectedreadrole.EarlierDurableResourceHistory(root, bad, recipes); err == nil {
			t.Fatalf("foreign history accepted: %s", cut)
		}
	}
}
