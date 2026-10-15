package main

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func TestFamily145DurableResourceWholeCallerBoundaryAndExactInverses(t *testing.T) {
	root := filepath.Clean("../../..")
	pins, err := selectedreadrole.ReadDurableResourcePins(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := selectedreadrole.ValidateDurableResourceCurrent(root, pins); err != nil {
		t.Fatal(err)
	}
	files := map[string]bool{}
	for _, pin := range pins {
		files[pin.File] = true
		if !pin.Removed {
			prior, err := selectedreadrole.EarlierDurableResourceSource(root, pin.File, pin.Function, pin.After)
			if err != nil || prior != pin.Before {
				t.Fatalf("exact declaration inverse %s: %v", pin.Function, err)
			}
			if _, err := selectedreadrole.EarlierDurableResourceSource(root, pin.File, pin.Function, pin.After+"\n"); err == nil {
				t.Fatalf("foreign source accepted: %s", pin.Function)
			}
		}
	}
	if len(files) != 8 {
		t.Fatalf("durable resource caller files=%d, want exactly eight", len(files))
	}
	callers := map[string]int{}
	for path := range files {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := durableResourceCallerEscape(raw); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if call, ok := node.(*ast.CallExpr); ok {
				if name, ok := call.Fun.(*ast.Ident); ok {
					callers[name.Name]++
				}
			}
			return true
		})
	}
	if callers["forEachDurableDataStore"] != 22 || callers["forEachDataRunLifecycleStore"] != 19 {
		t.Fatalf("native caller boundary=%d/%d, want22/19", callers["forEachDurableDataStore"], callers["forEachDataRunLifecycleStore"])
	}
}

func durableResourceCallerEscape(raw []byte) error {
	return family145DescriptorReadinessEscape(raw)
}

func TestFamily145DurableResourceCallerHostileControls(t *testing.T) {
	for _, body := range []string{
		"func fixture() { storetest.Database(selected) }",
		"func fixture() { storetest.AdmitPostgresRuntimeStore(t, db) }",
		"func fixture() { db.QueryRowContext(ctx, query) }",
		"func fixture() { db.ExecContext(ctx, query) }",
		"func fixture() { db.BeginTx(ctx, nil) }",
	} {
		if err := durableResourceCallerEscape([]byte("package hostile\n" + body)); err == nil {
			t.Fatalf("raw fixture authority survived: %s", body)
		}
	}
}

func TestFamily145DurableResourceAppendOnlyHistoryAndHostileInputs(t *testing.T) {
	root := filepath.Clean("../../..")
	for _, recipes := range []bool{false, true} {
		prior, err := selectedreadrole.PriorDurableResourceHistory(root, recipes)
		if err != nil {
			t.Fatal(err)
		}
		expected, err := selectedreadrole.ComposeDurableResourceHistory(root, prior, recipes)
		if err != nil {
			t.Fatal(err)
		}
		actual := family145TransitionBytes
		if recipes {
			actual = recipeBytes
		}
		actual, err = selectedreadrole.BeforeSelectedForkExecutionHistory(root, actual, recipes)
		if err != nil {
			t.Fatal(err)
		}
		var have, want any
		if json.Unmarshal(actual, &have) != nil || json.Unmarshal(expected, &want) != nil || !reflect.DeepEqual(have, want) {
			t.Fatal("history differs from the complete exact durable resource composition")
		}
		recovered, err := selectedreadrole.EarlierDurableResourceHistory(root, actual, recipes)
		if err != nil || !bytes.Equal(recovered, prior) {
			t.Fatalf("complete signed predecessor recovery: %v", err)
		}
		assertDurableResourceHistoryPreservesOriginals(t, prior, actual, recipes)
		assertDurableResourceHistoryHostiles(t, root, actual, recipes)
	}
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
