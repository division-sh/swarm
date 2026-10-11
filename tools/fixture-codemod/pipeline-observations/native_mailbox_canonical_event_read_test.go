package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func nativeMailboxCanonicalEventRecipes(t *testing.T) []recipe {
	t.Helper()
	var rows, found []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if row.Family == "native-api-mailbox-canonical-event-reader" {
			found = append(found, row)
		}
	}
	if len(found) != 2 {
		t.Fatalf("mailbox event reader recipes=%d, want2", len(found))
	}
	return found
}

func nativeMailboxCanonicalEventWorkload(t *testing.T, source string) string {
	t.Helper()
	if strings.Contains(source, "func TestMailboxDecideHTTPReleasesProposedEffectThroughProviderOnBothStores(") {
		source = nativeProposedEffectPublicSource(source)
	}
	fn := projectionShapeFunction(t, source)
	ast.Inspect(fn, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || formattedNativeReadNode(call.Fun) != "loadMailboxWritePersistedEvent" {
			return true
		}
		if len(call.Args) == 4 && formattedNativeReadNode(call.Args[1]) == "db" && formattedNativeReadNode(call.Args[2]) == "tc.name" {
			call.Args = []ast.Expr{call.Args[0], ast.NewIdent("originalSelectedOwner"), call.Args[3]}
		} else if len(call.Args) == 3 && formattedNativeReadNode(call.Args[1]) == "persistence" {
			call.Args[1] = ast.NewIdent("originalSelectedOwner")
		}
		return true
	})
	return formattedNativeReadNode(fn)
}

func TestNativeMailboxCanonicalEventReadRetainsFullWorkAndExactOwner(t *testing.T) {
	for _, row := range nativeMailboxCanonicalEventRecipes(t) {
		if strings.HasPrefix(row.Function, "Test") {
			if nativeMailboxCanonicalEventWorkload(t, row.Before) != nativeMailboxCanonicalEventWorkload(t, row.After) {
				t.Fatalf("mailbox HTTP/provider/workload/assertions changed: %s", row.Function)
			}
			continue
		}
		want := `func loadMailboxWritePersistedEvent(t *testing.T, selected any, eventID string) events.Event {
			t.Helper()
			return storetest.LoadCanonicalEventRecord(t, context.Background(), selected, eventID)
		}`
		if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
			t.Fatal("mailbox canonical decoder/context/key/owner binding changed")
		}
	}
}

func TestNativeMailboxCanonicalEventReadRejectsChangedIdentityAndAssertions(t *testing.T) {
	probes := 0
	for _, row := range nativeMailboxCanonicalEventRecipes(t) {
		if !strings.HasPrefix(row.Function, "Test") {
			continue
		}
		for _, change := range [][2]string{
			{"loadMailboxWritePersistedEvent(t, persistence, decisionEventID)", "loadMailboxWritePersistedEvent(t, wrongOwner, decisionEventID)"},
			{"loadMailboxWritePersistedEvent(t, persistence, decisionEventID)", "loadMailboxWritePersistedEvent(t, persistence, wrongEventID)"},
			{"requestEvent.Type() != events.EventType", "requestEvent.Type() == events.EventType"},
			{"got != 1", "got < 1"},
		} {
			broken := strings.Replace(row.After, change[0], change[1], 1)
			if broken == row.After {
				t.Fatalf("mailbox adversary no longer matches: %s", change[0])
			}
			probes++
			if nativeMailboxCanonicalEventWorkload(t, row.After) == nativeMailboxCanonicalEventWorkload(t, broken) {
				t.Fatalf("mailbox weakened proof admitted: %s", change[0])
			}
		}
	}
	if probes != 4 {
		t.Fatalf("mailbox adversaries missing: %d", probes)
	}
}

func TestNativeMailboxCanonicalEventCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "apiv1", "*_test.go"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("enumerate API caller sources: %v/%d", err, len(paths))
	}
	actual := map[string][]string{}
	for _, path := range paths {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok && formattedNativeReadNode(call.Fun) == "loadMailboxWritePersistedEvent" {
					actual[fn.Name.Name] = append(actual[fn.Name.Name], formattedNativeReadNode(call))
				}
				return true
			})
		}
	}
	expected := map[string][]string{
		"TestMailboxDecideHTTPReleasesProposedEffectThroughProviderOnBothStores": {
			"loadMailboxWritePersistedEvent(t, persistence, decisionEventID)",
			"loadMailboxWritePersistedEvent(t, persistence, continuation.RequestEventID)",
		},
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("mailbox canonical read consumers differ: got=%v want=%v", actual, expected)
	}
}
