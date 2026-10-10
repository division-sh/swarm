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

func nativeAPILatestEventIdentityRecipes(t *testing.T) []recipe {
	t.Helper()
	wanted := map[string]bool{
		"latestEventIDByName": true,
		"TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness":    true,
		"TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError": true,
	}
	var rows, found []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if wanted[row.Function] {
			found = append(found, row)
			delete(wanted, row.Function)
		}
	}
	if len(wanted) != 0 || len(found) != 3 {
		t.Fatalf("latest event identity recipes missing/duplicate: %v/%d", wanted, len(found))
	}
	return found
}

func TestNativeAPILatestEventIdentityPreservesCompleteReplayWorkAndKeys(t *testing.T) {
	for _, row := range nativeAPILatestEventIdentityRecipes(t) {
		if strings.HasPrefix(row.Function, "Test") {
			if nativeAPIPhysicalCardinalityWorkload(t, row.Before) != nativeAPIPhysicalCardinalityWorkload(t, row.After) {
				t.Fatalf("audit/fanout/idempotency/replay assertions changed: %s", row.Function)
			}
			continue
		}
		want := `func latestEventIDByName(t *testing.T,selected any,eventName,excludeEventID string)string{
			t.Helper()
			eventID,err:=storetest.ReadLatestNamedEventIdentityStorage(context.Background(),selected,eventName,excludeEventID)
			if err!=nil{t.Fatalf("latest event by name %s: %v",eventName,err)}
			return eventID
		}`
		if formattedNativeReadNode(projectionShapeFunction(t, row.After)) != formattedNativeReadNode(projectionShapeFunction(t, want)) {
			t.Fatal("latest event name/exclusion/owner/context/refusal changed")
		}
	}
}

func TestNativeAPILatestEventIdentityRejectsWrongOwnerExclusionAndAssertion(t *testing.T) {
	probes := 0
	for _, row := range nativeAPILatestEventIdentityRecipes(t) {
		if !strings.HasPrefix(row.Function, "Test") {
			continue
		}
		for _, change := range [][2]string{
			{"latestEventIDByName(t, pg,", "latestEventIDByName(t, wrongOwner,"},
			{"latestEventIDByName(t, pg, \"scan.requested\", original.EventID)", "latestEventIDByName(t, pg, \"scan.requested\", wrongExcludedID)"},
			{"count != 2", "count < 2"},
		} {
			broken := strings.Replace(row.After, change[0], change[1], 1)
			if broken == row.After {
				t.Fatalf("latest identity hostile control stopped matching: %s/%s", row.Function, change[0])
			}
			probes++
			if nativeAPIPhysicalCardinalityWorkload(t, row.After) == nativeAPIPhysicalCardinalityWorkload(t, broken) {
				t.Fatalf("weakened latest identity proof admitted: %s/%s", row.Function, change[0])
			}
		}
	}
	if probes != 6 {
		t.Fatalf("latest identity adversaries missing: %d", probes)
	}
}

func TestNativeAPILatestEventIdentityCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "apiv1", "*_test.go"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("enumerate API sources: %v/%d", err, len(paths))
	}
	actual := map[string][]string{}
	for _, path := range paths {
		bytes, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, bytes, parser.AllErrors)
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
				if ok && formattedNativeReadNode(call.Fun) == "latestEventIDByName" {
					actual[fn.Name.Name] = append(actual[fn.Name.Name], formattedNativeReadNode(call))
				}
				return true
			})
		}
	}
	want := map[string][]string{
		"TestOperatorEventReplayStoresIdempotencyBeforeAuditPublishReadiness":    {"latestEventIDByName(t, pg, \"scan.requested\", original.EventID)"},
		"TestOperatorEventReplayStoresIdempotencyBeforeDirectPublishFanoutError": {"latestEventIDByName(t, pg, \"scan.requested\", original.EventID)"},
	}
	if !reflect.DeepEqual(actual, want) {
		t.Fatalf("latest identity callers differ: got=%v want=%v", actual, want)
	}
}
