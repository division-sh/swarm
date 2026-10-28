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

func nativeAPIExactRunCountRecipes(t *testing.T) []recipe {
	t.Helper()
	wanted := map[string]bool{"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence": true,
		"TestOperatorEventPublishPrivateTargetCannotAuthorizePublication":                     true,
		"TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence": true,
		"TestOperatorEventPublishSQLiteExplicitRunFollowUpUsesSelectedRun":                    true,
		"TestOperatorRunStartHandlersFailClosedBeforePersistence":                             true,
		"TestOperatorRuntimeContextManagerRoutesExistingRunByStoredBundle":                    true,
		"TestOperatorRuntimeContextManagerRejectsExistingRunUnavailableSourceStates":          true,
		"TestOperatorRuntimeContextManagerRejectsExistingRunRequestedHashMismatch":            true,
		"TestOperatorRuntimeContextManagerFailsClosedForDeactivatedBundle":                    true,
		"countRunRowsByID":      true,
		"countEventRowsByRunID": true}
	var all []recipe
	if err := json.Unmarshal(recipeBytes, &all); err != nil {
		t.Fatal(err)
	}
	var found []recipe
	for _, row := range all {
		if wanted[row.Function] {
			found = append(found, row)
			delete(wanted, row.Function)
		}
	}
	if len(wanted) != 0 || len(found) != 11 {
		t.Fatalf("exact-run recipes missing/duplicate: %v/%d", wanted, len(found))
	}
	return found
}
func nativeAPIExactRunCountWorkload(t *testing.T, source string) string {
	t.Helper()
	return nativeAPIPhysicalCardinalityWorkload(t, nativeAPISQLiteCounterWorkload(t, source))
}

func TestNativeAPIExactRunCountsRejectWrongKeyOwnerOrAssertion(t *testing.T) {
	probes := 0
	for _, row := range nativeAPIExactRunCountRecipes(t) {
		for _, change := range [][2]string{{", runID)", ", anotherRunID)"}, {"(t, pg,", "(t, anotherOwner,"}, {"got != 1", "got < 1"}} {
			broken := strings.Replace(row.After, change[0], change[1], 1)
			if broken == row.After {
				continue
			}
			probes++
			if nativeAPIExactRunCountWorkload(t, row.After) == nativeAPIExactRunCountWorkload(t, broken) {
				t.Fatalf("changed exact-run proof admitted: %s/%s", row.Function, change[0])
			}
		}
	}
	if probes < 5 {
		t.Fatalf("missing exact-run adversaries: %d", probes)
	}
}
func TestNativeAPIExactRunCountCallerInventoryIsComplete(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "internal", "apiv1", "*_test.go"))
	if err != nil {
		t.Fatal(err)
	}
	actual := map[string]map[string]int{}
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
				if !ok {
					return true
				}
				name := formattedNativeReadNode(call.Fun)
				if name != "countRunRowsByID" && name != "countEventRowsByRunID" {
					return true
				}
				if actual[fn.Name.Name] == nil {
					actual[fn.Name.Name] = map[string]int{}
				}
				actual[fn.Name.Name][name]++
				switch formattedNativeReadNode(call.Args[1]) {
				case "pg", "fixture.pg", "sqliteStore":
				default:
					t.Fatalf("raw/foreign exact-run counter owner: %s", fn.Name.Name)
				}
				return true
			})
		}
	}
	expected := map[string]map[string]int{"TestOperatorEventPublishExplicitRunFollowUpRequiresRecipientBeforePersistence": {"countRunRowsByID": 1, "countEventRowsByRunID": 1},
		"TestOperatorEventPublishPrivateTargetCannotAuthorizePublication":                     {"countEventRowsByRunID": 1},
		"TestOperatorEventPublishExistingRunTargetRouteRejectsInvalidTargetBeforePersistence": {"countEventRowsByRunID": 1},
		"TestOperatorEventPublishSQLiteExplicitRunFollowUpUsesSelectedRun":                    {"countEventRowsByRunID": 1},
		"TestOperatorRunStartHandlersFailClosedBeforePersistence":                             {"countEventRowsByRunID": 1},
		"TestOperatorRuntimeContextManagerRoutesExistingRunByStoredBundle":                    {"countEventRowsByRunID": 1},
		"TestOperatorRuntimeContextManagerRejectsExistingRunUnavailableSourceStates":          {"countEventRowsByRunID": 1},
		"TestOperatorRuntimeContextManagerRejectsExistingRunRequestedHashMismatch":            {"countEventRowsByRunID": 1},
		"TestOperatorRuntimeContextManagerFailsClosedForDeactivatedBundle":                    {"countEventRowsByRunID": 1}}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("exact-run consumers differ: got=%v want=%v", actual, expected)
	}
}
