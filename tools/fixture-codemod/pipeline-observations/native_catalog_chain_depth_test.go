package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const nativeChainDepthProjection = `relation, err := storetest.ReadChainDepthDeadLetterStorage(testAuthorActivityContext(context.Background()), selected, catalogRuntimeRunID, entityID)
if err != nil { t.Fatalf("query chain_depth_exceeded dead-letter relation: %v", err) }
relationCount, chainDepth := relation.Count, relation.Depth
handlerNode, failureClass, originalEvent := relation.HandlerNode, relation.FailureClass, relation.OriginalEvent
diagnosticCount, err := storetest.CountChainDepthDiagnosticStorage(testAuthorActivityContext(context.Background()), selected, catalogRuntimeRunID, entityID, handlerNodeID)
if err != nil { t.Fatalf("query chain_depth_exceeded diagnostic: %v", err) }
activityCount, err := storetest.CountRecordedDeadLetterStorage(testAuthorActivityContext(context.Background()), selected, catalogRuntimeRunID)
if err != nil { t.Fatalf("query chain_depth_exceeded author activity: %v", err) }
`

func catalogChainDepthRecipePreservesAssertions(row recipe) bool {
	source, valid := restoreRootReplyProjection(row, row.After, "\trelation, err :=", "\n\tgot :=", nativeChainDepthProjection, "\tquery := catalogDialectQuery")
	if !valid {
		return false
	}
	source = strings.Replace(source, "selected catalogOperatorEventLister", "db *sql.DB", 1)
	want, err := canonicalFunction(row.Before)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeCatalogChainDepthRecipePreservesExactConjunction(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-catalog-chain-depth-observation" {
			continue
		}
		count++
		if !catalogChainDepthRecipePreservesAssertions(row) {
			t.Fatal("chain witness changed run/entity, failure, ownership, exact conjunction or error propagation")
		}
		for _, pair := range [][2]string{
			{"selected, catalogRuntimeRunID, entityID", "foreignOwner, foreignRunID, entityID"},
			{"entityID, handlerNodeID", "entityID, foreignHandlerNode"},
			{"relationCount == 1", "relationCount >= 1"}, {"diagnosticCount == 1", "diagnosticCount >= 1"},
			{"activityCount == 1", "activityCount >= 1"}, {"chainDepth == 6", "chainDepth >= 6"},
			{"handlerNodeID+\":chain.e6\"", "handlerNodeID"}, {"originalEvent == \"chain.e6\"", "originalEvent != \"\""},
			{"if got != want", "if got == want"}, {"if err != nil", "if false"},
		} {
			mutant := row
			mutant.After = strings.Replace(row.After, pair[0], pair[1], 1)
			if mutant.After == row.After || catalogChainDepthRecipePreservesAssertions(mutant) {
				t.Fatalf("weakened chain proof accepted: %v", pair)
			}
		}
	}
	if count != 1 {
		t.Fatalf("chain witness recipes=%d, want one complete shared consumer", count)
	}
}

func chainStorageQueries(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", "package witness\n"+source, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		text, err := strconv.Unquote(literal.Value)
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(strings.TrimSpace(text), "SELECT COUNT(*)") {
			out = append(out, strings.Join(strings.Fields(text), ""))
		}
		return true
	})
	return out
}

func TestNativeCatalogChainDepthOwnersKeepEveryOriginalSQLPredicate(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original []string
	for _, row := range rows {
		if row.Family == "native-catalog-chain-depth-observation" {
			original = chainStorageQueries(t, row.Before)
		}
	}
	if len(original) != 6 {
		t.Fatalf("original chain SQL=%d, want all three dialect pairs", len(original))
	}
	for i, owner := range []struct{ path, function string }{
		{"delivery/dead_letter_owner.go", "ReadChainDepthDeadLetterStorage"},
		{"eventrecord/causal_observation.go", "CountChainDepthDiagnosticStorage"},
		{"authoractivity/readadapter/read.go", "CountRecordedDeadLetterStorage"},
	} {
		data, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", owner.path))
		if err != nil {
			t.Fatal(err)
		}
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, owner.path, data, 0)
		if err != nil {
			t.Fatal(err)
		}
		fn, err := uniqueFunction(file, owner.function)
		if err != nil {
			t.Fatal(err)
		}
		function := string(data[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
		queries := chainStorageQueries(t, function)
		if len(queries) != 2 || queries[0] != original[2*i] || queries[1] != original[2*i+1] {
			t.Fatalf("%s changed an original dialect predicate", owner.function)
		}
	}
}
