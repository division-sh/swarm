package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func assertFamily145RefusalProofs(t testing.TB, name, before, after string) {
	t.Helper()
	if !family145RefusalProofsPreserved(name, before, after) {
		t.Fatal("refusal witness retained raw/foreign authority or lost an exact zero-mutation assertion")
	}
}

func family145RefusalProofsPreserved(name, before, after string) bool {
	var owner string
	switch name {
	case "assertNoForkExecutionRowsForRun":
		owner = "storetest.ReadSelectedForkRefusalStorage(ctx, selected, forkRunID)"
	case "assertNoSelectedContractExecutionMutationForSource":
		owner = "storetest.ReadSelectedSourceRefusalStorage(ctx, selected, sourceRunID, sourceEventID)"
	}
	return owner != "" && strings.Contains(after, owner) && !strings.Contains(after, "*sql.DB") &&
		!strings.Contains(after, "QueryRowContext") && !strings.Contains(after, "context.Background()") &&
		reflect.DeepEqual(family145RefusalBusinessAssertions(before), family145RefusalBusinessAssertions(after))
}

func family145RefusalBusinessAssertions(source string) []string {
	var out []string
	ast.Inspect(sourceOutcomeAST(source), func(node ast.Node) bool {
		conditional, ok := node.(*ast.IfStmt)
		if ok && (family145NodeString(conditional.Cond) == "count != 0" || family145NodeString(conditional.Cond) == "forkEvents != 0") {
			out = append(out, family145NodeString(conditional))
		}
		return true
	})
	return out
}

func family145SQLLiterals(source string) map[string]bool {
	file := source
	if !strings.HasPrefix(source, "package ") {
		file = "package proof\n" + source
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), "predicate.go", file, parser.AllErrors)
	if err != nil {
		panic(err)
	}
	out := map[string]bool{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok {
			return true
		}
		text, err := strconv.Unquote(literal.Value)
		if err == nil && strings.Contains(text, "SELECT COUNT(*)") {
			text = strings.ReplaceAll(text, "::uuid", "")
			text = strings.Join(strings.Fields(text), "")
			out[text] = true
		}
		return true
	})
	return out
}

func TestFamily145RefusalMetadataOwnersExecuteEveryCountAndRejectSyntheticZero(t *testing.T) {
	source, err := os.ReadFile("../../../internal/store/internal/backend/runforkpersistence/selected_refusal_observation.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "owner.go", source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var owners int
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		var coordinate string
		switch fn.Name.Name {
		case "ReadSelectedSourceRefusalStorageTx":
			coordinate = "sourceRunID"
		case "ReadSelectedForkRefusalStorageTx":
			coordinate = "forkRunID"
		default:
			continue
		}
		owners++
		var loop string
		for _, statement := range fn.Body.List {
			if repeated, ok := statement.(*ast.RangeStmt); ok {
				loop = family145NodeString(repeated)
			}
		}
		want := "for name, query := range queries { var count int; if err := tx.QueryRowContext(ctx, query, " + coordinate + ").Scan(&count); err != nil { return nil, err }; out[name] = count }"
		canonical := sourceOutcomeAST("func proof() {" + want + "}").Decls[0].(*ast.FuncDecl).Body.List[0]
		if loop != family145NodeString(canonical) {
			t.Fatalf("%s may skip, alter, hide or synthesize an original count", fn.Name.Name)
		}
		if strings.Replace(loop, "out[name] = count", "out[name] = 0", 1) == family145NodeString(canonical) ||
			strings.Replace(loop, "range queries", "range omitted", 1) == family145NodeString(canonical) {
			t.Fatal("synthetic-zero or incomplete execution counterexample was accepted")
		}
	}
	if owners != 2 {
		t.Fatalf("refusal metadata owners=%d, want two", owners)
	}
}
