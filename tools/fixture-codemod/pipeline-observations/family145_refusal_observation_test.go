package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

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
