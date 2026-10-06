package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"testing"
)

var nativeReadSetupRoots = map[string]string{
	"TestSelectedStoreRunReadHandlersExecuteAcrossBackends":                         "internal/apiv1/selected_store_read_supported_surface_test.go",
	"TestFanOutReadAPISelectedStores":                                               "internal/apiv1/operator_fan_out_test.go",
	"TestOperatorEntityHandlersServeContractEntityTypesFromPostgres":                "internal/apiv1/operator_entity_test.go",
	"TestOperatorRunControlHandlersTypedResourceErrors":                             "internal/apiv1/operator_run_control_test.go",
	"TestOperatorRunStopDoesNotReplayCommittedTransitionAfterReconciliationFailure": "internal/apiv1/operator_run_control_test.go",
	"TestOperatorRunStartHandlersLeaveSplitControlMethodsUnavailable":               "internal/apiv1/operator_run_start_test.go",
}

func TestNativeReadSetupRecipesPreserveEveryWorkloadStatement(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-api-sql-free-setup" {
			continue
		}
		if nativeReadSetupRoots[row.Function] != row.File || seen[row.Function] {
			t.Fatalf("unknown or duplicated native setup: %s/%s", row.File, row.Function)
		}
		seen[row.Function] = true
		expected, err := nativeReadSetupAfter(row.Before)
		actual, parseErr := canonicalFunction(row.After)
		if err != nil || parseErr != nil || actual != expected {
			t.Fatalf("native setup changed workload/assertion statements: %s: %v / %v", row.Function, err, parseErr)
		}
	}
	if len(seen) != len(nativeReadSetupRoots) {
		t.Fatalf("native setup family count=%d, want %d", len(seen), len(nativeReadSetupRoots))
	}
}

func nativeReadSetupAfter(source string) (string, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "setup.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return "", err
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	count := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for i := 0; i < len(block.List); i++ {
			consumed, replacement := nativeReadSetupStatements(block.List[i:])
			if consumed != 0 {
				block.List = append(append(block.List[:i:i], replacement), block.List[i+consumed:]...)
				count++
			}
		}
		return true
	})
	if count != 1 {
		return "", fmt.Errorf("native setup count=%d, want one", count)
	}
	raw := false
	ast.Inspect(fn, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && (id.Name == "db" || id.Name == "cleanup") {
			raw = true
		}
		return true
	})
	if raw {
		return "", fmt.Errorf("native setup still has a raw pool or cleanup use")
	}
	return formattedNativeReadNode(fn), nil
}

func nativeReadSetupStatements(statements []ast.Stmt) (int, ast.Stmt) {
	if len(statements) < 2 {
		return 0, nil
	}
	start := formattedNativeReadNode(statements[0])
	consumed := 2
	if start == "_, db, cleanup := testutil.StartPostgres(t)" {
		if len(statements) < 3 || formattedNativeReadNode(statements[1]) != "t.Cleanup(cleanup)" {
			return 0, nil
		}
		consumed = 3
	} else if start != "_, db, _ := testutil.StartPostgres(t)" {
		return 0, nil
	}
	admitted, ok := statements[consumed-1].(*ast.AssignStmt)
	if !ok || len(admitted.Lhs) != 1 || len(admitted.Rhs) != 1 || admitted.Tok != token.DEFINE {
		return 0, nil
	}
	owner, ok := admitted.Lhs[0].(*ast.Ident)
	if !ok || (owner.Name != "selected" && owner.Name != "pg") || formattedNativeReadNode(admitted.Rhs[0]) != "storetest.AdmitPostgresRuntimeStore(t, db)" {
		return 0, nil
	}
	expr, _ := parser.ParseExpr("storetest.StartPostgresRuntimeStoreWithReopen(t)")
	return consumed, &ast.AssignStmt{Lhs: []ast.Expr{ast.NewIdent(owner.Name), ast.NewIdent("_")}, Tok: token.DEFINE, Rhs: []ast.Expr{expr}}
}

func formattedNativeReadNode(node ast.Node) string {
	var out bytes.Buffer
	if err := format.Node(&out, token.NewFileSet(), node); err != nil {
		panic(err)
	}
	return out.String()
}

func TestNativeReadSetupRefusesLivePoolAndChangedConstruction(t *testing.T) {
	for _, source := range []string{
		`func proof(t any) { _, db, _ := testutil.StartPostgres(t); pg := storetest.AdmitPostgresRuntimeStore(t, db); consume(pg); _ = db.Ping() }`,
		`func proof(t any) { _, db, cleanup := testutil.StartPostgres(t); extra(); t.Cleanup(cleanup); pg := storetest.AdmitPostgresRuntimeStore(t, db); consume(pg) }`,
		`func proof(t any) { _, db, _ := testutil.StartPostgres(other); pg := storetest.AdmitPostgresRuntimeStore(t, db); consume(pg) }`,
		`func proof(t any) { _, db, _ := testutil.StartPostgres(t); pg := storetest.AdmitPostgresRuntimeStore(t, replacement()); consume(pg) }`,
	} {
		if _, err := nativeReadSetupAfter(source); err == nil {
			t.Fatalf("unreviewed setup matched: %s", source)
		}
	}
}
