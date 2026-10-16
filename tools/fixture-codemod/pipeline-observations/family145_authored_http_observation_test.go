package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func family145AuthoredHTTPObservationError(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "http.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return err
	}
	want := map[string]int{
		"storetest.StartPostgresRuntimeStore(t)":            1,
		"buildSelectedForkProofContainer(t, ctx, selected)": 1,
		"storetest.CountSelectedAuthoredHTTPSettledSurfaceAttemptsStorage(ctx, selected, proof.RuntimeExecutionID, surface.ID)": 1,
		"storetest.CountSelectedExecutionAttemptsStorage(ctx, selected, proof.RuntimeExecutionID)":                              1,
		"storetest.CountSelectedAuthoredHTTPAttemptsStorage(ctx, selected, proof.RuntimeExecutionID)":                           1,
	}
	conditions := map[string]int{"requests.Load() != 1": 3, "persisted != 1": 2, "err != nil || effectCount != 1": 1}
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if _, exact := want[family145NodeString(call)]; exact {
				want[family145NodeString(call)]--
			}
		}
		if conditional, ok := node.(*ast.IfStmt); ok {
			if _, exact := conditions[family145NodeString(conditional.Cond)]; exact {
				conditions[family145NodeString(conditional.Cond)]--
			}
		}
		return true
	})
	for name, remaining := range want {
		if remaining != 0 {
			return fmt.Errorf("authored HTTP witness lost original native construction or exact read scope: %s", name)
		}
	}
	for name, remaining := range conditions {
		if remaining != 0 {
			return fmt.Errorf("authored HTTP witness lost exact physical count or no-dispatch assertion: %s", name)
		}
	}
	for _, raw := range []string{"*sql.DB", "db.", "AdmitPostgresRuntimeStore", "testutil.StartPostgres", "storetest.Database"} {
		if strings.Contains(source, raw) {
			return fmt.Errorf("authored HTTP proof retained %s", raw)
		}
	}
	return nil
}
