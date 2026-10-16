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

func family145DiagnosticLineageError(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "lineage.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return err
	}
	want := map[string]int{
		"storetest.StartPostgresRuntimeStore(t)": 1,
		"storetest.CountRunEventOrSourceEventStorage(ctx, pg, result.Materialization.ForkRunID, diagnosticEventID)":          1,
		"storetest.CountSelectedForkSourceEventLineageStorage(ctx, pg, result.Materialization.ForkRunID, diagnosticEventID)": 1,
		"storetest.CountSelectedForkSourceEventLineageStorage(ctx, pg, result.Materialization.ForkRunID, sourceEventID)":     1,
	}
	conditions := map[string]int{"diagnosticCopies != 0": 1, "diagnosticExecutionLineage != 0": 1, "selectedExecutionLineage != 1": 1}
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
	for call, remaining := range want {
		if remaining != 0 {
			return fmt.Errorf("diagnostic lineage lost original native owner or exact coordinate: %s", call)
		}
	}
	for condition, remaining := range conditions {
		if remaining != 0 {
			return fmt.Errorf("diagnostic lineage lost exact absence or success assertion: %s", condition)
		}
	}
	for _, raw := range []string{"db.", "*sql.DB", "AdmitPostgresRuntimeStore", "testutil.StartPostgres", "storetest.Database"} {
		if strings.Contains(source, raw) {
			return fmt.Errorf("diagnostic lineage retained raw capability %s", raw)
		}
	}
	return nil
}
