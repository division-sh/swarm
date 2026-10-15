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

func TestFamily145DiagnosticLineagePreservesNativeOwnerExactCoordinatesAndAssertions(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		if row.Function != "TestExecuteSelectedContractRunForkTreatsDiagnosticPlatformOutcomeAsLineage" {
			continue
		}
		found++
		if err := family145DiagnosticLineageError(row.After); err != nil {
			t.Fatal(err)
		}
		for _, pair := range [][2]string{
			{"CountRunEventOrSourceEventStorage", "CountRunExactEventStorage"},
			{"ForkRunID, diagnosticEventID)", "ForkRunID, sourceEventID)"},
			{"ForkRunID, sourceEventID)", "ForkRunID, diagnosticEventID)"},
			{"diagnosticCopies != 0", "diagnosticCopies > 1"},
			{"diagnosticExecutionLineage != 0", "diagnosticExecutionLineage > 1"},
			{"selectedExecutionLineage != 1", "selectedExecutionLineage > 1"},
		} {
			if err := family145DiagnosticLineageError(strings.Replace(row.After, pair[0], pair[1], 1)); err == nil {
				t.Fatal("diagnostic lineage accepted a changed scope, coordinate or assertion")
			}
		}
		old, oldErr := runSetupAssertions(row.Before)
		native, nativeErr := runSetupAssertions(row.After)
		if oldErr != nil || nativeErr != nil || strings.Join(old, "\n") != strings.Join(native, "\n") {
			t.Fatal("diagnostic lineage lost an original frontier, route, diagnostic or lineage assertion")
		}
	}
	if found != 1 {
		t.Fatalf("diagnostic lineage inventory=%d want1", found)
	}
}
