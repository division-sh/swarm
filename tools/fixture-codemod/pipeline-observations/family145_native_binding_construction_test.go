package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func family145BindingBehavior(source string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "bindings.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	var assertions []string
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || family145NodeString(call.Fun) != "t.Fatalf" {
			return true
		}
		formatted := family145NodeString(call)
		for _, transport := range []string{"open served PostgreSQL handle", "open standalone PostgreSQL handle", "ping served PostgreSQL handle", "ping standalone PostgreSQL handle"} {
			if strings.Contains(formatted, transport) {
				return true
			}
		}
		assertions = append(assertions, formatted)
		return true
	})
	return assertions, nil
}

func family145NativeBindingConstructionError(function, before, after string) error {
	old, oldErr := family145BindingBehavior(before)
	native, nativeErr := family145BindingBehavior(after)
	if oldErr != nil || nativeErr != nil || strings.Join(old, "\n") != strings.Join(native, "\n") {
		return fmt.Errorf("native binding construction lost a behavioral or persistence assertion")
	}
	for _, raw := range []string{"*sql.DB", "sql.Open", "db.", "servedDB", "standaloneDB", "AdmitPostgresRuntimeStore", "run_fork_selected_contract_bindings", "ForkMaterializationRunOrigin", "InsertExistingRunRootEventRecord"} {
		if strings.Contains(after, raw) {
			return fmt.Errorf("native binding construction retained %s", raw)
		}
	}
	for _, owner := range []string{"storetest.CommitSemanticEvent(t, ctx, selected,", "captureSelectedExecutionSourceRevision(t, selected, sourceRunID)", "selected.MaterializeRunFork(ctx,", "ContractSelection: &selection", "forkRunID := fork.ForkRunID"} {
		if !strings.Contains(after, owner) {
			return fmt.Errorf("native binding construction lost its exact original source or fork owner: %s", owner)
		}
	}
	if function == "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority" {
		for _, required := range []string{
			"selected, openPeer := storetest.StartPostgresRuntimeStoreWithReopen(t)",
			"servedStore := openPeer()", "standaloneStore := openPeer()",
			"storetest.CountClosedFirstSelectedRuntimeExecutionStorage(ctx, selected, forkRunID)",
			"storetest.CountSelectedExecutionSettledAttemptsStorage(ctx, selected, proof.RuntimeExecutionID)",
			"storetest.CountCompletionAttemptBudgetReservationsStorage(ctx, selected, handle.Attempt().AttemptID)",
			"authorities != 1 || attempts != 1 || reservations != 0", "providerLaunches.Load() != 1",
		} {
			if !strings.Contains(after, required) {
				return fmt.Errorf("competition lost independent peers, original scope or exact settlement: %s", required)
			}
		}
	}
	return nil
}

func TestFamily145NativeBindingConstructionAndCompetitionStayClosed(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var reviewed int
	for _, row := range rows {
		if row.Function != "buildSelectedForkProofContainer" && row.Function != "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority" {
			continue
		}
		reviewed++
		if err := family145NativeBindingConstructionError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, pair := range [][2]string{
			{"selected.MaterializeRunFork(ctx,", "foreign.MaterializeRunFork(ctx,"},
			{"forkRunID := fork.ForkRunID", "forkRunID := uuid.NewString()"},
		} {
			if err := family145NativeBindingConstructionError(row.Function, row.Before, strings.Replace(row.After, pair[0], pair[1], 1)); err == nil {
				t.Fatal("foreign materializer or fabricated fork accepted")
			}
		}
		if row.Function == "TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority" {
			for _, pair := range [][2]string{
				{"standaloneStore := openPeer()", "standaloneStore := servedStore"},
				{"ctx, selected, proof.RuntimeExecutionID", "ctx, selected, foreignExecutionID"},
				{"authorities != 1 || attempts != 1 || reservations != 0", "authorities != 1 || attempts != 1"},
			} {
				if err := family145NativeBindingConstructionError(row.Function, row.Before, strings.Replace(row.After, pair[0], pair[1], 1)); err == nil {
					t.Fatal("collapsed peers, foreign execution or lost settlement predicate accepted")
				}
			}
		}
	}
	if reviewed != 2 {
		t.Fatalf("native component binding inventory=%d want2", reviewed)
	}
	paths := []string{"execution_test.go", "selected_execution_authority_fixture_test.go"}
	for _, path := range paths {
		data, err := os.ReadFile("../../../internal/runtime/runforkexecution/" + path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "INSERT INTO run_fork_selected_contract_bindings") {
			t.Fatalf("independent component binding writer survived in %s", path)
		}
	}
}
