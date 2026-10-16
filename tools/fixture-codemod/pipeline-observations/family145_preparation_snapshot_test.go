package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
)

type preparationSnapshotTransition struct{ File, Function, Before, After string }

func composedPreparationSnapshotRows(rows []preparationSnapshotTransition) ([]preparationSnapshotTransition, error) {
	const prefix = "internal/runtime/runforkexecution/"
	keys := map[string]bool{
		prefix + "recovery_process_test.go/TestSelectedForkCommittedProcessDeathBothStores":                true,
		prefix + "source_cleanup_test.go/TestSelectedSourceCleanupBothStores":                              true,
		prefix + "preparation_process_parity_test.go/TestSelectedForkPreparationProcessBoundaryBothStores": true,
		prefix + "preparation_process_parity_test.go/selectedPreparationDatabaseSnapshot":                  true,
		prefix + "constructed_agent_input_execution_test.go/proveSelectedConstructedAgentInput":            true,
		prefix + "workspace_preparation_test.go/proveSelectedWorkspacePreparation":                         true,
		prefix + "execution_test.go/proveSelectedForkClaudeOAuth":                                          true,
	}
	var out []preparationSnapshotTransition
	positions := map[string]int{}
	for _, row := range rows {
		if !strings.Contains(row.Before, "selectedPreparationDatabaseSnapshot(") && !strings.Contains(row.After, "selectedPreparationDatabaseSnapshot(") {
			continue
		}
		key := row.File + "/" + row.Function
		if !keys[key] {
			return nil, fmt.Errorf("unlisted snapshot transition: %s", key)
		}
		if index, found := positions[key]; found {
			if out[index].After != row.Before {
				return nil, fmt.Errorf("snapshot continuation lost its exact intermediate source: %s", key)
			}
			out[index].After = row.After
		} else {
			positions[key] = len(out)
			out = append(out, row)
		}
	}
	if len(out) != len(keys) {
		return nil, fmt.Errorf("snapshot finite owner/caller transition inventory=%d want%d", len(out), len(keys))
	}
	return out, nil
}

func family145PreparationSnapshotOwnerError(source string) error {
	want := `func selectedPreparationDatabaseSnapshot(t testing.TB, selected any) map[string]storetest.SelectedForkStorageTableSnapshot {
		t.Helper()
		out, err := storetest.ReadSelectedForkApplicationStorageSnapshot(context.Background(), selected)
		if err != nil { t.Fatal(err) }
		return out
	}`
	actual, err := canonicalFunction(source)
	expected, wantErr := canonicalFunction(want)
	if err != nil || wantErr != nil || actual != expected {
		return fmt.Errorf("preparation snapshot retained another interpreter, changed its scope or substituted non-original ownership")
	}
	return nil
}

func family145PreparationSnapshotCalls(source string, native bool, owner string) (int, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "snapshot.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return 0, err
	}
	var count int
	var invalid bool
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || family145NodeString(call.Fun) != "selectedPreparationDatabaseSnapshot" {
			return true
		}
		count++
		if !native {
			invalid = invalid || len(call.Args) != 3 || family145NodeString(call.Args[0]) != "t" || family145NodeString(call.Args[1]) != "db" || family145NodeString(call.Args[2]) != "backend"
		} else {
			invalid = invalid || len(call.Args) != 2 || family145NodeString(call.Args[0]) != "t" || family145NodeString(call.Args[1]) != owner
		}
		return true
	})
	if invalid {
		return 0, fmt.Errorf("snapshot caller lost its exact original selected observer or testing scope")
	}
	return count, nil
}
