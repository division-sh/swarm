package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
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

func TestFamily145PreparationSnapshotRetiresInterpreterAndAllFifteenPoolCallers(t *testing.T) {
	var transitions []preparationSnapshotTransition
	if err := json.Unmarshal(family145TransitionBytes, &transitions); err != nil {
		t.Fatal(err)
	}
	transitions, err := composedPreparationSnapshotRows(transitions)
	if err != nil {
		t.Fatal(err)
	}
	var owners, callers int
	for _, row := range transitions {
		if row.Function == "selectedPreparationDatabaseSnapshot" {
			owners++
			if err := family145PreparationSnapshotOwnerError(row.After); err != nil {
				t.Fatal(err)
			}
			for _, hostile := range []string{
				strings.Replace(row.After, "context.Background(), selected", "context.Background(), rawPool", 1),
				strings.Replace(row.After, "ReadSelectedForkApplicationStorageSnapshot", "ReadOnlyConvenientTables", 1),
			} {
				if err := family145PreparationSnapshotOwnerError(hostile); err == nil {
					t.Fatal("raw owner or partial snapshot accepted")
				}
			}
			continue
		}
		before, err := family145PreparationSnapshotCalls(row.Before, false, "")
		if err != nil {
			t.Fatal(err)
		}
		if before == 0 {
			continue
		}
		owner := "selected"
		if row.Function == "TestSelectedForkCommittedProcessDeathBothStores" {
			owner = "capabilityStore"
		}
		after, err := family145PreparationSnapshotCalls(row.After, true, owner)
		if err != nil || before != after {
			t.Fatalf("lost original snapshot occurrence or owner: %s/%s/%d/%d/%v", row.File, row.Function, before, after, err)
		}
		callers += after
		if _, err := family145PreparationSnapshotCalls(strings.ReplaceAll(row.After, "DatabaseSnapshot(t, "+owner+")", "DatabaseSnapshot(t, rawPool)"), true, owner); err == nil {
			t.Fatal("raw or foreign snapshot caller accepted")
		}
	}
	if owners != 1 || callers != 15 {
		t.Fatalf("preparation snapshot owner/caller inventory=%d/%d want1/15", owners, callers)
	}
	files, err := filepath.Glob("../../../internal/runtime/runforkexecution/*test.go")
	if err != nil {
		t.Fatal(err)
	}
	var live int
	for _, path := range files {
		source, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range file.Decls {
			fn, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			owner := "selected"
			if fn.Name.Name == "TestSelectedForkCommittedProcessDeathBothStores" {
				owner = "capabilityStore"
			}
			count, err := family145PreparationSnapshotCalls(family145NodeString(fn), true, owner)
			if err != nil {
				t.Fatalf("unmigrated snapshot caller: %s/%s/%v", path, fn.Name, err)
			}
			live += count
		}
	}
	if live != callers {
		t.Fatalf("unaccounted snapshot entry points: %d want%d", live, callers)
	}
	for _, path := range []string{"preparation_process_parity_test.go", "source_cleanup_test.go"} {
		source, err := os.ReadFile(filepath.Join("../../../internal/runtime/runforkexecution", path))
		if err != nil {
			t.Fatal(err)
		}
		for _, raw := range []string{"*sql.DB", "BeginTx", "storetest.Database", "AdmitPostgresRuntimeStore", "testutil.StartPostgres"} {
			if strings.Contains(string(source), raw) {
				t.Fatalf("snapshot-only constructor family retained %s in %s", raw, path)
			}
		}
	}
}

func TestFamily145PreparationSnapshotCompositionRejectsChangedLaterRows(t *testing.T) {
	var rows []preparationSnapshotTransition
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	for index, row := range rows {
		if row.Function != "proveSelectedForkClaudeOAuth" || !strings.Contains(row.Before, "selectedPreparationDatabaseSnapshot(t, selected)") {
			continue
		}
		for _, field := range []string{"file", "function", "before"} {
			mutant := slices.Clone(rows)
			switch field {
			case "file":
				mutant[index].File += ".foreign"
			case "function":
				mutant[index].Function += "Foreign"
			case "before":
				mutant[index].Before += "\n"
			}
			if _, err := composedPreparationSnapshotRows(mutant); err == nil {
				t.Fatal("changed/foreign later snapshot transition accepted:", field)
			}
		}
		return
	}
	t.Fatal("the exact later snapshot continuation is missing")
}
