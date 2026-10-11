package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
	var transitions []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &transitions); err != nil {
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
