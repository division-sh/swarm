package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func family145DurableDataConstructionProofError(before, after string) error {
	if strings.Contains(after, "AdmitPostgresRuntimeStore(") || strings.Count(after, "StartPostgresRuntimeStoreWithReopen(t, dsn)") != 1 {
		return fmt.Errorf("durable-data shared construction retained supplied authority or lost the native location")
	}
	normalized := strings.Replace(after, "dsn, db, _ := testutil.StartPostgres(t)", "_, db, _ := testutil.StartPostgres(t)", 1)
	normalized = strings.Replace(normalized, "selected, _ := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)", "selected := storetest.AdmitPostgresRuntimeStore(t, db)", 1)
	if normalized != before {
		return fmt.Errorf("durable-data construction changed backend choice, callback, setup/readback or assertions")
	}
	return nil
}

func TestFamily145DurableDataConstructionPreservesEntireUnchangedConsumerFamily(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	prior, err := selectedreadrole.BeforeDurableResourceHistory("../../..", family145TransitionBytes, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(prior, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "forEachDurableDataStore" {
			continue
		}
		matched++
		if err := family145DurableDataConstructionProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range []string{"StartPostgresRuntimeStoreWithReopen(t, dsn)", "run(t, selected, db)", "StartSQLiteRuntimeStore(t)", "run(t, selected, storetest.Database(selected))", "t.Run(\"postgres\"", "t.Run(\"sqlite\""} {
			if !strings.Contains(row.After, cut) || family145DurableDataConstructionProofError(row.Before, strings.Replace(row.After, cut, "foreignDurableDataOwner()", 1)) == nil {
				t.Fatalf("durable-data backend/location/consumer mutation survived: %s", cut)
			}
		}
		actual := family145ConversationForkOwnerSource(t, "durable_data_selected_store_test.go", row.Function)
		if actual != row.After {
			t.Fatal("durable-data shared construction escaped its exact source")
		}
	}
	if matched != 1 {
		t.Fatalf("durable-data constructor=%d/1", matched)
	}
	callers := family145DurableDataCallerInventory(t)
	raw, err := json.Marshal(callers)
	if err != nil {
		t.Fatal(err)
	}
	if len(callers) != 22 || fmt.Sprintf("%x", sha256.Sum256(raw)) != "aa02e101d7b0279ae7f31d7bbdf3fca4e3b8d91a2926fa06e23b771ae56c56b2" {
		t.Fatalf("durable-data callers=%d digest=%x; original caller bodies/arguments/registration must remain unchanged", len(callers), sha256.Sum256(raw))
	}
}

func family145DurableDataCallerInventory(t *testing.T) map[string]string {
	t.Helper()
	root := filepath.Join("..", "..", "..")
	callers := map[string]string{}
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "store"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		source, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.AllErrors)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && family145DurableDataCallerReferences(t, function) != 0 {
				key := filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator))) + "/" + function.Name.Name
				current, err := selectedreadrole.EarlierDurableResourceSource(root, filepath.ToSlash(strings.TrimPrefix(path, root+string(filepath.Separator))), function.Name.Name, family145NodeString(function))
				if err != nil {
					return err
				}
				callers[key] = current
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return callers
}

func family145DurableDataCallerReferences(t *testing.T, function *ast.FuncDecl) int {
	t.Helper()
	references, calls := 0, 0
	ast.Inspect(function.Body, func(node ast.Node) bool {
		if ident, ok := node.(*ast.Ident); ok && ident.Name == "forEachDurableDataStore" {
			references++
		}
		if call, ok := node.(*ast.CallExpr); ok && family145NodeString(call.Fun) == "forEachDurableDataStore" {
			calls++
			if len(call.Args) != 2 || family145NodeString(call.Args[0]) != "t" {
				t.Fatal("durable-data caller changed its owning test or argument shape")
			}
		}
		return true
	})
	if references != calls {
		t.Fatal("durable-data constructor acquired an indirect/aliased consumer")
	}
	return calls
}
