package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
)

func checkManagerDeliveryRecipe(t *testing.T, row recipe) {
	t.Helper()
	actualName := "ProveNative" + strings.TrimPrefix(row.Function, "Test")
	actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, actualName))
	current := row.After
	if row.Successor != "" {
		current = row.Successor
	}
	expected, wantErr := canonicalFunction(current)
	if err != nil || wantErr != nil || actual != expected {
		t.Fatalf("manager caller source diverged: %s", row.Function)
	}
	mutant := strings.Replace(row.After, "newNativeDelivery(t)", "newForeignDelivery(t)", 1)
	if mutant == row.After {
		t.Fatal("manager authority mutant stopped matching")
	}
	if _, changed, err := rewriteFunction(row.File, []byte("package probe\n"+mutant), row); err == nil || changed {
		t.Fatalf("foreign construction accepted: %s", row.Function)
	}
}

func TestManagerDeliveryWholeFamilySemanticRepairsStaySourcePinned(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, row := range rows {
		if row.Family != "native-manager-delivery-owner" {
			continue
		}
		count++
		checkManagerDeliveryRecipe(t, row)
	}
	if count != 10 {
		t.Fatalf("semantic manager roots=%d,want10", count)
	}
}

func TestManagerDeliveryFakePersistenceCapabilityIsRetired(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal", "runtime", "manager"), func(path string, entry fs.DirEntry, failure error) error {
		if failure != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return failure
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, data, parser.AllErrors)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			if id, ok := node.(*ast.Ident); ok && (id.Name == "managerDeliveryTestStore" || id.Name == "newManagerDeliveryTestStore" || id.Name == "ensureDelivery" || id.Name == "seedSelectedExecution") {
				t.Errorf("retired manager persistence escape remains: %s/%s", path, id.Name)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
