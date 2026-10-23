package main

import (
	"github.com/division-sh/swarm/internal/checkoutsource"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
