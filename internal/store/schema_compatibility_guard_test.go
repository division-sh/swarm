package store

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacySchemaRepairRootsHaveNoProductionCallers(t *testing.T) {
	forbidden := map[string]struct{}{
		"ensurePostgresCanonicalFailureSchema": {},
		"ensureSQLiteCanonicalFailureSchema":   {},
		"ensureSchemaCompatibilityColumns":     {},
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	files := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(files, filepath.Clean(name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			var called string
			switch fn := call.Fun.(type) {
			case *ast.Ident:
				called = fn.Name
			case *ast.SelectorExpr:
				called = fn.Sel.Name
			}
			if _, blocked := forbidden[called]; blocked {
				t.Errorf("production schema repair root %s remains callable at %s", called, files.Position(call.Pos()))
			}
			return true
		})
	}
}
