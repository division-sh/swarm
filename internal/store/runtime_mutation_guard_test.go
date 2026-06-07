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

func TestSQLiteRuntimeMutationWritersConsumeCanonicalBoundary(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("list store files: %v", err)
	}
	allowed := map[string]string{
		"BeginEventTx":                 "legacy TransactionalEventStore fallback; SQLite production publish uses RunEventTransaction",
		"runRuntimeMutationOnceLocked": "canonical SQLite runtime mutation executor opens the owned transaction while serialized",
		"runRuntimeMutationOnce":       "canonical SQLite runtime mutation executor flushes successful post-commit actions",
		"runRuntimeMutation":           "canonical SQLite runtime mutation executor",
		"RunRuntimeMutation":           "canonical SQLite runtime mutation executor",
		"RunEventTransaction":          "canonical SQLite event transaction executor",
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !sqliteRuntimeStoreReceiver(fn) {
				continue
			}
			if _, ok := allowed[fn.Name.Name]; ok {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch selectorPath(sel) {
				case "s.DB.BeginTx", "s.DB.Exec", "s.DB.ExecContext":
					pos := fset.Position(sel.Pos())
					t.Fatalf("%s:%d: SQLiteRuntimeStore.%s bypasses RunRuntimeMutation with %s", file, pos.Line, fn.Name.Name, selectorPath(sel))
				}
				return true
			})
		}
	}
}

func TestProductionSQLitePipelineStoreConsumesRuntimeMutationRunner(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "cmd", "swarm", "main.go"))
	if err != nil {
		t.Fatalf("read cmd/swarm/main.go: %v", err)
	}
	text := string(data)
	if !strings.Contains(text, "NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(sqliteStore.DB, sqliteStore)") {
		t.Fatal("cmd/swarm SQLite store construction must wire WorkflowInstanceStore through SQLiteRuntimeStore.RunRuntimeMutation")
	}
	if strings.Contains(text, "NewSQLiteWorkflowInstanceStore(sqliteStore.DB)") {
		t.Fatal("cmd/swarm SQLite store construction uses legacy WorkflowInstanceStore without the runtime mutation boundary")
	}
}

func sqliteRuntimeStoreReceiver(fn *ast.FuncDecl) bool {
	if fn == nil || fn.Recv == nil || len(fn.Recv.List) == 0 {
		return false
	}
	switch expr := fn.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		if ident, ok := expr.X.(*ast.Ident); ok {
			return ident.Name == "SQLiteRuntimeStore"
		}
	case *ast.Ident:
		return expr.Name == "SQLiteRuntimeStore"
	}
	return false
}

func selectorPath(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.SelectorExpr:
		base := selectorPath(e.X)
		if base == "" {
			return e.Sel.Name
		}
		return base + "." + e.Sel.Name
	default:
		return ""
	}
}
