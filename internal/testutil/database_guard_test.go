package testutil

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func TestDatabaseAcquisitionRequiresTypedRequirement(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	var violations []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !databaseGuardScansFile(root, path) {
			return err
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := databaseGuardCallName(call.Fun)
			position := fset.Position(call.Pos())
			fail := func(reason string) {
				rel, _ := filepath.Rel(root, path)
				violations = append(violations, fmt.Sprintf("%s:%d: %s", rel, position.Line, reason))
			}
			switch name {
			case "StartPostgres", "StartEmptyPostgres":
				fail("retired untyped PostgreSQL acquisition")
			case "AcquirePostgres":
				if len(call.Args) < 2 {
					fail("PostgreSQL acquisition omits DatabaseRequirement")
				}
			case "StartSQLiteRuntimeStore":
				if len(call.Args) < 2 {
					fail("SQLite store acquisition omits DatabaseRequirement")
				}
			case "StartSQLiteRuntimeStoreWithContext":
				if len(call.Args) < 3 {
					fail("SQLite store acquisition omits DatabaseRequirement")
				}
			case "newBootstrappedSQLiteRuntimeStoreForTest":
				if len(call.Args) < 2 {
					fail("SQLite temp helper omits DatabaseRequirement")
				}
			case "newBootstrappedSQLiteRuntimeStoreForPath":
				if len(call.Args) < 3 {
					fail("SQLite path helper omits DatabaseRequirement")
				}
			case "NewSQLiteRuntimeStore":
				if len(call.Args) == 0 || !databaseGuardTypedSQLitePath(call.Args[0], file) {
					fail("SQLite runtime constructor bypasses typed path declaration")
				}
			case "Open":
				if databaseGuardIsSQLiteOpen(call) && !databaseGuardCallIs(call.Args[1], "SQLiteDeclaredDSN") {
					fail("direct SQLite open bypasses typed DSN declaration")
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(violations) > 0 {
		t.Fatalf("untyped test database acquisition bypasses:\n%s", strings.Join(violations, "\n"))
	}
}

func databaseGuardCallName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return value.Sel.Name
	default:
		return ""
	}
}

func databaseGuardCallIs(expr ast.Expr, name string) bool {
	call, ok := expr.(*ast.CallExpr)
	return ok && databaseGuardCallName(call.Fun) == name
}

func databaseGuardTypedSQLitePath(expr ast.Expr, file *ast.File) bool {
	if databaseGuardCallIs(expr, "SQLiteDeclaredPath") || databaseGuardCallIs(expr, "SQLitePath") {
		return true
	}
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	typed := false
	ast.Inspect(file, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) == 0 || len(assign.Rhs) == 0 {
			return true
		}
		lhs, ok := assign.Lhs[0].(*ast.Ident)
		if ok && lhs.Name == ident.Name && (databaseGuardCallIs(assign.Rhs[0], "DeclareSQLitePath") || databaseGuardCallIs(assign.Rhs[0], "SQLitePath")) {
			typed = true
			return false
		}
		return true
	})
	return typed
}

func databaseGuardScansFile(root, path string) bool {
	if !strings.HasSuffix(path, ".go") {
		return false
	}
	if strings.HasSuffix(path, "_test.go") {
		return true
	}
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	return strings.HasPrefix(rel, filepath.Join("internal", "testutil")+string(filepath.Separator)) ||
		strings.HasPrefix(rel, filepath.Join("internal", "store", "storetest")+string(filepath.Separator))
}

func databaseGuardIsSQLiteOpen(call *ast.CallExpr) bool {
	if len(call.Args) < 2 {
		return false
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return false
	}
	driver, err := strconv.Unquote(literal.Value)
	return err == nil && driver == "sqlite"
}
