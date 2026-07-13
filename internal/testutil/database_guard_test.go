package testutil

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var databaseRequirementConstructors = map[string]bool{
	"SQLiteDefaultTemp":     true,
	"SQLiteFreshFile":       true,
	"SQLiteSharedFile":      true,
	"PostgresRowState":      true,
	"PostgresFreshPhysical": true,
	"PostgresEmptyPhysical": true,
}

var databasePrimitiveRequirementIndex = map[string]int{
	"AcquirePostgres":                          1,
	"StartSQLiteRuntimeStore":                  1,
	"StartSQLiteRuntimeStoreWithContext":       2,
	"newBootstrappedSQLiteRuntimeStoreForTest": 1,
	"newBootstrappedSQLiteRuntimeStoreForPath": 2,
	"SQLitePath":         1,
	"SQLiteDeclaredPath": 1,
	"DeclareSQLitePath":  0,
	"SQLiteDeclaredDSN":  1,
}

type databaseGuardSource struct {
	path string
	fset *token.FileSet
	file *ast.File
}

type databaseGuardFunction struct {
	name               string
	path               string
	fset               *token.FileSet
	file               *ast.File
	decl               *ast.FuncDecl
	requirementParams  map[string]bool
	requirementIndex   []int
	calls              []*ast.CallExpr
	directAcquisition  bool
	reachesAcquisition bool
}

func TestDatabaseAcquisitionRequiresTypedRequirement(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
	sources, err := databaseGuardLoadSources(root)
	if err != nil {
		t.Fatal(err)
	}
	violations := databaseGuardViolations(root, sources)
	if len(violations) > 0 {
		t.Fatalf("untyped test database acquisition bypasses:\n%s", strings.Join(violations, "\n"))
	}
}

func TestDatabaseGuardRejectsHiddenWrapperDefaults(t *testing.T) {
	_, thisFile, _, _ := runtime.Caller(0)
	fixtureRoot := filepath.Join(filepath.Dir(thisFile), "testdata", "database_guard")
	entries, err := os.ReadDir(fixtureRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		t.Run(strings.TrimSuffix(entry.Name(), ".go"), func(t *testing.T) {
			path := filepath.Join(fixtureRoot, entry.Name())
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			violations := databaseGuardViolations(fixtureRoot, []databaseGuardSource{{path: path, fset: fset, file: file}})
			if len(violations) == 0 || !strings.Contains(strings.Join(violations, "\n"), "hides database isolation") {
				t.Fatalf("violations = %v, want hidden database isolation rejection", violations)
			}
		})
	}
}

func databaseGuardLoadSources(root string) ([]databaseGuardSource, error) {
	var sources []databaseGuardSource
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !databaseGuardScansFile(root, path) {
			return err
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			return err
		}
		sources = append(sources, databaseGuardSource{path: path, fset: fset, file: file})
		return nil
	})
	return sources, err
}

func databaseGuardViolations(root string, sources []databaseGuardSource) []string {
	functionsByPackage := make(map[string]map[string][]*databaseGuardFunction)
	var functions []*databaseGuardFunction
	for _, source := range sources {
		packageKey := filepath.Dir(source.path) + "\x00" + source.file.Name.Name
		if functionsByPackage[packageKey] == nil {
			functionsByPackage[packageKey] = make(map[string][]*databaseGuardFunction)
		}
		for _, declaration := range source.file.Decls {
			decl, ok := declaration.(*ast.FuncDecl)
			if !ok || decl.Body == nil {
				continue
			}
			fn := &databaseGuardFunction{
				name: decl.Name.Name, path: source.path, fset: source.fset, file: source.file, decl: decl,
				requirementParams: make(map[string]bool),
			}
			fieldIndex := 0
			if decl.Type.Params != nil {
				for _, field := range decl.Type.Params.List {
					count := len(field.Names)
					if count == 0 {
						count = 1
					}
					if databaseGuardTypeName(field.Type) == "DatabaseRequirement" {
						for offset := 0; offset < count; offset++ {
							fn.requirementIndex = append(fn.requirementIndex, fieldIndex+offset)
						}
						for _, name := range field.Names {
							fn.requirementParams[name.Name] = true
						}
					}
					fieldIndex += count
				}
			}
			ast.Inspect(decl.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				fn.calls = append(fn.calls, call)
				name := databaseGuardCallName(call.Fun)
				if _, ok := databasePrimitiveRequirementIndex[name]; ok || name == "NewSQLiteRuntimeStore" || databaseGuardIsSQLiteOpen(call) {
					fn.directAcquisition = true
					fn.reachesAcquisition = true
				}
				return true
			})
			functions = append(functions, fn)
			functionsByPackage[packageKey][fn.name] = append(functionsByPackage[packageKey][fn.name], fn)
		}
	}

	changed := true
	for changed {
		changed = false
		for _, fn := range functions {
			if fn.reachesAcquisition {
				continue
			}
			packageFunctions := functionsByPackage[filepath.Dir(fn.path)+"\x00"+fn.file.Name.Name]
			for _, call := range fn.calls {
				targets := packageFunctions[databaseGuardCallName(call.Fun)]
				if len(targets) == 1 && targets[0].reachesAcquisition {
					fn.reachesAcquisition = true
					changed = true
					break
				}
			}
		}
	}

	var violations []string
	fail := func(fn *databaseGuardFunction, node ast.Node, reason string) {
		rel, _ := filepath.Rel(root, fn.path)
		violations = append(violations, fmt.Sprintf("%s:%d: %s", rel, fn.fset.Position(node.Pos()).Line, reason))
	}
	for _, fn := range functions {
		rootDeclaration := databaseGuardRootDeclaration(fn.name)
		if fn.reachesAcquisition && !rootDeclaration && len(fn.requirementIndex) == 0 {
			fail(fn, fn.decl.Name, fmt.Sprintf("helper %s hides database isolation instead of accepting DatabaseRequirement", fn.name))
		}
		packageFunctions := functionsByPackage[filepath.Dir(fn.path)+"\x00"+fn.file.Name.Name]
		for _, call := range fn.calls {
			name := databaseGuardCallName(call.Fun)
			if name == "StartPostgres" || name == "StartEmptyPostgres" {
				fail(fn, call, "retired untyped PostgreSQL acquisition")
				continue
			}
			if databaseRequirementConstructors[name] && !rootDeclaration {
				fail(fn, call, fmt.Sprintf("helper %s hides database isolation with %s", fn.name, name))
			}
			if requirementIndex, ok := databasePrimitiveRequirementIndex[name]; ok {
				databaseGuardRequireForwardedArgument(fn, call, requirementIndex, rootDeclaration, fail)
			}
			switch name {
			case "NewSQLiteRuntimeStore":
				if len(call.Args) == 0 || !databaseGuardTypedSQLitePath(call.Args[0]) {
					fail(fn, call, "SQLite runtime constructor bypasses typed path declaration")
				}
			case "Open":
				if databaseGuardIsSQLiteOpen(call) && !databaseGuardCallIs(call.Args[1], "SQLiteDeclaredDSN") {
					fail(fn, call, "direct SQLite open bypasses typed DSN declaration")
				}
			}
			targets := packageFunctions[name]
			if len(targets) != 1 || !targets[0].reachesAcquisition {
				continue
			}
			for _, requirementIndex := range targets[0].requirementIndex {
				databaseGuardRequireForwardedArgument(fn, call, requirementIndex, rootDeclaration, fail)
			}
		}
	}
	sort.Strings(violations)
	return violations
}

func databaseGuardRequireForwardedArgument(fn *databaseGuardFunction, call *ast.CallExpr, index int, rootDeclaration bool, fail func(*databaseGuardFunction, ast.Node, string)) {
	if index >= len(call.Args) {
		fail(fn, call, "database acquisition omits DatabaseRequirement")
		return
	}
	if rootDeclaration {
		return
	}
	ident, ok := call.Args[index].(*ast.Ident)
	if !ok || !fn.requirementParams[ident.Name] {
		fail(fn, call, fmt.Sprintf("helper %s does not forward its DatabaseRequirement parameter", fn.name))
	}
}

func databaseGuardTypeName(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.SelectorExpr:
		return value.Sel.Name
	default:
		return ""
	}
}

func databaseGuardRootDeclaration(name string) bool {
	return strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Benchmark") || strings.HasPrefix(name, "Fuzz") || strings.HasPrefix(name, "Example")
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

func databaseGuardTypedSQLitePath(expr ast.Expr) bool {
	if databaseGuardCallIs(expr, "SQLiteDeclaredPath") || databaseGuardCallIs(expr, "SQLitePath") {
		return true
	}
	ident, ok := expr.(*ast.Ident)
	if !ok || ident.Obj == nil {
		return false
	}
	switch declaration := ident.Obj.Decl.(type) {
	case *ast.AssignStmt:
		for index, lhs := range declaration.Lhs {
			declared, ok := lhs.(*ast.Ident)
			if ok && declared.Obj == ident.Obj && index < len(declaration.Rhs) {
				return databaseGuardCallIs(declaration.Rhs[index], "DeclareSQLitePath") || databaseGuardCallIs(declaration.Rhs[index], "SQLitePath")
			}
		}
	case *ast.ValueSpec:
		for index, declared := range declaration.Names {
			if declared.Obj == ident.Obj && index < len(declaration.Values) {
				return databaseGuardCallIs(declaration.Values[index], "DeclareSQLitePath") || databaseGuardCallIs(declaration.Values[index], "SQLitePath")
			}
		}
	}
	return false
}

func databaseGuardScansFile(root, path string) bool {
	if !strings.HasSuffix(path, ".go") || strings.Contains(path, string(filepath.Separator)+"testdata"+string(filepath.Separator)) {
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
