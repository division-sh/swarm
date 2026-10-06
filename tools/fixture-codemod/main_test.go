package main

import (
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNotifyItemMetadataOwnerRemovalIsFiniteAndIdempotent(t *testing.T) {
	for helper, count := range map[string]int{"loadNotifyAllChildrenItemEvents": 6, "assertNotifyAllChildrenMetadata": 7, "dumpNotifyAllChildrenRuntimeState": 4} {
		for _, mutation := range []string{"none", "foreign", "shadow", "unknown-helper", "wrong-arity", "effectful-db", "already-migrated"} {
			t.Run(helper+"/"+mutation, func(t *testing.T) {
				pkg := types.NewPackage(conformancePackage, "conformance")
				if mutation == "foreign" {
					pkg = types.NewPackage("example/other", "other")
				}
				sqlpkg := types.NewPackage("database/sql", "sql")
				db := types.NewPointer(types.NewNamed(types.NewTypeName(0, sqlpkg, "DB", nil), types.NewStruct(nil, nil), nil))
				params := []*types.Var{}
				args := []ast.Expr{}
				for i := 0; i < count; i++ {
					typ := types.Type(types.Typ[types.Int])
					if i == 3 {
						typ = db
					}
					params = append(params, types.NewVar(0, pkg, "", typ))
					args = append(args, ast.NewIdent("arg"))
				}
				if mutation == "already-migrated" {
					params = append(params[:3], params[4:]...)
				}
				if mutation == "wrong-arity" {
					args = args[:len(args)-1]
				}
				if mutation == "effectful-db" {
					args[3] = &ast.CallExpr{Fun: ast.NewIdent("openDB")}
				}
				name := ast.NewIdent(helper)
				if mutation == "unknown-helper" {
					name.Name = "unknown"
				}
				obj := types.Object(types.NewFunc(0, pkg, name.Name, types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false)))
				if mutation == "shadow" {
					obj = types.NewVar(0, pkg, name.Name, db)
				}
				_, ok := matchNotifyObserverOwner(&types.Info{Uses: map[*ast.Ident]types.Object{name: obj}}, &ast.CallExpr{Fun: name, Args: args})
				if ok != (mutation == "none") {
					t.Fatalf("match=%v", ok)
				}
			})
		}
	}
}

func TestStoretestAliasRejectsDotAndBlankImports(t *testing.T) {
	for _, name := range []string{"", "fixture", ".", "_"} {
		imp := &ast.ImportSpec{Path: &ast.BasicLit{Kind: token.STRING, Value: `"` + storetestPackage + `"`}}
		if name != "" {
			imp.Name = ast.NewIdent(name)
		}
		got := storetestAlias(&ast.File{Imports: []*ast.ImportSpec{imp}})
		want := name
		if name == "" {
			want = "storetest"
		}
		if name == "." || name == "_" {
			want = ""
		}
		if got != want {
			t.Fatalf("alias=%q want=%q", got, want)
		}
	}
}

func TestPatternClassificationKeepsEndpointsAndPlumbingSeparate(t *testing.T) {
	for _, tc := range []struct{ member, want string }{
		{"call:database/sql.(*database/sql.Row).Scan", "cursor-plumbing"},
		{"call:database/sql.(*database/sql.Rows).Close", "cursor-plumbing"},
		{"call:database/sql.(*database/sql.DB).Close", "open-and-connection-lifetime"},
		{"call:database/sql.(*database/sql.DB).QueryRowContext", "sql-read-endpoints"},
		{"call:database/sql.(*database/sql.Tx).ExecContext", "sql-write-endpoints"},
		{"call:database/sql.(*database/sql.Tx).Commit", "transaction-protocol"},
		{"call:example.fixture", "other-raw-bearing-shared-helper-calls"},
	} {
		if got := patternClass(tc.member); got != tc.want {
			t.Fatalf("%s: %s want %s", tc.member, got, tc.want)
		}
	}
}

func TestConformanceDispatcherRefusesUntypedUnknownAndDeferredFamilies(t *testing.T) {
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
	fn := &ast.FuncDecl{Name: ast.NewIdent("unrelated")}
	for _, name := range []string{"unknown", "newNotifyAllChildrenRuntime", "assertRuntimeDBCount", "waitObjectChannelDisposition", "requireServedEventPublishEntityState"} {
		call := &ast.CallExpr{Fun: ast.NewIdent(name), Args: []ast.Expr{ast.NewIdent("t"), ast.NewIdent("db")}}
		if field, replacement := rewriteConformanceCall(token.NewFileSet(), info, fn, call, nil); field != "" || replacement != "" {
			t.Fatalf("untyped or deferred family %s accepted: %s/%s", name, field, replacement)
		}
	}
}

func TestMigrationApplicationPreflightsEveryFileBeforeWrites(t *testing.T) {
	root := t.TempDir()
	first, second := filepath.Join(root, "first_test.go"), filepath.Join(root, "second_test.go")
	source := []byte("package fixture\nfunc original() {}\n")
	for _, path := range []string{first, second} {
		if err := os.WriteFile(path, source, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	start := strings.Index(string(source), "func")
	files := []migrationFile{
		{path: first, source: source, edits: []change{{start: start, end: len(source), replacement: "func changed() {}"}}},
		{path: second, source: source, edits: []change{{start: start, end: len(source), replacement: "func broken("}}},
	}
	if err := applyMigrationFiles(files); err == nil {
		t.Fatal("invalid later edit accepted")
	}
	for _, path := range []string{first, second} {
		got, err := os.ReadFile(path)
		if err != nil || string(got) != string(source) {
			t.Fatalf("partial migration wrote %s: %s/%v", path, got, err)
		}
	}
	files[1].edits[0].replacement = "func changed() {}"
	if err := applyMigrationFiles(files); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{first, second} {
		got, err := os.ReadFile(path)
		if err != nil || !strings.Contains(string(got), "func changed()") {
			t.Fatalf("valid finite migration failed: %s/%v", got, err)
		}
	}
}
