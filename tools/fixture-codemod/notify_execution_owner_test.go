package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func notifyExecutionSQLType() *types.Pointer {
	pkg := types.NewPackage("database/sql", "sql")
	return types.NewPointer(types.NewNamed(types.NewTypeName(0, pkg, "DB", nil), types.NewStruct(nil, nil), nil))
}

func TestNotifyExecutionHelperDropsOnlyUnusedPoolWithExactReadOwner(t *testing.T) {
	for _, variant := range []string{"native", "still-used", "wrong-owner", "wrong-read", "unknown-root"} {
		t.Run(variant, func(t *testing.T) {
			name, read, extra := "assertNotifyAllChildrenRunPersisted", "ReadNotifyRunPresence", ""
			if variant == "still-used" {
				extra = "db.Query(query)"
			}
			if variant == "wrong-read" {
				read = "ReadNotifyFlowInstanceCount"
			}
			if variant == "unknown-root" {
				name = "unknownRunHelper"
			}
			source := []byte("package conformance\nfunc " + name + "(t *testing.T,ctx context.Context,backend notifyAllChildrenStore,db *sql.DB,run string){got,err:=storetest." + read + "(ctx,backend,run);if err!=nil{t.Fatal(err)};if got!=1{t.Fatal(got)};" + extra + "}")
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Types: map[ast.Expr]types.TypeAndValue{}}
			owner := types.NewPackage(storetestPackage, "storetest")
			if variant == "wrong-owner" {
				owner = types.NewPackage("example/foreign", "foreign")
			}
			fn := file.Decls[0].(*ast.FuncDecl)
			info.Types[fn.Type.Params.List[3].Type] = types.TypeAndValue{Type: notifyExecutionSQLType()}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok && id.Name == read {
					info.Uses[id] = types.NewFunc(0, owner, read, types.NewSignatureType(nil, nil, nil, nil, nil, false))
				}
				return true
			})
			out, matched := rewriteNotifyExecutionOwner(fset, info, fn, source)
			if matched != (variant == "native") {
				t.Fatalf("match=%v variant=%s output=%s", matched, variant, out)
			}
			if matched && (strings.Contains(out, "db *sql.DB") || !strings.Contains(out, "if got != 1") || !strings.Contains(out, "t.Fatal(err)")) {
				t.Fatalf("changed oracle: %s", out)
			}
		})
	}
}

func TestNotifyExecutionCallerKeepsLivePoolsAndTemporalComments(t *testing.T) {
	for _, variant := range []string{"native", "extra-query", "foreign-helper", "effectful-pool", "already-native"} {
		t.Run(variant, func(t *testing.T) {
			extra, args := "", "t,ctx,backend,db,run"
			if variant == "extra-query" {
				extra = "db.Query(query)"
			}
			if variant == "effectful-pool" {
				args = "t,ctx,backend,openPool(),run"
			}
			if variant == "already-native" {
				args = "t,ctx,backend,run"
			}
			source := []byte("package conformance\nfunc proof(){backend,db:=tc.setup(t)\n// Keep the later membership change before historical replay.\nassertNotifyAllChildrenRunPersisted(" + args + ");assertExactReplay();" + extra + "}")
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture.go", source, parser.ParseComments)
			if err != nil {
				t.Fatal(err)
			}
			pkg := types.NewPackage(conformancePackage, "conformance")
			helperPkg := pkg
			if variant == "foreign-helper" {
				helperPkg = types.NewPackage("example/foreign", "foreign")
			}
			dbType := notifyExecutionSQLType()
			db := types.NewVar(0, pkg, "db", dbType)
			params := []*types.Var{types.NewVar(0, pkg, "t", types.Typ[types.Int]), types.NewVar(0, pkg, "ctx", types.Typ[types.Int]), types.NewVar(0, pkg, "backend", types.Typ[types.Int]), types.NewVar(0, pkg, "db", dbType), types.NewVar(0, pkg, "run", types.Typ[types.Int])}
			helper := types.NewFunc(0, helperPkg, "assertNotifyAllChildrenRunPersisted", types.NewSignatureType(nil, nil, nil, types.NewTuple(params...), nil, false))
			info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{}, Types: map[ast.Expr]types.TypeAndValue{}}
			fn := file.Decls[0].(*ast.FuncDecl)
			ast.Inspect(fn, func(node ast.Node) bool {
				if id, ok := node.(*ast.Ident); ok {
					if id.Name == "db" {
						info.Uses[id] = db
						info.Types[id] = types.TypeAndValue{Type: dbType}
					}
					if id.Name == helper.Name() {
						info.Uses[id] = helper
					}
				}
				if call, ok := node.(*ast.CallExpr); ok && exprString(call.Fun) == "openPool" {
					info.Types[call] = types.TypeAndValue{Type: dbType}
				}
				return true
			})
			definition := fn.Body.List[0].(*ast.AssignStmt).Lhs[1].(*ast.Ident)
			delete(info.Uses, definition)
			info.Defs[definition] = db
			out, matched := rewriteNotifyExecutionPoolBinding(fset, info, fn, file.Comments)
			want := variant == "native" || variant == "extra-query"
			if matched != want {
				t.Fatalf("match=%v variant=%s output=%s", matched, variant, out)
			}
			if matched && (!strings.Contains(out, "assertExactReplay()") || !strings.Contains(out, "// Keep the later membership change before historical replay.") || !strings.Contains(out, "assertNotifyAllChildrenRunPersisted(t, ctx, backend, run)")) {
				t.Fatalf("lost proof/comment: %s", out)
			}
			if variant == "native" && !strings.Contains(out, "backend, _ := tc.setup(t)") {
				t.Fatalf("unused pool was retained: %s", out)
			}
			if variant == "extra-query" && (!strings.Contains(out, "backend, db := tc.setup(t)") || !strings.Contains(out, "db.Query(query)")) {
				t.Fatalf("live pool was silently erased: %s", out)
			}
		})
	}
}
