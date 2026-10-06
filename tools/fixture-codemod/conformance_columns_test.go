package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestConformanceColumnRewriteRefusesChangedInventoryAndForeignOwners(t *testing.T) {
	for _, mutation := range []string{"none", "foreign-reader", "foreign-getter", "foreign-bootstrap", "changed-table", "changed-column", "effectful-getter", "unknown-root", "already-migrated"} {
		t.Run(mutation, func(t *testing.T) {
			source := `package conformance
func requireCanonicalConversationSurface(t *testing.T, ctx context.Context, pg *store.PostgresStore) {
 t.Helper();storetest.BootstrapPostgresRuntimeStore(t,pg)
 requireTableColumns(t,ctx,storetest.DatabaseForTest(pg),"agent_turns","turn_id","turn_blocks")
 requireTableColumns(t,ctx,storetest.DatabaseForTest(pg),"agent_conversation_audits","session_id")
}`
			switch mutation {
			case "changed-table":
				source = strings.ReplaceAll(source, "agent_turns", "other_turns")
			case "changed-column":
				source = strings.ReplaceAll(source, "turn_blocks", "other_blocks")
			case "effectful-getter":
				source = strings.ReplaceAll(source, "DatabaseForTest(pg)", "DatabaseForTest(openOwner())")
			case "unknown-root":
				source = strings.ReplaceAll(source, "requireCanonicalConversationSurface", "requireUnknownSurface")
			case "already-migrated":
				source = strings.ReplaceAll(source, "storetest.BootstrapPostgresRuntimeStore(t,pg)", "storetest.CheckConversationStorageColumns(ctx,pg)")
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture_test.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Uses: map[*ast.Ident]types.Object{}}
			ast.Inspect(file, func(node ast.Node) bool {
				id, ok := node.(*ast.Ident)
				if !ok {
					return true
				}
				pkg := storetestPackage
				switch id.Name {
				case "requireTableColumns":
					pkg = conformancePackage
					if mutation == "foreign-reader" {
						pkg = "example/foreign"
					}
				case "DatabaseForTest":
					if mutation == "foreign-getter" {
						pkg = "example/foreign"
					}
				case "BootstrapPostgresRuntimeStore":
					if mutation == "foreign-bootstrap" {
						pkg = "example/foreign"
					}
				default:
					return true
				}
				info.Uses[id] = types.NewFunc(0, types.NewPackage(pkg, "fixture"), id.Name, types.NewSignatureType(nil, nil, nil, nil, nil, false))
				return true
			})
			updated, ok := rewriteConformanceColumnOwner(fset, info, file.Decls[0].(*ast.FuncDecl))
			if ok != (mutation == "none") {
				t.Fatalf("matched=%t output=%s", ok, updated)
			}
			if ok && (strings.Contains(updated, "DatabaseForTest") || strings.Contains(updated, "requireTableColumns") || strings.Contains(updated, "BootstrapPostgresRuntimeStore") || !strings.Contains(updated, "storetest.CheckConversationStorageColumns(ctx, pg)")) {
				t.Fatal("native owner or exact fixed witness changed")
			}
		})
	}
}

func TestConformanceMutationColumnCallerRequiresExactNativeLexicalOwner(t *testing.T) {
	for _, mutation := range []string{"none", "foreign-helper", "effectful-db", "wrong-db-type", "missing-owner", "raw-owner", "foreign-owner", "changed-root", "already-migrated"} {
		t.Run(mutation, func(t *testing.T) {
			source := `package conformance
func TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites(t *testing.T){requireMutationSurface(t,db)}`
			if mutation == "effectful-db" {
				source = strings.ReplaceAll(source, "t,db", "t,openDB()")
			}
			if mutation == "changed-root" {
				source = strings.ReplaceAll(source, "TestCanonicalMutationSurface_ReconstructsTrackedEntityStateForWorkflowWrites", "TestOther")
			}
			if mutation == "already-migrated" {
				source = strings.ReplaceAll(source, "t,db", "t,selected")
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture_test.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			fn := file.Decls[0].(*ast.FuncDecl)
			call := fn.Body.List[0].(*ast.ExprStmt).X.(*ast.CallExpr)
			info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Types: map[ast.Expr]types.TypeAndValue{}, Scopes: map[ast.Node]*types.Scope{}}
			pkg := conformancePackage
			if mutation == "foreign-helper" {
				pkg = "example/foreign"
			}
			id := call.Fun.(*ast.Ident)
			info.Uses[id] = types.NewFunc(0, types.NewPackage(pkg, "fixture"), id.Name, types.NewSignatureType(nil, nil, nil, nil, nil, false))
			dbType := types.Type(types.NewPointer(types.NewNamed(types.NewTypeName(0, types.NewPackage("database/sql", "sql"), "DB", nil), types.NewStruct(nil, nil), nil)))
			if mutation == "wrong-db-type" {
				dbType = types.Typ[types.String]
			}
			info.Types[call.Args[1]] = types.TypeAndValue{Type: dbType}
			scope := types.NewScope(nil, fn.Pos(), fn.End(), "fixture")
			info.Scopes[fn] = scope
			ownerType := types.Type(types.NewPointer(types.NewNamed(types.NewTypeName(0, types.NewPackage("github.com/division-sh/swarm/internal/store/internal/runtimepersistence", "fixture"), "PostgresStore", nil), types.NewStruct(nil, nil), nil)))
			if mutation == "raw-owner" {
				ownerType = dbType
			}
			if mutation == "foreign-owner" {
				ownerType = types.NewPointer(types.NewNamed(types.NewTypeName(0, types.NewPackage("example/foreign", "fixture"), "PostgresStore", nil), types.NewStruct(nil, nil), nil))
			}
			if mutation != "missing-owner" {
				scope.Insert(types.NewVar(fn.Pos(), nil, "selected", ownerType))
			}
			owner, ok := matchConformanceMutationColumnOwner(info, fn, call)
			if ok != (mutation == "none") || ok && owner != "selected" {
				t.Fatalf("owner=%s matched=%t", owner, ok)
			}
		})
	}
}
