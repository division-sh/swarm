package main

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"testing"
)

func TestContainsRawSQLRejectsConcreteNamedCarriers(t *testing.T) {
	const source = `package fixture
import (
  "context"
  "database/sql"
)
type Mutation struct { tx *sql.Tx }
type Backend struct { db *sql.DB }
type Dialect string
type Options struct { dialect Dialect }
type Owner struct{}
func (*Owner) Mutate(context.Context, *Mutation) error { return nil }
func (*Owner) Backend() *Backend { return nil }
func (*Owner) Configure(Options) error { return nil }
`
	pkg := typeCheckFixture(t, source)
	owner := pkg.Scope().Lookup("Owner").Type()
	methods := types.NewMethodSet(types.NewPointer(owner))
	want := map[string]bool{"Mutate": false, "Backend": false, "Configure": false}
	for index := 0; index < methods.Len(); index++ {
		method := methods.At(index).Obj().(*types.Func)
		if _, ok := want[method.Name()]; ok {
			want[method.Name()] = containsRawSQL(method.Type())
		}
	}
	for method, detected := range want {
		if !detected {
			t.Errorf("concrete named carrier on %s was not detected", method)
		}
	}
}

func typeCheckFixture(t *testing.T, source string) *types.Package {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "fixture.go", source, parser.AllErrors)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	conf := types.Config{Importer: importer.Default()}
	pkg, err := conf.Check("github.com/division-sh/swarm/internal/store/internal/fixture", fset, []*ast.File{file}, nil)
	if err != nil {
		t.Fatalf("type-check fixture: %v", err)
	}
	return pkg
}
