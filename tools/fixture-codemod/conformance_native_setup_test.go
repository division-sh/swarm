package main

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
	"testing"
)

func TestUnusedConformanceDatabaseRewriteRequiresResolvedUnusedOwner(t *testing.T) {
	for _, extra := range []string{"", "_ = db", "_ = db.Ping()"} {
		t.Run(extra, func(t *testing.T) {
			source := `package conformance
import "database/sql"
func newFanInBarrierRuntimeForSource(t any, backend any, db *sql.DB, source any) { ` + extra + ` }
func newFanInBarrierRuntime(t any, backend any, db *sql.DB, source any) { newFanInBarrierRuntimeForSource(t,backend,db,source) }
func caller(db *sql.DB){newFanInBarrierRuntime(nil,nil,db,nil)}
`
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, "fixture.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
			if _, err := (&types.Config{Importer: importer.Default()}).Check(conformancePackage, set, []*ast.File{file}, info); err != nil {
				t.Fatal(err)
			}
			family := ignoredConformanceDatabaseFamily(info, []*ast.File{file})
			fn := file.Decls[1].(*ast.FuncDecl)
			output, ok := rewriteIgnoredConformanceDatabase(set, info, fn, family)
			if ok != (extra == "") {
				t.Fatalf("used database matched=%v output=%s", ok, output)
			}
			if ok && strings.Contains(output, "*sql.DB") {
				t.Fatal("raw parameter survived")
			}
			forward := file.Decls[2].(*ast.FuncDecl)
			output, ok = rewriteIgnoredConformanceDatabase(set, info, forward, family)
			if ok != (extra == "") || ok && strings.Contains(output, "db") {
				t.Fatalf("forwarding family was not completely narrowed: %s", output)
			}
		})
	}
}

func TestUnusedConformanceDatabaseFamilyRejectsPartialOrEffectfulPropagation(t *testing.T) {
	for name, extra := range map[string]string{
		"direct":    "func caller(db *sql.DB){newFanInBarrierRuntime(nil,nil,db,nil)}",
		"effectful": "func getDB()*sql.DB{return nil};func caller(){newFanInBarrierRuntime(nil,nil,getDB(),nil)}",
		"alias":     "var caller = newFanInBarrierRuntime",
		"recursive": "func caller(db *sql.DB){newFanInBarrierRuntime(nil,nil,db,nil)}",
		"missing":   "func caller(db *sql.DB){newFanInBarrierRuntimeForSource(nil,nil,db,nil)}",
	} {
		t.Run(name, func(t *testing.T) {
			source := `package conformance
import "database/sql"
func newFanInBarrierRuntimeForSource(t any, backend any, db *sql.DB, source any){}
func newFanInBarrierRuntime(t any, backend any, db *sql.DB, source any){newFanInBarrierRuntimeForSource(t,backend,db,source)}
`
			if name == "recursive" {
				source = strings.Replace(source, "{newFanInBarrierRuntimeForSource(t,backend,db,source)}", "{newFanInBarrierRuntime(t,backend,db,source)}", 1)
			}
			if name == "missing" {
				source = strings.Replace(source, "func newFanInBarrierRuntime(t any, backend any, db *sql.DB, source any){newFanInBarrierRuntimeForSource(t,backend,db,source)}", "", 1)
			}
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, "owners.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			caller, err := parser.ParseFile(set, "callers.go", "package conformance\nimport \"database/sql\"\n"+extra, 0)
			if err != nil {
				t.Fatal(err)
			}
			files := []*ast.File{file, caller}
			info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
			if _, err := (&types.Config{Importer: importer.Default(), DisableUnusedImportCheck: true}).Check(conformancePackage, set, files, info); err != nil {
				t.Fatal(err)
			}
			family := ignoredConformanceDatabaseFamily(info, files)
			if (len(family) == 2) != (name == "direct") {
				t.Fatalf("family matched=%v", family)
			}
		})
	}
}

func TestConformanceFamilySelectionDoesNotMutateUnselectedAST(t *testing.T) {
	source := `package conformance
import "database/sql"
func newFanInBarrierRuntimeForSource(t any, backend any, db *sql.DB, source any){}
func newFanInBarrierRuntime(t any, backend any, db *sql.DB, source any){newFanInBarrierRuntimeForSource(t,backend,db,source)}
`
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "owners.go", source, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
	if _, err := (&types.Config{Importer: importer.Default()}).Check(conformancePackage, set, []*ast.File{file}, info); err != nil {
		t.Fatal(err)
	}
	family := ignoredConformanceDatabaseFamily(info, []*ast.File{file})
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		var before, after bytes.Buffer
		if err := format.Node(&before, set, fn); err != nil {
			t.Fatal(err)
		}
		if _, _, matched := rewriteSelectedConformanceOwner(set, info, fn, map[string]bool{"CanonicalStorageColumns": true}, family); matched {
			t.Fatal("unselected owner matched")
		}
		if err := format.Node(&after, set, fn); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before.Bytes(), after.Bytes()) {
			t.Fatal("unselected family mutated the AST")
		}
		if _, _, matched := rewriteSelectedConformanceOwner(set, info, fn, map[string]bool{"UnusedConformanceDatabase": true}, family); !matched {
			t.Fatal("selected safe owner did not match")
		}
	}
}
