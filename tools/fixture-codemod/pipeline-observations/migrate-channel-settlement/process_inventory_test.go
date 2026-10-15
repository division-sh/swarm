package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/packages"
)

func TestChannelProcessFactoriesReturnOnlyBorrowedObservation(t *testing.T) {
	root, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	rows := inventory(root, processHelpers)
	factories := map[string]bool{}
	for _, row := range rows {
		if isProcessFactory(row.Function) {
			factories[row.Function] = true
		}
	}
	if len(factories) != 4 {
		t.Fatalf("process factory inventory=%d, want four", len(factories))
	}
	for _, result := range []types.Type{types.NewPointer(types.Typ[types.Int]), types.NewInterfaceType(nil, nil).Complete(), types.Typ[types.String]} {
		value := types.NewSignatureType(nil, nil, nil, nil, types.NewTuple(types.NewVar(token.NoPos, nil, "harness", types.Typ[types.Int]), types.NewVar(token.NoPos, nil, "carrier", result)), false)
		if err := checkProcessFactoryResult(value); err == nil {
			t.Fatal("raw, foreign or incomplete factory carrier accepted")
		}
	}
}

func TestChannelProcessInventoryRejectsIndirectAndInitializerEscapes(t *testing.T) {
	for _, test := range []struct {
		name, source string
		calls        int
		refuse       bool
	}{
		{"direct", "func caller(){startObjectChannelJourney()}", 1, false},
		{"shadow", "func caller(){startObjectChannelJourney:=func(){};startObjectChannelJourney()}", 0, false},
		{"alias", "func caller(){alias:=startObjectChannelJourney;alias()}", 0, true},
		{"callback", "func caller(){consume(startObjectChannelJourney)};func consume(func()){}", 0, true},
		{"initializer", "var alias=startObjectChannelJourney", 0, true},
		{"initializer-call", "var value=func()int{startObjectChannelJourney();return 1}()", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) { checkChannelProcessInventoryCase(t, test.source, test.calls, test.refuse) })
	}
}

func checkChannelProcessInventoryCase(t *testing.T, source string, want int, refuse bool) {
	t.Helper()
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "owned.go", "package serveapp\nfunc startObjectChannelJourney(){}\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Uses: map[*ast.Ident]types.Object{}, Defs: map[*ast.Ident]types.Object{}}
	checked, err := new(types.Config).Check("github.com/division-sh/swarm/internal/serveapp", set, []*ast.File{file}, info)
	if err != nil {
		t.Fatal(err)
	}
	pkg := &packages.Package{Fset: set, Types: checked, TypesInfo: info, Syntax: []*ast.File{file}}
	covered := map[*ast.Ident]bool{}
	count := 0
	for _, node := range file.Decls {
		if fn, ok := node.(*ast.FuncDecl); ok {
			calls, callErr := referencesWithNames(pkg, fn, covered, processHelpers)
			count += len(calls)
			if callErr != nil {
				err = callErr
			}
		}
	}
	if coverageErr := completeReferencesWithNames(pkg, covered, processHelpers); coverageErr != nil {
		err = coverageErr
	}
	if (err != nil) != refuse || !refuse && count != want {
		t.Fatalf("process inventory calls=%d err=%v, want=%d refusal=%t", count, err, want, refuse)
	}
	// The older source transform keeps its own closed helper namespace.
	if err := completeReferences(pkg, nil); err != nil {
		t.Fatalf("process additions changed the historical settlement inventory: %v", err)
	}
}

func TestChannelProcessInventoryRejectsExcludedOwnedUses(t *testing.T) {
	root := t.TempDir()
	for _, directory := range []string{"internal/serveapp", "internal/store/storetest"} {
		if err := os.MkdirAll(filepath.Join(root, directory), 0755); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(root, "internal/serveapp/excluded_test.go")
	if err := os.WriteFile(path, []byte("//go:build never\n\npackage serveapp\nfunc hidden(){startObjectChannelJourney()}"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := checkExcludedSourcesWithNames(root, nil, processHelpers); err == nil {
		t.Fatal("excluded source escaped the complete process inventory")
	}
	if err := checkExcludedSourcesWithNames(root, map[string]bool{path: true}, processHelpers); err != nil {
		t.Fatal("active source was not left to typed classification:", err)
	}
}
