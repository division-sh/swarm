package main

import (
	_ "embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runidentity"
)

//go:embed family145-run-identity-preimages.json
var runIdentityBytes []byte

var runIdentityPins = func() []runidentity.Pin {
	pins, err := runidentity.Decode(runIdentityBytes)
	if err != nil {
		panic(err)
	}
	return pins
}()

func runIdentityReferenceError(path string, file *ast.File, normalized map[token.Pos]string) error {
	allowed := map[token.Pos]bool{}
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "readServedRunPhysicalIdentity" {
			if path != "internal/serveapp/run_identity_observation_test.go" {
				return fmt.Errorf("run identity acquired a second owner: %s", path)
			}
			allowed[fn.Name.Pos()] = true
		}
		for _, pin := range runIdentityPins {
			if pin.File == path && pin.Function == fn.Name.Name && pin.After == normalized[fn.Pos()] {
				recordRunIdentityReferences(allowed, fn)
			}
		}
		if path == "internal/serveapp/run_identity_observation_test.go" && fn.Name.Name == "TestServedRunIdentityRefusesMissingSnapshotOwner" {
			recordRunIdentityReferences(allowed, fn)
		}
	}
	var failure error
	ast.Inspect(file, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "readServedRunPhysicalIdentity" && !allowed[id.Pos()] {
			failure = fmt.Errorf("run identity unmanifested consumer/alias: %s", path)
		}
		return true
	})
	return failure
}

func TestFamily145RunIdentityReferencePrecheckPreservesRestoredReferenceRefusals(t *testing.T) {
	var pin runidentity.Pin
	for _, candidate := range runIdentityPins {
		if candidate.Function != "readServedRunPhysicalIdentity" && strings.Contains(candidate.After, "readServedRunPhysicalIdentity") {
			pin = candidate
			break
		}
	}
	if pin.Function == "" {
		t.Fatal("reference conservation requires its original pinned consumer")
	}
	// A model of the normalized output, not a grant to invented live source.
	// Both paths authorize positions in CURRENT AST nodes, never restored nodes.
	native := strings.ReplaceAll(pin.After, "readServedRunPhysicalIdentity", "nativeIdentityObservation")
	for _, source := range []string{
		native,
		native + "\nvar text = `readServedRunPhysicalIdentity`\n",
		native + "\nvar escaped = readServedRunPhysicalIdentity\n",
		pin.After,
		"func foreign(){ alias := readServedRunPhysicalIdentity; _ = alias }",
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "model.go", "package proof\n"+source, parser.AllErrors)
		if err != nil {
			t.Fatal(err)
		}
		legacy, scoped := map[token.Pos]string{}, map[token.Pos]string{}
		for _, declaration := range file.Decls {
			if fn, ok := declaration.(*ast.FuncDecl); ok {
				legacy[fn.Pos()] = pin.After // Restores watched identifiers even from native-only source.
				if runIdentityContainsReference(fn) {
					scoped[fn.Pos()] = pin.After
				}
			}
		}
		before, after := runIdentityReferenceError(pin.File, file, legacy), runIdentityReferenceError(pin.File, file, scoped)
		if (before == nil) != (after == nil) {
			t.Fatalf("reference precheck changed restored-reference admission: before=%v after=%v", before, after)
		}
		if strings.Contains(source, "escaped") || strings.Contains(source, "func foreign") {
			if after == nil {
				t.Fatal("restored reference hid a current initializer or foreign function alias")
			}
		}
	}
}

func runIdentityContainsReference(fn *ast.FuncDecl) bool {
	found := false
	ast.Inspect(fn, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && id.Name == "readServedRunPhysicalIdentity" {
			found = true
		}
		return !found
	})
	return found
}

func recordRunIdentityReferences(allowed map[token.Pos]bool, fn *ast.FuncDecl) {
	ast.Inspect(fn, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "readServedRunPhysicalIdentity" {
			allowed[id.Pos()] = true
		}
		return true
	})
}
