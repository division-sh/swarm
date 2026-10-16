package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runidentity"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedsettlement"
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

func family145CheckRunIdentityWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range runIdentityPins {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("run identity escaped its exact scope, cuts or assertions:", file, function)
			}
			return true
		}
	}
	return false
}

func runIdentityHostile(t *testing.T, pin runidentity.Pin, finite recipe, cut string) {
	t.Helper()
	if strings.Contains(pin.After, cut) {
		mutant := strings.Replace(pin.After, cut, "unreviewedRunIdentityCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatal("changed run identity owner/key/assertion/cut accepted:", pin.Function, cut)
		}
	}
	for _, edit := range runidentity.Rules {
		if edit.File != pin.File || edit.Function != pin.Function {
			continue
		}
		mutant := strings.Replace(pin.Before, edit.Edits[len(edit.Edits)-1].Before, "unreviewedRunPredicate()", 1)
		if _, err := runphysical.Transform(edit, mutant); err == nil {
			t.Fatal("unknown predecessor predicate accepted:", pin.Function)
		}
	}
}

func runIdentityOwnedFile(t *testing.T, path string, file *ast.File) (int, int) {
	t.Helper()
	if err := runIdentityFileError(path, file); err != nil {
		t.Fatal(err)
	}
	calls, definitions := 0, 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "readServedRunPhysicalIdentity" {
			definitions++
		}
		if fn.Name.Name == "TestServedRunIdentityRefusesMissingSnapshotOwner" {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && requestkey.Node(call.Fun) == "readServedRunPhysicalIdentity" {
				calls++
			}
			return true
		})
	}
	return calls, definitions
}

func runIdentityFileError(path string, file *ast.File) error {
	normalized := map[token.Pos]string{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if !runIdentityContainsReference(fn) {
			continue
		}
		current, err := servedsettlement.SetupEarlierSource("../../..", path, fn.Name.Name, requestkey.Node(fn))
		if err != nil {
			return err
		}
		normalized[fn.Pos()] = sourceBlobEarlierInventoryEndpoint(path, fn.Name.Name, current)
	}
	return runIdentityReferenceError(path, file, normalized)
}

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

func runIdentityEarlierHistory(t *testing.T, raw []byte, recipes bool) []byte {
	t.Helper()
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatal(err)
	}
	prior, _ := restoreRunIdentityHistory(t, rows, recipes)
	result, err := json.Marshal(prior)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func restoreRunIdentityHistory(t *testing.T, rows []map[string]any, recipes bool) ([]map[string]any, int) {
	rows = restoreSourceForkHistory(t, rows, recipes)
	rows = restorePublicRollbackHistory(t, rows, recipes)
	retained, additions := []map[string]any{}, 0
	for _, row := range rows {
		for _, pin := range runIdentityPins {
			if row["File"] != pin.File || row["Function"] != pin.Function {
				continue
			}
			if pin.Function == "TestServedResetPendingStartupUsesAdmittedSourcesBothStores" && row["Before"] == pin.Before && row["After"] == pin.After {
				additions++
				row = nil
				break
			}
			if recipes && row["Successor"] == pin.After {
				delete(row, "Successor")
			} else if !recipes && row["After"] == pin.After {
				row["After"] = pin.Before
			}
		}
		if row != nil {
			retained = append(retained, row)
		}
	}
	return retained, additions
}
