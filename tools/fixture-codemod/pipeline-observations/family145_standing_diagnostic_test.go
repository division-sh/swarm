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
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/standingdiagnostic"
)

//go:embed family145-standing-diagnostic-preimages.json
var standingDiagnosticBytes []byte
var standingDiagnosticManifest = func() standingdiagnostic.Manifest {
	manifest, err := standingdiagnostic.Decode(standingDiagnosticBytes)
	if err != nil {
		panic(err)
	}
	return manifest
}()

func family145CheckStandingDiagnosticWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range standingDiagnosticManifest.Pins {
		if pin.File == file && pin.Function == function && pin.Before != pin.After {
			if current != pin.After {
				t.Fatal("standing diagnostic amendment escaped its exact owner/retirement:", function)
			}
			return true
		}
	}
	return false
}

func standingDiagnosticRewriteControls(t *testing.T, pin standingdiagnostic.Pin) {
	t.Helper()
	if standingdiagnostic.Previous(standingDiagnosticManifest, pin.File, pin.Function, pin.After) != pin.Before || standingdiagnostic.Previous(standingDiagnosticManifest, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("diagnostic source amendment lost its exact complete identity")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	out, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
	if err != nil || !changed {
		t.Fatalf("diagnostic finite rewrite: %s %v", pin.Function, err)
	}
	if _, changed, err := rewriteFunction(pin.File, out, finite); err != nil || changed {
		t.Fatalf("diagnostic replay is not inert: %v", err)
	}
	for _, token := range []string{"standingStoreDiagnostics", "\"sqlite\"", "\"postgres\"", "dsn", "path"} {
		if !strings.Contains(pin.After, token) {
			continue
		}
		mutant := strings.Replace(pin.After, token, "unreviewedDiagnosticAuthority", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatal("foreign diagnostic endpoint accepted:", token)
		}
	}
}

func isExactStandingDiagnosticNewRecipe(row recipe) bool {
	for _, pin := range standingDiagnosticManifest.Pins {
		if pin.Before == pin.After {
			continue
		}
		want := recipe{Family: "native-postgres-admission-family145", File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		if reflect.DeepEqual(row, want) {
			return true
		}
	}
	return false
}

func TestFamily145StandingDiagnosticOwnersKeepThreeCutsAndDisplayFailurePolicies(t *testing.T) {
	for _, endpoint := range standingDiagnosticManifest.Endpoints {
		current, err := os.ReadFile(filepath.Join("../../..", endpoint.File))
		if err != nil || requestKeyDeclaration(t, current, endpoint.Function) != endpoint.After {
			t.Fatal("diagnostic owner, exact query, cuts or display policy changed:", endpoint.Function, err)
		}
		for _, cut := range []string{"OpenReleaseProcessReadOnlyInspection", "defer owner.Close()", "InspectSnapshot", "ORDER BY event_name", "ORDER BY created_at", "payload::text", "COALESCE(status, '')", "rows.Err()", "rows.Close()", "rows.Scan", "!postgres", "return nil,", "err == nil", "strings.Join(parts, \",\")"} {
			if !strings.Contains(endpoint.After, cut) {
				continue
			}
			mutant := strings.Replace(endpoint.After, cut, "unreviewedDiagnosticCut", 1)
			if diagnosticEndpointMatches(endpoint.File, endpoint.Function, mutant) {
				t.Fatal("changed owner/scope/order/scan/query policy accepted:", endpoint.Function, cut)
			}
		}
	}
}

func diagnosticEndpointMatches(file, function, current string) bool {
	for _, endpoint := range standingDiagnosticManifest.Endpoints {
		if endpoint.File == file && endpoint.Function == function {
			return current == endpoint.After
		}
	}
	return false
}

func standingDiagnosticRetirementError(raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "diagnostic.go", raw, parser.AllErrors)
	if err != nil {
		return err
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || !(strings.HasPrefix(fn.Name.Name, "standing") || strings.HasPrefix(fn.Name.Name, "renderStanding") || strings.HasPrefix(fn.Name.Name, "ReadStanding")) {
			continue
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			if err != nil {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			err = standingDiagnosticCallRetirementError(call)
			return err == nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func standingDiagnosticCallRetirementError(call *ast.CallExpr) error {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil
	}
	if strings.HasPrefix(selector.Sel.Name, "ReadStandingReceipt") {
		return fmt.Errorf("diagnostic constructs a replacement receipt owner")
	}
	if selector.Sel.Name != "Query" && selector.Sel.Name != "QueryContext" {
		return nil
	}
	for _, arg := range call.Args {
		literal, ok := arg.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			continue
		}
		text, _ := strconv.Unquote(literal.Value)
		if strings.Contains(text, "pipeline_receipts") {
			return fmt.Errorf("obsolete receipt query remains executable")
		}
	}
	return nil
}

func standingDiagnosticFileInventory(t *testing.T, path string, file *ast.File, allowed map[string]map[string]bool) int {
	t.Helper()
	count := 0
	owned := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || !standingDiagnosticHelper(id.Name) {
				return true
			}
			if !allowed[path][fn.Name.Name] {
				t.Fatal("unmanifested standing diagnostic consumer:", path, fn.Name.Name)
			}
			owned[id.Pos()] = true
			count++
			return true
		})
		if standingDiagnosticHelper(fn.Name.Name) {
			owned[fn.Name.Pos()] = true
		}
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && standingDiagnosticHelper(id.Name) && !owned[id.Pos()] {
			t.Fatal("standing diagnostic alias/initializer escaped the finite chain:", path, id.Name)
		}
		return true
	})
	return count
}

func standingDiagnosticHelper(name string) bool {
	return name == "standingPostgresDiagnostics" || name == "standingSQLiteDiagnostics" || name == "standingMemoryStoreDiagnostics"
}
