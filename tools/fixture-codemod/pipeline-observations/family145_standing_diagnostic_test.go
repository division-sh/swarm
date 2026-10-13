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

func TestFamily145StandingDiagnosticWholeChainRetainsExactPredecessorsAndCallers(t *testing.T) {
	manifest, err := standingdiagnostic.Decode(standingDiagnosticBytes)
	if err != nil {
		t.Fatal(err)
	}
	changed := 0
	for index, pin := range manifest.Pins {
		rule := standingdiagnostic.Rules[index]
		if rule.File != pin.File || rule.Function != pin.Function || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("diagnostic rule/source identity changed")
		}
		previous, err := exec.Command("git", "show", standingdiagnostic.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, previous, pin.Function) != pin.Before {
			t.Fatal("diagnostic predecessor is not source-pinned:", pin.Function, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || requestKeyDeclaration(t, current, pin.Function) != pin.After {
			t.Fatal("diagnostic caller, assertion or teardown changed:", pin.Function, err)
		}
		after, err := standingdiagnostic.Transform(rule, pin.Before)
		if err != nil || after != pin.After {
			t.Fatal("diagnostic endpoint is not its finite transform:", pin.Function, err)
		}
		if pin.Before != pin.After {
			changed++
			standingDiagnosticRewriteControls(t, pin)
		}
	}
	if changed != 2 {
		t.Fatalf("diagnostic changed declarations=%d want2", changed)
	}
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

func TestFamily145StandingDiagnosticAmendmentPreservesEveryPriorRecipeAndTransition(t *testing.T) {
	previous, err := exec.Command("git", "show", standingdiagnostic.Predecessor+":tools/fixture-codemod/pipeline-observations/recipes.json").Output()
	if err != nil {
		t.Fatal(err)
	}
	var before, after []recipe
	if err := json.Unmarshal(previous, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(runIdentityEarlierHistory(t, recipeBytes, true), &after); err != nil {
		t.Fatal(err)
	}
	var retained []recipe
	for _, row := range after {
		if !isExactStandingDiagnosticNewRecipe(row) {
			retained = append(retained, row)
		}
	}
	if len(after) != len(before)+2 || !reflect.DeepEqual(before, retained) {
		t.Fatal("standing diagnostic amendment changed an immutable prior Before/After/Successor record")
	}
	previous, err = exec.Command("git", "show", standingdiagnostic.Predecessor+":tools/fixture-codemod/pipeline-observations/family145-transitions.json").Output()
	if err != nil {
		t.Fatal(err)
	}
	before, after = nil, nil
	if err := json.Unmarshal(previous, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(runIdentityEarlierHistory(t, family145TransitionBytes, false), &after); err != nil {
		t.Fatal(err)
	}
	current := map[string]recipe{}
	for _, row := range after {
		current[row.File+"/"+row.Function] = row
	}
	for _, row := range before {
		if !reflect.DeepEqual(row, current[row.File+"/"+row.Function]) {
			t.Fatal("standing diagnostics changed a prior source transition:", row.Function)
		}
	}
	if len(after) != len(before)+2 {
		t.Fatal("standing diagnostic transition inventory escaped the two reviewed declarations")
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

func TestFamily145StandingDiagnosticReceiptRetirementIsTargetedAndRejectsReintroduction(t *testing.T) {
	for _, pin := range standingDiagnosticManifest.Pins {
		if pin.Before == pin.After {
			continue
		}
		if err := standingDiagnosticRetirementError([]byte("package proof\n" + pin.Before)); err == nil {
			t.Fatal("old executable receipt query passed retirement:", pin.Function)
		}
		if err := standingDiagnosticRetirementError([]byte("package proof\n" + pin.After)); err != nil {
			t.Fatal(err)
		}
		mutant := strings.Replace(pin.After, "{", "{ tx.QueryContext(ctx, `SELECT event_id, status FROM pipeline_receipts ORDER BY updated_at`);", 1)
		if err := standingDiagnosticRetirementError([]byte("package proof\n" + mutant)); err == nil {
			t.Fatal("reintroduced receipt query passed retirement:", pin.Function)
		}
	}
	for _, endpoint := range standingDiagnosticManifest.Endpoints {
		if err := standingDiagnosticRetirementError([]byte("package proof\n" + endpoint.After)); err != nil {
			t.Fatal(err)
		}
		if endpoint.Function == "renderStandingStoreDiagnostics" {
			mutant := strings.Replace(endpoint.After, "{", "{ storetest.ReadStandingReceiptDiagnostics(ctx, owner);", 1)
			if err := standingDiagnosticRetirementError([]byte("package proof\n" + mutant)); err == nil {
				t.Fatal("replacement receipt owner passed targeted retirement")
			}
		}
	}
	// Immutable history and legitimate canonical receipt labels are not live queries.
	if err := standingDiagnosticRetirementError([]byte("package proof\nvar history = `SELECT * FROM pipeline_receipts`\nfunc unrelated() { count := CountDeploymentPipelineReceiptsStorage(); print(count, \"pipeline_receipts\") }")); err != nil {
		t.Fatal("retirement became a global string/receipt ban:", err)
	}
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

func TestFamily145StandingDiagnosticOwnedCallerInventoryIsFinite(t *testing.T) {
	root := "../../.."
	allowed := map[string]map[string]bool{}
	for _, pin := range standingDiagnosticManifest.Pins {
		if allowed[pin.File] == nil {
			allowed[pin.File] = map[string]bool{}
		}
		allowed[pin.File][pin.Function] = true
	}
	// New direct execution/absence controls also exercise the closed diagnostic chain.
	allowed["internal/serveapp/standing_diagnostic_native_test.go"] = map[string]bool{"TestStandingDiagnosticsReadOnlyWholeChainBothStores": true, "TestStandingDiagnosticsEmptyDataBothStores": true, "TestStandingSQLiteDiagnosticsMissingDatabaseStaysMissing": true}
	calls := 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		calls += standingDiagnosticFileInventory(t, filepath.ToSlash(rel), file, allowed)
		return nil
	})
	if err != nil || calls != 9 {
		t.Fatalf("standing diagnostic direct/helper calls=%d want9 err=%v", calls, err)
	}
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
