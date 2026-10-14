package main

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

//go:embed family145-run-physical-preimages.json
var runPhysicalBytes []byte

var runPhysicalRows = func() []runphysical.Pin {
	pins, err := runphysical.Decode(runPhysicalBytes)
	if err != nil {
		panic(err)
	}
	return pins
}()

func runPhysicalPins(t *testing.T) []runphysical.Pin {
	t.Helper()
	if fmt.Sprintf("%x", sha256.Sum256(runPhysicalBytes)) != runphysical.Digest {
		t.Fatal("physical run count source manifest changed")
	}
	return runPhysicalRows
}

func family145RunPhysicalPrevious(file, function, current string) string {
	return runphysical.Previous(runPhysicalRows, file, function, current)
}

func family145RunPhysicalCallerInverse(function, current string) string {
	for _, pin := range runPhysicalRows {
		if pin.Function == function {
			return runphysical.Previous(runPhysicalRows, pin.File, function, current)
		}
	}
	return current
}

func family145CheckRunPhysicalWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range runPhysicalPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("physical run count escaped its exact owner, cuts or assertions:", file, function)
			}
			return true
		}
	}
	return false
}

func TestFamily145RunPhysicalWholeChainPreservesPredicatesAssertionsAndCuts(t *testing.T) {
	pins := runPhysicalPins(t)
	if len(runphysical.Rules) != len(pins) {
		t.Fatal("physical run count rule inventory changed")
	}
	calls := 0
	for index, pin := range pins {
		rule := runphysical.Rules[index]
		if rule.File != pin.File || rule.Function != pin.Function || rule.Calls != pin.Calls || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("physical run count rule/preimage drift:", pin.Function)
		}
		before, err := exec.Command("git", "show", runphysical.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, before, pin.Function) != pin.Before {
			t.Fatal("physical run count is not its signed predecessor:", pin.Function, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || servedSetupEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("physical run count output changed:", pin.Function, err)
		}
		after, err := runphysical.Transform(rule, pin.Before)
		if err != nil || after != pin.After {
			t.Fatal("physical run count escaped its mechanical transform:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		output, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("physical run count finite rewrite failed:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, output, finite); err != nil || changed {
			t.Fatal("physical run count finite rewrite is not idempotent:", pin.Function, err)
		}
		runPhysicalHostiles(t, pin, finite)
		calls += pin.Calls
	}
	if calls != 10 {
		t.Fatalf("physical run count sites=%d want10", calls)
	}
}

func runPhysicalHostiles(t *testing.T, pin runphysical.Pin, finite recipe) {
	t.Helper()
	for _, cut := range []string{"rt.ReceiverStateReader", "h.rt.Bus.Store()", "h.ctx", "foreign.RunID()", "seed.RunID", "forkRunID", "runID", "err != nil", " != ", "readServedRunFieldRows", "readServedRunConstructionHeaders", "CountRunFlowInstanceStorage", "t.Fatal", "wantCompanions", "waitServedRunDeliveryQuiescence"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedPhysicalRunCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || family145RunPhysicalPrevious(pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed physical count owner/identity/assertion received acceptance:", pin.Function, cut)
		}
	}
	if family145RunPhysicalPrevious("foreign.go", pin.Function, pin.After) != pin.After || family145RunPhysicalPrevious(pin.File, pin.Function+"Foreign", pin.After) != pin.After {
		t.Fatal("physical run count inverse accepted a foreign complete key")
	}
}

func TestFamily145RunPhysicalOwnedCallerInventoryIsFinite(t *testing.T) {
	root := "../../.."
	pins := runPhysicalPins(t)
	callers, definitions := 0, 0
	err := checkoutsource.WalkDir(root, filepath.Join(root, "internal/serveapp"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		c, d := runPhysicalOwnedFile(t, pins, filepath.ToSlash(rel), file)
		callers += c
		definitions += d
		return nil
	})
	if err != nil || callers != 9 || definitions != 2 {
		t.Fatalf("physical run count served inventory=%d/%d want9/2 err=%v", callers, definitions, err)
	}
}

func runPhysicalOwnedFile(t *testing.T, pins []runphysical.Pin, path string, file *ast.File) (int, int) {
	t.Helper()
	allowed := map[token.Pos]bool{}
	calls, definitions := 0, 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if isRunPhysicalReader(fn.Name.Name) {
			if path != "internal/serveapp/run_physical_cardinality_test.go" {
				t.Error("physical run count acquired a second helper owner:", path)
			}
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || !isRunPhysicalReader(id.Name) {
				return true
			}
			allowed[id.Pos()] = true
			if fn.Name.Name == "TestServedRunPhysicalCountsRefuseMissingSnapshotOwner" {
				return true
			}
			calls++
			current := servedSetupEarlierSource(t, path, fn.Name.Name, requestkey.Node(fn))
			if runphysical.Previous(pins, path, fn.Name.Name, current) == current {
				t.Error("physical run count caller is not the reviewed whole rule:", path, fn.Name.Name)
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && isRunPhysicalReader(id.Name) && !allowed[id.Pos()] {
			t.Error("physical run count escaped as an alias or initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}

func isRunPhysicalReader(name string) bool {
	return name == "readServedRunFieldRows" || name == "readServedRunConstructionHeaders"
}

func TestFamily145RunPhysicalExistingOwnersRetainExactScopeAndCoordinator(t *testing.T) {
	for _, owner := range [][2]string{
		{"internal/store/internal/runtimepersistence/test_workflow_projection_storage.go", "CountWorkflowRunFieldRowsForTest"},
		{"internal/store/internal/runtimepersistence/test_selected_runtime_outcome_observation.go", "CountRunFlowInstanceStorageForTest"},
		{"internal/store/internal/backend/pipelinepersistence/workflow_projection_observation.go", "CountWorkflowFieldRowsForRun"},
		{"internal/store/internal/backend/pipelinepersistence/runtime_outcome_construction_observation.go", "CountRunFlowInstanceStorageTx"},
	} {
		before, err := exec.Command("git", "show", runphysical.Predecessor+":"+owner[0]).Output()
		current, readErr := os.ReadFile(filepath.Join("../../..", owner[0]))
		if err != nil || readErr != nil || requestKeyDeclaration(t, before, owner[1]) != requestKeyDeclaration(t, current, owner[1]) {
			t.Fatal("physical run count changed its existing predicate/authority/error policy:", owner, err, readErr)
		}
	}
	current, err := os.ReadFile("../../../internal/serveapp/run_physical_cardinality_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for name, port := range map[string]string{"readServedRunFieldRows": "CountWorkflowRunFieldRows", "readServedRunConstructionHeaders": "CountRunFlowInstanceStorage"} {
		body := requestKeyDeclaration(t, current, name)
		if !strings.Contains(body, "reader.InspectSnapshot(context.Background()") || !strings.Contains(body, "storetest."+port+"(ctx, reader, runID)") || strings.Count(body, "storetest.") != 1 || strings.Contains(body, "Close(") || strings.Contains(body, "Database(") {
			t.Fatal("physical run count escaped its original read-only snapshot role:", name)
		}
	}
}
