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
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/workflowstate"
)

//go:embed family145-workflow-state-preimages.json
var family145WorkflowStateBytes []byte

func workflowStatePins(t *testing.T) []workflowstate.Pin {
	t.Helper()
	pins, err := workflowstate.Decode(family145WorkflowStateBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145WorkflowStateCallerInverse(function, current string) string {
	current = family145ServedEventCountCallerInverse(function, current)
	pins, err := workflowstate.Decode(family145WorkflowStateBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return workflowstate.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckWorkflowStateWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range workflowStatePins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("workflow state observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func workflowStateOriginalDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := exec.Command("git", "show", workflowstate.Predecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return requestKeyDeclaration(t, raw, function)
}

func workflowStateCurrentDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", path))
	if err != nil {
		t.Fatal(err)
	}
	entry := servedObservationEarlierSource(t, path, function, requestKeyDeclaration(t, raw, function))
	return family145ServedEventCountPrevious(t, path, function, entry)
}

func workflowStateHostiles(t *testing.T, pin workflowstate.Pin) {
	t.Helper()
	pins := workflowStatePins(t)
	if workflowstate.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || workflowstate.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("workflow state inverse admitted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"InspectSnapshot", "ReadWorkflowStateEntityStorage", "CountRunStateEntityStorage", "reader", "ReceiverStateReader", "runID", "prefix", "state", "t.Fatal", " != ", " == ", "found", "15 * time.Second", "20 * time.Millisecond", "want", "servedInspectionRunDebugSummary", "entityID", "pause", "fork", "source", "gate", "InstancePath"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedStatusCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || workflowstate.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed workflow state identity/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func workflowStateOwnedFile(t *testing.T, path string, file *ast.File) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if workflowstate.Helpers[fn.Name.Name] {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && workflowstate.Helpers[id.Name] {
				calls++
				allowed[id.Pos()] = true
				current := servedObservationEarlierSource(t, path, fn.Name.Name, requestkey.Node(fn))
				current = family145ServedEventCountPrevious(t, path, fn.Name.Name, current)
				if workflowstate.Previous(workflowStatePins(t), path, fn.Name.Name, current) == current {
					t.Error("workflow state caller escaped exact chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && workflowstate.Helpers[id.Name] && !allowed[id.Pos()] {
			t.Error("workflow state helper escaped as alias/initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}

func TestFamily145WorkflowStateNativePredicatesRemainExact(t *testing.T) {
	for _, row := range []struct{ helper, owner, query string }{
		{"requireLifecycleFlowEntity", "internal/store/internal/backend/pipelinepersistence/workflow_state_entity_observation.go", "ReadWorkflowStateEntityStorageTx"},
		{"requirePendingInputStateCount", "internal/store/internal/backend/entityruntime/run_state_cardinality.go", "CountRunStateEntityStorageTx"},
	} {
		var before string
		for _, pin := range workflowStatePins(t) {
			if pin.Function == row.helper {
				before = workflowStateOriginalDeclaration(t, pin.File, pin.Function)
			}
		}
		oldSQL := channelAnchorSQL(t, before)
		newSQL := channelAnchorSQL(t, workflowStateCurrentDeclaration(t, row.owner, row.query))
		if len(oldSQL) != 1 || !reflect.DeepEqual(oldSQL, newSQL) {
			t.Fatal("physical state predicate changed:", row.helper, oldSQL, newSQL)
		}
	}
}
