package main

import (
	_ "embed"
	"go/ast"
	"go/token"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runscalar"
)

//go:embed family145-run-scalar-preimages.json
var family145RunScalarBytes []byte

func runScalarPins(t *testing.T) []runscalar.Pin {
	t.Helper()
	pins, err := runscalar.Decode(family145RunScalarBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145RunScalarCallerInverse(function, current string) string {
	current = family145DeliveryStatusCallerInverse(function, current)
	pins, err := runscalar.Decode(family145RunScalarBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return runscalar.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckRunScalarWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145DeliveryStatusCallerInverse(function, current)
	for _, pin := range runScalarPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("run scalar observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func runScalarHostiles(t *testing.T, pin runscalar.Pin) {
	t.Helper()
	pins := runScalarPins(t)
	if runscalar.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || runscalar.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("scalar inverse admitted foreign/missing source")
	}
	if pin.After == "" {
		if pin.Function != "servedEventPublishRowCount" {
			t.Fatal("unexpected retired scalar helper")
		}
		return
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"InspectSnapshot", "CountPhysicalRunIdentity", "CountApplicationRunEvents", "CountExactRunEventDeliveries", "reader", "captureSelectedRuntimePersistence", "observer", "eventID", "runID", "backend", "scope", "t.Fatal", "got !=", " != ", "return", "requireServedPublicationPossessionLossAbort", "commitLost"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedScalarCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || runscalar.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed scalar predicate/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func runScalarOwnedFile(t *testing.T, path string, file *ast.File) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "servedEventPublishScalarCount" {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && id.Name == "servedEventPublishScalarCount" {
				calls++
				allowed[id.Pos()] = true
				current := servedObservationEarlierSource(t, path, fn.Name.Name, requestkey.Node(fn))
				current = family145DeliveryStatusCallerInverse(fn.Name.Name, current)
				if runscalar.Previous(runScalarPins(t), path, fn.Name.Name, current) == current {
					t.Error("scalar caller escaped complete source-pinned chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && (id.Name == "servedEventPublishRowCount" || id.Name == "servedEventPublishScalarCount" && !allowed[id.Pos()]) {
			t.Error("scalar helper escaped as retired reference/alias/initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}
