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
