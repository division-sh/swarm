package main

import (
	_ "embed"
	"fmt"
	"go/ast"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/servedeventcount"
)

//go:embed family145-served-event-count-preimages.json
var family145ServedEventCountBytes []byte

func servedEventCountPins(t *testing.T) []servedeventcount.Pin {
	t.Helper()
	pins, err := servedeventcount.Decode(family145ServedEventCountBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ServedEventCountPrevious(t *testing.T, file, function, current string) string {
	t.Helper()
	return servedeventcount.Previous(servedEventCountPins(t), file, function, current)
}

func family145ServedEventCountCallerInverse(function, current string) string {
	pins, err := servedeventcount.Decode(family145ServedEventCountBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return servedeventcount.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckServedEventCountWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range servedEventCountPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("served event count escaped exact identity/assertion/cut:", file, function)
			}
			return true
		}
	}
	return false
}

func servedEventCountHostiles(t *testing.T, pin servedeventcount.Pin) {
	t.Helper()
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	pins := servedEventCountPins(t)
	for _, cut := range []string{"readServedEventNameCountForRun", "ReceiverStateReader", "seed.RunID", "fork.ForkRunID", "runID", "name", "selected", "ack", "work.seeded", "mailbox.card_decided", "work.completed", "out.DecisionEvents", "out.OutcomeEvents", "publications", "copied", "count", " != ", " == ", "t.Fatal", "time.After", "time.Sleep", "kill()", "restart()", "stop()", "context.Background()"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignCountCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || servedeventcount.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("foreign context/identity/assertion/lifetime received inverse:", pin.Function, cut)
		}
	}
	if servedeventcount.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After || servedeventcount.Previous(pins, pin.File, pin.Function+"Foreign", pin.After) != pin.After {
		t.Fatal("served event count inverse accepted a foreign complete key")
	}
}

func servedEventCountInventoryFile(path string, file *ast.File, counts map[string]int) error {
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		_, oldQueries, err := servedeventcount.Transform(requestkey.Node(fn))
		if err != nil || oldQueries != 0 {
			return fmt.Errorf("served event count raw query survived: %s/%s: %v", path, fn.Name.Name, err)
		}
		if servedeventcount.Consumers[fn.Name.Name] != 0 {
			counts[fn.Name.Name] += servedEventCountCalls(fn)
		}
	}
	return nil
}

func servedEventCountCalls(fn *ast.FuncDecl) int {
	count := 0
	ast.Inspect(fn, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if function, ok := call.Fun.(*ast.Ident); ok && function.Name == "readServedEventNameCountForRun" {
				count++
			}
		}
		return true
	})
	return count
}
