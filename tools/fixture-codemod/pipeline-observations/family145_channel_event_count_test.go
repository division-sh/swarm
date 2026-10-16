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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeleventcount"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-channel-event-count-preimages.json
var family145ChannelEventCountBytes []byte

func channelEventCountPins(t *testing.T) []channeleventcount.Pin {
	t.Helper()
	pins, err := channeleventcount.Decode(family145ChannelEventCountBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ChannelEventCountPrevious(t *testing.T, file, function, current string) string {
	t.Helper()
	return channeleventcount.Previous(channelEventCountPins(t), file, function, current)
}

func family145ChannelEventCountCallerInverse(function, current string) string {
	pins, err := channeleventcount.Decode(family145ChannelEventCountBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channeleventcount.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckChannelEventCountWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range channelEventCountPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("channel event count escaped exact identity/assertion/cut:", file, function)
			}
			return true
		}
	}
	return false
}

func channelEventCountHostiles(t *testing.T, pin channeleventcount.Pin) {
	t.Helper()
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	pins := channelEventCountPins(t)
	for _, cut := range []string{"channelAnchorStorageObservation", "CountRunNamedEventStorage", "seed.RunID", "fork.ForkRunID", "runID", "mailbox.card_decided", "mailbox.card_superseded", "decisions", "supersessionCount", "events", " != ", " == ", "t.Fatal", "parentBefore", "time.After", "time.Sleep", "readParent", "kill()", "h.stop(t)", "context.Background()"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "foreignCountCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || channeleventcount.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("foreign context/identity/assertion/lifetime received inverse:", pin.Function, cut)
		}
	}
	if channeleventcount.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After || channeleventcount.Previous(pins, pin.File, pin.Function+"Foreign", pin.After) != pin.After {
		t.Fatal("channel event count inverse accepted a foreign complete key")
	}
}

func channelEventCountInventoryFile(path string, file *ast.File, counts map[string]int) error {
	for _, declaration := range file.Decls {
		fn, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		_, oldQueries, err := channeleventcount.Transform(requestkey.Node(fn))
		if err != nil || oldQueries != 0 {
			return fmt.Errorf("channel event count raw query survived: %s/%s: %v", path, fn.Name.Name, err)
		}
		if channeleventcount.Consumers[fn.Name.Name] {
			counts[fn.Name.Name] += channelEventCountCalls(fn)
		}
	}
	return nil
}

func channelEventCountCalls(fn *ast.FuncDecl) int {
	count := 0
	ast.Inspect(fn, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if method, ok := call.Fun.(*ast.SelectorExpr); ok && method.Sel.Name == "CountRunNamedEventStorage" {
				count++
			}
		}
		return true
	})
	return count
}
