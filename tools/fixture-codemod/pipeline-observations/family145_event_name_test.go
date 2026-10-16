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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/eventname"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-event-name-preimages.json
var family145EventNameBytes []byte

func eventNamePins(t *testing.T) []eventname.Pin {
	t.Helper()
	pins, err := eventname.Decode(family145EventNameBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145EventNameCallerInverse(function, current string) string {
	current = family145ChannelAnchorCallerInverse(function, current)
	pins, err := eventname.Decode(family145EventNameBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return eventname.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckEventNameWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	for _, pin := range eventNamePins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("event name observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func eventNameOriginalDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := exec.Command("git", "show", eventname.Predecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return requestKeyDeclaration(t, raw, function)
}

func eventNameCurrentDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", path))
	if err != nil {
		t.Fatal(err)
	}
	return sourceBlobPreviousDeclaration(t, path, function, requestKeyDeclaration(t, raw, function))
}

func eventNameHostiles(t *testing.T, pin eventname.Pin) {
	t.Helper()
	pins := eventNamePins(t)
	if eventname.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || eventname.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("event name inverse admitted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"InspectSnapshot", "CountEventNameStorage", "CountRunNamedEventStorage", "reader", "ReceiverStateReader", "eventID", "runID", "statuses...", "status", "subscriberID", "subscriberType", "t.Fatal", " != ", " == ", "got == want", "servedProofPollDeadline", "duration", "25 * time.Millisecond", "servedInspectionRunDebugSummary", "waitForReady", "captureSelectedRuntimePersistence", "releaseOnce.Do", "250*time.Millisecond"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedStatusCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || eventname.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed event name identity/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func eventNameOwnedFile(t *testing.T, path string, file *ast.File, proof sourceBlobDeclarationInvocation) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if eventname.Helpers[fn.Name.Name] {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && eventname.Helpers[id.Name] {
				calls++
				allowed[id.Pos()] = true
				current := proof.previous(t, path, fn.Name.Name, requestkey.Node(fn))
				if eventname.Previous(eventNamePins(t), path, fn.Name.Name, current) == current {
					t.Error("event name caller escaped exact chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && eventname.Helpers[id.Name] && !allowed[id.Pos()] {
			t.Error("event name helper escaped as alias/initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}
