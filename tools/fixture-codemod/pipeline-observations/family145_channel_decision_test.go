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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeldecision"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-channel-decision-preimages.json
var family145ChannelDecisionBytes []byte

func channelDecisionPins(t *testing.T) []channeldecision.Pin {
	t.Helper()
	pins, err := channeldecision.Decode(family145ChannelDecisionBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ChannelDecisionCallerInverse(function, current string) string {
	current = family145ChannelEventCountCallerInverse(function, current)
	pins, err := channeldecision.Decode(family145ChannelDecisionBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channeldecision.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckChannelDecisionWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145ChannelEventCountPrevious(t, file, function, current)
	for _, pin := range channelDecisionPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("channel decision observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func channelDecisionOriginalDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := exec.Command("git", "show", channeldecision.Predecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return requestKeyDeclaration(t, raw, function)
}

func channelDecisionCurrentDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", path))
	if err != nil {
		t.Fatal(err)
	}
	return family145ChannelEventCountPrevious(t, path, function, sourceBlobPreviousDeclaration(t, path, function, requestKeyDeclaration(t, raw, function)))
}

func family145ChannelDecisionPrevious(t *testing.T, file, function, current string) string {
	t.Helper()
	current = family145ChannelEventCountPrevious(t, file, function, current)
	return channeldecision.Previous(channelDecisionPins(t), file, function, current)
}

func channelDecisionHostiles(t *testing.T, pin channeldecision.Pin) {
	t.Helper()
	pins := channelDecisionPins(t)
	if channeldecision.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || channeldecision.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("channel decision inverse admitted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"channelAnchorStorageObservation", "ReadChannelAnchorDecisionStorage", "reader", "cardID", "child", "status", "verdict", "t.Fatal", " != ", " == ", "deadline", "20 * time.Millisecond", "5 * time.Second", "15 * time.Second", "parentBefore", "decisionErr", "replayedDecision", "PauseNext", "release()", "kill()", "recovery", "generation"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedStatusCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || channeldecision.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed channel decision identity/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func channelDecisionOwnedFile(t *testing.T, path string, file *ast.File, proof sourceBlobDeclarationInvocation) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if channeldecision.Helpers[fn.Name.Name] != 0 {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && channeldecision.Helpers[id.Name] != 0 {
				calls++
				allowed[id.Pos()] = true
				current := proof.previous(t, path, fn.Name.Name, requestkey.Node(fn))
				if family145ChannelDecisionPrevious(t, path, fn.Name.Name, current) == current {
					t.Error("channel decision caller escaped exact chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && channeldecision.Helpers[id.Name] != 0 && !allowed[id.Pos()] {
			t.Error("channel decision helper escaped as alias/initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}
