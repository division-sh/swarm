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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelintent"
)

//go:embed family145-channel-intent-preimages.json
var family145ChannelIntentBytes []byte

func channelIntentPins(t *testing.T) []channelintent.Pin {
	t.Helper()
	pins, err := channelintent.Decode(family145ChannelIntentBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ChannelIntentCallerInverse(function, current string) string {
	current = family145ChannelAnchorCallerInverse(function, current)
	pins, err := channelintent.Decode(family145ChannelIntentBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channelintent.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckChannelIntentWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145ChannelAnchorPrevious(t, file, function, current)
	for _, pin := range channelIntentPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("intent owner/capture/lifetime escaped full source rule:", file, function)
			}
			return true
		}
	}
	return false
}

func verifyIntentPredecessor(t *testing.T, pin channelintent.Pin) {
	t.Helper()
	if pin.Before == "" {
		return
	}
	raw, err := exec.Command("git", "show", channelintent.Predecessor+":"+pin.File).Output()
	if err != nil || intentDeclaration(t, raw, pin.Function) != pin.Before {
		t.Fatal("intent preimage is not its signed predecessor:", pin.File, pin.Function, err)
	}
}

func intentDeclaration(t *testing.T, raw []byte, name string) string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", raw, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.Name == name {
			return channelintent.Node(fn)
		}
		if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.TYPE && len(group.Specs) == 1 && "type/"+group.Specs[0].(*ast.TypeSpec).Name.Name == name {
			return channelintent.Node(group)
		}
	}
	return ""
}

func intentHostiles(t *testing.T, pin channelintent.Pin) {
	t.Helper()
	if pin.After == "" || strings.HasPrefix(pin.Function, "type/") || pin.Before == "" {
		return
	}
	pins := channelIntentPins(t)
	if channelintent.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || channelintent.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("intent inverse accepted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"intentIdentity", "successorIntent", "intentReader", "Interface.Key()", "Provider", "deadline", "5 * time.Second", "20 * time.Millisecond", "first && second", "found", "release()", "kill()", "reflect.DeepEqual", "!= beforeACK", "selectedChannelDraftState", "childIntent", "State != \"pending\"", "Disposition != \"\"", "t.Fatal", "originalSource", "parentBefore"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedIntentCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || channelintent.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("lost intent identity/assertion/cut accepted:", pin.Function, cut)
		}
	}
}
