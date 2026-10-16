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

func TestFamily145ChannelDecisionTransformRefusesForeignCarriersAndShadowedCalls(t *testing.T) {
	for _, before := range []string{
		`func foreign(t *testing.T) { waitUncertainCopyDecision(t, foreign.DB, "x") }`,
		`func foreign(t *testing.T) { waitUncertainCopyDecision(t, db, "x") }`,
		`func TestChannelSourceLifecyclePublicJourney(t *testing.T) { waitUncertainCopyDecision(t, db, "x", "extra") }`,
		`func TestChannelSourceLifecyclePublicJourney(t *testing.T) { waitUncertainCopyDecision := foreign; waitUncertainCopyDecision(t, db, "x") }`,
	} {
		if _, _, err := channeldecision.Transform(before); err == nil {
			t.Fatal("unclassified carrier/arity/owner accepted:", before)
		}
	}
}

func TestFamily145ChannelDecisionWholeChainPreservesEveryPollAndRefusalCut(t *testing.T) {
	proof := sourceBlobDeclarationsForInvocation(t)
	calls := 0
	seen := map[string]bool{}
	for _, pin := range channelDecisionPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("changed/duplicate channel decision predecessor:", key)
		}
		seen[key] = true
		original, err := exec.Command("git", "show", channeldecision.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, original, pin.Function) != pin.Before {
			t.Fatal("channel decision source is not its signed predecessor:", key, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || family145ChannelEventCountPrevious(t, pin.File, pin.Function, proof.previous(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function))) != pin.After {
			t.Fatal("channel decision current source escaped its whole rule:", key, err)
		}
		after, count, err := channeldecision.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("channel decision finite transform escaped its rule:", key, err)
		}
		calls += count
		channelDecisionHostiles(t, pin)
	}
	if calls != 3 || len(seen) != 5 {
		t.Fatalf("channel decision inventory=%d/%d want3/5", calls, len(seen))
	}
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

func TestFamily145ChannelDecisionOwnedInventoryHasNoRawCallerOrAlias(t *testing.T) {
	proof := sourceBlobDeclarationsForInvocation(t)
	root := "../../.."
	calls, definitions := 0, 0
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
		n, d := channelDecisionOwnedFile(t, filepath.ToSlash(rel), file, proof)
		calls += n
		definitions += d
		return nil
	})
	if err != nil || calls != 3 || definitions != 1 {
		t.Fatalf("channel decision complete inventory=%d/%d want3/1 err=%v", calls, definitions, err)
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

func TestFamily145ChannelDecisionReusesTheExactExistingCanonicalQuery(t *testing.T) {
	const path = "internal/store/internal/backend/decisioncard/channel_anchor_observation.go"
	const name = "ReadChannelAnchorDecisionStorageTx"
	if channelDecisionOriginalDeclaration(t, path, name) != channelDecisionCurrentDeclaration(t, path, name) {
		t.Fatal("existing status/verdict owner changed instead of being reused")
	}
	for _, pin := range channelDecisionPins(t) {
		if pin.Function == "waitUncertainCopyDecision" {
			before := channelAnchorSQL(t, pin.Before)
			after := channelAnchorSQL(t, channelDecisionCurrentDeclaration(t, path, name))
			if len(before) != 1 || len(after) != 1 || before[0] != after[0] {
				t.Fatal("canonical decision predicate differs from its consumer")
			}
		}
	}
}
