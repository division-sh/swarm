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
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channelanchor"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
)

//go:embed family145-channel-anchor-preimages.json
var family145ChannelAnchorBytes []byte

func channelAnchorPins(t *testing.T) []channelanchor.Pin {
	t.Helper()
	pins, err := channelanchor.Decode(family145ChannelAnchorBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func family145ChannelAnchorPrevious(t *testing.T, file, function, current string) string {
	t.Helper()
	current = family145ChannelDecisionPrevious(t, file, function, current)
	return channelanchor.Previous(channelAnchorPins(t), file, function, current)
}

func family145ChannelAnchorCallerInverse(function, current string) string {
	current = family145ChannelDecisionCallerInverse(function, current)
	current = family145WorkflowStateCallerInverse(function, current)
	pins, err := channelanchor.Decode(family145ChannelAnchorBytes)
	if err != nil {
		panic(err)
	}
	for _, pin := range pins {
		if pin.Function == function {
			return channelanchor.Previous(pins, pin.File, function, current)
		}
	}
	return current
}

func family145CheckChannelAnchorWholeRule(t *testing.T, file, function, current string) bool {
	t.Helper()
	current = family145ChannelDecisionPrevious(t, file, function, current)
	for _, pin := range channelAnchorPins(t) {
		if pin.File == file && pin.Function == function {
			if current != pin.After {
				t.Fatal("channel anchor observation escaped exact complete rule:", file, function)
			}
			return true
		}
	}
	return false
}

func channelAnchorOriginalDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := exec.Command("git", "show", channelanchor.Predecessor+":"+path).Output()
	if err != nil {
		t.Fatal(err)
	}
	return requestKeyDeclaration(t, raw, function)
}

func channelAnchorCurrentDeclaration(t *testing.T, path, function string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("../../..", path))
	if err != nil {
		t.Fatal(err)
	}
	return sourceBlobPreviousDeclaration(t, path, function, requestKeyDeclaration(t, raw, function))
}

func TestFamily145ChannelAnchorNativeQueriesRetainExactPhysicalPredicates(t *testing.T) {
	for _, row := range []struct{ helper, owner, query string }{
		{"waitChannelAnchorCardInState", "internal/store/internal/backend/decisioncard/channel_anchor_observation.go", "ReadChannelAnchorCardsStorageTx"},
		{"waitChannelAnchorDecision", "internal/store/internal/backend/decisioncard/channel_anchor_observation.go", "ReadChannelAnchorDecisionStorageTx"},
		{"waitChannelAnchorReceipt", "internal/store/internal/backend/channeldelivery/source_receipt_observation.go", "ReadFirstSentSourceReceiptStorageTx"},
		{"loadServedStandingOwnerByFlow", "internal/store/internal/backend/standingdisposition/flow_storage_observation.go", "ReadStandingFlowStorageTx"},
	} {
		var old string
		for _, pin := range channelAnchorPins(t) {
			if pin.Function == row.helper {
				old = channelAnchorOriginalDeclaration(t, pin.File, pin.Function)
			}
		}
		before := channelAnchorSQL(t, old)
		after := channelAnchorSQL(t, channelAnchorCurrentDeclaration(t, row.owner, row.query))
		if len(before) == 0 || !reflect.DeepEqual(before, after) {
			t.Fatalf("native owner changed physical predicate/order/limit: %s before=%v after=%v", row.helper, before, after)
		}
	}
}

func TestFamily145ChannelAnchorCleanupIsRegisteredBeforeAnyChildCanOpenReader(t *testing.T) {
	const path = "internal/serveapp/channel_onboarding_identity_e2e_test.go"
	const function = "newChannelOnboardingE2EHarness"
	before := channelAnchorOriginalDeclaration(t, path, function)
	old := "return &channelOnboardingE2EHarness{opts: opts, provider: provider, telegram: telegram, credentialPath: credentialPath, backend: backend, storeDSN: storeDSN}"
	replacement := "h := &channelOnboardingE2EHarness{opts: opts, provider: provider, telegram: telegram, credentialPath: credentialPath, backend: backend, storeDSN: storeDSN}\nt.Cleanup(func() {\nif h.anchorObservation != nil {\nif err := h.anchorObservation.Close(); err != nil { t.Error(err) }\n}\n})\nreturn h"
	if strings.Count(before, old) != 1 {
		t.Fatal("parent constructor predecessor changed")
	}
	expected, err := requestkey.Normalize(strings.Replace(before, old, replacement, 1))
	if err != nil || channelAnchorCurrentDeclaration(t, path, function) != expected {
		t.Fatal("parent cleanup/creation/return escaped its exact extension:", err)
	}
	for _, mutant := range []string{
		strings.Replace(expected, "t.Cleanup", "child.Cleanup", 1),
		strings.Replace(expected, "return h", "return h\nneverRegisterCleanup()", 1),
		strings.Replace(expected, "h.anchorObservation.Close()", "skipClose()", 1),
	} {
		if mutant == expected {
			t.Fatal("cleanup control did not change its observed cut")
		}
		row := recipe{File: path, Function: function, Before: before, After: expected}
		if _, _, err := rewriteFunction(path, []byte("package proof\n"+mutant), row); err == nil {
			t.Fatal("lost/late/child-owned cleanup accepted")
		}
	}
}

func channelAnchorSQL(t *testing.T, source string) []string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "query.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	var queries []string
	ast.Inspect(file, func(node ast.Node) bool {
		if literal, ok := node.(*ast.BasicLit); ok && literal.Kind == token.STRING {
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(value, "SELECT ") {
				queries = append(queries, value)
			}
		}
		return true
	})
	return queries
}

func TestFamily145ChannelAnchorTransformRefusesForeignCarriersAndShadowedCalls(t *testing.T) {
	for _, before := range []string{
		`func foreign(t *testing.T) { waitChannelAnchorReceipt(t, foreign.DB, "x") }`,
		`func foreign(t *testing.T) { waitChannelAnchorReceipt(t, db, "x") }`,
		`func TestChannelSourceLifecyclePublicJourney(t *testing.T) { waitChannelAnchorReceipt(t, db, "x", "extra") }`,
		`func TestChannelSourceLifecyclePublicJourney(t *testing.T) { waitChannelAnchorReceipt := foreign; waitChannelAnchorReceipt(t, db, "x") }`,
	} {
		if _, _, err := channelanchor.Transform(before); err == nil {
			t.Fatal("unclassified carrier/arity/owner accepted:", before)
		}
	}
}

func TestFamily145ChannelAnchorWholeChainPreservesEveryPollAndRefusalCut(t *testing.T) {
	calls := 0
	seen := map[string]bool{}
	for _, pin := range channelAnchorPins(t) {
		key := pin.File + "/" + pin.Function
		if seen[key] || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("changed/duplicate channel anchor predecessor:", key)
		}
		seen[key] = true
		original, err := exec.Command("git", "show", channelanchor.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, original, pin.Function) != pin.Before {
			t.Fatal("channel anchor source is not its signed predecessor:", key, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || family145ChannelDecisionPrevious(t, pin.File, pin.Function, sourceBlobPreviousDeclaration(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function))) != pin.After {
			t.Fatal("channel anchor current source escaped its whole rule:", key, err)
		}
		after, count, err := channelanchor.Transform(pin.Before)
		if err != nil || after != pin.After || count != pin.Calls {
			t.Fatal("channel anchor finite transform escaped its rule:", key, err)
		}
		calls += count
		channelAnchorHostiles(t, pin)
	}
	if calls != 54 || len(seen) != 26 {
		t.Fatalf("channel anchor inventory=%d/%d want54/26", calls, len(seen))
	}
}

func channelAnchorHostiles(t *testing.T, pin channelanchor.Pin) {
	t.Helper()
	pins := channelAnchorPins(t)
	if channelanchor.Previous(pins, pin.File, pin.Function, pin.After) != pin.Before || channelanchor.Previous(pins, "foreign.go", pin.Function, pin.After) != pin.After {
		t.Fatal("channel anchor inverse admitted foreign/missing source")
	}
	finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
	for _, cut := range []string{"channelAnchorStorageObservation", "ReadChannelAnchorCardsStorage", "ReadFirstSentSourceReceiptStorage", "ReadStandingFlowStorage", "reader", "runID", "cardID", "FlowInstance", "status", "kind", "t.Fatal", " != ", " == ", "deadline", "20 * time.Millisecond", "DecodeAnchor", "sql.ErrNoRows", "LIMIT 1", "PauseNext", "release()", "kill()", "recovery", "generation", "ReadChannelAnchorDecisionStorage"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedStatusCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil || channelanchor.Previous(pins, pin.File, pin.Function, mutant) != mutant {
			t.Fatal("changed channel anchor identity/assertion/lifetime accepted:", pin.Function, cut)
		}
	}
}

func TestFamily145ChannelAnchorOwnedInventoryHasNoRawCallerOrAlias(t *testing.T) {
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
		n, d := channelAnchorOwnedFile(t, filepath.ToSlash(rel), file)
		calls += n
		definitions += d
		return nil
	})
	if err != nil || calls != 54 || definitions != 6 {
		t.Fatalf("channel anchor complete inventory=%d/%d want54/6 err=%v", calls, definitions, err)
	}
}

func channelAnchorOwnedFile(t *testing.T, path string, file *ast.File) (int, int) {
	t.Helper()
	calls, definitions := 0, 0
	allowed := map[token.Pos]bool{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if channelanchor.Helpers[fn.Name.Name] != 0 {
			definitions++
			allowed[fn.Name.Pos()] = true
		}
		ast.Inspect(fn, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if ok && channelanchor.Helpers[id.Name] != 0 {
				calls++
				allowed[id.Pos()] = true
				current := family145ChannelDecisionPrevious(t, path, fn.Name.Name, sourceBlobPreviousDeclaration(t, path, fn.Name.Name, requestkey.Node(fn)))
				if channelanchor.Previous(channelAnchorPins(t), path, fn.Name.Name, current) == current {
					t.Error("channel anchor caller escaped exact chain:", path, fn.Name.Name)
				}
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && channelanchor.Helpers[id.Name] != 0 && !allowed[id.Pos()] {
			t.Error("channel anchor helper escaped as alias/initializer:", path, id.Name)
		}
		return true
	})
	return calls, definitions
}
