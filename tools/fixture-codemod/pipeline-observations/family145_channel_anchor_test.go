package main

import (
	_ "embed"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

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

func channelAnchorOwnedFile(t *testing.T, path string, file *ast.File, proof sourceBlobDeclarationInvocation) (int, int) {
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
				native, err := channelProcessNativeControl("../../..", path, fn.Name.Name, requestkey.Node(fn))
				if err != nil {
					t.Error(err)
				}
				if native {
					return true
				}
				current := family145ChannelDecisionPrevious(t, path, fn.Name.Name, proof.previous(t, path, fn.Name.Name, requestkey.Node(fn)))
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
