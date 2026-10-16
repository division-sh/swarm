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
