package main

import (
	_ "embed"
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/sourcefork"
)

//go:embed family145-source-fork-cardinality-preimages.json
var sourceForkBytes []byte

func sourceForkPins(t *testing.T) []sourcefork.Pin {
	t.Helper()
	pins, err := sourcefork.Decode(sourceForkBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func sourceForkHostileCuts(t *testing.T, pin sourcefork.Pin, finite recipe) {
	t.Helper()
	for _, cut := range []string{"readServedSourceForkCardinality", "CountSelectedForkRunsForSource", "f.ctx", "f.selected", "reader", "ReceiverStateReader", "sourceRunID", "initial.RunID", "seed.RunID", "childCount", "forks", "err != nil", "t.Fatal", "children != 0", "childCount != 1", "forkRuns != 0", "waitServedRunDeliveryQuiescence", "requestServedJSONRPC", "deploymentForkRPC", "Materialization", "retry.Error", "operationBefore"} {
		if strings.Contains(pin.After, cut) {
			mutant := strings.Replace(pin.After, cut, "foreignSourceForkCut", 1)
			if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
				t.Fatal("changed source fork owner/identity/assertion/cut accepted:", pin.Function, cut)
			}
		}
	}
	if _, err := runphysical.Transform(sourcefork.Rules[sourceForkRuleIndex(pin)], strings.Replace(pin.Before, "forked_from_run_id", "foreign_source_id", 1)); err == nil {
		t.Fatal("changed predecessor predicate accepted:", pin.Function)
	}
}

func sourceForkRuleIndex(pin sourcefork.Pin) int {
	for index, rule := range sourcefork.Rules {
		if rule.File == pin.File && rule.Function == pin.Function {
			return index
		}
	}
	panic("unmanifested source fork rule")
}

func sourceForkSingleSourceSQL(file *ast.File) error {
	var failure error
	ast.Inspect(file, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		value, err := strconv.Unquote(literal.Value)
		if err != nil {
			failure = err
			return false
		}
		compact := strings.Join(strings.Fields(strings.ToLower(value)), "")
		if compact == "selectcount(*)fromrunswhereforked_from_run_id=$1" || compact == "selectcount(*)fromrunswhereforked_from_run_id=$1::uuid" {
			failure = fmt.Errorf("single-source physical fork count bypasses existing native owner")
		}
		return true
	})
	return failure
}
