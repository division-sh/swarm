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
	"strconv"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/rundelivery"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

//go:embed family145-run-delivery-cardinality-preimages.json
var runDeliveryBytes []byte

func runDeliveryHostileCuts(t *testing.T, pin rundelivery.Pin, finite recipe) {
	t.Helper()
	for _, cut := range []string{"readServedRunDeadLetterCount", "readServedRunNonterminalDeliveryCount", "CountRunDeadLetterDeliveries", "CountRunDeliveryStorage", "CountDeploymentPendingDeliveriesStorage", "f.ctx", "f.selected", "childID", "runID", "ReceiverStateReader", "ctx,", "failed", "stillUnsettled", "err != nil", "t.Fatal", "waitServedRunDeliveryQuiescence", "run.diagnose", "deploymentForkRPC", "ReadinessProbe", "calls.Load()", "post[0].State", "providerCalls", "providerCalls", "result.ForkRunID", "time.Sleep", "time.After", "forkContext"} {
		if !strings.Contains(pin.After, cut) {
			continue
		}
		mutant := strings.Replace(pin.After, cut, "unreviewedRunDeliveryCut", 1)
		if _, _, err := rewriteFunction(pin.File, []byte("package proof\n"+mutant), finite); err == nil {
			t.Fatal("changed run delivery context/owner/identity/polling/replay cut accepted:", pin.Function, cut)
		}
	}
	rule := rundelivery.Rules[runDeliveryRuleIndex(pin)]
	for _, edit := range rule.Edits {
		mutant := strings.Replace(pin.Before, edit.Before, strings.Replace(edit.Before, "event_deliveries", "foreign_deliveries", 1), 1)
		if mutant == pin.Before {
			t.Fatal("predecessor predicate mutation stopped matching:", pin.Function)
		}
		if _, err := runphysical.Transform(rule, mutant); err == nil {
			t.Fatal("changed targeted predecessor delivery predicate accepted:", pin.Function)
		}
	}
}

func runDeliveryRuleIndex(pin rundelivery.Pin) int {
	for index, rule := range rundelivery.Rules {
		if rule.File == pin.File && rule.Function == pin.Function {
			return index
		}
	}
	panic("unmanifested run delivery rule")
}

func runDeliveryStandaloneSQL(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "QueryRow" && selector.Sel.Name != "QueryRowContext" {
			return true
		}
		for _, argument := range call.Args {
			literal, ok := argument.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				continue
			}
			query, err := strconv.Unquote(literal.Value)
			if err != nil {
				found = true
				continue
			}
			compact := strings.Join(strings.Fields(strings.ToLower(query)), "")
			for _, suffix := range []string{"", "andstatus='dead_letter'", "andstatusnotin('delivered','dead_letter')", "andstatusin('pending','in_progress')"} {
				if compact == "selectcount(*)fromevent_deliverieswhererun_id=$1"+suffix {
					found = true
				}
			}
		}
		return true
	})
	return found
}
