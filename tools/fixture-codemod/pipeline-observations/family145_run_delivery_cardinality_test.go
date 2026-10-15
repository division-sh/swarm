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

func TestFamily145RunDeliveryCardinalityWholeRuleKeepsExactPredicatesAndTemporalCuts(t *testing.T) {
	pins, err := rundelivery.Decode(runDeliveryBytes)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	for index, pin := range pins {
		before, err := exec.Command("git", "show", rundelivery.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, before, pin.Function) != pin.Before || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("run delivery predecessor changed:", pin.Function, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err != nil || servedSetupEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("run delivery current endpoint changed:", pin.Function, err)
		}
		after, err := runphysical.Transform(rundelivery.Rules[index], pin.Before)
		if err != nil || after != pin.After || pin.Calls != rundelivery.Rules[index].Calls {
			t.Fatal("run delivery escaped the finite query-expression rule:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		out, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("run delivery finite rewrite refused:", err)
		}
		if _, changed, err := rewriteFunction(pin.File, out, finite); err != nil || changed {
			t.Fatal("run delivery rewrite is not inert:", err)
		}
		runDeliveryHostileCuts(t, pin, finite)
		calls += pin.Calls
	}
	if calls != 6 {
		t.Fatal("run delivery standalone inventory changed:", calls)
	}
}

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

func TestFamily145RunDeliveryCardinalityKeepsAllExistingNativeOwners(t *testing.T) {
	for _, owner := range [][2]string{
		{"internal/store/storetest/served_delivery_observation.go", "CountRunDeadLetterDeliveries"},
		{"internal/store/internal/runtimepersistence/test_served_delivery_observation.go", "CountRunDeadLetterDeliveriesForTest"},
		{"internal/store/internal/backend/delivery/read_projections.go", "CountRunDeadLetterDeliveriesTx"},
		{"internal/store/storetest/gate_recovery_fixture.go", "CountRunNonterminalDeliveryStorage"},
		{"internal/store/internal/runtimepersistence/test_gate_recovery_fixture.go", "CountRunNonterminalDeliveryStorageForTest"},
		{"internal/store/internal/backend/delivery/read_projections.go", "CountRunNonterminalDeliveryStorageTx"},
		{"internal/store/storetest/deployment_resource_observation.go", "CountDeploymentPendingDeliveriesStorage"},
		{"internal/store/internal/runtimepersistence/test_deployment_resource_observation.go", "CountDeploymentPendingDeliveriesStorageForTest"},
		{"internal/store/internal/backend/delivery/read_projections.go", "CountDeploymentPendingDeliveriesStorageTx"},
		{"internal/store/storetest/cli_fork_fixture.go", "CountRunDeliveryStorage"},
		{"internal/store/internal/runtimepersistence/test_cli_fork_storage.go", "CountRunDeliveryStorageForTest"},
		{"internal/store/internal/backend/delivery/read_projections.go", "CountRunDeliveryStorageTx"},
	} {
		before, err := exec.Command("git", "show", rundelivery.Predecessor+":"+owner[0]).Output()
		current, readErr := os.ReadFile(filepath.Join("../../..", owner[0]))
		if err != nil || readErr != nil || requestKeyDeclaration(t, before, owner[1]) != requestKeyDeclaration(t, current, owner[1]) {
			t.Fatal("run delivery native owner changed:", owner, err, readErr)
		}
	}
}

func TestFamily145RunDeliveryCardinalityStandaloneInventoryIsClosed(t *testing.T) {
	for _, directory := range []string{"internal/serveapp", "internal/runtime/conformance"} {
		err := checkoutsource.WalkDir("../../..", filepath.Join("../../..", directory), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
			if err != nil {
				return err
			}
			if runDeliveryStandaloneSQL(file) {
				return fmt.Errorf("%s: standalone run delivery count bypasses existing native owner", path)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range []string{"SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1", "SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status='dead_letter'", "SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status NOT IN ('delivered','dead_letter')", "SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')"} {
		for _, source := range []string{"package proof\nfunc escape(){db.QueryRow(" + strconv.Quote(query) + ",run)}", "package proof\nvar escape=func(){db.QueryRowContext(ctx," + strconv.Quote(query) + ",run)}"} {
			file, err := parser.ParseFile(token.NewFileSet(), "escape.go", source, parser.AllErrors)
			if err != nil || !runDeliveryStandaloneSQL(file) {
				t.Fatal("standalone/initializer count reintroduction was not rejected:", query, err)
			}
		}
	}
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
