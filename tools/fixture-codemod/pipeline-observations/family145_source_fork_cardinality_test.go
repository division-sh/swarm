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
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
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

func TestFamily145SourceForkCardinalityWholeRuleKeepsIdentityAssertionsAndCuts(t *testing.T) {
	calls := 0
	for index, pin := range sourceForkPins(t) {
		before, err := exec.Command("git", "show", sourcefork.Predecessor+":"+pin.File).Output()
		if err != nil || requestKeyDeclaration(t, before, pin.Function) != pin.Before || fmt.Sprintf("%x", sha256.Sum256([]byte(pin.Before))) != pin.SHA256 {
			t.Fatal("source fork count predecessor is not complete signed source:", pin.Function, err)
		}
		current, err := os.ReadFile(filepath.Join("../../..", pin.File))
		if err == nil {
			current, err = selectedreadrole.CatchUp2542HistoricalSource("../../..", pin.File, current)
		}
		if err != nil || servedObservationEarlierSource(t, pin.File, pin.Function, requestKeyDeclaration(t, current, pin.Function)) != pin.After {
			t.Fatal("source fork count output escaped its whole body:", pin.Function, err)
		}
		after, err := runphysical.Transform(sourcefork.Rules[index], pin.Before)
		if err != nil || after != pin.After || pin.Calls != sourcefork.Rules[index].Calls {
			t.Fatal("source fork count escaped finite propagation:", pin.Function, err)
		}
		finite := recipe{File: pin.File, Function: pin.Function, Before: pin.Before, After: pin.After}
		out, changed, err := rewriteFunction(pin.File, []byte("package proof\n"+pin.Before), finite)
		if err != nil || !changed {
			t.Fatal("source fork count finite transform refused:", pin.Function, err)
		}
		if _, changed, err := rewriteFunction(pin.File, out, finite); err != nil || changed {
			t.Fatal("source fork count transform is not inert:", pin.Function, err)
		}
		sourceForkHostileCuts(t, pin, finite)
		calls += pin.Calls
	}
	if calls != 8 {
		t.Fatalf("source fork count inventory=%d want8", calls)
	}
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

func TestFamily145SourceForkCardinalityRetainsNativeOwnerAndBothPointConsumers(t *testing.T) {
	for _, owner := range [][2]string{
		{"internal/store/storetest/run_fork.go", "CountSelectedForkRunsForSource"},
		{"internal/store/internal/runtimepersistence/test_selected_runtime_outcome_observation.go", "CountSelectedForkRunsForSourceForTest"},
		{"internal/store/internal/backend/runforkpersistence/selected_refusal_observation.go", "CountSelectedForkRunsForSourceTx"},
		{"internal/runtime/conformance/fan_out_resource_selected_fork_test.go", "TestDeploymentSourceChangedPinPublicForkAndLostResponse"},
		{"internal/runtime/conformance/fan_out_resource_selected_fork_test.go", "TestEventSourceChangedPinPublicForkAndLostResponse"},
	} {
		before, err := exec.Command("git", "show", sourcefork.Predecessor+":"+owner[0]).Output()
		current, readErr := os.ReadFile(filepath.Join("../../..", owner[0]))
		if err != nil || readErr != nil || requestKeyDeclaration(t, before, owner[1]) != requestKeyDeclaration(t, current, owner[1]) {
			t.Fatal("source fork owner or point consumer changed:", owner, err, readErr)
		}
	}
	raw, err := os.ReadFile("../../../internal/serveapp/source_fork_cardinality_observation_test.go")
	want := `func readServedSourceForkCardinality(reader servedEventCountReader, sourceRun string)(int,error){
if reader==nil{return 0,fmt.Errorf("source fork count requires the original selected reader")};var count int
err:=reader.InspectSnapshot(context.Background(),func(ctx context.Context)error{var err error;count,err=storetest.CountSelectedForkRunsForSource(ctx,reader,sourceRun);return err})
if err!=nil{return 0,err};return count,nil
}`
	if err != nil || requestKeyDeclaration(t, raw, "readServedSourceForkCardinality") != requestKeyDeclaration(t, []byte("package proof\n"+want), "readServedSourceForkCardinality") {
		t.Fatal("source fork read escaped its one borrowed cut or failure acknowledgment:", err)
	}
}

func TestFamily145SourceForkCardinalitySingleSourceRawInventoryIsClosed(t *testing.T) {
	for _, directory := range []string{"internal/serveapp", "internal/runtime/conformance"} {
		err := checkoutsource.WalkDir("../../..", filepath.Join("../../..", directory), func(path string, entry fs.DirEntry, err error) error {
			if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") {
				return err
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.AllErrors)
			if err != nil {
				return err
			}
			if err := sourceForkSingleSourceSQL(file); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range []string{"SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1", "SELECT count(*) FROM runs WHERE forked_from_run_id = $1::uuid"} {
		file, err := parser.ParseFile(token.NewFileSet(), "hostile.go", "package proof\nvar escaped="+strconv.Quote(query), parser.AllErrors)
		if err != nil || sourceForkSingleSourceSQL(file) == nil {
			t.Fatal("source fork raw literal/initializer reintroduction was not rejected:", query, err)
		}
	}
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
