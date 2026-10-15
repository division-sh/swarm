package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func nativeFanOutBarrierSource(t *testing.T, source string) (string, int) {
	t.Helper()
	fs := token.NewFileSet()
	file, err := parser.ParseFile(fs, "barrier.go", "package fixture\n"+source, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn := file.Decls[0].(*ast.FuncDecl)
	if fn.Name.Name == "advanceFanOutBarriersForTest" {
		fn.Type.Params.List = append(fn.Type.Params.List[:3], fn.Type.Params.List[4:]...)
	}
	calls := 0
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name, ok := call.Fun.(*ast.Ident)
		if ok && name.Name == "advanceFanOutBarriersForTest" {
			calls++
			call.Args = append(call.Args[:3], call.Args[4:]...)
		}
		return true
	})
	var out bytes.Buffer
	if err := format.Node(&out, fs, fn); err != nil {
		t.Fatal(err)
	}
	return out.String(), calls
}

func TestNativeFanOutBarrierFixtureDropsRawAuthorityAcrossAll27Calls(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched, calls := 0, 0
	for _, row := range rows {
		if row.Family != "native-fanout-barrier-authority" {
			continue
		}
		matched++
		changed, count := nativeFanOutBarrierSource(t, row.Before)
		calls += count
		want, err := canonicalFunction(changed)
		got, afterErr := canonicalFunction(row.After)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		currentMatches := row.Successor == "" && actual == got
		if row.Successor != "" {
			if row.Function == "runForkBarrierFixedRevisionMatrix" {
				currentMatches = exactBarrierReadbackEndpointD1(row, actual)
			} else {
				_, successorErr := fixtureSetupCurrentEndpoint2617(row, actual)
				currentMatches = successorErr == nil
			}
		}
		if err != nil || afterErr != nil || sourceErr != nil || want != got || !currentMatches {
			t.Fatalf("%s changed barrier workload/owner/clock beyond unused argument removal", row.Function)
		}
		for _, cut := range []string{"advanceFanOutBarriersAttempt(ctx, selected,", "time.Now().UTC()", "base.Add(", "fixture.runID", "assertFanOutBarrierState(", "t.Fatal(", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedBarrierCut", 1)
			if mutant == row.After {
				continue
			}
			value, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && value == want {
				t.Fatalf("lost barrier mutation/temporal/failure assertion accepted: %s", cut)
			}
		}
	}
	if matched != 16 || calls != 27 {
		t.Fatalf("barrier propagation coverage=%d functions/%d calls,want16/27", matched, calls)
	}
}

// D1's settled explicit-child readback contract: reviewer-a6182379071/6183378580.
func exactBarrierReadbackEndpointD1(row recipe, actual string) bool {
	if row.Family != "native-fanout-barrier-authority" ||
		row.File != "internal/store/internal/runtimepersistence/fan_out_delivery_barrier_selected_store_test.go" ||
		row.Function != "runForkBarrierFixedRevisionMatrix" || row.Removed || row.Mechanical != nil ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "4f4a2a02fd3f6de98a62fc481907b45b5ff6ce35f14f1388615882a7aedb307f" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "9f4ec60231773fade1ac37f1b0a93695c49f93bb52e45aeccbeb0f120571ab19" {
		return false
	}
	expected := row.After
	for _, edit := range [][2]string{
		{"MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)", "MaterializeRunFork(context.Context, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)\n\t\t\t\tRequireMaterializedRunFork(context.Context, string, runfork.RunForkMaterializeRequest) (runfork.RunForkMaterialization, error)"},
		{"repeated, err := forkOwner.MaterializeRunFork(ctx, request)", "repeated, err := forkOwner.RequireMaterializedRunFork(ctx, materialized.ForkRunID, request)"},
	} {
		if strings.Count(expected, edit[0]) != 1 {
			return false
		}
		expected = strings.Replace(expected, edit[0], edit[1], 1)
	}
	return canonicalComparatorEqual2617(row.Successor, expected) && canonicalComparatorEqual2617(actual, expected)
}

func TestD1BarrierReadbackEndpointRejectsUnreviewedSuccessors(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "runForkBarrierFixedRevisionMatrix" {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		if !exactBarrierReadbackEndpointD1(row, actual) || exactBarrierReadbackEndpointD1(row, row.After) {
			t.Fatal("exact current readback endpoint or historical distinction changed")
		}
		for _, cut := range []string{"materialized.ForkRunID", "RequireMaterializedRunFork(ctx,", "snapshotForkHistoricalExecutionTables", "t.Fatal(", "t.Fatalf("} {
			mutant := row
			mutant.Successor = strings.Replace(row.Successor, cut, "unreviewedReadback", 1)
			if mutant.Successor == row.Successor || exactBarrierReadbackEndpointD1(mutant, mutant.Successor) {
				t.Fatalf("changed identity/owner/conservation/assertion accepted: %s", cut)
			}
		}
		for _, mutate := range []func(*recipe){
			func(r *recipe) { r.Family += "-unlisted" },
			func(r *recipe) { r.File = "unlisted.go" },
			func(r *recipe) { r.Function += "Unlisted" },
			func(r *recipe) { r.Before = r.After },
			func(r *recipe) { r.After = r.Before },
			func(r *recipe) { r.Successor = "" },
			func(r *recipe) { r.Removed = true },
			func(r *recipe) { r.Mechanical = &mechanicalSnapshot{} },
		} {
			mutant := row
			mutate(&mutant)
			if exactBarrierReadbackEndpointD1(mutant, actual) {
				t.Fatal("unlisted or crossed historical endpoint accepted")
			}
		}
	}
	if matched != 1 {
		t.Fatalf("D1 readback endpoints=%d, want1", matched)
	}
}
