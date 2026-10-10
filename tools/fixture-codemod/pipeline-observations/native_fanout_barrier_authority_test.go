package main

import (
	"bytes"
	"encoding/json"
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
		if err != nil || afterErr != nil || sourceErr != nil || want != got || actual != got {
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
