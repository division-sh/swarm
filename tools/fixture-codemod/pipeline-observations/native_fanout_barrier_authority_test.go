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

// #2720/6247003514 authorizes only these two observations at these exact cuts.
const n19FanOutBarrierIdentity = "native-fanout-barrier-authority/internal/store/internal/runtimepersistence/fan_out_delivery_barrier_selected_store_test.go/TestFanOutDeliveryBarrierMixedDispositionLifecycleOnBothStores"

const n19ArmedBarrierAnchor = "\t\t\tassertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 0)\n"

const n19ArmedBarrierObservation = `			report, err := owner.(selectedFanOutDiagnosticOwner).LoadRunDebugReport(ctx, fixture.runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if projected := operatorread.ProjectRunOperationalStatus(report); report.TestQuiescence.FanOutBarriers != 1 || projected.State != "running" || len(projected.Attributions) != 0 {
				t.Fatalf("armed delivery barrier was diagnosed as an agent stall: status=%+v work=%+v", projected, report.TestQuiescence)
			}
`

const n19PendingBarrierAnchor = `			if summary.BarrierArmed != 0 || summary.BarrierPending != 1 || summary.BarrierTerminal != 0 || !summary.BlocksCompletion() {
				t.Fatalf("mixed barrier public summary = %#v", summary)
			}
`

const n19PendingBarrierObservation = `			report, err = owner.(selectedFanOutDiagnosticOwner).LoadRunDebugReport(ctx, fixture.runID, operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if projected := operatorread.ProjectRunOperationalStatus(report); report.TestQuiescence.FanOutBarriers != 1 || projected.State != "running" || len(projected.Attributions) != 0 {
				t.Fatalf("pending barrier outcome was diagnosed as an agent stall: status=%+v work=%+v", projected, report.TestQuiescence)
			}
`

func validateN19FanOutBarrierSuccessor(row recipe, actual string) error {
	if row.Family+"/"+row.File+"/"+row.Function != n19FanOutBarrierIdentity || row.Removed || row.Mechanical != nil || row.Successor == "" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.Before))) != "5e8ad53e06a0c3754656052527a0a30037852d36c25d1b92cd84eda143e4e204" ||
		fmt.Sprintf("%x", sha256.Sum256([]byte(row.After))) != "95e050bc5850d2d1bea10ace12dfb0a05a5808db686d3d7515f58303599b4f36" {
		return fmt.Errorf("N19 barrier identity or historical endpoint changed")
	}
	restored := row.Successor
	for _, observation := range []struct{ anchor, block string }{
		{n19ArmedBarrierAnchor, n19ArmedBarrierObservation},
		{n19PendingBarrierAnchor, n19PendingBarrierObservation},
	} {
		if strings.Count(row.After, observation.anchor) != 1 || strings.Count(restored, observation.block) != 1 ||
			strings.Count(restored, observation.anchor+observation.block) != 1 {
			return fmt.Errorf("N19 barrier observation missing, duplicated or moved")
		}
		restored = strings.Replace(restored, observation.anchor+observation.block, observation.anchor, 1)
	}
	prior, priorErr := canonicalFunction(row.After)
	reversed, reverseErr := canonicalFunction(restored)
	current, currentErr := canonicalFunction(row.Successor)
	live, liveErr := canonicalFunction(actual)
	if priorErr != nil || reverseErr != nil || currentErr != nil || liveErr != nil || reversed != prior || current != live {
		return fmt.Errorf("N19 barrier successor changed historical work or differs from live source")
	}
	return nil
}

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
		if row.Family+"/"+row.File+"/"+row.Function == n19FanOutBarrierIdentity {
			currentMatches = validateN19FanOutBarrierSuccessor(row, actual) == nil
		} else if row.Successor != "" {
			_, successorErr := fixtureSetupCurrentEndpoint2617(row, actual)
			currentMatches = successorErr == nil
		}
		if err != nil || afterErr != nil || sourceErr != nil || want != got || !currentMatches {
			t.Fatalf("%s changed barrier workload/owner/clock beyond unused argument removal", row.Function)
		}
		if row.Family+"/"+row.File+"/"+row.Function == n19FanOutBarrierIdentity {
			for name, source := range map[string]string{
				"missing_armed_observation":   strings.Replace(row.Successor, n19ArmedBarrierObservation, "", 1),
				"missing_pending_observation": strings.Replace(row.Successor, n19PendingBarrierObservation, "", 1),
				"changed_armed_observation": strings.Replace(row.Successor, n19ArmedBarrierObservation,
					strings.Replace(n19ArmedBarrierObservation, `projected.State != "running"`, `projected.State == "running"`, 1), 1),
				"changed_pending_observation": strings.Replace(row.Successor, n19PendingBarrierObservation,
					strings.Replace(n19PendingBarrierObservation, `len(projected.Attributions) != 0`, `len(projected.Attributions) == 0`, 1), 1),
				"duplicate_observation": strings.Replace(row.Successor, n19PendingBarrierObservation,
					n19PendingBarrierObservation+n19PendingBarrierObservation, 1),
				"moved_observation": strings.Replace(strings.Replace(row.Successor, n19ArmedBarrierObservation, "", 1),
					n19PendingBarrierAnchor, n19ArmedBarrierObservation+n19PendingBarrierAnchor, 1),
				"lost_historical_assertion": strings.Replace(row.Successor, `assertFanOutBarrierTimerCount(t, ctx, db, fixture.runID, 1)`, "", 1),
				"changed_workload":          strings.Replace(row.Successor, `fanOutBarrierRoute("multi-c")`, `fanOutBarrierRoute("foreign")`, 1),
				"changed_clock":             strings.Replace(row.Successor, `base.Add(6*time.Second)`, `base.Add(60*time.Second)`, 1),
				"historical_fallback":       row.After,
			} {
				t.Run("N19/"+name, func(t *testing.T) {
					if source == row.Successor {
						t.Fatal("negative control did not change source")
					}
					mutant := row
					mutant.Successor = source
					if validateN19FanOutBarrierSuccessor(mutant, source) == nil {
						t.Fatal("matching unreviewed successor and live source accepted")
					}
				})
			}
			for _, field := range []string{"family", "file", "function", "before", "after", "missing_successor", "removed", "mechanical"} {
				t.Run("N19/crossed_"+field, func(t *testing.T) {
					mutant := row
					switch field {
					case "family":
						mutant.Family += "-unlisted"
					case "file":
						mutant.File = "unlisted.go"
					case "function":
						mutant.Function += "Unlisted"
					case "before":
						mutant.Before = row.After
					case "after":
						mutant.After = row.Successor
					case "missing_successor":
						mutant.Successor = ""
					case "removed":
						mutant.Removed = true
					case "mechanical":
						mutant.Mechanical = &mechanicalSnapshot{}
					}
					if validateN19FanOutBarrierSuccessor(mutant, actual) == nil {
						t.Fatal("crossed identity, history or missing successor accepted")
					}
				})
			}
			t.Run("N19/live_cannot_fall_back", func(t *testing.T) {
				for _, source := range []string{row.After, strings.Replace(row.Successor, n19ArmedBarrierObservation, "", 1)} {
					if validateN19FanOutBarrierSuccessor(row, source) == nil {
						t.Fatal("reviewed successor accepted live source without its new assertions")
					}
				}
			})
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
