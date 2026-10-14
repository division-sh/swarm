package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"strings"
	"testing"
)

var nativeNestedAPIConstructionRoots = map[string]int{
	"TestOperatorRunStartHandlersFailClosedBeforePersistence":       12,
	"TestOperatorEventReplaySubsetAndFailClosedCases":               7,
	"TestOperatorAgentReplayFailClosedCases":                        3,
	"TestOperatorEventReplayQueuesWhenDispatchGated":                1,
	"TestOperatorReplayMockOnlyRejectsLiveOriginalBeforeMutation":   1,
	"TestOperatorEventPublishFlowScopedEventNameFailuresFailClosed": 2,
	"TestOperatorEventPublishHandlersFailClosedBeforePersistence":   6,
}

func nativeNestedAPIConstructionSource(t *testing.T, source string) string {
	t.Helper()
	fn := projectionShapeFunction(t, source)
	if nativeNestedAPIConstructionRoots[fn.Name.Name] == 0 {
		return source
	}
	if fn.Name.Name == "TestOperatorEventReplaySubsetAndFailClosedCases" {
		fn = projectionShapeFunction(t, nativeNestedReplayObservationSource(source))
	}
	if fn.Name.Name == "TestOperatorRunStartHandlersFailClosedBeforePersistence" {
		fn = projectionShapeFunction(t, nativeNestedRunStartObservationSource(source))
	}
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		for index := 0; index+1 < len(block.List); index++ {
			if formattedNativeReadNode(block.List[index]) != "_, db, _ := testutil.StartPostgres(t)" ||
				formattedNativeReadNode(block.List[index+1]) != "pg := storetest.AdmitPostgresRuntimeStore(t, db)" {
				continue
			}
			selected := block.List[index+1].(*ast.AssignStmt)
			native, err := parser.ParseExpr("storetest.StartPostgresRuntimeStore(t)")
			if err != nil {
				t.Fatal(err)
			}
			selected.Rhs[0] = native
			block.List = append(append(block.List[:index:index], selected), block.List[index+2:]...)
		}
		return true
	})
	return formattedNativeReadNode(fn)
}

func TestNativeNestedAPIConstructionClosesEveryReviewedLocalPool(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		expected := nativeNestedAPIConstructionRoots[row.Function]
		if expected == 0 {
			continue
		}
		matched++
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		expectedBody, err := nativeNestedAPIConstructionExpectedBody(row)
		if err != nil {
			t.Fatal(err)
		}
		want, err := canonicalFunction(expectedBody)
		got, actualErr := canonicalFunction(actual)
		if err != nil || actualErr != nil || want != got || strings.Contains(actual, "testutil.StartPostgres") || strings.Contains(actual, "AdmitPostgresRuntimeStore") || strings.Count(actual, "storetest.StartPostgresRuntimeStore(t)") != expected {
			t.Fatalf("nested API construction regained raw/foreign setup: %s", row.Function)
		}
		for _, replacement := range []string{"storetest.StartSQLiteRuntimeStore(t)", "storetest.StartPostgresRuntimeStore(otherTest)", "storetest.AdmitPostgresRuntimeStore(t, db)"} {
			mutant := strings.Replace(actual, "storetest.StartPostgresRuntimeStore(t)", replacement, 1)
			changed, mutantErr := canonicalFunction(mutant)
			if mutant == actual || (mutantErr == nil && changed == want) {
				t.Fatal("foreign nested API construction admitted")
			}
		}
	}
	if matched != len(nativeNestedAPIConstructionRoots) {
		t.Fatalf("nested API construction recipes=%d,want%d", matched, len(nativeNestedAPIConstructionRoots))
	}
}

func nativeNestedAPIConstructionExpectedBody(row recipe) (string, error) {
	if row.File != "internal/apiv1/operator_event_replay_test.go" || row.Function != "TestOperatorEventReplayQueuesWhenDispatchGated" {
		return row.After, nil
	}
	// Only this caller's body at reviewed checkpoint a394ade4e is a successor.
	if row.Family != "native-api-delivery-receipt-count-consumer" || row.Removed || row.Mechanical != nil ||
		!historicalCurrentSourcePinned(row.Before, "a9c4ff26b4ebe249af37de019ead6383e3e1ba250427bf4f3e2ed3ef71a1af49") ||
		!historicalCurrentSourcePinned(row.After, "c2540920cad3ff24caa1ab179499c356360814a6de1eaaba8579107105d236d3") ||
		!historicalCurrentSourcePinned(row.Successor, "759bf4f0146d8b290afeca1108b24ae8b8ad25aba9d7d5b91f1e6b91ffda08d3") {
		return "", fmt.Errorf("gated replay changed its historical snapshots or exact reviewed successor")
	}
	return row.Successor, nil
}

func TestNativeNestedAPIConstructionRejectsUnreviewedGatedReplaySuccessors(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var selected *recipe
	for i := range rows {
		if rows[i].File == "internal/apiv1/operator_event_replay_test.go" && rows[i].Function == "TestOperatorEventReplayQueuesWhenDispatchGated" {
			if selected != nil {
				t.Fatal("ambiguous gated replay recipe")
			}
			selected = &rows[i]
		}
	}
	if selected == nil {
		t.Fatal("missing gated replay recipe")
	}
	if _, err := nativeNestedAPIConstructionExpectedBody(*selected); err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"opts := tc.opts", "scan.requested", "assertNoReplayEvent(t, ch)", "got != 1", "got != 0", "storetest.StartPostgresRuntimeStore(t)"} {
		t.Run(cut, func(t *testing.T) {
			mutant := *selected
			mutant.Successor = strings.Replace(mutant.Successor, cut, "unreviewedCut", 1)
			if mutant.Successor == selected.Successor {
				t.Fatal("mutation missed its intended cut")
			}
			if _, err := nativeNestedAPIConstructionExpectedBody(mutant); err == nil {
				t.Fatal("unreviewed gated replay body admitted")
			}
		})
	}
	for _, change := range []string{"foreign_file", "foreign_function"} {
		t.Run(change, func(t *testing.T) {
			mutant := *selected
			if change == "foreign_file" {
				mutant.File = "foreign.go"
			} else {
				mutant.Function = "TestForeignCaller"
			}
			body, err := nativeNestedAPIConstructionExpectedBody(mutant)
			if err != nil || body != mutant.After {
				t.Fatal("gated replay successor escaped its exact file/function")
			}
		})
	}
}
