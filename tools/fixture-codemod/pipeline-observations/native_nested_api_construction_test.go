package main

import (
	"encoding/json"
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
		want, err := canonicalFunction(row.After)
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
