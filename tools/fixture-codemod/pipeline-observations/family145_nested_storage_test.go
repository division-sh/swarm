package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

const nestedFamilyPath = "internal/runtime/conformance/"

var family145NestedReadCalls = map[string][]string{
	nestedFamilyPath + "a2_repeated_fan_out_conformance_test.go|TestA2RepeatedFanOutTriggerIsolationOnBothStores":             {"storetest.CountRepeatedFanOutTriggerOutcomes(ctx, rt.selected, runID, secondDelivery)"},
	nestedFamilyPath + "a2_repeated_fan_out_conformance_test.go|assertA2RepeatedIntent":                                       {"storetest.ReadRepeatedFanOutIntent(ctx, selected, key)"},
	nestedFamilyPath + "a2_repeated_fan_out_conformance_test.go|assertA2RepeatedCompletions":                                  {"storetest.ReadRepeatedFanOutCompletionEventIDs(ctx, rt.selected, runID)"},
	nestedFamilyPath + "a2_repeated_fan_out_conformance_test.go|a2RepeatedFanOutSnapshot":                                     {"storetest.ReadRepeatedFanOutSnapshot(ctx, selected, runID)"},
	nestedFamilyPath + "fan_out_b17_mixed_dependency_test.go|TestB17MixedNestedDependencyCapacityOneBothStores":               {"storetest.ReadNestedFanOutBarrierStatus(ctx, rt.selected, held.Key)", "storetest.ReadNestedRunFanOutOwed(ctx, rt.selected, runID)", "storetest.ReadNestedSiblingSourceEventID(ctx, rt.selected, runID, signal.EventID)"},
	nestedFamilyPath + "fan_out_b17_mixed_dependency_test.go|b17MixedNestedRuntime":                                           {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStore(t)"},
	nestedFamilyPath + "fan_out_b17_mixed_dependency_test.go|Intercept":                                                       {"storetest.CountNestedSuccessfulPipelineReceipt(ctx, p.selected, p.predecessor)", "storetest.CountPipelineEventReceiptStorage(ctx, p.selected, p.current)", "storetest.CountNestedContinuationHandoffs(ctx, p.selected, p.predecessor)", "storetest.CountNestedContinuationHandoffs(ctx, p.selected, p.current)"},
	nestedFamilyPath + "fan_out_b17_mixed_dependency_test.go|b17RequirePipelineReceipt":                                       {"storetest.CountPipelineEventReceiptStorage(ctx, selected, eventID)"},
	nestedFamilyPath + "fan_out_nested_cancellation_conformance_test.go|TestIssue2394NestedHeldChildCancellationBothStores":   {"storetest.ReadNestedFanOutBarrierStatus(ctx, rt.selected, parent.Key)", "storetest.ReadNestedEntityFieldsRevision(ctx, rt.selected, runID, path)", "storetest.ReadNestedCommittedOutcomeEventIDs(ctx, rt.selected, runID)", "storetest.ReadNestedPipelineReceiptOutcome(ctx, rt.selected, notifyID)", "storetest.ReadNestedAccountIntentKeys(ctx, rt.selected, runID)"},
	nestedFamilyPath + "fan_out_nested_capacity_conformance_test.go|proveNestedCapacityOneHandoff":                            {"storetest.ReadNestedFanOutBarrierStatus(ctx, rt.selected, held.Key)", "storetest.ReadNestedRunFanOutOwed(ctx, rt.selected, runID)", "storetest.ReadNestedRunNamedEventIDs(ctx, rt.selected, runID, name)", "storetest.ReadNestedRunEntityStates(ctx, rt.selected, runID)"},
	nestedFamilyPath + "fan_out_nested_interrupted_group_test.go|TestIssue2394NestedGroupHandoffAcknowledgmentLossBothStores": {},
	nestedFamilyPath + "fan_out_nested_interrupted_group_test.go|assertNestedPipelineReceipt":                                 {"storetest.ReadNestedPipelineReceiptOutcome(ctx, selected, id)"},
	nestedFamilyPath + "fan_out_nested_interrupted_group_test.go|assertNestedInterruptedFinalState":                           {"storetest.ReadNestedRunEntityStateFields(ctx, rt.selected, runID)"},
	nestedFamilyPath + "fan_out_nested_prepared_release_test.go|assertReleasedBeforeRetry":                                    {"storetest.ReadNestedFanOutCursorClaim(ctx, selected, key)", "storetest.CountNestedRunFanOutOutcomes(ctx, selected, runID)", "storetest.CountNestedRunEventIdentity(ctx, selected, runID, id)"},
	nestedFamilyPath + "fan_out_nested_readback_helpers_test.go|nestedEventIDs":                                               {"storetest.ReadNestedSourceEventIDs(ctx, selected, runID, eventName, parentID)"},
	nestedFamilyPath + "fan_out_nested_readback_helpers_test.go|assertNestedPredecessorsSettledWhileChildHeld":                {"storetest.ReadNestedPipelineReceiptOutcome(ctx, selected, parent.ID)"},
	nestedFamilyPath + "fan_out_nested_readback_helpers_test.go|assertNestedSiblingTaskEffects":                               {"nestedEventIDs(t, ctx, selected, runID, taskName, parent.ID)", "nestedEventIDs(t, ctx, selected, runID, effectName, id)"},
	nestedFamilyPath + "fan_out_nested_readback_helpers_test.go|assertNestedExactBarrier":                                     {"storetest.ReadNestedFanOutBarrier(ctx, selected, key)"},
	nestedFamilyPath + "fan_out_nested_serving_helpers_test.go|newNestedServingRuntime":                                       {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStore(t)"},
}

func family145NestedReadProofError(function, beforeSource, afterSource string, calls []string) error {
	for _, call := range calls {
		want := 1
		if function == "TestIssue2394NestedHeldChildCancellationBothStores" && strings.HasPrefix(call, "storetest.ReadNestedEntityFieldsRevision(") {
			want = 2
		}
		if strings.Count(afterSource, call) != want {
			return fmt.Errorf("nested read lost exact source/owner/cut: %s/%s", function, call)
		}
	}
	for _, escape := range []string{"db.", "*sql.DB", "AdmitPostgresRuntimeStore", "DatabaseForTest", "testutil.StartPostgres"} {
		if strings.Contains(afterSource, escape) {
			return fmt.Errorf("nested carrier restored raw authority: %s/%s", function, escape)
		}
	}
	before, beforeErr := family145NestedBusinessAssertions(beforeSource)
	after, afterErr := family145NestedBusinessAssertions(afterSource)
	if beforeErr != nil || afterErr != nil || strings.Join(before, "\n") != strings.Join(after, "\n") {
		return fmt.Errorf("nested migration lost a business assertion: %s/%v/%v", function, beforeErr, afterErr)
	}
	if function == "Intercept" {
		cut := "err == nil && (prior != 1 || current != 1 || handoffs != 1 || currentHandoffs != 1)"
		if !strings.Contains(afterSource, cut) || !strings.Contains(afterSource, `fmt.Errorf("nested boundary: predecessor receipt=%d handoff=%d, current receipt=%d handoff=%d", prior, handoffs, current, currentHandoffs)`) {
			return fmt.Errorf("nested execution boundary lost exact receipt/handoff refusal")
		}
	}
	return nil
}

func family145NestedBusinessAssertions(source string) ([]string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "nested.go", "package proof\n"+source, parser.AllErrors)
	if err != nil {
		return nil, err
	}
	ast.Inspect(file, func(node ast.Node) bool {
		block, ok := node.(*ast.BlockStmt)
		if !ok {
			return true
		}
		var kept []ast.Stmt
		for _, stmt := range block.List {
			conditional, ok := stmt.(*ast.IfStmt)
			if ok && conditional.Init != nil {
				init := family145NodeString(conditional.Init)
				if strings.HasPrefix(init, "err := rows.Scan(") || strings.HasPrefix(init, "err := completionRows.Scan(") || init == "err := rows.Err()" {
					continue
				}
			}
			kept = append(kept, stmt)
		}
		block.List = kept
		return true
	})
	return runSetupAssertions(family145NodeString(file.Decls[0]))
}

func TestFamily145NestedReadCarrierConservesEveryConsumerAndRejectsForeignCuts(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, row := range rows {
		key := row.File + "|" + row.Function
		calls, ok := family145NestedReadCalls[key]
		if !ok {
			continue
		}
		if seen[key] {
			t.Fatalf("nested consumer duplicated: %s", key)
		}
		seen[key] = true
		if err := family145NestedReadProofError(row.Function, row.Before, row.After, calls); err != nil {
			t.Fatal(err)
		}
		for _, call := range calls {
			if err := family145NestedReadProofError(row.Function, row.Before, strings.Replace(row.After, call, "foreignCut()", 1), calls); err == nil {
				t.Fatalf("nested consumer accepted foreign/omitted cut: %s/%s", key, call)
			}
		}
		hostile := strings.Replace(row.After, "{", "{ db.QueryRowContext(ctx, rawSQL);", 1)
		if err := family145NestedReadProofError(row.Function, row.Before, hostile, calls); err == nil {
			t.Fatalf("raw nested carrier returned: %s", key)
		}
		if strings.Contains(row.After, "t.Fatalf(") {
			hostile = strings.Replace(row.After, "t.Fatalf(", "t.Logf(", 1)
			if err := family145NestedReadProofError(row.Function, row.Before, hostile, calls); err == nil {
				t.Fatalf("nested business assertion weakened: %s", key)
			}
		}
		if row.Function == "Intercept" {
			hostile = strings.Replace(row.After, "prior != 1", "false", 1)
			if err := family145NestedReadProofError(row.Function, row.Before, hostile, calls); err == nil {
				t.Fatal("nested predecessor refusal lost")
			}
		}
	}
	if len(seen) != len(family145NestedReadCalls) {
		t.Fatalf("nested consumer inventory incomplete: %d/%d", len(seen), len(family145NestedReadCalls))
	}
}
