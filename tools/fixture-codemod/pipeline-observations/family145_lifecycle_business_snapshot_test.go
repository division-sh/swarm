package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"path/filepath"
	"strings"
	"testing"
)

var family145LifecycleBusinessCallers = map[string]int{
	"TestReleaseCompiledLifecycleJourneysBothStores":                      2,
	"runLifecycleRootForkPolicy":                                          4,
	"TestServedCompiledGateOutcomeRestartOnBothStores":                    2,
	"proveServedCompiledGateOutcomeRestart":                               2,
	"TestServedCompiledGateAdvanceOnlyOnBothStores":                       2,
	"TestServedCompiledTransitionNestedCarrierCollisionOnBothStores":      2,
	"TestServedCompiledLoopNodeRecoveryReexecutionOnBothStores":           4,
	"TestServedCompiledLoopTransitionReplayOnBothStores":                  4,
	"TestServedCompiledGateFrozenTransitionEvidenceOnBothStores":          7,
	"TestServedCompiledTransitionRestartOnBothStores":                     2,
	"TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores":     3,
	"TestServedCompiledTransitionStaticForkEvidenceOnBothStores":          4,
	"TestServedCompiledTransitionNestedTemplatesFirstJourneyOnBothStores": 2,
}

func family145LifecycleBusinessCallerProofError(function, before, after string) error {
	after = family145WriterStageCallerInverse(function, after)
	const native = "lifecycleStoredSnapshot(t, rt.ReceiverStateReader, "
	want := family145LifecycleBusinessCallers[function]
	if want == 0 || strings.Count(after, native) != want || strings.Contains(after, "lifecycleStoredSnapshot(t, rt, ") {
		return fmt.Errorf("lifecycle caller lost its original inspection owner/cardinality: %s", function)
	}
	if strings.ReplaceAll(after, native, "lifecycleStoredSnapshot(t, rt, ") != before {
		return fmt.Errorf("lifecycle caller changed source/identity/assertions/waits/fault/cleanup cuts: %s", function)
	}
	return nil
}

func family145LifecycleBusinessReaderProofError(before, after string) error {
	const expected = `func lifecycleStoredSnapshot(t *testing.T, reader receiverProofStateReader, runID string) string {
	t.Helper()
	var rows []storetest.LifecycleBusinessStorageRow
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		rows, err = storetest.ReadLifecycleBusinessStorage(ctx, reader, runID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	result := ""
	for _, row := range rows {
		result += fmt.Sprintf("%s/%s/%d/%t:%s/%s/%s\n", row.EntityID, row.CurrentState, row.Revision, row.Fields.Valid, row.Fields.String, row.Config, row.Accumulator)
	}
	if result == "" {
		t.Fatal("no persisted lifecycle proof rows")
	}
	return result
}`
	if after != expected || !strings.Contains(before, "var fields sql.NullString") || !strings.Contains(before, "fields.Valid, fields.String, config, loops") || !strings.Contains(before, `t.Fatal("no persisted lifecycle proof rows")`) {
		return fmt.Errorf("lifecycle witness lost physical NULL validity/raw fields/config/accumulator/empty refusal")
	}
	return nil
}

func TestFamily145LifecycleBusinessSnapshotRetainsExactLeftJoinAndAll40CallerCuts(t *testing.T) {
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(servedSetupEarlierHistory(t, family145TransitionBytes, false), &rows); err != nil {
		t.Fatal(err)
	}
	seen, helpers := map[string]bool{}, 0
	for _, row := range rows {
		if row.Function == "lifecycleStoredSnapshot" {
			helpers++
			family145LifecycleBusinessCheckReader(t, row.Before, row.After)
		} else if family145LifecycleBusinessCallers[row.Function] != 0 {
			seen[row.Function] = true
			if err := family145LifecycleBusinessCallerProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			for _, cut := range []string{"lifecycleStoredSnapshot(t, rt.ReceiverStateReader, ", "t.Fatal"} {
				if !strings.Contains(row.After, cut) || family145LifecycleBusinessCallerProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignLifecycleCut()", 1)) == nil {
					t.Fatalf("lifecycle caller owner/assertion mutation survived: %s/%s", row.Function, cut)
				}
			}
		} else {
			continue
		}
		actual := family145ConversationForkOwnerSource(t, "../serveapp/"+filepath.Base(row.File), row.Function)
		if actual != row.After {
			t.Fatalf("lifecycle consumer escaped its finite complete body: %s", row.Function)
		}
	}
	if helpers != 1 || len(seen) != len(family145LifecycleBusinessCallers) {
		t.Fatalf("lifecycle helper=%d/1 consumers=%d/%d", helpers, len(seen), len(family145LifecycleBusinessCallers))
	}
	counts := family145ForkReceiverJoinedOwnedInventory(t)
	for function, want := range family145LifecycleBusinessCallers {
		if counts["lifecycle-business/"+function] != want {
			t.Fatalf("lifecycle owned caller count %s=%d/%d", function, counts["lifecycle-business/"+function], want)
		}
	}
}

func family145LifecycleBusinessRecordCall(t *testing.T, function string, call *ast.CallExpr, counts map[string]int) {
	t.Helper()
	if family145NodeString(call.Fun) != "lifecycleStoredSnapshot" {
		return
	}
	if family145LifecycleBusinessCallers[function] == 0 || len(call.Args) != 3 || family145NodeString(call.Args[1]) != "rt.ReceiverStateReader" {
		t.Fatal("lifecycle snapshot caller escaped its finite original inspection owner")
	}
	counts["lifecycle-business/"+function]++
}

func family145LifecycleBusinessCheckReader(t *testing.T, before, after string) {
	t.Helper()
	if err := family145LifecycleBusinessReaderProofError(before, after); err != nil {
		t.Fatal(err)
	}
	owner := family145ConversationForkOwnerSource(t, "internal/backend/pipelinepersistence/workflow_instance_read.go", "ReadLifecycleBusinessStorageTx")
	if family145ForkReceiverJoinedQuery(t, before) != family145ForkReceiverJoinedQuery(t, owner) {
		t.Fatal("lifecycle owner changed original LEFT JOIN/columns/exact run/order")
	}
	for _, cut := range []string{"reader.InspectSnapshot", "ctx, reader, runID", "return err", "if err != nil", "row.Revision", "row.Fields.Valid", "row.Fields.String", "row.Config", "row.Accumulator", `if result == ""`} {
		if family145LifecycleBusinessReaderProofError(before, strings.Replace(after, cut, "foreignLifecycleFact()", 1)) == nil {
			t.Fatalf("lifecycle reader lost NULL/field/config/revision/order/error/empty cut: %s", cut)
		}
	}
	for _, pair := range [][2]string{{"LEFT JOIN", "JOIN"}, {"f.run_id=$1", "f.run_id=$2"}, {"ORDER BY f.entity_id", "ORDER BY f.instance_path"}} {
		hostile := strings.Replace(before, pair[0], pair[1], 1)
		finite := recipe{File: "internal/serveapp/lifecycle_transition_served_test.go", Function: "lifecycleStoredSnapshot", Before: before, After: after}
		if hostile == before {
			t.Fatal("lifecycle near-match lost original predicate")
		}
		if _, _, err := rewriteFunction(finite.File, []byte("package proof\n"+hostile), finite); err == nil {
			t.Fatal("lifecycle transform accepted foreign join/run/order semantics")
		}
	}
}
