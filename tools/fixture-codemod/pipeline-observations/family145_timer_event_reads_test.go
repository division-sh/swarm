package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145TimerEventReadCuts = map[string][]string{
	"workflowTimerPersistedEventID": {
		"eventID, err := storetest.ReadWorkflowTimerEventIdentity(context.Background(), selected.events, runID, runtimecontracts.WorkflowStageTimerInternalEvent)",
		"return eventID",
	},
	"workflowTimerEventCount": {
		"count, err := storetest.CountRunEventNameStorage(context.Background(), selected.events, runID, eventType)",
		"return count",
	},
	"workflowTimerDeliveryTrace": {
		"storage, err := storetest.ReadWorkflowTimerDeliveryTrace(context.Background(), selected.events, runID)",
		`return []string{"readback error: " + err.Error()}`,
		"trace := storage.Lines",
		`if storage.ScanError != ""`,
		`return append(trace, "scan error: "+storage.ScanError)`,
		"if runtimeLog, err := storetest.ReadLatestWorkflowTimerRuntimeLogText(context.Background(), selected.events); err == nil",
		`trace = append(trace, "runtime_log="+runtimeLog)`,
		"return trace",
	},
	"TestRecurringWorkflowTimerFiresRestoresAndCancelsOnBothStores": {
		`if handlerCompletion.Status != "completed"`,
		"storage, err := storetest.ReadWorkflowTimerNodeFailure(ctx, selected.events, cancelEvent.ID(), controllerNode.Key())",
		"deliveryStatus, failure := storage.Status, storage.Failure",
	},
}

func family145TimerEventReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("timer event observation changed original firing, receipt, cancellation or identity assertions: %s: %v", function, err)
	}
	cursor := -1
	for _, cut := range family145TimerEventReadCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("timer event observation changed diagnostic order, owner, scope or formatting: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "selected.db") || strings.Contains(after, "selected.postgres") || strings.Contains(after, "QueryRowContext") {
		return fmt.Errorf("timer event observation retained raw authority: %s", function)
	}
	return nil
}

func TestFamily145TimerEventReadTailKeepsOriginalScopesAssertionsAndIndependentLog(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, row := range rows {
		cuts := family145TimerEventReadCuts[row.Function]
		if cuts == nil {
			continue
		}
		if found[row.Function] != "" {
			t.Fatalf("ambiguous timer event consumer: %s", row.Function)
		}
		found[row.Function] = row.Before
		if err := family145TimerEventReadProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145TimerEventReadProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignTimerEventEvidence()", 1)) == nil {
				t.Fatalf("lost timer event scope/diagnostic cut passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145TimerEventReadProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original timer execution assertion passed: %s", condition)
			}
		}
	}
	if len(found) != len(family145TimerEventReadCuts) {
		t.Fatalf("timer event read inventory=%d/%d", len(found), len(family145TimerEventReadCuts))
	}
	for _, tc := range []struct{ path, function, caller string }{
		{"eventrecord/template_storage_observation.go", "ReadWorkflowTimerEventIdentityTx", "workflowTimerPersistedEventID"},
		{"delivery/read_projections.go", "ReadWorkflowTimerNodeFailureTx", "TestRecurringWorkflowTimerFiresRestoresAndCancelsOnBothStores"},
	} {
		physical := family145ConversationForkOwnerSource(t, "internal/backend/"+tc.path, tc.function)
		actual := family145A2ClaimLiteralQueries(t, physical)
		original := family145A2ClaimLiteralQueries(t, found[tc.caller])
		if len(actual) != 2 || !reflect.DeepEqual(actual, original) {
			t.Fatalf("%s changed exact original dialects, casts, predicates or columns: %v/%v", tc.function, actual, original)
		}
	}
	joined := family145ConversationForkOwnerSource(t, "internal/backend/delivery/read_projections.go", "ReadWorkflowTimerDeliveryTraceTx")
	latest := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/template_storage_observation.go", "ReadLatestWorkflowTimerRuntimeLogTextTx")
	actual := append(family145A2ClaimLiteralQueries(t, joined), family145A2ClaimLiteralQueries(t, latest)...)
	if original := family145A2ClaimLiteralQueries(t, found["workflowTimerDeliveryTrace"]); !reflect.DeepEqual(actual, original) || len(actual) != 4 {
		t.Fatalf("timer diagnostic changed run filter, LEFT JOIN, NULLs, order or global newest-log scope: %v/%v", actual, original)
	}
	for _, cut := range []string{`out.ScanError = err.Error()`, `eventName+" subscriber="+subscriberID+" status="+status`, "rows.Err()", "rows.Close()"} {
		if strings.Count(joined, cut) != 1 {
			t.Fatalf("timer diagnostic lost scan formatting or row cleanup: %s", cut)
		}
	}
}
