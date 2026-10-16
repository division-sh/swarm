package main

import (
	"fmt"
	"reflect"
	"strings"
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
