package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145TimerPhysicalReadCuts = map[string][]string{
	"workflowTimerPersistedActivation": {
		"storage, err := storetest.ReadWorkflowTimerActivationStorage(context.Background(), selected.events, runID, entityID)",
		"taskID, status := storage.TaskID, storage.Status",
		"timeridentity.ParseWorkflowTimerActivationTaskID(taskID)",
		"return ref, status",
	},
	"workflowTimerPersistedOccurrence": {
		"storage, err := storetest.ReadWorkflowTimerOccurrenceStorage(context.Background(), selected.events, runID, entityID)",
		"taskID, dueAtValue, status := storage.TaskID, storage.FireAt, storage.Status",
		"dueAt, err := workflowTimerTestTimeValue(dueAtValue)",
		"timeridentity.ParseWorkflowTimerActivationTaskID(taskID)",
		"return timeridentity.WorkflowTimerOccurrenceRef{Activation: ref, DueAt: dueAt.UTC()}, status",
	},
	"assertWorkflowTimerServedRows": {
		"got, err := storetest.CountWorkflowTimerStatusRows(context.Background(), selected.events, runID, entityID, status)",
	},
	"waitWorkflowTimerRowStatus": {
		"deadline := time.Now().Add(5 * time.Second)",
		"for time.Now().Before(deadline) {",
		"count, err := storetest.CountWorkflowTimerStatusRows(context.Background(), selected.events, runID, entityID, status)",
		"time.Sleep(10 * time.Millisecond)",
	},
}

func family145TimerPhysicalReadProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("physical timer read lost original identity, clock, cardinality or wait assertions: %s: %v", function, err)
	}
	cursor := -1
	for _, cut := range family145TimerPhysicalReadCuts[function] {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("physical timer read changed context, scope, driver time, destination or wait behavior: %s", cut)
		}
		cursor = at
	}
	if strings.Contains(after, "selected.db") || strings.Contains(after, "selected.postgres") || strings.Contains(after, "QueryRowContext") {
		return fmt.Errorf("physical timer helper retained raw authority or dialect ownership: %s", function)
	}
	return nil
}

func TestFamily145TimerPhysicalHelpersKeepIdentityClockWaitAndScope(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, row := range rows {
		cuts := family145TimerPhysicalReadCuts[row.Function]
		if cuts == nil {
			continue
		}
		if found[row.Function] != "" {
			t.Fatalf("ambiguous physical timer helper: %s", row.Function)
		}
		found[row.Function] = row.Before
		if err := family145TimerPhysicalReadProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range cuts {
			if family145TimerPhysicalReadProofError(row.Function, row.Before, strings.Replace(row.After, cut, "foreignTimerEvidence()", 1)) == nil {
				t.Fatalf("lost identity/time/wait/scope binding passed: %s", cut)
			}
		}
		assertions, err := family145DescriptorBusinessAssertions(row.After)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			condition := strings.SplitN(assertion, " => ", 2)[0]
			if family145TimerPhysicalReadProofError(row.Function, row.Before, strings.Replace(row.After, condition, "false", 1)) == nil {
				t.Fatalf("lost original timer error/identity/cardinality assertion passed: %s", condition)
			}
		}
	}
	if len(found) != len(family145TimerPhysicalReadCuts) {
		t.Fatalf("physical timer helper inventory=%d/%d", len(found), len(family145TimerPhysicalReadCuts))
	}
	for _, tc := range []struct{ function, caller string }{
		{"ReadWorkflowTimerActivationStorageTx", "workflowTimerPersistedActivation"},
		{"ReadWorkflowTimerOccurrenceStorageTx", "workflowTimerPersistedOccurrence"},
		{"CountWorkflowTimerStatusRowsTx", "assertWorkflowTimerServedRows"},
		{"CountWorkflowTimerStatusRowsTx", "waitWorkflowTimerRowStatus"},
	} {
		physical := family145ConversationForkOwnerSource(t, "internal/backend/genericschedule/test_workflow_scheduler_observation.go", tc.function)
		actual := family145A2ClaimLiteralQueries(t, physical)
		original := family145A2ClaimLiteralQueries(t, found[tc.caller])
		if len(actual) != 2 || !reflect.DeepEqual(actual, original) {
			t.Fatalf("%s changed original dialects, projection/task-type, run/entity/status or casts: %v/%v", tc.function, actual, original)
		}
	}
}
