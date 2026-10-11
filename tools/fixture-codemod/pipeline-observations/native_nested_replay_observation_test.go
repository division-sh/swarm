package main

import (
	"strings"
	"testing"
)

const nestedReplayOldNodeStatus = "\t\tvar status string\n\t\tif err := db.QueryRowContext(ctx, `\n\t\t\tSELECT status FROM event_deliveries\n\t\t\tWHERE event_id = $1::uuid AND subscriber_type = 'node' AND subscriber_id = $2\n\t\t`, eventID, workflowRuntimeNodeID).Scan(&status); err != nil {\n\t\t\tt.Fatalf(\"load node-only delivery status: %v\", err)\n\t\t}\n\t\tif status != string(runtimedelivery.StatusPending) {\n\t\t\tt.Fatalf(\"node-only delivery status = %q, want pending\", status)\n\t\t}\n"
const nestedReplayNativeNodeStatus = "\t\tpending, err := storetest.ReadServedDeliveryStatusCount(ctx, pg, eventID, \"node\", workflowRuntimeNodeID, string(runtimedelivery.StatusPending))\n\t\tif err != nil {\n\t\t\tt.Fatalf(\"load node-only delivery status: %v\", err)\n\t\t}\n\t\tif pending != 1 {\n\t\t\tt.Fatalf(\"pending node-only deliveries = %d, want one\", pending)\n\t\t}\n"
const nestedReplayOldAgentStatus = "\t\t\tvar persistedStatus string\n\t\t\tif err := db.QueryRowContext(ctx, `\n\t\t\t\tSELECT COALESCE(status, '')\n\t\t\t\tFROM event_deliveries\n\t\t\t\tWHERE event_id = $1::uuid AND subscriber_type = 'agent' AND subscriber_id = 'agent-a'\n\t\t\t`, original.EventID).Scan(&persistedStatus); err != nil {\n\t\t\t\tt.Fatalf(\"load original delivery after nonterminal replay: %v\", err)\n\t\t\t}\n"
const nestedReplayNativeAgentStatus = "\t\t\tpersistedStatus, err := storetest.ReadExactAgentDeliveryStatus(ctx, pg, original.EventID, \"agent-a\")\n\t\t\tif err != nil {\n\t\t\t\tt.Fatalf(\"load original delivery after nonterminal replay: %v\", err)\n\t\t\t}\n"

func nativeNestedReplayObservationSource(source string) string {
	source = strings.Replace(source, "seedCompleteReplayRun(t, ctx, db, false, runID, time.Now().UTC())", "storetest.RequireRun(t, ctx, pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: time.Now().UTC(), BundleHash: runStartTestBundleHash})", 1)
	source = strings.Replace(source, nestedReplayOldNodeStatus, nestedReplayNativeNodeStatus, 1)
	return strings.Replace(source, nestedReplayOldAgentStatus, nestedReplayNativeAgentStatus, 1)
}

func TestNativeNestedReplayKeepsExactPhysicalStatusAndOriginalRunSetup(t *testing.T) {
	const root = "TestOperatorEventReplaySubsetAndFailClosedCases"
	actual := selectedCausalObservationBody(t, "internal/apiv1/operator_event_replay_test.go", root)
	for _, cut := range []string{
		"storetest.RequireRun(t, ctx, pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID, StartedAt: time.Now().UTC(), BundleHash: runStartTestBundleHash})",
		"storetest.ReadServedDeliveryStatusCount(ctx, pg, eventID, \"node\", workflowRuntimeNodeID, string(runtimedelivery.StatusPending))",
		"if pending != 1",
		"storetest.ReadExactAgentDeliveryStatus(ctx, pg, original.EventID, \"agent-a\")",
		"if persistedStatus != string(status)",
	} {
		if !strings.Contains(actual, cut) {
			t.Fatalf("nested replay lost exact selected status/identity cut: %s", cut)
		}
	}
	if strings.Contains(actual, "db.Query") || strings.Contains(actual, "seedCompleteReplayRun(") {
		t.Fatal("nested replay retains its raw run/status interpreter")
	}
}
