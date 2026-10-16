package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

var family145SystemNodeCompletionCalls = map[string][]string{
	"TestOperatorRunCompletionSystemNodeFlowConvergesSupportedSurfaces": {
		"storetest.StartPostgresRuntimeStore(t)", "newScopedAPITestEventBus(t, pg,", "startAPIRunLifecycleExecutor(t, pg, source, bundleHash)",
		"completeAPITestDurableWorkflowOptions(t, pg, bus,", "waitForRunGetStatus(t, handler, pg, lifecycleExecutor, runID, \"completed\")",
		"triggerEventIDForRun(t, pg, runID)", "assertPipelineReceiptSucceeded(t, pg, eventID)",
		"assertSystemNodeOutcomePersisted(t, pg, eventID, pipelineNodeID)", "assertSystemNodeDeliverySettled(t, pg, eventID, pipelineNodeID)",
		"assertSystemNodeOutcomePersisted(t, pg, eventID, rootNodeID)", "assertSystemNodeDeliverySettled(t, pg, eventID, rootNodeID)",
		"assertFlowEntityTerminal(t, pg, runID, runtimepipeline.FlowInstanceEntityID(\"discovery\"), \"discovery\", \"discovery\", \"done\")",
	},
	"waitForRunGetStatus": {
		"requireAPIV1Convergence(t,", "storetest.ReadPostgresCompletionCandidateClockStorage(context.Background(), selected, runID)",
		"revision, dueAt := clock.Revision, clock.DueAt", "executor.ActiveCandidates()", "dueAt.Valid",
	},
	"triggerEventIDForRun": {
		"storetest.ReadPostgresRunTriggerEventIdentityStorage(context.Background(), selected, runID)",
	},
	"assertSystemNodeDeliverySettled": {
		"storetest.ReadPostgresSystemNodeDeliveryStorage(context.Background(), selected, eventID, nodeID)",
		"status, reason, settledAt := observed.Status, observed.Reason, observed.SettledAt",
	},
	"assertSystemNodeOutcomePersisted": {
		"storetest.ReadPostgresSystemNodeOutcomeStorage(context.Background(), selected, eventID, nodeID)",
		"outcome, reason := observed.Outcome, observed.Reason",
	},
	"assertPipelineReceiptSucceeded": {
		"storetest.ReadPostgresExactPipelineReceiptOutcomeStorage(context.Background(), selected, eventID)",
	},
	"assertFlowEntityTerminal": {
		"storetest.ReadPostgresFlowEntityTerminalStorage(context.Background(), selected, runID, entityID, flowInstance)",
		"entityType, state := observed.EntityType, observed.State",
	},
}

func family145SystemNodeCompletionProofError(function, before, after string) error {
	for _, caller := range []string{"triggerEventIDForRun", "assertPipelineReceiptSucceeded", "assertSystemNodeOutcomePersisted", "assertSystemNodeDeliverySettled", "assertFlowEntityTerminal"} {
		before = strings.ReplaceAll(before, caller+"(t, db,", caller+"(t, pg,")
	}
	before = strings.ReplaceAll(before, "waitForRunGetStatus(t, handler, db,", "waitForRunGetStatus(t, handler, pg,")
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, call := range family145SystemNodeCompletionCalls[function] {
		if !strings.Contains(after, call) {
			return fmt.Errorf("system-node completion lost original owner, coordinate or execution cut: %s", call)
		}
	}
	return nil
}

func assertFamily145SystemNodePhysicalQueries(t *testing.T, original map[string]string) {
	t.Helper()
	for _, port := range []struct{ file, owner, predecessor string }{
		{"runlifecycle/run_lifecycle_candidates.go", "ReadPostgresCompletionCandidateClockStorageTx", "waitForRunGetStatus"},
		{"runlifecycle/standalone_fixture_storage.go", "ReadPostgresRunTriggerEventIdentityStorageTx", "triggerEventIDForRun"},
		{"delivery/read_projections.go", "ReadPostgresSystemNodeDeliveryStorageTx", "assertSystemNodeDeliverySettled"},
		{"delivery/read_projections.go", "ReadPostgresSystemNodeOutcomeStorageTx", "assertSystemNodeOutcomePersisted"},
		{"pipelinepersistence/template_storage_observation.go", "ReadPostgresExactPipelineReceiptOutcomeStorageTx", "assertPipelineReceiptSucceeded"},
		{"entityruntime/template_storage_observation.go", "ReadPostgresFlowEntityTerminalStorageTx", "assertFlowEntityTerminal"},
	} {
		owned := family145ConversationForkOwnerSource(t, "internal/backend/"+port.file, port.owner)
		want, got := family145ConnectorSQL(t, "package proof\n"+original[port.predecessor]), family145ConnectorSQL(t, "package proof\n"+owned)
		if len(want) != 1 || !reflect.DeepEqual(got, want) {
			t.Fatalf("system-node physical query changed columns/NULL/predicate/multiplicity: %s got=%v want=%v", port.owner, got, want)
		}
		bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_system_node_completion_storage.go", strings.TrimSuffix(port.owner, "Tx")+"ForTest")
		if !strings.Contains(bridge, "validatePostgresPublicationIdentities(selected,") || !strings.Contains(bridge, "readFanOutSemanticStorage(ctx, selected, nil,") || !strings.Contains(bridge, port.owner+"(ctx, tx,") {
			t.Fatalf("system-node bridge lost original PostgreSQL coordinator/identity/error ownership: %s", port.owner)
		}
	}
}
