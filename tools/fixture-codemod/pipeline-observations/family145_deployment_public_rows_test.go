package main

import (
	"encoding/json"
	"strings"
	"testing"
)

var family145DeploymentPublicRowCuts = map[string][]string{
	"assertDeploymentResourceRows": {
		"logDeploymentResourceFailure(t, f.selected, runID)", "ctx := f.ctx", "if len(compiled.Rows) > 100", "waitDeploymentResourceLargeRun(t, f, runID, len(compiled.Rows))", "waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 3*time.Minute)",
		"storetest.CountRunFanOutIntentStorage(ctx, f.selected, runID)", "&intents", "storetest.CountDeploymentCommittedOutcomesStorage(ctx, f.selected, runID)", "&committed",
		"storetest.CountAccountDeploymentReceiversStorage(ctx, f.selected, runID)", "&children", "storetest.CountPortfolioDeliveredDeploymentRowsStorage(ctx, f.selected, runID)", "&delivered",
		"count, err := check.read()", "*check.value = count", "storetest.CountRunNamedEventStorage(ctx, f.selected, runID, \"portfolio/account.registered\")", "&events",
		"storetest.CountRunDeliveryStorage(ctx, f.selected, runID)", "&deliveries", "storetest.CountDeploymentPipelineReceiptsStorage(ctx, f.selected, runID)", "&receipts", "storetest.CountDeploymentRouteObligationsStorage(ctx, f.selected, runID)", "&routes",
		"storetest.ReadDeploymentDeliveryStatusStorage(ctx, f.selected, runID)", "status, count := row.Status, row.Count",
		"storetest.ReadPortfolioAccountResourcePinStorage(ctx, f.selected, runID)", "storetest.ReadDeploymentIntentVersionStorage(ctx, f.selected, runID)", "want := make(map[string]any, len(compiled.Rows))", "deploymentResourcePublicEvents(t, ctx, server, runID, cursor)",
	},
	"TestDeploymentResourceRunStartEmptyVersionDoesNotInventReceiverBothStores": {
		"waitNotifyAllChildrenRuntimeWithin(t, f.runtime, runID, 30*time.Second)",
		"\"row events\"", "storetest.CountRunNamedEventStorage(f.ctx, f.selected, runID, \"portfolio/account.registered\")", "\"row deliveries\"", "storetest.CountPortfolioAllDeploymentRowsStorage(f.ctx, f.selected, runID)",
		"\"receivers\"", "storetest.CountAccountDeploymentReceiversStorage(f.ctx, f.selected, runID)", "\"outcomes\"", "storetest.CountNestedRunFanOutOutcomes(f.ctx, f.selected, runID)", "count, err := check.read()", "if err != nil || count != 0",
	},
}

func family145DeploymentPublicRowOwnedQueries(t *testing.T) map[string]bool {
	t.Helper()
	owners := map[string][]string{
		"internal/durabledata/deployment_observation.go":                     {"ReadPortfolioAccountResourcePinStorageTx"},
		"internal/backend/pipelinepersistence/deployment_observation.go":     {"CountAccountDeploymentReceiversStorageTx", "CountDeploymentRouteObligationsStorageTx", "CountDeploymentCommittedOutcomesStorageTx", "CountDeploymentPipelineReceiptsStorageTx", "ReadDeploymentIntentVersionStorageTx"},
		"internal/backend/pipelinepersistence/owner_operations.go":           {"CountFanOutIntentsForRun"},
		"internal/backend/pipelinepersistence/nested_fan_out_observation.go": {"CountNestedRunFanOutOutcomesTx"},
		"internal/backend/delivery/read_projections.go":                      {"CountPortfolioDeliveredDeploymentRowsStorageTx", "CountPortfolioAllDeploymentRowsStorageTx", "CountRunDeliveryStorageTx", "ReadDeploymentDeliveryStatusStorageTx"},
	}
	owned := map[string]bool{}
	for path, functions := range owners {
		for _, function := range functions {
			queries := family145A2ClaimLiteralQueries(t, family145ConversationForkOwnerSource(t, path, function))
			if len(queries) != 1 || owned[queries[0]] {
				t.Fatalf("ambiguous deployment public query owner: %s %v", function, queries)
			}
			owned[queries[0]] = true
		}
	}
	queries := family145A2ClaimLiteralQueries(t, family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/run_name_cardinality.go", "CountRunNamedEventStorageTx"))
	if len(queries) != 1 || queries[0] != "SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name=$2" {
		t.Fatal("canonical named-event owner changed")
	}
	owned[strings.Replace(queries[0], "$2", "'portfolio/account.registered'", 1)] = true
	return owned
}

func TestFamily145DeploymentPublicRowAll15QueryOccurrencesHaveExactConsumedOwners(t *testing.T) {
	owned := family145DeploymentPublicRowOwnedQueries(t)
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	consumed, occurrences := map[string]bool{}, 0
	for _, row := range rows {
		if family145DeploymentPublicRowCuts[row.Function] == nil {
			continue
		}
		for _, query := range family145A2ClaimLiteralQueries(t, row.Before) {
			query = strings.ReplaceAll(query, "status, COUNT(*)", "status,COUNT(*)")
			if !owned[query] {
				t.Fatalf("deployment public query lost exact scope/columns/order: %s", query)
			}
			consumed[query] = true
			occurrences++
		}
	}
	if occurrences != 15 || len(consumed) != len(owned) || len(owned) != 13 {
		t.Fatalf("deployment public query consumption occurrences=%d/15 consumed=%d owned=%d/13", occurrences, len(consumed), len(owned))
	}
}
