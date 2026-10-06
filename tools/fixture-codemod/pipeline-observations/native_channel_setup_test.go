package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

var nativeChannelSetupRoots = map[string]string{
	"TestChannelProjectedActivityResultJournalsAndReplaysAcrossSelectedStores":                 "activity_engine_test.go",
	"TestChannelActivityPostCommitAcknowledgmentLossStateBlocksRedispatchAcrossSelectedStores": "activity_engine_test.go",
	"TestActivityTerminalPostCommitErrorPublishesJournaledResultBothStores":                    "acknowledged_store_result_consumer_test.go",
	"TestActivityJournalFixtureTerminalAcknowledgementSurvivesPostCommitError":                 "activity_journal_acknowledged_fixture_test.go",
}

func TestNativeChannelRecipesPreserveProjectionPublicationAndTemporalCuts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, row := range rows {
		if row.Family != "native-channel-terminal" {
			continue
		}
		if nativeChannelSetupRoots[row.Function] == "" || row.File != "internal/runtime/pipeline/"+nativeChannelSetupRoots[row.Function] || seen[row.Function] {
			t.Fatalf("unknown or duplicated channel consumer: %s/%s", row.File, row.Function)
		}
		seen[row.Function] = true
		before, after := channelWorkloadAndAssertions(t, row.Before), channelWorkloadAndAssertions(t, row.After)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("channel workload or terminal assertion changed: %s\nbefore=%v\nafter=%v", row.Function, before, after)
		}
		if !strings.Contains(row.After, "fixture.RequireRun(ctx, runID)") || strings.Contains(row.After, "newDurablePipelineCoordinatorForTest") {
			t.Fatalf("channel consumer retained reconstructed setup: %s", row.Function)
		}
	}
	if len(seen) != len(nativeChannelSetupRoots) {
		t.Fatalf("channel cohort=%d, want four", len(seen))
	}
}

func channelWorkloadAndAssertions(t *testing.T, source string) []string {
	t.Helper()
	source = strings.NewReplacer("terminalOwner.", "store.", "faultCount()", "journal.faultCount").Replace(source)
	facts := mockWorkloadAndAssertions(t, source)
	var retained []string
	for index := 0; index < len(facts); index++ {
		// The obsolete fake-runner type assertion is replaced by a witnessed
		// native acknowledgment count, not by a different transaction protocol.
		if index+1 < len(facts) && (strings.Contains(facts[index+1], "activity journal fixture runner =") ||
			strings.Contains(facts[index+1], "cleanup fault acknowledgment count=")) {
			index++
			continue
		}
		retained = append(retained, facts[index])
	}
	return retained
}

func TestNativeChannelControlRejectsChangedNoRedispatchAndResultAssertions(t *testing.T) {
	before := `func proof(){ if calls != 1 || len(bus.publishes) != 2 || bus.publishes[1].ID()!=stored.ResultEventID { t.Fatal("terminal changed") } }`
	after := strings.Replace(before, "calls != 1", "calls != 2", 1)
	if reflect.DeepEqual(channelWorkloadAndAssertions(t, before), channelWorkloadAndAssertions(t, after)) {
		t.Fatal("channel proof weakened its no-redispatch assertion")
	}
}
