package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
)

var family145ResetCompletionCuts = []string{
	"storetest.StartPostgresRuntimeStore(t)", "acquireAdministrativeResetCapability(t, selected)",
	"storetest.InstallPostgresResetAPICompletionLoss(context.Background(), selected)", "sync.OnceValue(func() error", "defer func()",
	"storetest.RemovePostgresResetAPICompletionLoss(context.Background(), selected)", "if err := restoreCompletion(); err != nil",
	"storetest.ReadResetCompletionStorageRecord(context.Background(), selected)", "json.Unmarshal(raw, &admitted)",
	"storetest.CountPhysicalRunIdentity(context.Background(), selected, laterRun)", "storetest.CountResetCompletionStorageOperations(context.Background(), selected)",
	"now.Add(time.Minute)", "now.Add(48*time.Hour)", "storetest.RequireRun(t, ctx, selected,", "external.release()",
	"Planner: destructivereset.InventoryPlanner{Reader: selected}", "Locks: selected", "Quiescer: destructivereset.Quiescer{Store: selected}",
	"Cleaner: destructivereset.Cleaner{Store: administrativeResetCleanup{cap}}", "Containers: external",
}

func family145ResetCompletionProofError(before, after string) error {
	if err := family145ConnectorProofError("TestRuntimeNukeDurableReplaySurvivesAPICompletionLossAndExpiration", before, after); err != nil {
		return err
	}
	for _, cut := range family145ResetCompletionCuts {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("reset completion witness lost original owner/cut: %s", cut)
		}
	}
	if strings.Count(after, "restoreCompletion()") != 2 || strings.LastIndex(after, "if err := restoreCompletion();") > strings.Index(after, "storetest.ReadResetCompletionStorageRecord(") {
		return fmt.Errorf("transport fault removal moved past original record observation")
	}
	return nil
}

func TestFamily145ResetCompletionRetainsDurableReplayAndExactTransportFault(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		if row.Function != "TestRuntimeNukeDurableReplaySurvivesAPICompletionLossAndExpiration" {
			continue
		}
		found++
		if err := family145ResetCompletionProofError(row.Before, row.After); err != nil {
			t.Fatal(err)
		}
		for _, cut := range family145ResetCompletionCuts {
			if family145ResetCompletionProofError(row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost reset completion owner/cut accepted: %s", cut)
			}
		}
		if family145ResetCompletionProofError(row.Before, strings.Replace(row.After, "restoreCompletion()", "discardedRestore()", 1)) == nil {
			t.Fatal("lost assertion-failure restoration passed source proof")
		}
		manual := "\tif err := restoreCompletion(); err != nil {\n\t\tt.Fatal(err)\n\t}\n"
		read := "\traw, err := storetest.ReadResetCompletionStorageRecord(context.Background(), selected)\n"
		if !strings.Contains(row.After, manual) || !strings.Contains(row.After, read) {
			t.Fatal("exact restoration/record control source changed")
		}
		hostile := strings.Replace(row.After, manual, "", 1)
		hostile = strings.Replace(hostile, read, read+manual, 1)
		if family145ResetCompletionProofError(row.Before, hostile) == nil {
			t.Fatal("restoration moved past record observation passed source proof")
		}
		assertions, err := family145DescriptorBusinessAssertions(row.Before)
		if err != nil {
			t.Fatal(err)
		}
		for _, assertion := range assertions {
			hostile, err := family145BusAssertionCounterexample(row.After, strings.SplitN(assertion, " => ", 2)[0])
			if err != nil {
				t.Fatal(err)
			}
			if family145ResetCompletionProofError(row.Before, hostile) == nil {
				t.Fatalf("lost original durable replay assertion accepted: %s", assertion)
			}
		}
		assertFamily145ResetCompletionQueries(t, row.Before)
	}
	if found != 1 {
		t.Fatalf("finite reset-completion source inventory changed: %d", found)
	}
	data, err := os.ReadFile("../../../internal/apiv1/runtime_nuke_durable_replay_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := family145DescriptorReadinessEscape(data); err != nil {
		t.Fatal(err)
	}
}

func assertFamily145ResetCompletionQueries(t *testing.T, before string) {
	t.Helper()
	queries := family145ConnectorSQL(t, "package proof\n"+before)
	for _, port := range []struct {
		file, function string
		index          int
	}{
		{"internal/adminpersistence/reset_completion_fixture_observation.go", "ReadResetCompletionStorageRecordTx", 0},
		{"internal/runtimepersistence/test_api_event_storage.go", "CountPhysicalRunIdentityForTest", 1},
		{"internal/adminpersistence/reset_completion_fixture_observation.go", "CountResetCompletionStorageOperationsTx", 2},
	} {
		owned := family145ConversationForkOwnerSource(t, port.file, port.function)
		actual := family145ConnectorSQL(t, "package proof\n"+owned)
		if len(queries) != 3 || len(actual) != 1 || queries[port.index] != actual[0] {
			t.Fatalf("reset completion physical predicate/record/cardinality changed: %s", port.function)
		}
	}
	for _, fault := range []struct{ function, keyword string }{
		{"InstallPostgresResetAPICompletionLossTx", "CREATE"},
		{"RemovePostgresResetAPICompletionLossTx", "DROP"},
	} {
		owned := family145ConversationForkOwnerSource(t, "internal/apiidempotency/reset_completion_fixture.go", fault.function)
		original := family145GateRecoveryStatements(t, "package proof\n"+before, fault.keyword)
		actual := family145GateRecoveryStatements(t, "package proof\n"+owned, fault.keyword)
		if len(original) != 1 || !reflect.DeepEqual(original, actual) {
			t.Fatalf("transport completion fault phase/payload/statements changed: %v/%v", original, actual)
		}
	}
}
