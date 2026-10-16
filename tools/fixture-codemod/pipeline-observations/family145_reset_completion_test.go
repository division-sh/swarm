package main

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/requestkey"
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

func assertFamily145ResetCompletionQueries(t *testing.T, before string) {
	t.Helper()
	queries := family145ConnectorSQL(t, "package proof\n"+before)
	for _, port := range []struct {
		file, function string
		index          int
	}{
		{"internal/adminpersistence/reset_completion_fixture_observation.go", "ReadResetCompletionStorageRecordTx", 0},
		{"internal/backend/runlifecycle/run_identity_cardinality.go", "CountRunIdentityStorageTx", 1},
		{"internal/adminpersistence/reset_completion_fixture_observation.go", "CountResetCompletionStorageOperationsTx", 2},
	} {
		owned := family145ConversationForkOwnerSource(t, port.file, port.function)
		actual := family145ConnectorSQL(t, "package proof\n"+owned)
		if len(queries) != 3 || len(actual) != 1 || queries[port.index] != actual[0] {
			t.Fatalf("reset completion physical predicate/record/cardinality changed: %s", port.function)
		}
	}
	family145ResetCountBridge(t, "internal/runtimepersistence/test_api_event_storage.go", "CountPhysicalRunIdentityForTest", "runlifecycle.CountRunIdentityStorageTx", "runID")
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

func family145ResetCountBridge(t *testing.T, file, function, owner, identity string) {
	t.Helper()
	actual := family145ConversationForkOwnerSource(t, file, function)
	if err := family145ResetCountBridgeProofError(actual, function, owner, identity); err != nil {
		t.Fatal(err)
	}
	for _, cut := range []string{"validateChannelObservationOwner(selected)", "readServedDeliveryObservation(ctx, selected,", owner, "ctx, tx, " + identity, "return count, nil"} {
		mutant := strings.Replace(actual, cut, "foreignResetCountCut", 1)
		if mutant == actual || family145ResetCountBridgeProofError(mutant, function, owner, identity) == nil {
			t.Fatal("reset count bridge lost owner/identity/context/error semantics:", function, cut)
		}
	}
}

func family145ResetCountBridgeProofError(actual, function, owner, identity string) error {
	expected := fmt.Sprintf(`func %s(ctx context.Context, selected any, %s string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = %s(ctx, tx, %s)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}`, function, identity, owner, identity)
	got, err := requestkey.Normalize(actual)
	if err != nil {
		return err
	}
	want, err := requestkey.Normalize(expected)
	if err != nil || got != want {
		return fmt.Errorf("reset count bridge changed complete native owner/context/identity/error correspondence: %s: %v", function, err)
	}
	return nil
}
