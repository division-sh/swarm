package main

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145RunStateCalls = map[string][]string{
	"markRunStatusCompleted": {
		"storetest.ReadPostgresRunCompletionIdentityStorage(runStatusAuthorActivityContext(source), pg, eventID)",
		"runID, bundleHash := identity.RunID, identity.BundleHash",
		"storetest.ExecuteRunCompletionCandidate(runStatusAuthorActivityContext(source), pg, bundleHash, runID, stagecatalogfixture.NewFinalCatalog([]string{\"ready\"}, map[string][]string{\".\": {\"ready\"}}))",
	},
	"waitRunStatusEventSettlement": {
		"ctx := context.Background()", "storetest.ReadPostgresRunStatusSettlementStorage(ctx, selected, runID)",
		"eventCount, activeDeliveries := observed.EventCount, observed.ActiveDeliveries",
		"storetest.ReadPostgresRunStatusFailureDiagnosticsStorage(ctx, selected, runID)",
		"t.Logf(\"delivery status=%s failure=%s\", row.Status, row.Failure)",
		"if err == nil && eventCount >= wantEvents && activeDeliveries == 0", "time.Now().Add(3 * time.Second)", "time.Sleep(10 * time.Millisecond)",
	},
	"TestRunState_UsesDurableCompletedRunState": {
		"storetest.StartPostgresRuntimeStore(t)", "newRunStatusEventBus(t, pg)", "registerRunStatusEventCatalog(t, pg, source)",
		"publishRunStatusRootEvent(t, eb, source, runID, entityID)", "seedRunStatusEntityState(t, pg, source, runID, entityID)",
		"markRunStatusCompleted(t, pg, source, eventID)", "storetest.ReadPostgresRunStatusClockStorage(ctx, pg, runID)",
		"status := observed.Status", "endedAt = observed.EndedAt", "time.Now().Add(2 * time.Second)", "time.Sleep(10 * time.Millisecond)",
		"if err == nil && status == \"completed\" && endedAt.Valid",
	},
	"TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles": {
		"storetest.StartPostgresRuntimeStore(t)", "newRunStatusEventBus(t, pg)", "registerRunStatusEventCatalog(t, pg, source)",
		"runtimemanager.NewAgentManagerWithOptions(", "WorkOwner: workOwner", "installServeTestExactAgentReadiness(t, eb,",
		"publishRunStatusExistingRootEvent(t, eb, source, runID, entityID)", "seedRunStatusEntityState(t, pg, source, runID, entityID)",
		"storetest.ReadPostgresRunStatusCountsStorage(ctx, pg, runID)", "status, eventCount, entityCount = observed.Status, observed.EventCount, observed.EntityCount",
		"storetest.CountPostgresRunStatusSemanticEvents(ctx, pg, runID)", "storetest.CountPostgresRunStatusActiveDeliveries(ctx, pg, runID)",
		"storetest.CountPostgresExtraRunningStatusRows(ctx, pg, runID)", "waitRunStatusEventSettlement(t, pg, runID, 2)",
		"markRunStatusCompleted(t, pg, source, eventID)", "case <-time.After(time.Second):", "case <-agentStarted:",
		"time.Now().Add(3 * time.Second)", "time.Sleep(10 * time.Millisecond)", "if err == nil && status == \"completed\"",
	},
	"TestRunState_PreservesRunningTruthWhileManagerWorkIsActive": {
		"storetest.StartPostgresRuntimeStore(t)", "newRunStatusEventBus(t, pg)", "registerRunStatusEventCatalog(t, pg, source)",
		"runtimemanager.NewAgentManagerWithOptions(", "WorkOwner: workOwner", "installServeTestExactAgentReadiness(t, eb,",
		"publishRunStatusExistingRootEvent(t, eb, source, runID, entityID)", "seedRunStatusEntityState(t, pg, source, runID, entityID)",
		"storetest.ReadPostgresRunStatusClockStorage(ctx, pg, runID)", "status, endedAt = observed.Status, observed.EndedAt",
		"storetest.CountPostgresRunStatusActiveDeliveries(ctx, pg, runID)", "waitRunStatusEventSettlement(t, pg, runID, 2)",
		"markRunStatusCompleted(t, pg, source, eventID)", "case <-time.After(time.Second):", "case <-agentStarted:",
		"time.Sleep(120 * time.Millisecond)", "time.Now().Add(3 * time.Second)", "time.Sleep(10 * time.Millisecond)",
		"if err == nil && status == \"completed\" && endedAt.Valid",
	},
}

func family145RunStateProofError(function, before, after string) error {
	before = strings.ReplaceAll(before, "waitRunStatusEventSettlement(t, db,", "waitRunStatusEventSettlement(t, pg,")
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, call := range family145RunStateCalls[function] {
		if !strings.Contains(after, call) {
			return fmt.Errorf("run-state chain lost original owner, field, clock or cut: %s", call)
		}
	}
	if function == "TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles" || function == "TestRunState_PreservesRunningTruthWhileManagerWorkIsActive" {
		return family145RunStateJoinedCleanupError(after)
	}
	return nil
}

func family145RunStateJoinedCleanupError(source string) error {
	for _, cut := range []string{"releaseAgentWork := sync.OnceFunc(func() {\n\t\tclose(releaseAgent)\n\t})", "defer func()", "if err := am.Shutdown(); err != nil", "t.Errorf(\"join run-status manager: %v\", err)"} {
		if !strings.Contains(source, cut) {
			return fmt.Errorf("blocked run-state fixture lost failure-safe joined cleanup: %s", cut)
		}
	}
	if strings.Count(source, "close(releaseAgent)") != 1 || strings.Count(source, "releaseAgentWork()") != 2 ||
		strings.Index(source, "defer func()") > strings.Index(source, "am.Run(") ||
		strings.Index(source, "releaseAgentWork()") > strings.Index(source, "am.Shutdown()") ||
		strings.LastIndex(source, "releaseAgentWork()") < strings.Index(source, "case <-agentStarted:") ||
		strings.LastIndex(source, "releaseAgentWork()") > strings.Index(source, "waitRunStatusEventSettlement(") {
		return fmt.Errorf("blocked run-state fixture moved release/registration/join outside the original work window")
	}
	return nil
}

func assertFamily145RunStateCleanupCounterexamples(t *testing.T, function, after string) {
	t.Helper()
	if function != "TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles" && function != "TestRunState_PreservesRunningTruthWhileManagerWorkIsActive" {
		return
	}
	for _, hostile := range []string{
		strings.Replace(after, "releaseAgentWork()", "discardedRelease()", 1),
		strings.Replace(after, "sync.OnceFunc(func() {\n\t\tclose(releaseAgent)\n\t})", "func() { close(releaseAgent) }", 1),
		strings.Replace(after, "if err := am.Shutdown(); err != nil", "if err := discardedJoin(); err != nil", 1),
		strings.Replace(after, "case <-agentStarted:", "case <-time.After(time.Second):\nreleaseAgentWork()", 1),
	} {
		if family145RunStateJoinedCleanupError(hostile) == nil {
			t.Fatal("lost once-only release, assertion-failure join or original held-work window accepted")
		}
	}
}

func assertFamily145RunStateQueries(t *testing.T, original map[string]string) {
	t.Helper()
	for _, port := range []struct {
		file, owner, predecessor string
		query                    int
	}{
		{"runlifecycle/standalone_fixture_storage.go", "ReadPostgresRunCompletionIdentityStorageTx", "markRunStatusCompleted", 0},
		{"delivery/read_projections.go", "ReadPostgresRunStatusSettlementStorageTx", "waitRunStatusEventSettlement", 0},
		{"delivery/read_projections.go", "ReadPostgresRunStatusFailureDiagnosticsStorageTx", "waitRunStatusEventSettlement", 1},
		{"runlifecycle/standalone_fixture_storage.go", "ReadPostgresRunStatusClockStorageTx", "TestRunState_UsesDurableCompletedRunState", 0},
		{"runlifecycle/standalone_fixture_storage.go", "ReadPostgresRunStatusCountsStorageTx", "TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles", 0},
		{"eventrecord/template_storage_observation.go", "CountPostgresRunStatusSemanticEventsTx", "TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles", 1},
		{"delivery/read_projections.go", "CountPostgresRunStatusActiveDeliveriesTx", "TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles", 2},
		{"runlifecycle/standalone_fixture_storage.go", "CountPostgresExtraRunningStatusRowsTx", "TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles", 4},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/"+port.file, port.owner)
		old, got := family145ConnectorSQL(t, "package proof\n"+original[port.predecessor]), family145ConnectorSQL(t, "package proof\n"+owner)
		if port.query >= len(old) || len(got) != 1 || !reflect.DeepEqual(got, old[port.query:port.query+1]) {
			t.Fatalf("run-state physical statement changed column/NULL/scope/multiplicity: %s", port.owner)
		}
	}
}

func TestFamily145PhysicalSQLCorrespondenceRecognizesSelectWhitespaceNotLookalikes(t *testing.T) {
	queries := family145ConnectorSQL(t, `package proof
func fixture() {
 _ = "SELECT\n COUNT(*) FROM events WHERE event_name = 'a b'"
 _ = "SELECT\tCOUNT(*) FROM events WHERE event_name = 'a b'"
 _ = "SELECT COUNT(*) FROM events WHERE event_name = 'a b'"
 _ = "SELECTED COUNT(*) FROM events"
 _ = ""
}`)
	want := []string{
		"SELECTCOUNT(*)FROMeventsWHEREevent_name='a b'",
		"SELECTCOUNT(*)FROMeventsWHEREevent_name='a b'",
		"SELECTCOUNT(*)FROMeventsWHEREevent_name='a b'",
	}
	if !reflect.DeepEqual(queries, want) {
		t.Fatalf("SELECT newline/tab/space inventory, quoted content or lookalike refusal changed: got=%q want=%q", queries, want)
	}
}
