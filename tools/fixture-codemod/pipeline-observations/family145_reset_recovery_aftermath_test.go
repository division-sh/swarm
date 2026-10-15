package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145ResetRecoveryAftermathCuts = map[string][]string{
	"TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset": {
		"storetest.StartPostgresRuntimeStore(t)", "loadConformanceRuntimeWorkflowModule(t)",
		"conformanceSourceArtifactFact(t, module.source)", "storetest.RequireBundleDataCatalog(t, ctx, pg, bundle)",
		"requireCanonicalRuntimeLogSurface(t, ctx, pg)", "claimErr: errors.New(\"claim failed\")",
		"runtimepkg.NewRuntime(ctx, completeConformanceWorkflowDeps(pg,", "RecoveryOnStartup: true",
		"installConformanceRuntimeStartupGrant(t, ctx, pg, rt)", "rt.Start(ctx)",
		"storetest.CountEventNameStorage(ctx, pg, \"platform.reset\")",
		"storetest.ReadPostgresRecoveryFailedPayloadStorage(ctx, pg)",
		"json.Unmarshal(recoveryFailedPayloadRaw, &recoveryFailedPayload)",
		"json.Marshal(recoveryFailedPayload[\"failure\"])", "runtimefailures.UnmarshalEnvelope(failureRaw)",
	},
	"TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader": {
		"storetest.StartPostgresRuntimeStore(t)", "requireCanonicalRuntimeLogSurface(t, ctx, pg)",
		"storetest.RequireRun(t, ctx, pg,", "seedConformanceAgent(t, ctx, pg, runID, \"agent-1\")",
		"runtimepkg.NewRuntimeLogger(pg, executionposture.Live, conformanceRuntimeLogPayloadAdmitter)",
		"conformanceTestRuntimeOccurrence(t, authorActivityTestSourceArtifactFact.BundleHash())",
		"newScopedTestEventBus(t, pg, durableConformanceEventBusOptions(pg,", "WorkOwner: workOwner",
		"pg.SetSessionLockTTL(30 * time.Second)", "runtimesessions.Registry(pg)",
		"runtimeeffects.WithDifferentOwner(ctx, runtimeeffects.OwnerBuildTestInfrastructure)",
		"conformanceAgentIdentity(t, runID, \"agent-1\")", "registry.Acquire(leaseCtx, identity, \"conformance\")",
		"runtimemanager.NewAgentManagerWithOptions(bus, nil,", "am.ResetRuntimeStateWithSource(\"admin_cli\")",
		"pg.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{Component: \"runtime\", ActionOrEventType: \"reset_orphaned_sessions\", Limit: 10})",
		"storetest.ReadPostgresSessionTerminationStorage(ctx, pg, lease.SessionID)",
		"status, terminationReason, terminationDetail := termination.Status, termination.Reason, termination.Detail",
		"storetest.ReadPostgresResetPayloadStorage(ctx, pg)", "json.Unmarshal(resetPayloadRaw, &resetPayload)",
	},
}

func family145ResetRecoveryAftermathProofError(function, before, after string) error {
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, cut := range family145ResetRecoveryAftermathCuts[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("reset/recovery aftermath lost native owner or original execution cut: %s", cut)
		}
	}
	if strings.Index(after, "rt.Start(ctx)") > strings.Index(after, "storetest.CountEventNameStorage(") && strings.Contains(after, "rt.Start(ctx)") {
		return fmt.Errorf("startup failure observation moved before actual recovery")
	}
	if strings.Contains(after, "am.ResetRuntimeStateWithSource(") &&
		(strings.Index(after, "registry.Acquire(") > strings.Index(after, "am.ResetRuntimeStateWithSource(") ||
			strings.Index(after, "am.ResetRuntimeStateWithSource(") > strings.Index(after, "pg.ListOperatorRuntimeLogs(") ||
			strings.Index(after, "pg.ListOperatorRuntimeLogs(") > strings.Index(after, "storetest.ReadPostgresSessionTerminationStorage(")) {
		return fmt.Errorf("orphan-reset aftermath changed lease/reset/diagnostic/storage execution order")
	}
	reads := []string{"storetest.CountEventNameStorage(", "storetest.ReadPostgresRecoveryFailedPayloadStorage("}
	if strings.Contains(after, "am.ResetRuntimeStateWithSource(") {
		reads = []string{"storetest.ReadPostgresSessionTerminationStorage(", "storetest.ReadPostgresResetPayloadStorage("}
	}
	if strings.Count(after, reads[0]) != 1 || strings.Count(after, reads[1]) != 1 || strings.Index(after, reads[0]) >= strings.Index(after, reads[1]) {
		return fmt.Errorf("aftermath lost the two separate original reads or reversed their execution order")
	}
	return nil
}

func TestFamily145ResetRecoveryAftermathWholeGroupPreservesAssertionsAndExecutionCuts(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		if _, ok := family145ResetRecoveryAftermathCuts[row.Function]; !ok {
			continue
		}
		found++
		if err := family145ResetRecoveryAftermathProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatalf("%s: %v", row.Function, err)
		}
		for _, cut := range family145ResetRecoveryAftermathCuts[row.Function] {
			if family145ResetRecoveryAftermathProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost reset/recovery cut accepted: %s", cut)
			}
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
			if family145ResetRecoveryAftermathProofError(row.Function, row.Before, hostile) == nil {
				t.Fatalf("lost original reset/recovery business assertion accepted: %s", assertion)
			}
		}
	}
	if found != 2 {
		t.Fatalf("complete reset/recovery aftermath inventory=%d want2", found)
	}
}

func TestFamily145ResetRecoveryAftermathStoragePreservesEveryPhysicalQuery(t *testing.T) {
	var rows []struct{ Function, Before string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	original := map[string]string{}
	for _, row := range rows {
		original[row.Function] = row.Before
	}
	for _, port := range []struct {
		file, owner, predecessor string
		query                    int
	}{
		{"eventrecord/template_storage_observation.go", "ReadPostgresRecoveryFailedPayloadStorageTx", "TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset", 1},
		{"eventrecord/template_storage_observation.go", "ReadPostgresResetPayloadStorageTx", "TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader", 1},
		{"llmpersistence/session_storage_observation.go", "ReadPostgresSessionTerminationStorageTx", "TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader", 0},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/"+port.file, port.owner)
		old, got := family145ConnectorSQL(t, "package proof\n"+original[port.predecessor]), family145ConnectorSQL(t, "package proof\n"+owner)
		if len(old) != 2 || len(got) != 1 || !reflect.DeepEqual(got, old[port.query:port.query+1]) {
			t.Fatalf("aftermath physical statement changed columns, NULL behavior, scope or order: %s", port.owner)
		}
	}
	count := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_api_event_storage.go", "CountEventNameStorageForTest")
	want := family145ConnectorSQL(t, "package proof\n"+original["TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset"])[0]
	got := family145ConnectorSQL(t, "package proof\n"+count)
	if len(got) != 1 || strings.Replace(got[0], "$1", "'platform.reset'", 1) != want {
		t.Fatal("existing exact event-name count changed original whole-store physical scope")
	}
}

func TestFamily145ResetRecoveryAftermathBridgesKeepOriginalCoordinatorAndNoPublicQueryAuthority(t *testing.T) {
	for _, port := range []struct{ file, bridge, owner, identity string }{
		{"test_event_support.go", "ReadPostgresRecoveryFailedPayloadStorageForTest", "eventrecord.ReadPostgresRecoveryFailedPayloadStorageTx", ""},
		{"test_event_support.go", "ReadPostgresResetPayloadStorageForTest", "eventrecord.ReadPostgresResetPayloadStorageTx", ""},
		{"test_session_termination_storage.go", "ReadPostgresSessionTerminationStorageForTest", "llmpersistence.ReadPostgresSessionTerminationStorageTx(ctx, tx, sessionID)", ", sessionID"},
	} {
		bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/"+port.file, port.bridge)
		for _, cut := range []string{"validatePostgresPublicationIdentities(selected" + port.identity + ")", "readFanOutSemanticStorage(ctx, selected, nil,", port.owner} {
			if !strings.Contains(bridge, cut) {
				t.Fatalf("aftermath bridge lost original native reader or bounded identity: %s/%s", port.bridge, cut)
			}
		}
		public := family145ConversationForkOwnerSource(t, "storetest/reset_recovery_aftermath_storage.go", strings.TrimSuffix(port.bridge, "ForTest"))
		if !strings.Contains(public, "private."+port.bridge+"(ctx, selected"+port.identity+")") || strings.Contains(public, "sql.") {
			t.Fatalf("aftermath public port gained raw query/callback authority: %s", port.bridge)
		}
	}
}
