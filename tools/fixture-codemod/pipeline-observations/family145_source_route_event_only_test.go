package main

import (
	"fmt"
	"strings"
	"testing"
)

func family145SourceRouteMatrixProofError(before, after string) error {
	if strings.Count(after, "storetest.StartPostgresRuntimeStore(t)") != 1 {
		return fmt.Errorf("source-route matrix lost its original native owner")
	}
	normalized := strings.Replace(after, "\n\tctx := runtimeauthoractivity.WithScope", "\n\t_, db, _ := testutil.StartPostgres(t)\n\tctx := runtimeauthoractivity.WithScope", 1)
	for _, cut := range []struct {
		current, original string
		count             int
	}{
		{"storetest.StartPostgresRuntimeStore(t)", "storetest.AdmitPostgresRuntimeStore(t, db)", 1},
		{"storetest.InsertNativeExistingRunRootEvent(t, ctx, pg,", "storetest.InsertExistingRunRootEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres,", 1},
		{"storetest.InsertNativeCanonicalEventRecord(t, ctx, pg, event)", "storetest.InsertCanonicalEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres, event)", 1},
		{"storetest.CaptureSelectedRunForkRevision(t, ctx, pg, runID, runforkrevision.AllFamilies()...)", "captureRunForkRevision(t, ctx, db, runID)", 2},
	} {
		if strings.Count(normalized, cut.current) != cut.count || strings.Count(before, cut.original) != cut.count {
			return fmt.Errorf("source-route matrix changed selected owner, historical event arguments or separate capture cuts: %s", cut.current)
		}
		normalized = strings.ReplaceAll(normalized, cut.current, cut.original)
	}
	if normalized != before {
		return fmt.Errorf("source-route fixture migration changed original cases, assertions, record class or sequence")
	}
	return nil
}

func family145CanonicalRecordCallerProofError(function, before, after string) error {
	normalized := after
	switch function {
	case "TestEntityTools_GetEntityReturnsForkLocalMaterializedRevision", "TestEntityTools_SaveEntityFieldAfterForkActivationUsesForkRunID":
		normalized = strings.ReplaceAll(normalized, "storetest.InsertNativeCanonicalEventRecord(t, ctx, pg,", "storetest.InsertCanonicalEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres,")
	case "TestComposedStartupCreationPublicationHandoffOnBothStores":
		normalized = strings.Replace(normalized, "storetest.InsertNativeCanonicalEventRecord(t, activationCtx, persistence.deps.EventStore, trigger)", "dialect := authoractivityfixture.DialectSQLite\n\t\t\t\tif backend == \"postgres\" {\n\t\t\t\t\tdialect = authoractivityfixture.DialectPostgres\n\t\t\t\t}\n\t\t\t\tstoretest.InsertCanonicalEventRecord(t, activationCtx, db, dialect, trigger)", 1)
	case "TestContextualRuntimeProducerLineageReadbackParity":
		normalized = strings.Replace(normalized, "return storetest.StartPostgresRuntimeStore(t)", "_, db, cleanup := testutil.StartPostgres(t)\n\t\tt.Cleanup(cleanup)\n\t\treturn storetest.AdmitPostgresRuntimeStore(t, db)", 1)
		normalized = strings.Replace(normalized, "storetest.InsertNativeCanonicalEventRecord(t, ctx, selectedStore, event)", "dialect := authoractivityfixture.DialectSQLite\n\t\t\t\t\t\t\tif backend.name == \"postgres\" {\n\t\t\t\t\t\t\t\tdialect = authoractivityfixture.DialectPostgres\n\t\t\t\t\t\t\t}\n\t\t\t\t\t\t\tstoretest.InsertCanonicalEventRecord(t, ctx, selectedDatabase(t, selectedStore), dialect, event)", 1)
	default:
		return fmt.Errorf("unreviewed canonical-record caller: %s", function)
	}
	if normalized != before {
		return fmt.Errorf("canonical-record caller changed original construction, clocks, assertions, record mode or temporal cuts: %s", function)
	}
	return nil
}

func family145CanonicalRecordCaller(function string) bool {
	switch function {
	case "TestEntityTools_GetEntityReturnsForkLocalMaterializedRevision", "TestEntityTools_SaveEntityFieldAfterForkActivationUsesForkRunID", "TestComposedStartupCreationPublicationHandoffOnBothStores", "TestContextualRuntimeProducerLineageReadbackParity":
		return true
	}
	return false
}

var family145NativeRootCallerBindings = map[string][2]string{
	"TestPostgresAgentConversationOwnerBacksSupportedAPISurface":                        {"storetest.InsertNativeExistingRunRootEvent(t, ctx, selected,", "storetest.InsertExistingRunRootEventRecord(t, ctx, db, \"postgres\","},
	"TestSQLiteAgentConversationOwnerBacksSupportedAPISurface":                          {"storetest.InsertNativeExistingRunRootEvent(t, ctx, sqliteStore,", "storetest.InsertExistingRunRootEventRecord(t, ctx, storetest.DatabaseForTest(sqliteStore), \"sqlite\","},
	"seedTurnWithOutputContract":                                                        {"storetest.InsertNativeExistingRunRootEvent(t, f.ctx, f.store,", "storetest.InsertExistingRunRootEventRecord(t, f.ctx, f.db, authoractivityfixture.Dialect(f.backend.name),"},
	"seedServeRuntimeSQLiteAbandonWork":                                                 {"storetest.InsertNativeExistingRunRootEvent(t, ctx, sqliteStore,", "storetest.InsertExistingRunRootEventRecord(t, ctx, storetest.DatabaseForTest(sqliteStore), authoractivityfixture.DialectSQLite,"},
	"TestRunServeRuntimeAbandonActiveRunsQuiescesBeforeBundleMatchAdmission":            {"storetest.InsertNativeExistingRunRootEvent(t, ctx, runtimePG,", "storetest.InsertExistingRunRootEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres,"},
	"TestExecuteSelectedContractRunForkAdmitsSameSourceActiveDeliveryForkPointEmission": {"storetest.InsertNativeExistingRunRootEvent(t, ctx, pg,", "storetest.InsertExistingRunRootEventRecord(t, ctx, db, authoractivityfixture.DialectPostgres,"},
}

func family145NativeRootCallerProofError(function, before, after string) error {
	binding, ok := family145NativeRootCallerBindings[function]
	if !ok || strings.Count(after, binding[0]) != 1 || strings.Count(before, binding[1]) != 1 {
		return fmt.Errorf("root record lost its original native owner, context or exact append occurrence: %s", function)
	}
	normalized := strings.Replace(after, binding[0], binding[1], 1)
	if function == "TestExecuteSelectedContractRunForkAdmitsSameSourceActiveDeliveryForkPointEmission" {
		// Compose only the already-reviewed source constructor and capture owner.
		for _, cut := range [][2]string{
			{"seedSelectedConstructedRootHistory(t, ctx, pg,", "seedSelectedConstructedRootHistory(t, ctx, db, pg,"},
			{"captureSelectedExecutionSourceRevision(t, pg, sourceRunID)", "captureSelectedExecutionSourceRevision(t, db, sourceRunID)"},
		} {
			if strings.Count(normalized, cut[0]) != 1 || strings.Count(before, cut[1]) != 1 {
				return fmt.Errorf("reviewed source construction/capture binding disappeared: %s", cut[0])
			}
			normalized = strings.Replace(normalized, cut[0], cut[1], 1)
		}
	}
	if normalized != before {
		return fmt.Errorf("root record propagation changed an original clock, pending/turn/claim/replay cut or assertion: %s", function)
	}
	return nil
}

func TestFamily145NativeEventOnlyRecordUsesOriginalMutationAndStrictRecordOwner(t *testing.T) {
	owner := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_event_support.go", "InsertNativeCanonicalEventRecordForTest")
	for _, cut := range []string{"eventFixtureDialectForTest(selected)", "admitted.Class() == events.EventAdmissionSelectedForkReplay", "eventrecord.FromAdmitted(admitted, settlement)", "runEventFixtureMutationForTest(ctx, selected,", "eventrecordpostgres.Insert(ctx, attempt, record)", "eventrecordsqlite.Insert(ctx, attempt, record)", "attempt.WithSQL(ctx,", "eventrecordpostgres.Load(ctx, tx, record.EventID)", "eventrecordsqlite.Load(ctx, tx, record.EventID)", "!found || !record.Equal(existing)"} {
		if strings.Count(owner, cut) != 1 {
			t.Fatalf("native event-only record lost closed original owner or exact replay cut: %s", cut)
		}
	}
	for _, escape := range []string{"CommitSemanticEvent", "CommitFlowInstanceActivation", "NewTransaction", "EnsureRun", "time.Now("} {
		if strings.Contains(owner, escape) {
			t.Fatalf("event-only record gained construction/publication/reconstructed owner/clock: %s", escape)
		}
	}
}
