package main

import (
	"strings"
	"testing"
)

func mechanicalBusDispatchSeed(t *testing.T, row recipe) string {
	t.Helper()
	before := row.Before
	switch row.Function {
	case "newCompleteEventDispatchFixtureWithOrigin":
		before = strings.Replace(before, "seedCompleteEventDispatchRunWithOrigin(t, ctx, db, backend, runID, createdAt, origin)", "seedCompleteEventDispatchRunWithOrigin(t, ctx, selected, runID, createdAt, origin)", 1)
		before = nativeCompleteDispatchConstruction(before)
	case "seedCompleteEventDispatchRun":
		before = strings.Replace(before, "db *sql.DB, backend, runID string", "selected storetest.RunFixtureStore, runID string", 1)
		before = strings.Replace(before, "\t\tdb,\n\t\tbackend,", "\t\tselected,", 1)
	case "seedCompleteEventDispatchRunWithOrigin":
		before = strings.Replace(before, "\tdb *sql.DB,\n\tbackend, runID string,", "\tselected storetest.RunFixtureStore,\n\trunID string,", 1)
		before = strings.Replace(before, "\tif backend == \"postgres\" {\n\t\trunlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: origin, RunID: runID, StartedAt: startedAt})\n\t} else {\n\t\trunlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: origin, RunID: runID, StartedAt: startedAt})\n\t}", "\tstoretest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: origin, RunID: runID, StartedAt: startedAt})", 1)
	case "TestRunStopDrainsRecoveryBeforeMutationAndRequiredPublicationBothStores", "TestMixedPausedRunningRecoveryBothStores":
		before = strings.ReplaceAll(before, "seedCompleteEventDispatchRun(t, f.ctx, f.db, backend,", "seedCompleteEventDispatchRun(t, f.ctx, f.store,")
	case "TestRunContinueProcessesOnlyTargetRunDecisionRoutesOnSQLiteAndPostgres", "TestPipelineScanRunLocalBlockDoesNotStarveLaterRunOnSQLiteAndPostgres":
		before = strings.ReplaceAll(before, "seedCompleteEventDispatchRun(t, fixture.ctx, fixture.db, backend,", "seedCompleteEventDispatchRun(t, fixture.ctx, fixture.store,")
	default:
		t.Fatal("unknown complete-event lifecycle seed consumer")
	}
	return normalizeNativeBusStopReceipt(before)
}
