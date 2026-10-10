package main

import (
	"encoding/json"
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

func TestNativeBusDispatchSeedPreservesOriginClockAndEveryContinuationAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-dispatch-seed" {
			continue
		}
		matched++
		current := row
		row = historicalMechanicalRecipe(t, row)
		if mechanicalBusDispatchSeed(t, row) != row.After {
			t.Fatal("run origin, source, clock, continuation ordering or assertions changed")
		}
		actual, actualErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		expected, expectedErr := canonicalFunction(current.After)
		if actualErr != nil || expectedErr != nil || actual != expected {
			t.Fatalf("%s: complete-event seed diverged from finite migration", row.Function)
		}
	}
	if matched != 7 {
		t.Fatalf("complete-event seed recipes=%d,want7", matched)
	}
}
