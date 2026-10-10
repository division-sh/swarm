package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func normalizedForegroundClaimJoin(source string) string {
	source = strings.Replace(source, "\t\t\tvar publisher sync.WaitGroup\n\t\t\tvar releaseOnce sync.Once\n\t\t\treleaseDispatch := func() { releaseOnce.Do(func() { close(release) }) }\n\t\t\tdefer func() {\n\t\t\t\treleaseDispatch()\n\t\t\t\tpublisher.Wait()\n\t\t\t\tif err := foreground.WaitForQuiescence(context.Background()); err != nil {\n\t\t\t\t\tt.Errorf(\"join foreground fixture dispatch: %v\", err)\n\t\t\t\t}\n\t\t\t}()\n\t\t\tpublisher.Add(1)\n", "", 1)
	source = strings.Replace(source, "\t\t\t\tdefer publisher.Done()\n", "", 1)
	return strings.Replace(source, "\t\t\treleaseDispatch()\n", "\t\t\tclose(release)\n", 1)
}

func TestNativeBusForegroundClaimUsesOneCoordinatorAndPreservesEverySiblingAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-foreground-claims" {
			continue
		}
		matched++
		before := row.Before
		for _, pair := range [][2]string{
			{"(runtimebus.EventStore, runtimebus.EventStore, *sql.DB, string)", "foregroundClaimFixtureStore"},
			{"selected := storetest.StartSQLiteRuntimeStore(t)\n\t\t\t\treturn selected, selected, storetest.DatabaseForTest(selected), \"?\"", "return storetest.StartSQLiteRuntimeStore(t)"},
			{"_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\treturn storetest.AdmitPostgresRuntimeStore(t, db), storetest.AdmitPostgresRuntimeStore(t, db), db, \"$1::uuid\"", "return storetest.StartPostgresRuntimeStore(t)"},
			{"foregroundStore, siblingStore, db, _ := tc.open(t)", "selected := tc.open(t)"},
			{"\t\t\tif tc.name == \"sqlite\" {\n\t\t\t\trunlifecyclefixture.RequireSQLite(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})\n\t\t\t} else {\n\t\t\t\trunlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID})\n\t\t\t}", "\t\t\tstoretest.RequireRun(t, context.Background(), selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID})"},
			{"newScopedTestEventBus(foregroundStore,", "newScopedTestEventBus(selected,"},
			{"newScopedTestEventBus(siblingStore)", "newScopedTestEventBus(selected)"},
		} {
			before = strings.ReplaceAll(before, pair[0], pair[1])
		}
		if before != normalizedForegroundClaimJoin(row.After) {
			t.Fatal("foreground claim, acknowledged return, sibling exclusion or settlement assertions changed")
		}
		actual, err := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		want, wantErr := canonicalFunction(row.After)
		if err != nil || wantErr != nil || actual != want {
			t.Fatal("foreground claim consumer diverged from finite migration")
		}
	}
	if matched != 1 {
		t.Fatalf("foreground claim recipes=%d,want1", matched)
	}
}
