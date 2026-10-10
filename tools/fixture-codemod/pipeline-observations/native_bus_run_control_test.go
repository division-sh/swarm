package main

import (
	"strings"
	"testing"
)

func normalizedBusRunControlConstruction(t *testing.T, source string) string {
	t.Helper()
	before := "_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\n\tctx := testAuthorActivityContext(context.Background())\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)"
	after := "pg := storetest.StartPostgresRuntimeStore(t)\n\tctx := testAuthorActivityContext(context.Background())"
	source = strings.Replace(source, before, after, 1)
	source = strings.ReplaceAll(source, "seedRunControlTestRun(t, ctx, db,", "seedRunControlTestRun(t, ctx, pg,")
	value, err := canonicalFunction(source)
	if err != nil {
		return "parse-refused"
	}
	return value
}

const busRunControlSeedShape = `func seedRunControlTestRun(t *testing.T,ctx context.Context,selected storetest.RunFixtureStore,runID string) {
t.Helper()
storetest.RequireRun(t,ctx,selected,storetest.RunFixture{Origin:storetest.ScenarioSetupOrigin(),RunID:runID,BundleHash:authorActivityTestBundleHash})
}`

func busRunControlSeedPreserved(source string) bool {
	want, err := canonicalFunction(busRunControlSeedShape)
	got, parseErr := canonicalFunction(source)
	return err == nil && parseErr == nil && want == got
}

func TestNativeBusRunControlSeedUsesExactLifecycleOwnerAndSource(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-bus-run-control-construction")
	before := strings.Replace(row.Before, "db *sql.DB", "selected storetest.RunFixtureStore", 1)
	before = strings.Replace(before, "runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin()", "storetest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin()", 1)
	if before != row.After || !busRunControlSeedPreserved(row.After) {
		t.Fatal("run-control seed changed exact run, source, origin, default lifecycle or error handling")
	}
	actual := selectedCausalObservationBody(t, row.File, row.Function)
	for _, pair := range [][2]string{
		{"t, ctx, selected,", "t, ctx, foreign,"},
		{"RunID: runID", "RunID: otherRun"},
		{"BundleHash: authorActivityTestBundleHash", "BundleHash: otherHash"},
		{"storetest.ScenarioSetupOrigin()", "storetest.EventOrigin(t, otherEvent, \"task.requested\")"},
	} {
		mutant := strings.Replace(actual, pair[0], pair[1], 1)
		if mutant == actual || busRunControlSeedPreserved(mutant) {
			t.Fatalf("weakened native lifecycle seed admitted: %v", pair)
		}
	}
}
