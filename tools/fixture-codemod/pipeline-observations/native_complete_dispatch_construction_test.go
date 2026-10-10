package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeCompleteDispatchConstruction(source string) string {
	source = strings.Replace(source, "\tvar db *sql.DB\n", "", 1)
	source = strings.Replace(source, "\t\tsqlite := storetest.StartSQLiteRuntimeStore(t)\n\t\tselected, db = sqlite, storetest.DatabaseForTest(sqlite)", "\t\tselected = storetest.StartSQLiteRuntimeStore(t)", 1)
	source = strings.Replace(source, "\t\t_, postgresDB, cleanup := testutil.StartPostgres(t)\n\t\tt.Cleanup(cleanup)\n\t\tpostgres := storetest.AdmitPostgresRuntimeStore(t, postgresDB)\n\t\tselected, db = postgres, postgresDB", "\t\tselected = storetest.StartPostgresRuntimeStore(t)", 1)
	return strings.Replace(source, "store: selected, db: db, dialect: backend,", "store: selected,", 1)
}

func TestNativeCompleteDispatchConstructionRetainsAllOriginAndContinuationSetup(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var row recipe
	matched := 0
	for _, candidate := range rows {
		if candidate.Function == "newCompleteEventDispatchFixtureWithOrigin" {
			row = candidate
			matched++
		}
	}
	if matched != 1 {
		t.Fatalf("shared dispatch constructor recipes=%d, want1", matched)
	}
	row = historicalMechanicalRecipe(t, row)
	before := strings.Replace(row.Before, "seedCompleteEventDispatchRunWithOrigin(t, ctx, db, backend, runID, createdAt, origin)", "seedCompleteEventDispatchRunWithOrigin(t, ctx, selected, runID, createdAt, origin)", 1)
	want, err := canonicalFunction(nativeCompleteDispatchConstruction(before))
	got, actualErr := canonicalFunction(row.After)
	if err != nil || actualErr != nil || want != got || strings.Contains(row.After, "DatabaseForTest") {
		t.Fatal("shared constructor changed origin, clock, route, standing owner or decision posture")
	}
	for _, replacement := range []string{"storetest.StartSQLiteRuntimeStore(t)", "storetest.StartPostgresRuntimeStore(otherTest)", "storetest.AdmitPostgresRuntimeStore(t, postgresDB)"} {
		mutant := strings.Replace(row.After, "storetest.StartPostgresRuntimeStore(t)", replacement, 1)
		value, err := canonicalFunction(mutant)
		if mutant == row.After || (err == nil && value == want) {
			t.Fatal("foreign construction admitted")
		}
	}
}
