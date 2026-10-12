package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeSelectedContinuationConstructionRetainsEveryWorkload(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-selected-continuation-construction" {
			continue
		}
		matched++
		before := strings.Replace(row.Before, "\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\ts := storetest.AdmitPostgresRuntimeStore(t, db)", "\t\t\t\ts := storetest.StartPostgresRuntimeStore(t)", 1)
		before = strings.Replace(before, "\t\t\t\t_, db, _ := testutil.StartPostgres(t)\n\t\t\t\ts := storetest.AdmitPostgresRuntimeStore(t, db)", "\t\t\t\ts := storetest.StartPostgresRuntimeStore(t)", 1)
		want, err := canonicalFunction(before)
		got, afterErr := canonicalFunction(row.After)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		currentWant := before
		if row.Function == "TestSelectedForkProcessAdmissionBothStores" {
			old := `{"acquisition", func(a *startupownership.Authority) { a.AcquisitionID = uuid.NewString() }}`
			next := `{"lease_and_generation", func(a *startupownership.Authority) { a.AuthorityID = uuid.NewString(); a.AuthorityGeneration++ }}`
			if row.Successor == "" || strings.Count(before, old) != 1 {
				t.Fatal("exact process-authority successor is missing or ambiguous")
			}
			currentWant = strings.Replace(before, old, next, 1)
		}
		current, checkpointErr := checkpointRecipes([]byte("package proof\n"+actual), []recipe{row})
		if checkpointErr != nil || len(current) != 1 {
			t.Fatalf("continuation checkpoint is missing: %s/%v", row.Function, checkpointErr)
		}
		currentPin, pinErr := canonicalFunction(current[0].After)
		currentExpected, expectedErr := canonicalFunction(currentWant)
		if err != nil || afterErr != nil || sourceErr != nil || pinErr != nil || expectedErr != nil || want != got || currentPin != currentExpected || value != currentExpected {
			t.Fatalf("continuation workload drift: %s", row.Function)
		}
		for _, raw := range []string{"testutil.StartPostgres", "AdmitPostgresRuntimeStore", "storetest.Database", "*sql.DB"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("continuation retains raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStore(t)", "i < 200", "create(-1, \"target\")", "target.CardContentHash", "returned 2 items", "selected, owner = s", "process.ActiveCount() != baseline", "capability.Release(context.Background())", "successor.ProveCurrent(ctx)", "a.AuthorityGeneration++", "a.AuthorityID", "lease_and_generation", "a.AcquisitionID", "context.Canceled", "t.Fatalf(", "t.Fatal("} {
			mutant := strings.Replace(current[0].After, cut, "unreviewedContinuationCut", 1)
			if mutant == current[0].After {
				continue
			}
			if _, _, mutantErr := rewriteFunction(row.File, []byte("package proof\n"+mutant), current[0]); mutantErr == nil {
				t.Fatalf("continuation identity/workload/assertion mutation accepted: %s", cut)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("continuation recipes=%d,want2", matched)
	}
}
