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
		if err != nil || afterErr != nil || sourceErr != nil || want != got || value != got {
			t.Fatalf("continuation workload drift: %s", row.Function)
		}
		for _, raw := range []string{"testutil.StartPostgres", "AdmitPostgresRuntimeStore", "storetest.Database", "*sql.DB"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("continuation retains raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStore(t)", "i < 200", "create(-1, \"target\")", "target.CardContentHash", "returned 2 items", "selected, owner = s", "process.ActiveCount() != baseline", "capability.Release(context.Background())", "successor.ProveCurrent(ctx)", "a.AuthorityGeneration++", "a.AcquisitionID", "context.Canceled", "t.Fatalf(", "t.Fatal("} {
			mutant := strings.Replace(row.After, cut, "unreviewedContinuationCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("continuation identity/workload/assertion mutation accepted: %s", cut)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("continuation recipes=%d,want2", matched)
	}
}
