package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeCLIScopeMailboxConstructionPreservesWholeReadProof(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-cli-scope-mailbox-construction" {
			continue
		}
		matched++
		before := strings.Replace(row.Before, "\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\tselected = storetest.AdmitPostgresRuntimeStore(t, db)", "\t\t\t\tselected = storetest.StartPostgresRuntimeStore(t)", 1)
		before = strings.Replace(before, "\t\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\t\ts := storetest.AdmitPostgresRuntimeStore(t, db)", "\t\t\t\t\ts := storetest.StartPostgresRuntimeStore(t)", 1)
		want, err := canonicalFunction(before)
		got, afterErr := canonicalFunction(row.After)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || sourceErr != nil || want != got || value != want || strings.Contains(actual, "testutil.StartPostgres") || strings.Contains(actual, "AdmitPostgresRuntimeStore") {
			t.Fatalf("%s changed scope/mailbox proof outside native construction", row.Function)
		}
		for _, cut := range []string{"storetest.StartPostgresRuntimeStore(t)", "storetest.RequireRun(t, ctx, s,", "selected = s", "t.Fatal(", "t.Fatalf(", "if len(", "target.CardID"} {
			mutant := strings.Replace(row.After, cut, "unreviewedCLICut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("scope/card/owner/assertion mutation accepted: %s", cut)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("CLI native construction recipes=%d,want2", matched)
	}
}
