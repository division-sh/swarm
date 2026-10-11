package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeStartupActorConstructionPreservesAttachmentMemoryAndRestartProofs(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-startup-actor-construction" {
			continue
		}
		matched++
		before := strings.Replace(row.Before, "\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\tselected = storetest.AdmitPostgresRuntimeStore(t, db)", "\t\t\t\tselected = storetest.StartPostgresRuntimeStore(t)", 1)
		before = strings.Replace(before, "\t\t\t\t\t_, db, _ := testutil.StartPostgres(t)\n\t\t\t\t\tselected = storetest.AdmitPostgresRuntimeStore(t, db)", "\t\t\t\t\tselected = storetest.StartPostgresRuntimeStore(t)", 1)
		want, err := canonicalFunction(before)
		got, actualErr := canonicalFunction(row.After)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || actualErr != nil || sourceErr != nil || want != got || got != actual {
			t.Fatalf("startup source/admission/runtime/attachment/cleanup/assertions changed: %s", row.Function)
		}
		for _, replacement := range []string{"storetest.StartSQLiteRuntimeStore(t)", "storetest.StartPostgresRuntimeStore(otherTest)", "storetest.AdmitPostgresRuntimeStore(t, db)"} {
			mutant := strings.Replace(row.After, "storetest.StartPostgresRuntimeStore(t)", replacement, 1)
			value, err := canonicalFunction(mutant)
			if mutant == row.After || (err == nil && value == want) {
				t.Fatalf("foreign/reconstructed startup construction admitted: %s", replacement)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("startup construction recipes=%d,want2", matched)
	}
}
