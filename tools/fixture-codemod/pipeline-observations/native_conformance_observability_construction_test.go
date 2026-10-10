package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeConformanceObservabilityConstructionPreservesRuntimeAndPublicFacts(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-conformance-observability-construction" {
			continue
		}
		matched++
		before := strings.Replace(row.Before, "\t_, db, _ := testutil.StartPostgres(t)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
		before = strings.Replace(before, "\t_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
		want, err := canonicalFunction(before)
		got, actualErr := canonicalFunction(row.After)
		actual, sourceErr := canonicalFunction(selectedCausalObservationBody(t, row.File, row.Function))
		if err != nil || actualErr != nil || sourceErr != nil || want != got || got != actual {
			t.Fatalf("observability source/run/runtime/admission/logging/readback/assertions changed: %s", row.Function)
		}
		for _, replacement := range []string{"storetest.StartSQLiteRuntimeStore(t)", "storetest.StartPostgresRuntimeStore(otherTest)", "storetest.AdmitPostgresRuntimeStore(t, db)"} {
			mutant := strings.Replace(row.After, "storetest.StartPostgresRuntimeStore(t)", replacement, 1)
			value, err := canonicalFunction(mutant)
			if mutant == row.After || (err == nil && value == want) {
				t.Fatal("foreign/reconstructed observability constructor admitted")
			}
		}
	}
	if matched != 3 {
		t.Fatalf("observability construction recipes=%d,want3", matched)
	}
}
