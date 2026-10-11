package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeExecutionConstructionSource(source string) string {
	selected := "pg"
	if strings.Contains(source, "func TestSelectedContractAgentRuntimeBuildsCanonicalMockAdapter(") {
		selected = "selected"
	}
	source = strings.Replace(source, "\t_, db, cleanup := testutil.StartPostgres(t)", "\t"+selected+" := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "\t_, db, _ := testutil.StartPostgres(t)", "\t"+selected+" := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "\tt.Cleanup(cleanup)\n", "", 1)
	return strings.Replace(source, "\t"+selected+" := storetest.AdmitPostgresRuntimeStore(t, db)\n", "", 1)
}

func TestNativeExecutionConstructionPreservesWholeWorkloadAndOriginalOpening(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-execution-construction-tail" {
			continue
		}
		matched++
		if nativeExecutionConstructionSource(row.Before) != row.After {
			t.Fatal("native execution construction changed the workload or cleanup ordering")
		}
		assertNativeExecutionConstruction(t, row)
	}
	if matched != 2 {
		t.Fatalf("execution constructor recipes=%d,want2", matched)
	}
}

func assertNativeExecutionConstruction(t *testing.T, row recipe) {
	t.Helper()
	actual := selectedCausalObservationBody(t, row.File, row.Function)
	want, err := canonicalFunction(row.After)
	got, actualErr := canonicalFunction(actual)
	if err != nil || actualErr != nil || want != got || strings.Contains(actual, "testutil.StartPostgres") || strings.Contains(actual, "AdmitPostgresRuntimeStore") || strings.Count(actual, "storetest.StartPostgresRuntimeStore(t)") != 1 {
		t.Fatal("execution fixture regained raw or duplicate construction")
	}
	for _, replacement := range []string{"storetest.StartSQLiteRuntimeStore(t)", "storetest.StartPostgresRuntimeStore(otherTest)", "storetest.AdmitPostgresRuntimeStore(t, db)"} {
		mutant := strings.Replace(actual, "storetest.StartPostgresRuntimeStore(t)", replacement, 1)
		value, mutantErr := canonicalFunction(mutant)
		if mutant == actual || (mutantErr == nil && value == want) {
			t.Fatal("execution construction oracle accepted foreign authority")
		}
	}
}
