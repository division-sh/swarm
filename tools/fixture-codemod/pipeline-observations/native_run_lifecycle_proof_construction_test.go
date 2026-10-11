package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeRunLifecycleProofConstruction(source string, resultType string) string {
	old := "func(t *testing.T) " + resultType + " {\n\t\t\t_, db, _ := testutil.StartPostgres(t)\n\t\t\treturn AdmitPostgresRuntimeStore(t, db)\n\t\t}"
	fresh := "func(t *testing.T) " + resultType + " { return StartPostgresRuntimeStore(t) }"
	return strings.Replace(source, old, fresh, 1)
}

func TestNativeRunLifecycleProofConstructionPreservesBothStoreConsumers(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-run-lifecycle-proof-construction" {
			continue
		}
		matched++
		resultType := "runFixtureProofStore"
		if row.Function == "TestSourceRevisionFixtureUsesExactSelectedOwner" {
			resultType = "sourceRevisionProofStore"
		}
		want, err := canonicalFunction(nativeRunLifecycleProofConstruction(row.Before, resultType))
		got, actualErr := canonicalFunction(row.After)
		if err != nil || actualErr != nil || want != got || strings.Contains(row.After, "AdmitPostgresRuntimeStore") {
			t.Fatalf("lifecycle proof changed beyond native construction: %s", row.Function)
		}
		for _, replacement := range []string{"StartSQLiteRuntimeStore(t)", "StartPostgresRuntimeStore(otherTest)", "AdmitPostgresRuntimeStore(t, db)"} {
			mutant := strings.Replace(row.After, "StartPostgresRuntimeStore(t)", replacement, 1)
			value, err := canonicalFunction(mutant)
			if mutant == row.After || (err == nil && value == want) {
				t.Fatalf("foreign constructor passed preservation: %s", replacement)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("native lifecycle construction recipes=%d, want 2", matched)
	}
}
