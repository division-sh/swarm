package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeBusSourceHashPreservesEveryPublicationAndIdentityAssertion(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-source-hash" {
			continue
		}
		matched++
		before := row.Before
		for _, pair := range [][2]string{
			{"_, db, _ := testutil.StartPostgres(t)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)"},
			{"\tvar bundleHash string\n\tif err := db.QueryRowContext(context.Background(), `\n\t\tSELECT bundle_hash\n\t\tFROM runs\n\t\tWHERE run_id = $1::uuid\n\t`, runID).Scan(&bundleHash); err != nil {", "\tbundleHash, err := storetest.ReadSelectedForkRunBundleHash(context.Background(), pg, runID)\n\tif err != nil {"},
			{"runlifecyclefixture.RequirePostgres(t, context.Background(), db, runlifecyclefixture.Fixture{\n\t\tOrigin: runlifecyclefixture.EventOrigin(t, eventID, string(eventType)),", "storetest.RequireRun(t, context.Background(), pg, storetest.RunFixture{\n\t\tOrigin: storetest.EventOrigin(t, eventID, string(eventType)),"},
		} {
			before = strings.Replace(before, pair[0], pair[1], 1)
		}
		if before != row.After {
			t.Fatal("source admission, exact hash, direct target, lifecycle seed or publish assertion changed")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("source fixture diverged from finite migration")
		}
		owner := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_selected_fork_storage.go", "ReadSelectedForkRunBundleHashForTest")
		beforeQueries := eventDeliveryDiagnosticSQL(t, row.Before)
		afterQueries := eventDeliveryDiagnosticSQL(t, owner)
		if len(beforeQueries) != 1 || len(afterQueries) != 1 || strings.ReplaceAll(beforeQueries[0], "$1::uuid", "$1") != afterQueries[0] {
			t.Fatal("source observation substituted semantic eligibility for exact physical hash")
		}
	}
	if matched != 2 {
		t.Fatalf("source-hash roots=%d,want2", matched)
	}
}
