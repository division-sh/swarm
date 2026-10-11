package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeBusRefusalConstructionPreservesAllFiveCompleteWorkloads(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-refusal-duplicate-construction" {
			continue
		}
		matched++
		before := row.Before
		for _, pair := range [][2]string{
			{"_, db, _ := testutil.StartPostgres(t)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)"},
			{"_, db, _ := testutil.StartPostgres(t)\n\t\t\t\tselected = storetest.AdmitPostgresRuntimeStore(t, db)", "selected = storetest.StartPostgresRuntimeStore(t)"},
			{"_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\tctx := testAuthorActivityContext(context.Background())\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "pg := storetest.StartPostgresRuntimeStore(t)\n\tctx := testAuthorActivityContext(context.Background())"},
		} {
			before = strings.Replace(before, pair[0], pair[1], 1)
		}
		if before != row.After {
			t.Fatal("terminal/diagnostic refusal, duplicate no-op, zero-route or fork rollback assertion changed")
		}
		if selectedCausalObservationBody(t, row.File, row.Function) != row.After {
			t.Fatal("native refusal fixture diverged from finite migration")
		}
	}
	if matched != 2 {
		t.Fatalf("native refusal/duplicate recipes=%d,want2 plus three original root updates", matched)
	}
}
