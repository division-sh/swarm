package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeDiagnosisPaginationSource(source string) string {
	for _, cut := range [][2]string{
		{"(agentDiagnosePaginationStore, *sql.DB, bool)", "agentDiagnosePaginationStore"},
		{"\t\t\t\tselected := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)\n\t\t\t\treturn selected, storetest.Database(selected), true", "\t\t\t\treturn storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)"},
		{"\t\t\t\t_, db, cleanup := testutil.StartPostgres(t)\n\t\t\t\tt.Cleanup(cleanup)\n\t\t\t\tselected := storetest.AdmitPostgresRuntimeStore(t, db)\n\t\t\t\treturn selected, db, false", "\t\t\t\treturn storetest.StartPostgresRuntimeStore(t)"},
		{"selected, db, sqlite := backend.open(t, ctx)", "selected := backend.open(t, ctx)"},
		{"\t\t\tif sqlite {\n\t\t\t\trunlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),\n\t\t\t\t\tRunID: runID, StartedAt: now.Add(-time.Minute),\n\t\t\t\t})\n\t\t\t} else {\n\t\t\t\trunlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(),\n\t\t\t\t\tRunID: runID, StartedAt: now.Add(-time.Minute),\n\t\t\t\t})\n\t\t\t}\n", "\t\t\tstoretest.RequireRun(t, ctx, selected, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(),\n\t\t\t\tRunID: runID, StartedAt: now.Add(-time.Minute),\n\t\t\t})\n"},
	} {
		source = strings.ReplaceAll(source, cut[0], cut[1])
	}
	return source
}

func TestNativeDiagnosisPaginationRetainsExactPagesAndIdentity(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-diagnosis-pagination" {
			continue
		}
		matched++
		want, err := canonicalFunction(nativeDiagnosisPaginationSource(row.Before))
		got, afterErr := canonicalFunction(row.After)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || sourceErr != nil || want != got || value != want {
			t.Fatal("diagnosis pagination changed outside native construction and lifecycle setup")
		}
		for _, raw := range []string{"*sql.DB", "storetest.Database", "testutil.StartPostgres", "AdmitPostgresRuntimeStore", "runlifecyclefixture."} {
			if strings.Contains(actual, raw) {
				t.Fatalf("diagnosis fixture retains raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"storetest.StartPostgresRuntimeStore(t)", "storetest.RequireRun(t, ctx, selected", "now.Add(-time.Minute)", "sort.Strings(wantDeliveryIDs)", "firstDeliveryID != wantDeliveryIDs[0]", "secondDeliveryID != wantDeliveryIDs[1]", "next != \"\"", "queue_cursor", "QueueLimit: 1", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedDiagnosisCut", 1)
			if mutant == row.After {
				t.Fatalf("diagnosis proof cut missing: %s", cut)
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("diagnosis mutation accepted: %s", cut)
			}
		}
	}
	if matched != 1 {
		t.Fatalf("diagnosis recipes=%d,want1", matched)
	}
}
