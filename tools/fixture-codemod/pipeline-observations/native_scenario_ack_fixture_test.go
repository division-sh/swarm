package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func nativeScenarioAckRoot(source string) string {
	for _, cut := range [][2]string{
		{"(scenarioSetupAckSelectedStore, *sql.DB)", "scenarioSetupAckSelectedStore"},
		{"\t\t\t\tselected := storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())\n\t\t\t\treturn selected, storetest.DatabaseForTest(selected)", "\t\t\t\treturn storetest.StartSQLiteRuntimeStoreWithContext(t, context.Background())"},
		{"\t\t\t\t_, db, _ := testutil.StartPostgres(t)\n\t\t\t\treturn storetest.AdmitPostgresRuntimeStore(t, db), db", "\t\t\t\treturn storetest.StartPostgresRuntimeStore(t)"},
		{"selected, db := backend.open(t)", "selected := backend.open(t)"},
		{"assertScenarioSetupAckRows(t, db,", "assertScenarioSetupAckRows(t, selected,"},
	} {
		source = strings.ReplaceAll(source, cut[0], cut[1])
	}
	return source
}

func TestNativeScenarioAckRetainsPhysicalCountsResponseAndReplay(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	adapter := selectedCausalObservationBody(t, "internal/store/internal/runtimepersistence/test_scenario_setup_ack_storage.go", "ReadScenarioSetupAckStorageForTest")
	compact := strings.Join(strings.Fields(adapter), "")
	compact = strings.ReplaceAll(compact, "CAST(run_idASTEXT)", "run_id")
	compact = strings.ReplaceAll(compact, "CAST(entity_idASTEXT)", "entity_id")
	for _, row := range rows {
		if row.Family != "native-scenario-ack-fixture" {
			continue
		}
		matched++
		before := nativeScenarioAckRoot(row.Before)
		if row.Function == "assertScenarioSetupAckRows" {
			before = strings.Replace(before, "db *sql.DB", "selected any", 1)
			before = strings.Replace(before, "\tfor _, check := range []struct {\n\t\tname  string\n\t\tquery string\n\t\targs  []any\n\t\twant  int\n\t}{\n\t\t{\"run\", `SELECT COUNT(*) FROM runs WHERE run_id = $1`, []any{runID}, 1},\n\t\t{\"entity\", `SELECT COUNT(*) FROM entity_state WHERE run_id = $1 AND entity_id = $2`, []any{runID, entityID}, 1},\n\t\t{\"entity mutations\", `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1 AND entity_id = $2 AND writer_id = 'test.setup_entities'`, []any{runID, entityID}, 3},\n\t\t{\"idempotency completion\", `SELECT COUNT(*) FROM api_idempotency WHERE resource_id = $1`, []any{runID}, 1},\n\t} {\n\t\tvar got int\n\t\tif err := db.QueryRow(check.query, check.args...).Scan(&got); err != nil {\n\t\t\tt.Fatalf(\"count %s: %v\", check.name, err)\n\t\t}\n\t\tif got != check.want {\n\t\t\tt.Fatalf(\"%s rows = %d, want %d\", check.name, got, check.want)\n\t\t}\n\t}\n\tvar response string\n\tif err := db.QueryRow(`SELECT response FROM api_idempotency WHERE resource_id = $1`, runID).Scan(&response); err != nil {\n\t\tt.Fatalf(\"load idempotency completion: %v\", err)\n\t}\n", "\tobserved, err := storetest.ReadScenarioSetupAckStorage(context.Background(), selected, runID, entityID)\n\tif err != nil {\n\t\tt.Fatalf(\"read exact scenario setup acknowledgment: %v\", err)\n\t}\n\tfor _, check := range []struct {\n\t\tname string\n\t\tgot  int\n\t\twant int\n\t}{\n\t\t{\"run\", observed.Runs, 1},\n\t\t{\"entity\", observed.Entities, 1},\n\t\t{\"entity mutations\", observed.SetupMutations, 3},\n\t\t{\"idempotency completion\", observed.Completions, 1},\n\t} {\n\t\tif check.got != check.want {\n\t\t\tt.Fatalf(\"%s rows = %d, want %d\", check.name, check.got, check.want)\n\t\t}\n\t}\n", 1)
			before = strings.Replace(before, "json.Unmarshal([]byte(response), &completed)", "json.Unmarshal(observed.Response, &completed)", 1)
			for _, query := range eventDeliveryDiagnosticSQL(t, row.Before) {
				if !strings.Contains(compact, strings.Join(strings.Fields(query), "")) {
					t.Fatalf("ack physical predicate drift: %s", query)
				}
			}
		}
		want, err := canonicalFunction(before)
		got, afterErr := canonicalFunction(row.After)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || sourceErr != nil || want != got || value != got {
			t.Fatalf("ack fixture changed outside native ownership: %s", row.Function)
		}
		for _, raw := range []string{"*sql.DB", "db.Query", "storetest.Database", "testutil.StartPostgres", "AdmitPostgresRuntimeStore"} {
			if strings.Contains(actual, raw) {
				t.Fatalf("ack fixture retains raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"storetest.StartPostgresRuntimeStore(t)", "assertScenarioSetupAckRows(t, selected", "setup.calls != 1", "got.Entities[0] != committed.Entities[0]", "observed.Runs, 1", "observed.Entities, 1", "observed.SetupMutations, 3", "observed.Completions, 1", "json.Unmarshal(observed.Response", "completed.Entities[0].EntityID != entityID", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedAckCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("ack count/replay/response mutation accepted: %s", cut)
			}
		}
	}
	for _, cut := range []string{"validateChannelObservationOwner(selected)", "validateSelectedForkStorageIdentity(id)", "readServedDeliveryObservation(ctx, selected,", "Scan(&out.Runs, &out.Entities, &out.SetupMutations, &out.Completions)", "Scan(&response)", "append(json.RawMessage(nil), response...)", "return ScenarioSetupAckStorage{}, err"} {
		if !strings.Contains(adapter, cut) {
			t.Fatalf("ack owner lost %s", cut)
		}
	}
	if matched != 2 {
		t.Fatalf("ack fixture recipes=%d,want2", matched)
	}
}
