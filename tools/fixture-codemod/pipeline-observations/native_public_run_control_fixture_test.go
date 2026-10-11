package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func nativePublicRunControlSource(source string) string {
	source = strings.Replace(source, "\t_, db, cleanup := testutil.StartPostgres(t)\n\tt.Cleanup(cleanup)\n\tpg := storetest.AdmitPostgresRuntimeStore(t, db)", "\tpg := storetest.StartPostgresRuntimeStore(t)", 1)
	source = strings.Replace(source, "runlifecyclefixture.RequirePostgres(t, ctx, db, runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin()", "storetest.RequireRun(t, ctx, pg, storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin()", 1)
	return strings.ReplaceAll(source, "assertRunControlState(t, db,", "assertRunControlState(t, pg,")
}

func TestNativePublicRunControlKeepsPhysicalStatusAndReplayProof(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Function != "TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency" && row.Family != "native-public-run-control-fixture" {
			continue
		}
		matched++
		before := nativePublicRunControlSource(row.Before)
		if row.Function == "TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency" {
			before = nativeAPIEventConsumerWorkload(t, before)
		} else {
			before = strings.Replace(before, "db *sql.DB", "selected any", 1)
			before = strings.Replace(before, "\tvar runStatus, controlStatus string\n\tif err := db.QueryRowContext(context.Background(), `\n\t\tSELECT r.status, COALESCE(rc.control_status, '')\n\t\tFROM runs r\n\t\tLEFT JOIN run_control_state rc ON rc.run_id = r.run_id\n\t\tWHERE r.run_id = $1::uuid\n\t`, runID).Scan(&runStatus, &controlStatus); err != nil {\n\t\tt.Fatalf(\"load run control state: %v\", err)\n\t}\n", "\tobserved, err := storetest.ReadRunStopStorage(context.Background(), selected, runID)\n\tif err != nil {\n\t\tt.Fatalf(\"load run control state: %v\", err)\n\t}\n\trunStatus, controlStatus := observed.Status, observed.Control\n", 1)
			physical := selectedCausalObservationBody(t, "internal/store/internal/backend/runlifecycle/standalone_fixture_storage.go", "ReadStopRunControlStorageTx")
			normalize := func(source string) []string {
				queries := eventDeliveryDiagnosticSQL(t, strings.ReplaceAll(source, "SELECT\n", "SELECT \n"))
				for i, query := range queries {
					query = strings.Join(strings.Fields(query), "")
					query = strings.ReplaceAll(query, "rc.", "c.")
					query = strings.ReplaceAll(query, "state rc", "state c")
					query = strings.ReplaceAll(query, "run_control_staterc", "run_control_statec")
					queries[i] = strings.ReplaceAll(query, "$1::uuid", "$1")
				}
				return queries
			}
			if !reflect.DeepEqual(normalize(row.Before), normalize(physical)) {
				t.Fatal("run/control physical status/default/predicate drift")
			}
		}
		want, err := canonicalFunction(before)
		got, afterErr := canonicalFunction(row.After)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		if row.Function == "TestOperatorRunControlHandlersUseCanonicalOwnerAndIdempotency" {
			got, afterErr = canonicalFunction(nativeAPIEventConsumerWorkload(t, row.After))
		}
		value, sourceErr := canonicalFunction(actual)
		actualWant, actualWantErr := canonicalFunction(row.After)
		if err != nil || afterErr != nil || sourceErr != nil || actualWantErr != nil || want != got || value != actualWant {
			t.Fatalf("run control fixture drift: %s", row.Function)
		}
		for _, raw := range []string{"*sql.DB", "db.Query", "storetest.Database", "testutil.StartPostgres", "AdmitPostgresRuntimeStore", "runlifecyclefixture."} {
			if strings.Contains(actual, raw) {
				t.Fatalf("run control retains raw authority: %s", raw)
			}
		}
		for _, cut := range []string{"storetest.StartPostgresRuntimeStore(t)", "storetest.RequireRun(t, ctx, pg", "assertRunControlState(t, pg", "observed.Status, observed.Control", "runStatus != wantRunStatus", "controlStatus != wantControlStatus", "t.Fatalf("} {
			mutant := strings.Replace(row.After, cut, "unreviewedControlCut", 1)
			if mutant == row.After {
				continue
			}
			changed, mutantErr := canonicalFunction(mutant)
			if mutantErr == nil && changed == want {
				t.Fatalf("run control mutation accepted: %s", cut)
			}
		}
	}
	if matched != 2 {
		t.Fatalf("run control recipes=%d,want2", matched)
	}
}
