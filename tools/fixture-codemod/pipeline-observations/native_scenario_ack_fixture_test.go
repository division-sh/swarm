package main

import (
	"encoding/json"
	"fmt"
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

func q10ScenarioAckCurrentSource(row recipe) (string, error) {
	if row.File != "internal/apiv1/operator_test_setup_ack_test.go" || row.Family != "native-scenario-ack-fixture" || row.Successor != "" || row.Removed {
		return "", fmt.Errorf("unreviewed scenario acknowledgment endpoint")
	}
	type exactEdit struct {
		count         int
		before, after string
	}
	var edits []exactEdit
	switch row.Function {
	case "assertScenarioSetupAckRows":
		edits = []exactEdit{
			{1, `{"entity mutations", observed.SetupMutations, 3}`, `{"entity mutations", observed.SetupMutations, 1}`},
			{1, "\tvar completed testSetupEntitiesResult\n", `	var completed testSetupEntitiesResult
	projection, err := storetest.ReadTrackedEntityMutationProjectionStorage(context.Background(), selected, runID, entityID)
	if err != nil || len(projection.Mutations) != 1 {
		t.Fatalf("setup authored mutation projection = %+v, err=%v", projection, err)
	}
	mutation := projection.Mutations[0]
	if mutation.Domain != "authored_field" || mutation.Path != "note" || mutation.WriterType != "platform" || mutation.WriterID != "test.setup_entities" {
		t.Fatalf("setup mutation = %+v, want one platform/test.setup_entities authored note", mutation)
	}
`},
			{1, "completed.Entities[0].EntityID != entityID {", "completed.Entities[0].EntityID != entityID || completed.Entities[0].CurrentState != nil {"},
			{1, "\t\tt.Fatalf(\"stored idempotency completion = %#v, want committed run/entity\", completed)\n\t}\n", `		t.Fatalf("stored idempotency completion = %#v, want committed run/entity", completed)
	}
	var wire struct {
		Entities []map[string]json.RawMessage ` + "`json:\"entities\"`" + `
	}
	if err := json.Unmarshal(observed.Response, &wire); err != nil {
		t.Fatalf("decode idempotency completion wire shape: %v", err)
	}
	if len(wire.Entities) != 1 || string(wire.Entities[0]["current_state"]) != "null" {
		t.Fatalf("stored idempotency completion must retain explicit null current_state: %s", observed.Response)
	}
`},
		}
	case "TestOperatorTestSetupAcknowledgedCleanupErrorCompletesIdempotencyBothStores":
		edits = []exactEdit{
			{1, `validTestSetupEntity(entityID, "waiting", "seeded", true)`, `validTestSetupEntity(entityID, "seeded")`},
			{1, "committed.Entities[0].EntityID != entityID {", "committed.Entities[0].EntityID != entityID || committed.Entities[0].CurrentState != nil {"},
			{2, "\t\t\tassertScenarioSetupAckRows(t, selected, runID, entityID)\n", "\t\t\tassertScenarioSetupAckRows(t, selected, runID, entityID)\n\t\t\tassertTestSetupImportedEntity(t, selected, runID, entityID, \"seeded\")\n"},
		}
	default:
		return "", fmt.Errorf("unreviewed scenario acknowledgment function %s", row.Function)
	}
	current := row.After
	for _, edit := range edits {
		if strings.Count(current, edit.before) != edit.count {
			return "", fmt.Errorf("scenario acknowledgment source lost exact Q10 binding %q", edit.before)
		}
		current = strings.ReplaceAll(current, edit.before, edit.after)
	}
	return current, nil
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
		current, currentErr := q10ScenarioAckCurrentSource(row)
		currentValue, currentParseErr := canonicalFunction(current)
		actual := selectedCausalObservationBody(t, row.File, row.Function)
		value, sourceErr := canonicalFunction(actual)
		if err != nil || afterErr != nil || currentErr != nil || currentParseErr != nil || sourceErr != nil || want != got || value != currentValue {
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

func TestQ10ScenarioAckRetainsExactImportMutationAndReplayEvidence(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-scenario-ack-fixture" {
			continue
		}
		matched++
		current, err := q10ScenarioAckCurrentSource(row)
		if err != nil {
			t.Fatal(err)
		}
		guard := row
		guard.Before, guard.After = current, current
		mutations := []string{
			"observed.Runs, 1", "observed.Entities, 1", "observed.SetupMutations, 1", "observed.Completions, 1",
			"len(projection.Mutations) != 1", `mutation.Domain != "authored_field"`, `mutation.Path != "note"`,
			`mutation.WriterType != "platform"`, `mutation.WriterID != "test.setup_entities"`,
			"json.Unmarshal(observed.Response, &completed)", "completed.RunID != runID", "len(completed.Entities) != 1",
			"completed.Entities[0].EntityID != entityID", "completed.Entities[0].CurrentState != nil",
			"json.Unmarshal(observed.Response, &wire)", "len(wire.Entities) != 1", `string(wire.Entities[0]["current_state"]) != "null"`,
			"errors.Is(err, fault)", "committed.Entities[0].CurrentState != nil", "got.Entities[0] != committed.Entities[0]",
			"assertTestSetupImportedEntity(t, selected, runID, entityID, \"seeded\")", "setup.calls != 1",
		}
		exercised := 0
		for i, cut := range mutations {
			if !strings.Contains(current, cut) {
				continue
			}
			exercised++
			t.Run(fmt.Sprintf("%s/%d", row.Function, i), func(t *testing.T) {
				for occurrence := 0; occurrence < strings.Count(current, cut); occurrence++ {
					start := 0
					for n := 0; n < occurrence; n++ {
						start += strings.Index(current[start:], cut) + len(cut)
					}
					mutant := current[:start] + strings.Replace(current[start:], cut, "unreviewedAckCut", 1)
					if _, changed, err := rewriteFunction(guard.File, []byte("package proof\n"+mutant), guard); err == nil || changed {
						t.Fatalf("changed acknowledgment evidence admitted: %s occurrence%d", cut, occurrence)
					}
				}
			})
		}
		want := 17
		if row.Function != "assertScenarioSetupAckRows" {
			want = 5
		}
		if exercised != want {
			t.Fatalf("exact acknowledgment controls=%d,want%d", exercised, want)
		}
		if _, changed, err := rewriteFunction(guard.File, []byte("package proof\n"+row.After), guard); err == nil || changed {
			t.Fatal("historical lifecycle-bearing endpoint admitted as current field-only import")
		}
		for _, cut := range []string{"assertTestSetupImportedEntity", "q10ScenarioAckCurrentSource"} {
			unknown := row
			unknown.Function = cut
			if _, err := q10ScenarioAckCurrentSource(unknown); err == nil {
				t.Fatal("unreviewed acknowledgment endpoint admitted")
			}
		}
	}
	if matched != 2 {
		t.Fatalf("exact acknowledgment endpoints=%d,want2", matched)
	}
}
