package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

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
