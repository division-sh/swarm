package tools

import (
	"reflect"
	"testing"
)

func TestOrderedEntityFieldNamesFromInput_NormalizesSortsAndDedupes(t *testing.T) {
	got := orderedEntityFieldNamesFromInput([]string{" status ", "", "score", "status", "score", "priority"})
	want := []string{"priority", "score", "status"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("orderedEntityFieldNamesFromInput() = %#v, want %#v", got, want)
	}
}

func TestEntityStateBaseQuery_OptionallyIncludesFlowInstance(t *testing.T) {
	payload := map[string]any{
		"subject_id":    "subject-1",
		"entity_type":   "account",
		"flow_instance": " review/inst-1 ",
	}

	whereWithoutFlow, argsWithoutFlow := entityStateBaseQuery(payload, false)
	if whereWithoutFlow != " WHERE subject_id = $1::uuid AND entity_type = $2" {
		t.Fatalf("where without flow = %q", whereWithoutFlow)
	}
	if !reflect.DeepEqual(argsWithoutFlow, []any{"subject-1", "account"}) {
		t.Fatalf("args without flow = %#v", argsWithoutFlow)
	}

	whereWithFlow, argsWithFlow := entityStateBaseQuery(payload, true)
	if whereWithFlow != " WHERE subject_id = $1::uuid AND entity_type = $2 AND flow_instance = $3" {
		t.Fatalf("where with flow = %q", whereWithFlow)
	}
	if !reflect.DeepEqual(argsWithFlow, []any{"subject-1", "account", "review/inst-1"}) {
		t.Fatalf("args with flow = %#v", argsWithFlow)
	}
}
