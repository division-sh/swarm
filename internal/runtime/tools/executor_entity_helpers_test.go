package tools

import (
	"reflect"
	"testing"

	models "swarm/internal/runtime/core/actors"
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
		"flow_instance": " review/inst-1 ",
	}

	clausesWithoutFlow, argsWithoutFlow := entityStateBaseQuery(nil, models.AgentConfig{}, payload, false)
	whereWithoutFlow := joinEntityStateWhere(clausesWithoutFlow)
	if whereWithoutFlow != " WHERE subject_id = $1::uuid" {
		t.Fatalf("where without flow = %q", whereWithoutFlow)
	}
	if !reflect.DeepEqual(argsWithoutFlow, []any{"subject-1"}) {
		t.Fatalf("args without flow = %#v", argsWithoutFlow)
	}

	clausesWithFlow, argsWithFlow := entityStateBaseQuery(nil, models.AgentConfig{}, payload, true)
	whereWithFlow := joinEntityStateWhere(clausesWithFlow)
	if whereWithFlow != " WHERE subject_id = $1::uuid AND flow_instance = $2" {
		t.Fatalf("where with flow = %q", whereWithFlow)
	}
	if !reflect.DeepEqual(argsWithFlow, []any{"subject-1", "review/inst-1"}) {
		t.Fatalf("args with flow = %#v", argsWithFlow)
	}
}
