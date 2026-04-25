package tools

import (
	"reflect"
	"strings"
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

func TestExistingEntityFlowInstanceSchemaDocumentsRootSemantics(t *testing.T) {
	entries := genericEntityRuntimeContractSchemas(entityReadTargetInputSchema(nil))
	for _, toolName := range []string{"get_entity", "save_entity_field", "query_entities", "query_metrics", "search_entities"} {
		properties := entries[toolName].InputSchema["properties"].(map[string]any)
		flowSchema := properties["flow_instance"].(map[string]any)
		description := strings.TrimSpace(flowSchema["description"].(string))
		if !strings.Contains(description, "concrete flow instance path") || !strings.Contains(description, "declared semantic flow root") || !strings.Contains(description, "descendant") {
			t.Fatalf("%s flow_instance description = %q, want concrete path and semantic-root descendant guidance", toolName, description)
		}
	}
	createProperties := entries["create_entity"].InputSchema["properties"].(map[string]any)
	createFlowSchema := createProperties["flow_instance"].(map[string]any)
	if _, ok := createFlowSchema["description"]; ok {
		t.Fatalf("create_entity flow_instance gained existing-entity guidance: %#v", createFlowSchema)
	}
}
