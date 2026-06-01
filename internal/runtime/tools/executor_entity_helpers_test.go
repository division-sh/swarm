package tools

import (
	"context"
	"reflect"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
)

func TestOrderedEntityFieldNamesFromInput_NormalizesSortsAndDedupes(t *testing.T) {
	got := orderedEntityFieldNamesFromInput([]string{" status ", "", "score", "status", "score", "priority"})
	want := []string{"priority", "score", "status"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("orderedEntityFieldNamesFromInput() = %#v, want %#v", got, want)
	}
}

func TestEntityStateQuery_OptionallyIncludesFlowInstance(t *testing.T) {
	t.Parallel()

	payload := map[string]any{
		"flow_instance": " review/inst-1 ",
	}
	ctx := runtimecorrelation.WithRunID(context.Background(), "00000000-0000-0000-0000-000000000001")

	queryWithoutFlow, err := entityStateQueryForContractRun(ctx, nil, entityruntime.Contract{}, payload, false)
	if err != nil {
		t.Fatalf("query without flow: %v", err)
	}
	if queryWithoutFlow.RunID != "00000000-0000-0000-0000-000000000001" {
		t.Fatalf("run_id without flow = %q", queryWithoutFlow.RunID)
	}
	if queryWithoutFlow.RequestedFlowExact != "" || queryWithoutFlow.RequestedFlowScope.Root != "" {
		t.Fatalf("query without flow should not include requested flow: %#v", queryWithoutFlow)
	}

	queryWithFlow, err := entityStateQueryForContractRun(ctx, nil, entityruntime.Contract{}, payload, true)
	if err != nil {
		t.Fatalf("query with flow: %v", err)
	}
	if queryWithFlow.RequestedFlowExact != "review/inst-1" {
		t.Fatalf("requested flow exact = %q", queryWithFlow.RequestedFlowExact)
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
	if _, ok := createProperties["subject_id"]; ok {
		t.Fatalf("create_entity schema should not expose subject_id: %#v", createProperties)
	}
	searchProperties := entries["search_entities"].InputSchema["properties"].(map[string]any)
	if _, ok := searchProperties["subject_id"]; ok {
		t.Fatalf("search_entities schema should not expose subject_id: %#v", searchProperties)
	}
	createFlowSchema := createProperties["flow_instance"].(map[string]any)
	if _, ok := createFlowSchema["description"]; ok {
		t.Fatalf("create_entity flow_instance gained existing-entity guidance: %#v", createFlowSchema)
	}
}
