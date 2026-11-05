package tools

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/llm"
)

const flowDataReviewedUsage = "Read only data admitted for this actor and run. Use static_file with a delivered static_id, resource_row with one structured declaration and its key or position, or resource_rows with one structured declaration and a bounded page. Resource reads use the run-pinned version; continue only with the returned cursor. Do not provide filenames or host paths, select latest versions, or use this tool for mutable artifacts."

func TestFlowDataUsageMatchesDeliveredSchema(t *testing.T) {
	for _, kind := range []string{"static", "resource"} {
		t.Run(kind, func(t *testing.T) {
			source, _ := loadResourceDataToolSource(t)
			if kind == "static" {
				source, _ = loadFlowDataToolSource(t)
			}
			actor := flowDataActorWithIdentity(t, source, "usage")
			executor := NewExecutorWithOptions(nil, ExecutorOptions{WorkflowSource: source})
			var definition llm.ToolDefinition
			for _, item := range executor.ToolDefinitionsForActor(actor) {
				if item.Name == "read_flow_data" {
					definition = item
				}
			}
			if definition.Name == "" || definition.Usage != flowDataReviewedUsage {
				t.Fatalf("delivered definition %+v", definition)
			}
			if delivered := llm.DescriptionWithUsage(definition.Description, definition.Usage); !strings.HasSuffix(delivered, "\n\nUsage:\n"+flowDataReviewedUsage) {
				t.Fatalf("delivered description %q", delivered)
			}
			prior := definition
			prior.Usage = "Read only declared deploy-time reference files from your owning flow data root. Provide one filename from the delivered enum; do not use host paths or this tool for mutable artifacts."
			if llm.ToolDefinitionIdentity(definition) == llm.ToolDefinitionIdentity(prior) {
				t.Fatal("changed guidance retained old definition identity")
			}
		})
	}
}
