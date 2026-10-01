package tools

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"gopkg.in/yaml.v3"
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

func TestFlowDataToolSpecMatchesStructuredArms(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(pipeline.WorkflowRepoRoot(), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var spec map[string]any
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatal(err)
	}
	get := func(path ...string) any {
		var value any = spec
		for _, key := range path {
			value = value.(map[string]any)[key]
		}
		return value
	}
	generated := get("engine", "agent_session_management", "flow_data_access", "generated_tool").(map[string]any)
	inputs := generated["input"].(map[string]any)
	if len(inputs) != 3 || inputs["static_file"] == nil || inputs["resource_row"] == nil || inputs["resource_rows"] == nil {
		t.Fatalf("tool arms %+v", inputs)
	}
	if strings.Contains(generated["generated_for"].(string), "Only agents") {
		t.Fatal("spec still excludes resource-only grants")
	}
	output := generated["output"].(map[string]any)
	if output["size_bytes"] != nil || inputs["filename"] != nil {
		t.Fatal("retired tool-wire fields remain authoritative")
	}
	builtin := get("tool_model", "platform_builtin_tools", "tools", "read_flow_data").(string)
	for _, name := range []string{"static_file", "resource_row", "resource_rows", "data_access"} {
		if !strings.Contains(builtin, name) {
			t.Fatalf("builtin omits %s: %s", name, builtin)
		}
	}
}
