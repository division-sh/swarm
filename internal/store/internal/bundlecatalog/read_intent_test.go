package bundlecatalog

import (
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
)

func TestProjectBundleCatalogAgentDefinitionRejectsImpossibleIntentArtifact(t *testing.T) {
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents.worker.intent",
		"Perform the assigned work.",
	)
	if err != nil {
		t.Fatal(err)
	}
	base := map[string]any{
		"agent_id":            "worker",
		"intent_kind":         string(intent.Kind),
		"intent_source":       intent.Coordinate,
		"intent_provenance":   intent.Provenance,
		"intent_content_hash": intent.ContentHash,
		"intent_identity":     intent.Identity,
		"intent_content":      intent.Content,
	}
	for name, mutate := range map[string]func(map[string]any){
		"inline_file_coordinate": func(value map[string]any) { value["intent_source"] = "prompts/worker.md" },
		"local_traversal": func(value map[string]any) {
			value["intent_kind"] = string(runtimeagentintent.SourceLocal)
			value["intent_source"] = "../outside.md"
		},
		"arbitrary_provenance": func(value map[string]any) { value["intent_provenance"] = "operator-input" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := make(map[string]any, len(base))
			for key, value := range base {
				candidate[key] = value
			}
			mutate(candidate)
			candidate["intent_content_hash"] = "sha256:recomputed-by-hostile-producer"
			candidate["intent_identity"] = "agent-intent:v1:sha256:recomputed-by-hostile-producer"
			_, err := projectBundleCatalogAgentDefinition("", "", candidate)
			if err == nil || !strings.Contains(err.Error(), "intent") {
				t.Fatalf("projectBundleCatalogAgentDefinition error = %v, want impossible intent rejection", err)
			}
		})
	}
}
