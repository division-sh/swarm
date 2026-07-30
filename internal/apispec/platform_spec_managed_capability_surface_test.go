package apispec

import (
	"strings"
	"testing"
)

func TestPlatformSpecManagedCapabilitySurfaceCandidateDefinitionOwner(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	surface := mustMappingValue(t, root, "managed_agent_capability_surface")
	scope := mustMappingValue(t, surface, "scope")
	owner := mustMappingValue(t, scope, "candidate_definition_owner")

	for key, fragments := range map[string][]string{
		"emit_definition_owner": {"EmitRegistry.GenerateEmitToolsForActor", "GenerateEmitToolsForRole", "no emit_events"},
		"live_catalog_owner":    {"Executor.ToolDefinitionsForActorInContext", "ToolDefinitionsForActor", "context-free fallback"},
		"transport_rule":        {"consume the executor catalog unchanged", "preserve executor order and cardinality", "duplicate canonical definition names fail closed", "MUST NOT merge", "side-channel"},
	} {
		value := scalarValue(mustMappingValue(t, owner, key))
		for _, fragment := range fragments {
			if !strings.Contains(value, fragment) {
				t.Fatalf("candidate_definition_owner.%s missing %q:\n%s", key, fragment, value)
			}
		}
	}
}
