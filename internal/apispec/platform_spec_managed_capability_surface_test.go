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

func TestPlatformSpecManagedCapabilitySurfaceOwnsConcreteActorIdentity(t *testing.T) {
	root := loadPlatformSpecYAMLNode(t)
	surface := mustMappingValue(t, root, "managed_agent_capability_surface")
	externalEffects := mustMappingValue(t, root, "managed_external_effect_authority")

	for name, value := range map[string]string{
		"surface identity": scalarValue(mustMappingValue(t, mustMappingValue(t, surface, "surface_identity"), "rule")),
		"provider turn":    scalarValue(mustMappingValue(t, mustMappingValue(t, surface, "provider_turn"), "rule")),
		"persistence":      scalarValue(mustMappingValue(t, mustMappingValue(t, surface, "persistence"), "rule")),
		"effect context":   scalarValue(mustMappingValue(t, mustMappingValue(t, externalEffects, "context_authority"), "managed_agent")),
		"operation ID":     scalarValue(mustMappingValue(t, mustMappingValue(t, externalEffects, "logical_operation"), "identity")),
	} {
		if !strings.Contains(value, "typed concrete") {
			t.Fatalf("%s does not name typed concrete identity authority:\n%s", name, value)
		}
	}
	operation := scalarValue(mustMappingValue(t, mustMappingValue(t, externalEffects, "logical_operation"), "identity"))
	for _, fragment := range []string{"Same-slug concrete siblings are distinct", "successor lifecycle generations", "exact execution authorities"} {
		if !strings.Contains(operation, fragment) {
			t.Fatalf("logical operation identity missing %q:\n%s", fragment, operation)
		}
	}
}
