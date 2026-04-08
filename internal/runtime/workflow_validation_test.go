package runtime

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func testRuntimeWorkflowValidationBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	return bundle
}

func TestEnsureWorkflowBootWiring_RejectsTouchedValidationDriftThroughSharedPath(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	cases := []struct {
		name        string
		source      semanticview.Source
		errContains string
		wantErr     bool
	}{
		{
			name: "tool resolution warning",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", Tools: []string{"missing_tool"}},
				}
				return semanticview.Wrap(bundle)
			}(),
			wantErr: false,
		},
		{
			name: "missing emit schema",
			source: func() semanticview.Source {
				bundle := testRuntimeWorkflowValidationBundle()
				bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
					"agent-1": {ID: "agent-1", EmitEvents: []string{"missing.event"}},
				}
				return semanticview.Wrap(bundle)
			}(),
			errContains: "emit schema strict mode enabled",
			wantErr:     true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ensureWorkflowBootWiring(RuntimeOptions{
				WorkflowModule: semanticOnlyWorkflowRuntime{source: tc.source},
			})
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), tc.errContains) {
					t.Fatalf("ensureWorkflowBootWiring error = %v, want substring %q", err, tc.errContains)
				}
			} else if err != nil {
				t.Fatalf("ensureWorkflowBootWiring error = %v, want nil", err)
			}
		})
	}
}

func TestValidateWorkflowContractSurface_AllowsExplicitEventSchemas(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
	bundle := testRuntimeWorkflowValidationBundle()
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", EmitEvents: []string{"ready.event"}},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"ready.event": {
			Payload: runtimecontracts.EventPayloadSpec{
				Properties: map[string]runtimecontracts.EventFieldSpec{
					"id": {Type: "string"},
				},
			},
		},
	}
	source := semanticview.Wrap(bundle)

	result, err := ValidateWorkflowContractSurface(context.Background(), source, DefaultWorkflowContractValidationOptions(nil))
	if err != nil {
		t.Fatalf("ValidateWorkflowContractSurface: %v", err)
	}
	if len(result.MissingEmitSchemaEventTypes) != 0 {
		t.Fatalf("MissingEmitSchemaEventTypes = %#v, want none", result.MissingEmitSchemaEventTypes)
	}
}
