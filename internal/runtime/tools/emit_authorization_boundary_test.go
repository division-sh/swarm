package tools

import (
	"testing"

	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestEmitAuthorizationCannotBorrowRoleOrGenericToolPermission(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"permitted": {ID: "permitted", Role: "shared", EmitEvents: []string{"result.done"}},
			"forbidden": {ID: "forbidden", Role: "shared"},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"result.done": {Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{"result": {Type: "text"}},
		}}},
	})
	provider := runtimeauthority.NewSourceProvider(source)
	registry := NewEmitRegistry(source, provider)
	authorizer := NewToolAuthorizer(nil, func(actor models.AgentConfig, name string) toolAuthorizationDecision {
		return classifyToolAuthorization(actor, name, registry)
	})
	for _, tc := range []struct {
		name     string
		actor    models.AgentConfig
		registry *EmitRegistry
	}{
		{"shared_role", models.AgentConfig{ID: "forbidden", Role: "shared"}, registry},
		{"listed_tool", models.AgentConfig{ID: "forbidden", Role: "unrelated", Tools: []string{"emit_result_done"}}, registry},
		{"forged_emit_list", models.AgentConfig{ID: "forbidden", Role: "unrelated", EmitEvents: []string{"result.done"}}, registry},
		{"no_registry", models.AgentConfig{ID: "permitted", Role: "shared", EmitEvents: []string{"result.done"}, Tools: []string{"emit_result_done"}}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if decision := classifyToolAuthorization(tc.actor, "emit_result_done", tc.registry); decision.allowed {
				t.Errorf("non-declaration authority admitted emission: %+v", decision)
			}
			if tc.registry != nil {
				err := authorizer.Authorize(unmanagedToolTestContext(), tc.actor, "emit_result_done")
				requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "tool_not_allowed")
			}
		})
	}
	permitted := models.AgentConfig{ID: "permitted", Role: "shared", EmitEvents: []string{"result.done"}}
	if err := authorizer.Authorize(unmanagedToolTestContext(), permitted, "emit_result_done"); err != nil {
		t.Fatalf("exact declaration authorization rejected: %v", err)
	}
}

func TestEmitAuthorizationRejectsUnknownActorWithUniqueDeclaredRole(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"permitted": {ID: "permitted", Role: "shared", EmitEvents: []string{"result.done"}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"result.done": {Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{"result": {Type: "text"}},
		}}},
	})
	registry := NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	for _, id := range []string{"unknown", ""} {
		t.Run("actor="+id, func(t *testing.T) {
			actor := models.AgentConfig{ID: id, Role: "shared", EmitEvents: []string{"result.done"}}
			if definitions := registry.GenerateEmitToolsForActor(actor, nil); len(definitions) != 0 {
				t.Errorf("unknown actor borrowed unique role's generated tools: %#v", definitions)
			}
			for _, name := range []string{"emit_result_done", "mcp__runtime-tools__emit_result_done"} {
				authorizer := NewToolAuthorizer(nil, func(actor models.AgentConfig, name string) toolAuthorizationDecision {
					return classifyToolAuthorization(actor, name, registry)
				})
				err := authorizer.Authorize(unmanagedToolTestContext(), actor, name)
				if err == nil {
					t.Errorf("unknown actor borrowed unique role's authorization for %s", name)
				}
			}
		})
	}
}
