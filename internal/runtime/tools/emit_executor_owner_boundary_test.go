package tools

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

func TestEmitExecutorRejectsRoleBorrowingBeforePublication(t *testing.T) {
	source := wrapRootAgentBundle(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"permitted": {ID: "permitted", Role: "shared", EmitEvents: []string{"result.done"}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{"result.done": {Payload: runtimecontracts.EventPayloadSpec{
			Type: "object", Properties: map[string]runtimecontracts.EventFieldSpec{"result": {Type: "string"}},
		}}},
	})
	for _, id := range []string{"unknown", ""} {
		for _, name := range []string{"emit_result_done", "mcp__runtime-tools__emit_result_done"} {
			t.Run(id+"/"+name, func(t *testing.T) {
				bus := &telemetryBusStub{}
				executor := NewExecutorWithOptions(bus, ExecutorOptions{WorkflowSource: source})
				actor := models.AgentConfig{
					ID: id, Role: "shared", ExecutionMode: "live",
					EmitEvents: []string{"result.done"}, Tools: []string{name},
				}
				out, err := executor.Execute(models.WithActor(unmanagedToolTestContext(), actor), name, map[string]any{"result": "valid"})
				requireToolFailure(t, err, runtimefailures.ClassAuthorizationDenied, "tool_not_allowed")
				if out != nil || len(bus.published) != 0 {
					t.Fatalf("role-only actor reached publication: output=%#v events=%#v", out, bus.published)
				}
			})
		}
	}
}
