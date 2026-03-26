package tools_test

import (
	"context"
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
	runtimepipeline "swarm/internal/runtime/pipeline"
	runtimetools "swarm/internal/runtime/tools"
)

func TestCreateFlowInstanceTool_ExecutesWithPermission(t *testing.T) {
	var got runtimepipeline.FlowInstanceActivationRequest
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		FlowActivator: func(_ context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			got = req
			return nil
		},
	})
	ctx := runtimetools.WithActor(context.Background(), models.AgentConfig{
		ID:          "operator-1",
		EntityID:    "entity-1",
		Permissions: []string{"create_flow_instance"},
	})

	out, err := exec.Execute(ctx, "create_flow_instance", map[string]any{
		"template":    "review",
		"instance_id": "inst-1",
		"config": map[string]any{
			"mode": "fast",
		},
	})
	if err != nil {
		t.Fatalf("Execute(create_flow_instance): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok || strings.TrimSpace(asString(result["status"])) != "created" {
		t.Fatalf("unexpected tool output: %#v", out)
	}
	if got.TemplateID != "review" || got.InstanceID != "inst-1" || got.EntityID != "entity-1" {
		t.Fatalf("activation request = %#v", got)
	}
	if got.FlowPath != "review/inst-1" {
		t.Fatalf("flow path = %q, want review/inst-1", got.FlowPath)
	}
}

func TestCreateFlowInstanceTool_IgnoresConstrainedAllowedToolsWhenPermissioned(t *testing.T) {
	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
		FlowActivator: func(context.Context, runtimepipeline.FlowInstanceActivationRequest) error { return nil },
	})
	ctx := runtimetools.WithActor(context.Background(), models.AgentConfig{
		ID:          "operator-2",
		EntityID:    "entity-2",
		Permissions: []string{"create_flow_instance"},
		Config: mustJSONRaw(t, map[string]any{
			"allowed_tools": []string{"emit_other_event"},
		}),
	})

	if _, err := exec.Execute(ctx, "create_flow_instance", map[string]any{
		"template": "review",
	}); err != nil {
		t.Fatalf("expected permissioned create_flow_instance to bypass allowed_tools constraint: %v", err)
	}
}
