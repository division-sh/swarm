package pipeline

import "testing"

func TestBuildWorkflowExpressionContextIncludesStateValidationAndExecutionVars(t *testing.T) {
	ctx := buildWorkflowExpressionContext(workflowExpressionContextInput{
		State: WorkflowState{
			Stage:  PipelineStage("reviewing"),
			Status: "active",
			Metadata: map[string]any{
				"g_product_spec": true,
				"custom":         "value",
			},
		},
		ValidationState: &validationPipelineState{
			G1Research:    true,
			G2Spec:        false,
			G3CTO:         true,
			G4Brand:       false,
			RevisionCount: 4,
		},
		Payload: map[string]any{
			"entity_id": "ent-1",
		},
		Policy: map[string]any{
			"max_revisions": 5,
		},
		Accumulated: map[string]any{
			"count": 2,
		},
		FanOut: map[string]any{
			"item": "child-a",
		},
		ExtraVars: map[string]any{
			"manual": true,
		},
	})

	if got := ctx.Entity["current_state"]; got != "reviewing" {
		t.Fatalf("current_state = %v, want reviewing", got)
	}
	if got := ctx.Entity["state"]; got != "reviewing" {
		t.Fatalf("state = %v, want reviewing", got)
	}
	if got := ctx.Entity["stage"]; got != "reviewing" {
		t.Fatalf("stage = %v, want reviewing", got)
	}
	if got := ctx.Entity["status"]; got != "active" {
		t.Fatalf("status = %v, want active", got)
	}
	if got := ctx.Entity["revision_count"]; got != 4 {
		t.Fatalf("revision_count = %v, want 4", got)
	}

	gates, ok := ctx.Entity["gates"].(map[string]any)
	if !ok {
		t.Fatalf("entity.gates missing or wrong type: %T", ctx.Entity["gates"])
	}
	if got := gates["g1_research"]; got != true {
		t.Fatalf("g1_research = %v, want true", got)
	}
	if got := gates["g_product_spec"]; got != true {
		t.Fatalf("g_product_spec = %v, want true", got)
	}

	metadata, ok := ctx.Vars["metadata"].(map[string]any)
	if !ok {
		t.Fatalf("vars.metadata missing or wrong type: %T", ctx.Vars["metadata"])
	}
	if got := metadata["revision_count"]; got != 4 {
		t.Fatalf("vars.metadata.revision_count = %v, want 4", got)
	}
	if got := ctx.Vars["manual"]; got != true {
		t.Fatalf("vars.manual = %v, want true", got)
	}
	if got := ctx.Vars["accumulated"].(map[string]any)["count"]; got != 2 {
		t.Fatalf("vars.accumulated.count = %v, want 2", got)
	}
	if got := ctx.Vars["fan_out"].(map[string]any)["item"]; got != "child-a" {
		t.Fatalf("vars.fan_out.item = %v, want child-a", got)
	}
}

func TestBuildWorkflowExpressionContextPreservesExistingRevisionCount(t *testing.T) {
	ctx := buildWorkflowExpressionContext(workflowExpressionContextInput{
		State: WorkflowState{
			Metadata: map[string]any{
				"revision_count": 9,
			},
		},
		ValidationState: &validationPipelineState{RevisionCount: 2},
	})

	if got := ctx.Entity["revision_count"]; got != 9 {
		t.Fatalf("revision_count = %v, want existing metadata value 9", got)
	}
}
