package pipeline

import (
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/values"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestPipelineEngineEvaluatorConsumesAdmittedHandlerPayloadTypeForTemplateReply(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo,
		canonicalrouting.ExampleRoot(t, canonicalrouting.TemplateCreateMintedKey), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	resolution := semanticview.ResolveEventSchema(semanticview.Wrap(bundle), "producer", "validation.started")
	if !resolution.HasStructural {
		t.Fatal("receiver handler has no admitted structural payload type")
	}
	base := runtimeengine.BaseContext{
		FlowID: "producer",
		Event: values.Wrap(map[string]any{
			"trigger_event_type": "validator/ti-proof/validation.started",
		}),
		Payload: values.Wrap(map[string]any{"validation_case_id": "00000000-0000-0000-0000-000000000001", "candidate": "proof"}),
	}
	evaluator := pipelineEngineEvaluator{evaluator: newWorkflowExpressionEvaluator()}
	passed, err := evaluator.EvalBool(`payload.validation_case_id != ""`, base, workflowexpr.ValueExpressionOptions{PayloadType: &resolution.StructuralType})
	if err != nil || !passed {
		t.Fatalf("template reply evaluation = %t, %v", passed, err)
	}
	if _, err := evaluator.EvalBool(`payload.validation_case_id != ""`, base, workflowexpr.ValueExpressionOptions{}); err == nil {
		t.Fatal("missing admitted handler schema must fail closed, not resolve the sender event again")
	}
	if got := base.Event.Raw()["trigger_event_type"]; got != "validator/ti-proof/validation.started" {
		t.Fatalf("receiver schema binding rewrote event provenance: %v", got)
	}
}
