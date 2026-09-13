package engine

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

func TestComputeModuleSemanticResultFeedsIntegerArithmeticAndReplay(t *testing.T) {
	source, _ := sourceWithStructuredRendererModule(t)
	exec := newStructuredRendererExecutor(t, source)
	req := structuredRendererExecutionRequest(t, structuredRendererModuleSpec())
	req.Handler.Rules[1].Emit.Fields = map[string]runtimecontracts.ExpressionValue{
		"lines":  runtimecontracts.CELExpression("computed.rendered_bundle.line_count + 1"),
		"double": runtimecontracts.CELExpression("double(computed.rendered_bundle.line_count) + 1.0"),
	}
	result, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.EmitIntents) != 1 {
		t.Fatalf("emits=%d", len(result.EmitIntents))
	}
	var carrier map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(result.EmitIntents[0].Event.Payload(), &carrier); err != nil {
		t.Fatal(err)
	}
	got, err := workflowexpr.ProjectCELValue(carrier)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"lines": int64(7), "double": float64(7)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("module execution=%#v want %#v", got, want)
	}
	req.ExpectedComputeModuleTraces = result.ComputeModuleTraces
	replay, err := exec.ExecuteSemanticFixture(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(replay.EmitIntents) != 1 || string(replay.EmitIntents[0].Event.Payload()) != string(result.EmitIntents[0].Event.Payload()) {
		t.Fatal("module replay changed output")
	}
	for _, number := range []string{"7", "7.0", "7e0"} {
		output, err := decodeComputeModuleOutput("numeric", "row", []byte(`{"n":`+number+`}`), map[string]any{"type": "object", "properties": map[string]any{"n": map[string]any{"type": "integer"}}})
		if err != nil || output["n"] != int64(7) {
			t.Fatalf("%s output=%#v err=%v", number, output, err)
		}
	}
}
