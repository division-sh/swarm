package accumulator

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEffectiveSpecForHandlerConsumesCanonicalFanInAssociation(t *testing.T) {
	source := accumulatorFanInSource(t, []runtimecontracts.FlowInputEventPin{{
		Event: "work.requested",
		Resolution: runtimecontracts.FlowInputPinResolution{
			Mode:        runtimecontracts.FlowInputResolutionModeFanIn,
			Aggregation: "stream",
			Window:      "payload.window_id",
			DedupBy:     []string{"payload.work_id"},
		},
	}})

	node := identitytest.FlowNode(t, "worker", "worker-node")
	effective, err := EffectiveSpecForHandler(source, node, "work.requested", &runtimecontracts.AccumulateSpec{Into: "items"})
	if err != nil {
		t.Fatalf("effective spec: %v", err)
	}
	if effective.Window != "payload.window_id" || effective.DedupBy != "payload.work_id" {
		t.Fatalf("effective spec = %#v, want pin-owned window/dedup", effective)
	}

	byAlias, err := EffectiveSpecForHandler(source, node, "work", &runtimecontracts.AccumulateSpec{Into: "items"})
	if err != nil || byAlias.Window != "" || byAlias.DedupBy != "" {
		t.Fatalf("retired alias association = %#v err=%v, want no fan-in match", byAlias, err)
	}
}

func TestEffectiveSpecForHandlerRejectsRedeclaration(t *testing.T) {
	source := accumulatorFanInSource(t, []runtimecontracts.FlowInputEventPin{{
		Event:      "work.requested",
		Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeFanIn, Aggregation: "stream", Window: "payload.window_id", DedupBy: []string{"payload.work_id"}},
	}})
	node := identitytest.FlowNode(t, "worker", "worker-node")
	if _, err := EffectiveSpecForHandler(source, node, "work.requested", &runtimecontracts.AccumulateSpec{Window: "payload.other"}); err == nil || !strings.Contains(err.Error(), "must not redeclare") {
		t.Fatalf("redeclaration error = %v, want fail-closed", err)
	}
}

func accumulatorFanInSource(t testing.TB, inputPins []runtimecontracts.FlowInputEventPin) semanticview.Source {
	t.Helper()
	worker := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "worker"},
		Schema: runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{EventPins: inputPins}}},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"worker-node": {ID: "worker-node", EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{"work.requested": {Accumulate: &runtimecontracts.AccumulateSpec{Into: "items"}}}},
		},
		Path: "worker",
	}
	root := runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{worker}}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree:    flowmodel.Tree[runtimecontracts.FlowContractView]{Root: &root, ByID: map[string]*runtimecontracts.FlowContractView{"worker": &root.Children[0]}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{"worker": worker.Schema},
	}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatalf("compile workflow semantics: %v", err)
	}
	return semanticview.Wrap(bundle)
}
