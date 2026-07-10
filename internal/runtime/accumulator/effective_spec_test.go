package accumulator

import (
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestEffectiveSpecForHandlerConsumesCanonicalFanInAssociation(t *testing.T) {
	source := accumulatorFanInSource([]runtimecontracts.FlowInputEventPin{{
		Name:  "work",
		Event: "work.requested",
		Resolution: runtimecontracts.FlowInputPinResolution{
			Mode:        runtimecontracts.FlowInputResolutionModeFanIn,
			Aggregation: "stream",
			Window:      "payload.window_id",
			DedupBy:     []string{"payload.work_id"},
		},
	}})

	effective, err := EffectiveSpecForHandler(source, "worker", "worker-node", "work.requested", &runtimecontracts.AccumulateSpec{Into: "items"})
	if err != nil {
		t.Fatalf("effective spec: %v", err)
	}
	if effective.Window != "payload.window_id" || effective.DedupBy != "payload.work_id" {
		t.Fatalf("effective spec = %#v, want pin-owned window/dedup", effective)
	}

	byPin, err := EffectiveSpecForHandler(source, "worker", "worker-node", "work", &runtimecontracts.AccumulateSpec{Into: "items"})
	if err != nil || byPin.Window != effective.Window || byPin.DedupBy != effective.DedupBy {
		t.Fatalf("pin-name association = %#v err=%v, want same effective spec", byPin, err)
	}
}

func TestEffectiveSpecForHandlerRejectsRedeclarationAndAmbiguity(t *testing.T) {
	source := accumulatorFanInSource([]runtimecontracts.FlowInputEventPin{{
		Name:       "work",
		Event:      "work.requested",
		Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeFanIn, Window: "payload.window_id", DedupBy: []string{"payload.work_id"}},
	}})
	if _, err := EffectiveSpecForHandler(source, "worker", "worker-node", "work.requested", &runtimecontracts.AccumulateSpec{Window: "payload.other"}); err == nil || !strings.Contains(err.Error(), "must not redeclare") {
		t.Fatalf("redeclaration error = %v, want fail-closed", err)
	}

	ambiguous := accumulatorFanInSource([]runtimecontracts.FlowInputEventPin{
		{Name: "work-a", Event: "work.requested", Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeFanIn}},
		{Name: "work-b", Event: "work.requested", Resolution: runtimecontracts.FlowInputPinResolution{Mode: runtimecontracts.FlowInputResolutionModeFanIn}},
	})
	if _, _, err := FanInInputPinForHandler(ambiguous, "worker", "worker-node", "work.requested"); err == nil || !strings.Contains(err.Error(), "work-a work-b") {
		t.Fatalf("ambiguity error = %v, want both pin names", err)
	}
}

func accumulatorFanInSource(inputPins []runtimecontracts.FlowInputEventPin) semanticview.Source {
	inputEvents := make([]string, 0, len(inputPins))
	for _, pin := range inputPins {
		inputEvents = append(inputEvents, pin.EventType())
	}
	worker := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{ID: "worker", Flow: "worker", PackageKey: "flows/worker"},
		Schema: runtimecontracts.FlowSchemaDocument{Pins: runtimecontracts.FlowPins{Inputs: runtimecontracts.FlowInputPins{Events: inputEvents, EventPins: inputPins}}},
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
	return semanticview.Wrap(bundle)
}
