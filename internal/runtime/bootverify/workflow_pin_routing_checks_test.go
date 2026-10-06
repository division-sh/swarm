package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

func TestPinTargetResolutionAllowsPublicRootExportWithoutRuntimeConsumer(t *testing.T) {
	report := Run(context.Background(), semanticviewtest.WrapRootAgents(pinRoutingCheckBundle(false)), Options{})
	if reportContainsCheck(report.Errors(), "pin_target_resolution") {
		t.Fatalf("public root export needs no internal consumer: %#v", report.Errors())
	}
}

func TestPinTargetResolutionAllowsTypedSameFlowConsumer(t *testing.T) {
	report := Run(context.Background(), semanticviewtest.WrapRootAgents(pinRoutingCheckBundle(true)), Options{})
	if reportContainsCheck(report.Errors(), "pin_target_resolution") {
		t.Fatalf("same-flow consumer produced routing error: %#v", report.Errors())
	}
}

func pinRoutingCheckBundle(sameFlowConsumer bool) *runtimecontracts.WorkflowContractBundle {
	ready := runtimecontracts.EventCatalogEntry{}
	pin := runtimecontracts.FlowOutputEventPin{Event: "result.ready"}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{pin}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"request.started": {},
			"result.ready":    ready,
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"producer": {
				ExecutionType: "system_node",
				SubscribesTo:  []string{"request.started"},
				Produces:      []string{"result.ready"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"request.started": {Emit: runtimecontracts.EmitSpec{Event: "result.ready"}},
				},
			},
		},
	}
	if sameFlowConsumer {
		bundle.Nodes["consumer"] = runtimecontracts.SystemNodeContract{
			ExecutionType: "system_node",
			SubscribesTo:  []string{"result.ready"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"result.ready": {},
			},
		}
	}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	compileBootverifyRootSource(bundle)
	return bundle
}

func reportContainsCheck(findings []Finding, checkID string) bool {
	for _, finding := range findings {
		if strings.TrimSpace(finding.CheckID) == strings.TrimSpace(checkID) {
			return true
		}
	}
	return false
}

func useStagedLifecycleForFlow(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, initial string, states, terminals []string) {
	t.Helper()
	if bundle == nil {
		t.Fatal("bundle is nil")
	}
	flowID = strings.TrimSpace(flowID)
	schema, ok := bundle.FlowSchemas[flowID]
	if flowID == "" || !ok {
		t.Fatalf("flow schema %q missing", flowID)
	}
	terminalSet := map[string]struct{}{}
	for _, terminal := range terminals {
		terminalSet[strings.TrimSpace(terminal)] = struct{}{}
	}
	entries := make([]runtimecontracts.FlowStageDeclaration, 0, len(states))
	for _, state := range states {
		state = strings.TrimSpace(state)
		if state == "" {
			continue
		}
		_, terminal := terminalSet[state]
		entries = append(entries, runtimecontracts.FlowStageDeclaration{ID: state, Initial: state == strings.TrimSpace(initial), Terminal: terminal})
	}
	schema.StageDeclarations = runtimecontracts.FlowStageDeclarations{Declared: true, Entries: entries}
	bundle.FlowSchemas[flowID] = schema
	if bundle.Semantics.FlowInitial == nil {
		bundle.Semantics.FlowInitial = map[string]string{}
	}
	if bundle.Semantics.FlowStates == nil {
		bundle.Semantics.FlowStates = map[string][]string{}
	}
	if bundle.Semantics.FlowTerminal == nil {
		bundle.Semantics.FlowTerminal = map[string][]string{}
	}
	bundle.Semantics.FlowInitial[flowID] = schema.LoweredInitialState()
	bundle.Semantics.FlowStates[flowID] = schema.LoweredStates()
	bundle.Semantics.FlowTerminal[flowID] = schema.LoweredFinalStates()
	if view, ok := bundle.FlowViewByID(flowID); ok && view != nil {
		view.Schema = schema
	}
}
