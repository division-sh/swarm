package bootverify

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestPinTargetResolutionFailsClosedWithoutCanonicalConsumer(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(pinRoutingCheckBundle("", false, false)), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "target_required_missing") {
		t.Fatalf("expected target_required_missing, got %#v", report.Errors())
	}
}

func TestPinTargetResolutionAllowsTypedSameFlowConsumer(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(pinRoutingCheckBundle("", true, false)), Options{})
	if reportContainsCheck(report.Errors(), "pin_target_resolution") {
		t.Fatalf("same-flow consumer produced routing error: %#v", report.Errors())
	}
}

func TestPinTargetResolutionAllowsAcceptedExternalConsumer(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(pinRoutingCheckBundle("", false, true)), Options{})
	if reportContainsCheck(report.Errors(), "pin_target_resolution") {
		t.Fatalf("accepted external consumer produced routing error: %#v", report.Errors())
	}
}

func TestPinTargetResolutionAllowsHarnessObservationWithoutRuntimeConsumer(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(pinRoutingCheckBundle(runtimecontracts.FlowOutputSinkHarness, false, false)), Options{})
	if reportContainsCheck(report.Errors(), "pin_target_resolution") {
		t.Fatalf("validation-only harness output produced routing error: %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "event_consumer_exists", "result.ready") {
		t.Fatalf("harness observation produced no-consumer warning: %#v", report.Warnings())
	}
}

func TestPinTargetResolutionRejectsHarnessWithSameFlowConsumer(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(pinRoutingCheckBundle(runtimecontracts.FlowOutputSinkHarness, true, false)), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "sink: harness and a canonical runtime consumer") {
		t.Fatalf("expected harness/consumer conflict, got %#v", report.Errors())
	}
}

func TestPinTargetResolutionRejectsHarnessWithAcceptedExternalConsumer(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(pinRoutingCheckBundle(runtimecontracts.FlowOutputSinkHarness, false, true)), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "sink: harness and a canonical runtime consumer") {
		t.Fatalf("expected harness/external conflict, got %#v", report.Errors())
	}
}

func TestPinTargetResolutionRejectsHarnessConflictWithoutProducer(t *testing.T) {
	bundle := pinRoutingCheckBundle(runtimecontracts.FlowOutputSinkHarness, true, false)
	delete(bundle.Nodes, "producer")
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "pin_target_resolution", "sink: harness and a canonical runtime consumer") {
		t.Fatalf("expected declaration-level harness conflict, got %#v", report.Errors())
	}
}

func pinRoutingCheckBundle(sink runtimecontracts.FlowOutputSink, sameFlowConsumer, externalConsumer bool) *runtimecontracts.WorkflowContractBundle {
	ready := runtimecontracts.EventCatalogEntry{}
	if externalConsumer {
		ready.Swarm.Consumer = []string{"external_webhook"}
	}
	pin := runtimecontracts.FlowOutputEventPin{Name: "result_ready", Event: "result.ready", Sink: sink}
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Pins: runtimecontracts.FlowPins{Outputs: runtimecontracts.FlowOutputPins{EventPins: []runtimecontracts.FlowOutputEventPin{pin}}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"request.started": {Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"}},
			"result.ready":    ready,
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"producer": {
				ID:            "producer",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"request.started"},
				Produces:      []string{"result.ready"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"request.started": {Emit: runtimecontracts.EmitSpec{Event: "result.ready"}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowOutputs:         map[string][]string{"": {"result.ready"}},
			FlowOutputEventPins: map[string][]runtimecontracts.FlowOutputEventPin{"": {pin}},
		},
	}
	if sameFlowConsumer {
		bundle.Nodes["consumer"] = runtimecontracts.SystemNodeContract{
			ID:            "consumer",
			ExecutionType: "system_node",
			SubscribesTo:  []string{"result.ready"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"result.ready": {},
			},
		}
	}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
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
	schema.InitialState = ""
	schema.InitialStateDeclared = false
	schema.States = nil
	schema.StatesDeclared = false
	schema.TerminalStates = nil
	schema.TerminalStatesDeclared = false
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
	bundle.Semantics.FlowTerminal[flowID] = schema.LoweredTerminalStates()
	if view, ok := bundle.FlowViewByID(flowID); ok && view != nil {
		view.Schema = schema
	}
}
