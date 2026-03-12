package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
)

type HandlerPreview struct {
	Status          HandlerOutcomeStatus
	Stage           PipelineStage
	StatusText      string
	Metadata        map[string]any
	Emits           []string
	ActionsExecuted []string
	GuardsEvaluated []string
	RuleID          string
	SetsGate        string
	ClearGates      []string
	FanOutCount     int
	Computed        map[string]any
}

type previewWorkflowModule struct {
	bundle         *runtimecontracts.WorkflowContractBundle
	workflow       *WorkflowDefinition
	workflowNodes  []WorkflowNode
	guardRegistry  GuardRegistry
	actionRegistry ActionRegistry
}

func (m *previewWorkflowModule) ContractBundle() *runtimecontracts.WorkflowContractBundle {
	return m.bundle
}

func (m *previewWorkflowModule) WorkflowDefinition() *WorkflowDefinition {
	return m.workflow
}

func (m *previewWorkflowModule) WorkflowNodes() []WorkflowNode {
	out := make([]WorkflowNode, 0, len(m.workflowNodes))
	for _, node := range m.workflowNodes {
		out = append(out, node)
	}
	return out
}

func (m *previewWorkflowModule) GuardRegistry() GuardRegistry {
	return m.guardRegistry
}

func (m *previewWorkflowModule) ActionRegistry() ActionRegistry {
	return m.actionRegistry
}

func (*previewWorkflowModule) DiscoveryPolicy() DiscoveryPolicy { return genericTestModule{} }

func (*previewWorkflowModule) ScanPolicy() ScanPolicy { return genericTestModule{} }

func (*previewWorkflowModule) ScoringPolicy() ScoringPolicy { return genericTestModule{} }

func (*previewWorkflowModule) PayloadFactory() PayloadFactory { return genericTestModule{} }

type previewBus struct{}

func (previewBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (previewBus) Publish(context.Context, events.Event) error { return nil }

func (previewBus) PublishDirect(context.Context, events.Event, []string) error { return nil }

func (previewBus) ResolveSubscribedRecipients(string) []string { return nil }

func (previewBus) LogRuntime(context.Context, RuntimeLogEntry) {}

func PreviewContractHandlerExecution(ctx context.Context, bundle *runtimecontracts.WorkflowContractBundle, nodeID string, evt events.Event, state WorkflowState, policyOverrides map[string]any) (HandlerPreview, error) {
	if bundle == nil {
		return HandlerPreview{}, fmt.Errorf("workflow contract bundle is nil")
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return HandlerPreview{}, fmt.Errorf("node id is required")
	}
	handler, ok := bundle.NodeEventHandler(nodeID, strings.TrimSpace(string(evt.Type)))
	if !ok {
		return HandlerPreview{}, fmt.Errorf("missing handler %s/%s", nodeID, evt.Type)
	}

	previewBundle := cloneBundleForPreview(bundle, policyOverrides)
	workflow, err := LoadWorkflowDefinition(previewBundle)
	if err != nil {
		return HandlerPreview{}, err
	}
	nodes, err := LoadWorkflowNodes(previewBundle)
	if err != nil {
		return HandlerPreview{}, err
	}
	module := &previewWorkflowModule{
		bundle:         previewBundle,
		workflow:       workflow,
		workflowNodes:  nodes,
		guardRegistry:  NewContractGuardRegistry(previewBundle),
		actionRegistry: NewContractActionRegistry(previewBundle),
	}
	pc := NewFactoryPipelineCoordinatorWithOptions(previewBus{}, nil, FactoryPipelineCoordinatorOptions{Module: module})
	if pc == nil {
		return HandlerPreview{}, fmt.Errorf("preview coordinator is nil")
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now().UTC()
	}
	var emitted []events.Event
	ctx = context.WithValue(ctx, pipelineEmitCollectorKey{}, &emitted)
	outcome, err := executeHandlerStepsDetailed(withHandlerEngineContext(ctx, pc, nodeID, false), handler, evt, &state)
	if err != nil {
		return HandlerPreview{}, err
	}
	emits := append([]string{}, outcome.Emits...)
	if len(emitted) > 0 {
		emits = emits[:0]
		for _, item := range emitted {
			emits = append(emits, strings.TrimSpace(string(item.Type)))
		}
	}
	return HandlerPreview{
		Status:          outcome.Status,
		Stage:           state.Stage,
		StatusText:      state.Status,
		Metadata:        cloneStringAnyMap(state.Metadata),
		Emits:           emits,
		ActionsExecuted: append([]string{}, outcome.ActionsExecuted...),
		GuardsEvaluated: append([]string{}, outcome.GuardsEvaluated...),
		RuleID:          strings.TrimSpace(outcome.RuleID),
		SetsGate:        strings.TrimSpace(outcome.SetsGate),
		ClearGates:      append([]string{}, outcome.ClearGates...),
		FanOutCount:     outcome.FanOutCount,
		Computed:        cloneStringAnyMap(outcome.Computed),
	}, nil
}

func cloneBundleForPreview(bundle *runtimecontracts.WorkflowContractBundle, policyOverrides map[string]any) *runtimecontracts.WorkflowContractBundle {
	if bundle == nil {
		return nil
	}
	clone := *bundle
	clone.Policy = clonePolicyDocument(bundle.Policy)
	clone.MergedPolicy = clonePolicyDocument(bundle.MergedPolicy)
	if len(policyOverrides) == 0 {
		return &clone
	}
	if clone.MergedPolicy.Values == nil {
		clone.MergedPolicy.Values = map[string]runtimecontracts.PolicyValue{}
	}
	for key, value := range policyOverrides {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		current := clone.MergedPolicy.Values[key]
		current.Value = value
		clone.MergedPolicy.Values[key] = current
	}
	return &clone
}

func clonePolicyDocument(in runtimecontracts.PolicyDocument) runtimecontracts.PolicyDocument {
	out := runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{}}
	for key, value := range in.Values {
		out.Values[key] = value
	}
	return out
}
