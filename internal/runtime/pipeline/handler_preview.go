package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/identity"
	runtimeengine "empireai/internal/runtime/engine"
	"empireai/internal/runtime/semanticview"
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

func (m *previewWorkflowModule) SemanticSource() semanticview.Source {
	return semanticview.Wrap(m.bundle)
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

func (previewBus) EngineOutbox() runtimeengine.OutboxWriter { return noOpEngineOutbox{} }

func (previewBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}

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

	previewBundle := semanticview.CloneBundleForPreview(bundle, policyOverrides)
	workflow, err := LoadWorkflowDefinition(semanticview.Wrap(previewBundle))
	if err != nil {
		return HandlerPreview{}, err
	}
	nodes, err := LoadWorkflowNodes(semanticview.Wrap(previewBundle))
	if err != nil {
		return HandlerPreview{}, err
	}
	module := &previewWorkflowModule{
		bundle:         previewBundle,
		workflow:       workflow,
		workflowNodes:  nodes,
		guardRegistry:  NewContractGuardRegistry(semanticview.Wrap(previewBundle)),
		actionRegistry: NewContractActionRegistry(semanticview.Wrap(previewBundle)),
	}
	pc := NewFactoryPipelineCoordinatorWithOptions(previewBus{}, nil, FactoryPipelineCoordinatorOptions{Module: module})
	if pc == nil {
		return HandlerPreview{}, fmt.Errorf("preview coordinator is nil")
	}
	exec, err := runtimeengine.NewExecutor(coordinatorEngineDependencies(pc), newCoordinatorEngineEvaluator(pc))
	if err != nil {
		return HandlerPreview{}, err
	}
	node := runtimeengine.NewDeclarativeNode(nodeID, exec)
	if node == nil {
		return HandlerPreview{}, fmt.Errorf("preview executor is nil")
	}
	if evt.CreatedAt.IsZero() {
		evt.CreatedAt = time.Now().UTC()
	}
	result, err := node.Handle(ctx, runtimeengine.ExecutionRequest{
		EntityID: identity.NormalizeEntityID(workflowEventEntityID(evt)),
		NodeID:   identity.NormalizeNodeID(nodeID),
		Event:    evt,
		Handler:  handler,
		Preview:  true,
		State: runtimeengine.StateSnapshot{
			EntityID:     identity.NormalizeEntityID(state.VerticalID),
			CurrentState: strings.TrimSpace(string(state.Stage)),
			Metadata:     cloneStringAnyMap(state.Metadata),
			StateBuckets: map[string]any{},
		},
	})
	if err != nil {
		return HandlerPreview{}, err
	}
	emits := make([]string, 0, len(result.EmitIntents))
	for _, item := range result.EmitIntents {
		if eventType := strings.TrimSpace(string(item.Event.Type)); eventType != "" {
			emits = append(emits, eventType)
		}
	}
	return HandlerPreview{
		Status:          handlerOutcomeStatusFromEngine(result.Status),
		Stage:           NormalizePipelineStage(firstNonEmptyString(result.NextState, result.CurrentState, string(state.Stage))),
		StatusText:      state.Status,
		Metadata:        cloneStringAnyMap(result.StateMutation.Metadata),
		Emits:           emits,
		ActionsExecuted: append([]string{}, result.ActionsExecuted...),
		GuardsEvaluated: append([]string{}, result.GuardsEvaluated...),
		RuleID:          strings.TrimSpace(result.RuleID),
		SetsGate:        strings.TrimSpace(result.SetsGate),
		ClearGates:      append([]string{}, result.ClearGates...),
		FanOutCount:     result.FanOutCount,
		Computed:        cloneStringAnyMap(result.Computed),
	}, nil
}
