package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type HandlerPreview struct {
	Status          HandlerOutcomeStatus
	Stage           WorkflowStateID
	StatusText      string
	Metadata        map[string]any
	InitialValues   map[string]any
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

type previewBus struct{}

func (previewBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (previewBus) Publish(context.Context, events.Event) error { return nil }

func (previewBus) PublishDirect(context.Context, events.Event, []string) error { return nil }

func (previewBus) ResolveSubscribedRecipients(string) []string { return nil }

func (previewBus) LogRuntime(context.Context, RuntimeLogEntry) error { return nil }

func (previewBus) EngineOutbox() runtimeengine.OutboxWriter { return noOpEngineOutbox{} }

func (previewBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}

func PreviewContractHandlerExecution(ctx context.Context, bundle *runtimecontracts.WorkflowContractBundle, nodeID string, evt events.Event, state WorkflowState, policyOverrides map[string]any) (HandlerPreview, error) {
	if bundle == nil {
		return HandlerPreview{}, ErrContractBundleNil
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return HandlerPreview{}, fmt.Errorf("node id is required")
	}
	previewBundle := semanticview.CloneBundleForPreview(bundle, policyOverrides)
	source := semanticview.Wrap(previewBundle)
	handler, ok := source.NodeEventHandler(nodeID, strings.TrimSpace(string(evt.Type())))
	if !ok {
		return HandlerPreview{}, fmt.Errorf("missing handler %s/%s", nodeID, evt.Type())
	}
	workflow, err := LoadWorkflowDefinition(source)
	if err != nil {
		return HandlerPreview{}, err
	}
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		return HandlerPreview{}, err
	}
	module := &previewWorkflowModule{
		bundle:         previewBundle,
		workflow:       workflow,
		workflowNodes:  nodes,
		guardRegistry:  NewContractGuardRegistry(source),
		actionRegistry: NewContractActionRegistry(source),
	}
	pc := NewPipelineCoordinatorWithOptions(previewBus{}, nil, PipelineCoordinatorOptions{Module: module})
	if pc == nil {
		return HandlerPreview{}, fmt.Errorf("preview coordinator is nil")
	}
	if evt.CreatedAt().IsZero() {
		evt = events.NewProjectionEvent(
			evt.ID(),
			evt.Type(),
			evt.SourceAgent(),
			evt.TaskID(),
			evt.Payload(),
			evt.ChainDepth(),
			evt.RunID(),
			evt.ParentEventID(),
			evt.NormalizedEnvelope(),
			time.Now().UTC(),
		).WithProducerType(evt.ProducerType()).WithExecutionMode(evt.ExecutionMode())
	}
	result, err := pc.executeNodeContractHandler(ctx, nodeID, handler, workflowTriggerContext{
		Event: evt,
		State: state,
	}, true)
	if err != nil {
		return HandlerPreview{}, err
	}
	emits := []string{}
	if result.Outcome != nil {
		emits = append(emits, result.Outcome.Emits...)
	}
	stage := state.Stage
	if result.Outcome != nil && strings.TrimSpace(result.Outcome.AdvancesTo) != "" {
		stage = NormalizeWorkflowStateID(result.Outcome.AdvancesTo)
	}
	actions := []string(nil)
	guards := []string(nil)
	ruleID := ""
	setsGate := ""
	clearGates := []string(nil)
	fanOutCount := 0
	computed := map[string]any(nil)
	status := HandlerOutcomeCompleted
	if result.Outcome != nil {
		status = result.Outcome.Status
		actions = append(actions, result.Outcome.ActionsExecuted...)
		guards = append(guards, result.Outcome.GuardsEvaluated...)
		ruleID = strings.TrimSpace(result.Outcome.RuleID)
		setsGate = strings.TrimSpace(result.Outcome.SetsGate)
		clearGates = append(clearGates, result.Outcome.ClearGates...)
		fanOutCount = result.Outcome.FanOutCount
		computed = cloneStringAnyMap(result.Outcome.Computed)
	}
	return HandlerPreview{
		Status:          status,
		Stage:           stage,
		StatusText:      state.Status,
		Metadata:        cloneStringAnyMap(result.PreviewMetadata),
		InitialValues:   cloneStringAnyMap(result.InitialValuesMaterialized),
		Emits:           emits,
		ActionsExecuted: actions,
		GuardsEvaluated: guards,
		RuleID:          ruleID,
		SetsGate:        setsGate,
		ClearGates:      clearGates,
		FanOutCount:     fanOutCount,
		Computed:        computed,
	}, nil
}
