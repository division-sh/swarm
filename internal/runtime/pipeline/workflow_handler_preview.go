package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
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
	RuleSelection   handlerselection.HandlerRuleSelectionFact
	Transition      *workflowlifecycle.Transition
	SetsGate        string
	ClearGates      []string
	FanOutCount     int
	Computed        map[string]any
}

type previewWorkflowModule struct {
	bundle         *runtimecontracts.WorkflowContractBundle
	workflowNodes  []WorkflowNode
	guardRegistry  GuardRegistry
	actionRegistry ActionRegistry
}

func (m *previewWorkflowModule) SemanticSource() semanticview.Source {
	return semanticview.Wrap(m.bundle)
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

func (previewBus) Publish(context.Context, events.Event) error { return nil }

func (previewBus) PublishDirect(context.Context, events.Event, []string) error { return nil }

func (previewBus) ResolveSubscribedRecipients(string) []string { return nil }

func (previewBus) LogRuntime(context.Context, RuntimeLogEntry) error { return nil }

func (previewBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return noOpEngineDispatcher{}
}

func PreviewContractHandlerExecution(ctx context.Context, bundle *runtimecontracts.WorkflowContractBundle, node identity.ExecutableNode, evt events.Event, snapshot runtimeengine.StateSnapshot, policyOverrides map[string]any) (HandlerPreview, error) {
	if bundle == nil {
		return HandlerPreview{}, ErrContractBundleNil
	}
	if !node.Valid() {
		return HandlerPreview{}, fmt.Errorf("exact executable node identity is required")
	}
	previewBundle := semanticview.CloneBundleForPreview(bundle, policyOverrides)
	source := semanticview.Wrap(previewBundle)
	handler, ok := source.ExecutableNodeEventHandler(node, strings.TrimSpace(string(evt.Type())))
	if !ok {
		return HandlerPreview{}, fmt.Errorf("missing handler %s/%s", node.Key(), evt.Type())
	}
	nodes, err := LoadWorkflowNodes(source)
	if err != nil {
		return HandlerPreview{}, err
	}
	module := &previewWorkflowModule{
		bundle:         previewBundle,
		workflowNodes:  nodes,
		guardRegistry:  NewContractGuardRegistry(source),
		actionRegistry: NewContractActionRegistry(source),
	}
	pc := newPreviewPipelineCoordinator(previewBus{}, PipelineCoordinatorOptions{Module: module})
	if pc == nil {
		return HandlerPreview{}, fmt.Errorf("preview coordinator is nil")
	}
	pc.previewState = &snapshot
	state := workflowStateFromEngine(snapshot)
	// An explicit event target selects the entity ahead of the supplied snapshot.
	if targetEntity := workflowEventEntityID(evt); targetEntity != "" {
		state.EntityID = targetEntity
	}
	result, err := pc.executeNodeContractHandler(ctx, node, handler, workflowTriggerContext{
		Event: evt,
		State: *state,
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
		ruleID = strings.TrimSpace(result.RuleSelection.DisplayLabel())
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
		RuleSelection:   result.RuleSelection,
		Transition:      result.Transition,
		SetsGate:        setsGate,
		ClearGates:      clearGates,
		FanOutCount:     fanOutCount,
		Computed:        computed,
	}, nil
}

func loadPreviewEngineState(snapshot runtimeengine.StateSnapshot, address runtimeengine.StateAddress) (runtimeengine.StateSnapshot, bool, error) {
	if snapshot.WorkflowName != "" && snapshot.WorkflowName != address.FlowID.String() {
		return runtimeengine.StateSnapshot{}, false, fmt.Errorf("preview state belongs to a different compiled flow")
	}
	if !snapshot.EntityID.IsZero() && snapshot.EntityID != address.EntityID {
		return runtimeengine.StateSnapshot{}, false, fmt.Errorf("preview state belongs to a different entity")
	}
	if path := snapshot.StateCarrier.Control.FlowPath; path != "" && path != address.Route.InstancePath {
		return runtimeengine.StateSnapshot{}, false, fmt.Errorf("preview state belongs to a different instance route")
	}
	if snapshot.CurrentState == "" {
		return runtimeengine.StateSnapshot{}, false, nil
	}
	carrier := snapshot.StateCarrier
	snapshot.StateCarrier = runtimeengine.NewStateCarrierWithOwners(carrier.Fields, carrier.Bookkeeping, carrier.Control, carrier.Gates, carrier.StateBuckets)
	return snapshot, true, nil
}
