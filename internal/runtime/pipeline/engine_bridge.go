package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

type HandlerOutcomeStatus string

const (
	HandlerOutcomeCompleted      HandlerOutcomeStatus = "success"
	HandlerOutcomeBlocked        HandlerOutcomeStatus = "reject"
	HandlerOutcomeDiscarded      HandlerOutcomeStatus = "discard"
	HandlerOutcomeRejected       HandlerOutcomeStatus = "reject"
	HandlerOutcomeTerminalReject HandlerOutcomeStatus = "terminal_reject"
	HandlerOutcomeKilled         HandlerOutcomeStatus = "kill"
	HandlerOutcomeEscalated      HandlerOutcomeStatus = "escalate"
	HandlerOutcomeWaiting        HandlerOutcomeStatus = "waiting"
	HandlerOutcomeFannedOut      HandlerOutcomeStatus = "fanned_out"
)

type handlerExecutionOutcome struct {
	Status           HandlerOutcomeStatus
	GuardsEvaluated  []string
	ActionsExecuted  []string
	AdvancesTo       string
	SetsGate         string
	ClearGates       []string
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
	Emits            []string
	RuleSelection    handlerselection.Observation
	FanOutCount      int
	Computed         map[string]any
	InterceptedEmits []runtimeengine.EmitIntent
}

type contractHandlerExecutionResult struct {
	Committed            bool
	CommittedStage       *runtimeengine.CommittedStage
	Plan                 handlerExecutionPlan
	Outcome              *handlerExecutionOutcome
	GuardsEvaluated      []string
	PreviewMetadata      map[string]any
	FollowUp             handlerCommittedFollowUp
	DiagnosticEmissions  []events.Event
	SettledDeliveryClaim *runtimedelivery.Claim
	Handled              bool
	RuleSelection        handlerselection.Observation
	Transition           *workflowlifecycle.Transition
}

// The selected mutation has already committed these exact events. The
// coordinator transfers them only after Executor releases the entity lock.
type handlerCommittedFollowUp struct {
	Emissions        []runtimeengine.EmitIntent
	ActivityIntents  []runtimeengine.ActivityIntent
	ActivityRequests []runtimeengine.EmitIntent
}

func copyCommittedIntents(intents []runtimeengine.EmitIntent) []runtimeengine.EmitIntent {
	if len(intents) == 0 {
		return nil
	}
	copyOf := make([]runtimeengine.EmitIntent, len(intents))
	for index, intent := range intents {
		copyOf[index] = intent
		copyOf[index].Event = intent.Event.Clone()
		copyOf[index].Recipients = append([]string(nil), intent.Recipients...)
	}
	return copyOf
}

func isJoinLifecycleEvent(eventType events.EventType) bool {
	eventName := strings.TrimSpace(string(eventType))
	return eventName == joinTimeoutEvent || eventName == joinCompleteEvent
}

func (pc *PipelineCoordinator) executeNodeContractHandler(
	ctx context.Context,
	node identity.ExecutableNode,
	handler runtimecontracts.SystemNodeEventHandler,
	triggerCtx workflowTriggerContext,
	preview bool,
	collectDiagnosticEmissionsOption ...bool,
) (contractHandlerExecutionResult, error) {
	collectDiagnosticEmissions := len(collectDiagnosticEmissionsOption) > 0 && collectDiagnosticEmissionsOption[0]
	if !node.Valid() {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, nil
	}
	source := pc.SemanticSource()
	handlerFact := MustDeliveryTargetHandler(node)
	flowID := handlerFact.ExecutionFlowID(source)
	if handler.Join != nil {
		if executionFlowID := strings.TrimSpace(pipelineFlowScope(ctx)); executionFlowID != "" {
			flowID = executionFlowID
		}
	}
	entityID := strings.TrimSpace(firstNonEmptyString(
		triggerCtx.State.EntityID,
		workflowEventEntityID(triggerCtx.Event),
	))
	stampedOwner, exactDelivery := stampedDeliveryTargetOwnership(ctx)
	application := DeliveryTargetApplication{}
	if exactDelivery {
		admittedApplication, ok := deliveryTargetApplicationFromContext(ctx)
		if !ok {
			exactHandler := handlerFact.ForEvent(events.EventType(firstNonEmptyString(triggerCtx.HandlerEventKey, string(triggerCtx.Event.Type()))))
			var err error
			if preview {
				admittedApplication, err = pc.prepareDeliveryTargetApplication(ctx, node.Key(), exactHandler, handler, triggerCtx.Event, stampedOwner, triggerCtx.State)
			} else {
				admittedApplication, err = pc.prepareDeliveryTargetApplication(ctx, node.Key(), exactHandler, handler, triggerCtx.Event, stampedOwner)
			}
			if err != nil {
				return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
			}
			ctx = withDeliveryTargetApplication(ctx, admittedApplication)
		}
		if admittedApplication.Owner() != stampedOwner {
			return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, fmt.Errorf("durable handler execution requires its exact delivery target application")
		}
		application = admittedApplication
		if err := application.Validate(); err != nil {
			return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
		}
		flowID = application.FlowID()
		entityID = application.EntityID()
		triggerCtx.Event = application.Event()
		triggerCtx.State = application.State()
	}
	if entityID == "" || (!exactDelivery && strings.TrimSpace(triggerCtx.State.EntityID) != entityID) {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, runtimeengine.ErrUnconstructedWorkflowTarget
	}
	if err := ValidateExecutionHandlerDeclaration(source, node, handler); err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
	}
	terminalRejected, err := terminalStateHandlerRejected(pc, flowID, triggerCtx.State, handler)
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
	}
	if handler.Join == nil && terminalRejected {
		outcome := &handlerExecutionOutcome{
			Status:          HandlerOutcomeTerminalReject,
			GuardsEvaluated: []string{"not_in_terminal_state"},
			RuleSelection:   handlerselection.Resolved(handlerselection.NotApplicable()),
		}
		plan := handlerExecutionPlanFromNodeHandler(source, node, strings.TrimSpace(string(triggerCtx.Event.Type())), handler)
		return contractHandlerExecutionResult{
			Plan:            plan,
			Outcome:         outcome,
			GuardsEvaluated: append([]string{}, outcome.GuardsEvaluated...),
			PreviewMetadata: cloneStringAnyMap(triggerCtx.State.Metadata),
			Handled:         true,
			RuleSelection:   handlerselection.Resolved(handlerselection.NotApplicable()),
		}, nil
	}
	ctx = withPipelineFlowScope(ctx, flowID)
	ctx = runtimecorrelation.WithInboundEvent(ctx, triggerCtx.Event)
	ctx = runtimecorrelation.WithHandlerID(ctx, node.Key()+":"+strings.TrimSpace(string(triggerCtx.Event.Type())))
	handlerEventKey := strings.TrimSpace(triggerCtx.HandlerEventKey)
	if handlerEventKey == "" {
		handlerEventKey = workflowNodeHandlerEventKeyForExecution(ctx, source, node, triggerCtx.Event)
	}
	deps := coordinatorEngineDependencies(pc)
	exec, err := runtimeengine.NewExecutor(deps, newCoordinatorEngineEvaluator(pc))
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, fmt.Errorf("build runtime engine: %w", err)
	}
	workflowVersion := ""
	if source != nil {
		workflowVersion = source.WorkflowVersion()
	}
	stateSnapshot, err := handlerExecutionStateSnapshot(handler, entityID, triggerCtx.State, flowID, workflowVersion)
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
	}
	statePath := firstNonEmptyString(triggerCtx.State.Control.FlowPath, triggerCtx.Event.FlowInstance())
	if exactDelivery {
		statePath = application.Route().InstancePath
	}
	stateRoute := application.Route()
	if !exactDelivery {
		stateRoute, err = canonicalHandlerRoute(source, flowID, statePath, triggerCtx.Event)
		if err != nil {
			return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
		}
	}
	producerSource, err := workflowNodeProducerSource(ctx, source, node, flowID, entityID, events.RouteIdentity{
		FlowID: flowID, FlowInstance: stateRoute.InstancePath, EntityID: entityID,
	})
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, fmt.Errorf("admit workflow node producer source: %w", err)
	}
	joinDeclaration, err := workflowJoinDeclarationForExecution(source, triggerCtx.Event, node, handlerEventKey, handler)
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
	}
	result, err := exec.Execute(ctx, runtimeengine.ExecutionRequest{
		EntityID:        identity.NormalizeEntityID(entityID),
		Node:            node,
		ExecutionFlowID: identity.NormalizeFlowID(flowID),
		Route:           stateRoute,
		Event:           triggerCtx.Event,
		ProducerSource:  producerSource,
		HandlerEventKey: handlerEventKey,
		JoinDeclaration: joinDeclaration,
		ChainDepth:      triggerCtx.Event.ChainDepth(),
		Handler:         handler,
		FanOutPlans:     source.FanOutPlansForHandler(node, handlerEventKey),
		Preview:         preview,
		State:           stateSnapshot,
	})
	if !preview {
		logComputeModuleReplayEvidence(ctx, pc.bus, node.Key(), triggerCtx.Event, result.ComputeModuleTraces)
		logLoopExecution(ctx, pc.bus, node.Key(), triggerCtx.Event, result.LoopTrace)
	}
	if err != nil && !result.Committed {
		return contractHandlerExecutionResult{
			SettledDeliveryClaim: result.SettledDeliveryClaim,
			Handled:              runtimeengine.IsHandledOutcome(result.Status),
			RuleSelection:        result.HandlerRuleSelection,
			Transition:           result.StateMutation.Transition,
		}, err
	}
	previewMetadata := previewMetadataAfterExecution(stateSnapshot, result.StateMutation)
	followUp := handlerCommittedFollowUp{}
	if result.Committed {
		followUp = handlerCommittedFollowUp{
			Emissions:        copyCommittedIntents(result.EmitIntents),
			ActivityIntents:  append([]runtimeengine.ActivityIntent(nil), result.ActivityIntents...),
			ActivityRequests: copyCommittedIntents(result.ActivityRequestIntents),
		}
	}
	diagnostics := &pipelineEmissionPlan{}
	if !preview {
		pc.recordInterceptedEmitDeadLetters(ctx, triggerCtx.Event, node.Key(), handlerOutcomeFromExecutionResult(result), emissionPlanWhen(collectDiagnosticEmissions, diagnostics))
	}
	handled := runtimeengine.IsHandledOutcome(result.Status)
	if result.Status == runtimeengine.OutcomeUnknown {
		return contractHandlerExecutionResult{
			Committed:            result.Committed,
			CommittedStage:       result.CommittedStage,
			Handled:              handled,
			FollowUp:             followUp,
			DiagnosticEmissions:  diagnostics.immutableEvents(),
			SettledDeliveryClaim: result.SettledDeliveryClaim,
			RuleSelection:        result.HandlerRuleSelection,
			Transition:           result.StateMutation.Transition,
		}, err
	}
	outcome := handlerOutcomeFromExecutionResult(result)
	plan := handlerExecutionPlanFromNodeHandler(source, node, strings.TrimSpace(string(triggerCtx.Event.Type())), handler)
	plan.AdvancesTo = firstNonEmptyString(outcome.AdvancesTo, plan.AdvancesTo)
	if len(outcome.Emits) > 0 {
		plan.EmitEvents = append([]string{}, outcome.Emits...)
		if len(outcome.Emits) == 1 {
			plan.Emit.Event = strings.TrimSpace(outcome.Emits[0])
		}
	}
	if outcome.SetsGate != "" {
		plan.SetsGate = outcome.SetsGate
	}
	plan.DataAccumulation = outcome.DataAccumulation
	return contractHandlerExecutionResult{
		Committed:            result.Committed,
		CommittedStage:       result.CommittedStage,
		Plan:                 plan,
		Outcome:              outcome,
		GuardsEvaluated:      append([]string{}, outcome.GuardsEvaluated...),
		PreviewMetadata:      previewMetadata,
		FollowUp:             followUp,
		DiagnosticEmissions:  diagnostics.immutableEvents(),
		SettledDeliveryClaim: result.SettledDeliveryClaim,
		Handled:              handled,
		RuleSelection:        result.HandlerRuleSelection,
		Transition:           result.StateMutation.Transition,
	}, err
}

func emissionPlanWhen(enabled bool, plan *pipelineEmissionPlan) *pipelineEmissionPlan {
	if !enabled {
		return nil
	}
	return plan
}

func logLoopExecution(ctx context.Context, bus Bus, nodeID string, evt events.Event, trace *runtimeengine.LoopExecutionTrace) {
	if bus == nil || trace == nil {
		return
	}
	_ = bus.LogRuntime(ctx, RuntimeLogEntry{
		Level: "info", Message: "Workflow loop operation committed", Component: strings.TrimSpace(nodeID),
		Action: "workflow_loop_" + strings.TrimSpace(trace.Operation), EventID: strings.TrimSpace(evt.ID()),
		EventType: strings.TrimSpace(string(evt.Type())), EntityID: workflowEventEntityID(evt), Detail: trace,
	})
}

func previewMetadataAfterExecution(snapshot runtimeengine.StateSnapshot, mutation runtimeengine.StateMutation) map[string]any {
	carrier := snapshot.StateCarrier
	if mutation.StateCarrier.Fields != nil {
		carrier.Fields = cloneStringAnyMap(mutation.StateCarrier.Fields)
	}
	if mutation.StateCarrier.Bookkeeping != nil {
		carrier.Bookkeeping = cloneStringAnyMap(mutation.StateCarrier.Bookkeeping)
	}
	if len(mutation.StateCarrier.Gates) > 0 {
		carrier.Gates = workflowCloneBoolMap(mutation.StateCarrier.Gates)
	}
	return carrier.PersistedFields()
}

func handlerOutcomeFromExecutionResult(result runtimeengine.ExecutionResult) *handlerExecutionOutcome {
	out := &handlerExecutionOutcome{
		Status:           handlerOutcomeStatusFromEngine(result.Status),
		GuardsEvaluated:  append([]string{}, result.GuardsEvaluated...),
		ActionsExecuted:  append([]string{}, result.ActionsExecuted...),
		AdvancesTo:       strings.TrimSpace(result.NextState),
		SetsGate:         strings.TrimSpace(result.SetsGate),
		ClearGates:       append([]string{}, result.ClearGates...),
		DataAccumulation: result.StateMutation.DataAccumulation,
		RuleSelection:    result.HandlerRuleSelection,
		FanOutCount:      result.FanOutCount,
		Computed:         cloneStringAnyMap(result.Computed),
		InterceptedEmits: append([]runtimeengine.EmitIntent(nil), result.DeadLetterIntents...),
	}
	if len(result.EmitIntents) > 0 {
		out.Emits = make([]string, 0, len(result.EmitIntents))
		for _, intent := range result.EmitIntents {
			if eventType := strings.TrimSpace(string(intent.Event.Type())); eventType != "" {
				out.Emits = append(out.Emits, eventType)
			}
		}
	}
	return out
}

func handlerOutcomeStatusFromEngine(status runtimeengine.OutcomeStatus) HandlerOutcomeStatus {
	switch status {
	case runtimeengine.OutcomeCompleted:
		return HandlerOutcomeCompleted
	case runtimeengine.OutcomeBlocked:
		return HandlerOutcomeBlocked
	case runtimeengine.OutcomeDiscarded:
		return HandlerOutcomeDiscarded
	case runtimeengine.OutcomeRejected:
		return HandlerOutcomeRejected
	case runtimeengine.OutcomeKilled:
		return HandlerOutcomeKilled
	case runtimeengine.OutcomeEscalated:
		return HandlerOutcomeEscalated
	case runtimeengine.OutcomeWaiting:
		return HandlerOutcomeWaiting
	case runtimeengine.OutcomeFannedOut:
		return HandlerOutcomeFannedOut
	default:
		return HandlerOutcomeCompleted
	}
}

func terminalStateHandlerRejected(pc *PipelineCoordinator, flowID string, state WorkflowState, _ runtimecontracts.SystemNodeEventHandler) (bool, error) {
	if pc == nil || pc.SemanticSource() == nil || state.Stage == "" {
		return false, nil
	}
	graph, ok := semanticview.WorkflowStageTopology(pc.SemanticSource(), flowID)
	if !ok || graph.FlowID != flowID {
		return false, fmt.Errorf("selected flow %q has no exact compiled stage topology", flowID)
	}
	ref, err := graph.ResolveStoredStage(string(state.Stage))
	if err != nil {
		return false, err
	}
	return ref.IsFinal(), nil
}
