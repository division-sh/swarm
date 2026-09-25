package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
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
	Committed                 bool
	Plan                      handlerExecutionPlan
	Outcome                   *handlerExecutionOutcome
	GuardsEvaluated           []string
	PreviewMetadata           map[string]any
	InitialValuesMaterialized map[string]any
	FollowUp                  handlerCommittedFollowUp
	DiagnosticEmissions       []events.Event
	SettledDeliveryClaim      *runtimedelivery.Claim
	Handled                   bool
	RuleSelection             handlerselection.Observation
	Transition                *workflowlifecycle.Transition
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
	originalEntityID := entityID
	originalStateEntityID := strings.TrimSpace(triggerCtx.State.EntityID)
	if !exactDelivery {
		handlerEvent := events.EventType(firstNonEmptyString(triggerCtx.HandlerEventKey, string(triggerCtx.Event.Type())))
		resolvedEntityID, resolvedEvent, err := resolveHandlerEntityIDForFlowAtNode(source, node, handlerEvent, flowID, handler, entityID, triggerCtx.Event, &triggerCtx.State)
		if err != nil {
			return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
		}
		entityID, triggerCtx.Event = resolvedEntityID, resolvedEvent
	}
	if !exactDelivery && !handler.CreateEntity && entityID != "" && originalStateEntityID != "" && originalStateEntityID != entityID {
		stateRoute, err := canonicalHandlerRoute(
			source,
			flowID,
			firstNonEmptyString(triggerCtx.State.Control.FlowPath, triggerCtx.Event.FlowInstance()),
			triggerCtx.Event,
		)
		if err != nil {
			return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
		}
		flowOwner, err := runtimeflowidentity.NewRunScopedFlowInstance(triggerCtx.Event.RunID(), stateRoute)
		if err != nil {
			return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
		}
		currentState, err := pc.currentWorkflowState(ctx, flowOwner, identity.NormalizeEntityID(entityID))
		if err != nil {
			return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
		}
		triggerCtx.State = currentState
		if strings.TrimSpace(triggerCtx.State.EntityID) == "" {
			triggerCtx.State.EntityID = entityID
		}
	}
	if !exactDelivery && !handler.CreateEntity && entityID != "" && originalEntityID != "" && originalEntityID != entityID && strings.TrimSpace(triggerCtx.State.EntityID) == "" {
		triggerCtx.State.EntityID = entityID
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
	initialFieldValues := map[string]any(nil)
	if handler.CreateEntity {
		initialFieldValues = workflowEntitySchemaInitialValues(source, flowID)
	}
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
	stateRoute, err := canonicalHandlerRoute(
		source,
		flowID,
		statePath,
		triggerCtx.Event,
	)
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
	}
	producerSource, err := workflowNodeProducerSource(ctx, source, node, flowID, entityID, triggerCtx.Event.RoutingSource())
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, fmt.Errorf("admit workflow node producer source: %w", err)
	}
	joinDeclaration, err := workflowJoinDeclarationForExecution(source, triggerCtx.Event, node, handlerEventKey, handler)
	if err != nil {
		return contractHandlerExecutionResult{RuleSelection: handlerselection.NotReached()}, err
	}
	result, err := exec.Execute(ctx, runtimeengine.ExecutionRequest{
		EntityID:           identity.NormalizeEntityID(entityID),
		Node:               node,
		ExecutionFlowID:    identity.NormalizeFlowID(flowID),
		Route:              stateRoute,
		Event:              triggerCtx.Event,
		ProducerSource:     producerSource,
		HandlerEventKey:    handlerEventKey,
		JoinDeclaration:    joinDeclaration,
		ChainDepth:         triggerCtx.Event.ChainDepth(),
		Handler:            handler,
		FanOutPlans:        source.FanOutPlansForHandler(node, handlerEventKey),
		Preview:            preview,
		State:              stateSnapshot,
		InitialFieldValues: initialFieldValues,
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
	if handler.CreateEntity && result.StateMutation.StateCarrier.Fields == nil {
		result.StateMutation.StateCarrier.Fields = cloneStringAnyMap(stateSnapshot.StateCarrier.Fields)
	}
	previewMetadata := previewMetadataAfterExecution(stateSnapshot, result.StateMutation)
	initialValuesMaterialized := map[string]any(nil)
	if handler.CreateEntity {
		initialValuesMaterialized = workflowEntitySchemaInitialValues(source, flowID)
	}
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
		Committed:                 result.Committed,
		Plan:                      plan,
		Outcome:                   outcome,
		GuardsEvaluated:           append([]string{}, outcome.GuardsEvaluated...),
		PreviewMetadata:           previewMetadata,
		InitialValuesMaterialized: initialValuesMaterialized,
		FollowUp:                  followUp,
		DiagnosticEmissions:       diagnostics.immutableEvents(),
		SettledDeliveryClaim:      result.SettledDeliveryClaim,
		Handled:                   handled,
		RuleSelection:             result.HandlerRuleSelection,
		Transition:                result.StateMutation.Transition,
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

func resolveHandlerEntityIDForFlow(
	source semanticview.Source,
	flowID string,
	handler runtimecontracts.SystemNodeEventHandler,
	entityID string,
	evt events.Event,
	state *WorkflowState,
	targetOwnership ...events.DeliveryTargetOwnership,
) (string, events.Event, error) {
	return resolveHandlerEntityIDForFlowAtNode(source, identity.ExecutableNode{}, evt.Type(), flowID, handler, entityID, evt, state, targetOwnership...)
}

func resolveHandlerEntityIDForFlowAtNode(
	source semanticview.Source,
	node identity.ExecutableNode,
	handlerEvent events.EventType,
	flowID string,
	handler runtimecontracts.SystemNodeEventHandler,
	entityID string,
	evt events.Event,
	state *WorkflowState,
	targetOwnership ...events.DeliveryTargetOwnership,
) (string, events.Event, error) {
	entityID = strings.TrimSpace(entityID)
	if handler.CreateEntity {
		sourceEntityID := strings.TrimSpace(evt.EntityID())
		stampedEntityID := ""
		if len(targetOwnership) > 0 && targetOwnership[0].MaterializingEntity() {
			stampedEntityID = targetOwnership[0].Route().EntityID
		}
		instanceID := canonicalHandlerInstanceID(flowID, evt)
		instance := deriveFlowInstanceIdentity(source, flowID, instanceID)
		if source != nil && strings.TrimSpace(flowID) == strings.TrimSpace(semanticview.RootExecutionFlowID(source)) {
			route, err := canonicalHandlerRoute(source, flowID, "", evt)
			if err != nil {
				return "", evt, err
			}
			instance = FlowInstanceIdentity{Instance: runtimeflowidentity.Instance{
				TemplateID:    strings.TrimSpace(flowID),
				ScopeKey:      route.ScopeKey,
				InstanceID:    route.InstanceID,
				InstancePath:  route.InstancePath,
				EntityID:      runtimeflowidentity.EntityID(route.InstancePath),
				HasStoredPath: true,
			}}
		}
		if !instance.Route().Valid() {
			return "", evt, fmt.Errorf("create_entity requires an exact workflow instance route")
		}
		if stampedEntityID != "" && stampedEntityID != instance.EntityID {
			return "", evt, fmt.Errorf("create_entity stamped target %q disagrees with canonical future entity %q", stampedEntityID, instance.EntityID)
		}
		instance.ParentEntityID = sourceEntityID
		entityID = instance.EntityID
		if state != nil {
			entityType, err := requireWorkflowEntityType(source, flowID)
			if err != nil {
				return "", evt, err
			}
			initialStage, err := workflowInitialStateForFlow(source, flowID)
			if err != nil {
				return "", evt, err
			}
			state.EntityID = entityID
			state.Stage = NormalizeWorkflowStateID(initialStage)
			state.Status = ""
			state.Metadata = workflowCreateEntityFields(source, flowID)
			state.Control = workflowStateControlFromIdentity(instance, entityType)
		}
		envelope := events.EnvelopeForFlowInstance(evt.NormalizedEnvelope(), instance.InstancePath)
		resolved, err := events.ResolveEnvelope(evt, envelope)
		if err != nil {
			return "", evt, fmt.Errorf("carry created workflow instance route: %w", err)
		}
		return entityID, resolved, nil
	}
	var err error
	entityID, evt, err = ensureHandlerEntityIDAtNode(source, node, handlerEvent, flowID, handler, entityID, evt)
	if err != nil {
		return "", evt, err
	}
	if handlerExecutionEntityRequirementForNode(source, node, handlerEvent, flowID, handler).materializes() {
		statePath := ""
		if state != nil {
			statePath = state.Control.FlowPath
		}
		route, routeErr := canonicalHandlerRoute(source, flowID, statePath, evt)
		if routeErr != nil {
			return "", evt, routeErr
		}
		if err := prepareHandlerMaterializationStateAtNode(source, node, handlerEvent, flowID, handler, route, entityID, state); err != nil {
			return "", evt, err
		}
	}
	if state != nil && strings.TrimSpace(state.EntityID) == "" {
		state.EntityID = entityID
	}
	return entityID, evt, nil
}

func canonicalHandlerInstanceID(flowID string, evt events.Event) string {
	if targetInstance := strings.Trim(strings.TrimSpace(evt.TargetRoute().FlowInstance), "/"); targetInstance != "" {
		if idx := strings.LastIndex(targetInstance, "/"); idx >= 0 {
			return strings.TrimSpace(targetInstance[idx+1:])
		}
		return targetInstance
	}
	if flowInstance := strings.Trim(strings.TrimSpace(evt.FlowInstance()), "/"); flowInstance != "" {
		if idx := strings.LastIndex(flowInstance, "/"); idx >= 0 {
			return strings.TrimSpace(flowInstance[idx+1:])
		}
		return flowInstance
	}
	if strings.TrimSpace(flowID) == "" {
		if runID := strings.TrimSpace(evt.RunID()); runID != "" {
			return runID
		}
		return "root"
	}
	flowID = strings.Trim(strings.TrimSpace(flowID), "/")
	if idx := strings.LastIndex(flowID, "/"); idx >= 0 {
		return strings.TrimSpace(flowID[idx+1:])
	}
	return flowID
}

func workflowCreateEntityFields(source semanticview.Source, flowID string) map[string]any {
	fields := workflowEntitySchemaInitialValues(source, flowID)
	if len(fields) == 0 {
		return nil
	}
	return fields
}

func workflowStateControlFromIdentity(instance FlowInstanceIdentity, entityType string) runtimeengine.StateControl {
	return runtimeengine.StateControl{
		FlowPath: instance.InstancePath, StorageRef: instance.InstancePath, InstanceID: instance.InstanceID,
		EntityType:   strings.TrimSpace(entityType),
		ParentFlowID: instance.ParentRoute.FlowID, ParentFlowInstance: instance.ParentRoute.FlowInstance,
		ParentEntityID: instance.ParentEntityID,
	}
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
	if graph.StageCount() == 0 && state.Stage == "pending" {
		return false, nil
	}
	ref, err := graph.ResolveStage(string(state.Stage))
	if err != nil {
		return false, err
	}
	return ref.IsTerminal(), nil
}
