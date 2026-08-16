package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
	RuleID           string
	FanOutCount      int
	Computed         map[string]any
	InterceptedEmits []runtimeengine.EmitIntent
}

type contractHandlerExecutionResult struct {
	Transition                WorkflowTransition
	Plan                      handlerExecutionPlan
	Outcome                   *handlerExecutionOutcome
	GuardsEvaluated           []string
	PreviewMetadata           map[string]any
	InitialValuesMaterialized map[string]any
	Emissions                 []events.Event
	Handled                   bool
}

func (pc *PipelineCoordinator) executeAuthoritativeNodeHandler(ctx context.Context, evt events.Event, triggerCtx workflowTriggerContext) (contractHandlerExecutionResult, error) {
	source := pc.SemanticSource()
	if pc == nil || source == nil {
		return contractHandlerExecutionResult{}, nil
	}
	trigger := strings.TrimSpace(string(evt.Type()))
	if trigger == "" {
		return contractHandlerExecutionResult{}, nil
	}
	var (
		node            identity.ExecutableNode
		handler         runtimecontracts.SystemNodeEventHandler
		handlerEventKey string
		handlerFlowID   string
		matched         bool
	)
	if isJoinLifecycleEvent(events.EventType(trigger)) {
		resolution, ok, err := resolveWorkflowJoinOccurrence(source, evt)
		if err != nil {
			return contractHandlerExecutionResult{}, err
		}
		if ok {
			node = resolution.Ref.Node()
			handler = resolution.Handler
			handlerEventKey = resolution.Ref.HandlerEvent()
			handlerFlowID = resolution.Ref.FlowID()
			if handlerFlowID == "" {
				handlerFlowID = semanticview.RootExecutionFlowID(source)
			}
			matched = true
		}
	} else {
		for _, record := range source.ExecutableNodeRecords() {
			candidate, identityErr := record.Identity()
			if identityErr != nil {
				return contractHandlerExecutionResult{}, identityErr
			}
			resolved := workflowNodeEventHandlerResolutionForDeliveryContext(ctx, source, candidate, evt)
			if resolved.Failure != "" {
				return contractHandlerExecutionResult{}, fmt.Errorf("resolve workflow handler for node %s: %s", candidate.Key(), resolved.Failure)
			}
			if !resolved.Matched {
				continue
			}
			if matched {
				return contractHandlerExecutionResult{}, nil
			}
			node = candidate
			handler = resolved.Handler
			handlerEventKey = resolved.HandlerEventKey
			handlerFlowID = resolved.FlowID
			matched = true
		}
	}
	if !matched {
		return contractHandlerExecutionResult{}, nil
	}
	if strings.TrimSpace(triggerCtx.HandlerEventKey) == "" {
		triggerCtx.HandlerEventKey = handlerEventKey
	}
	ctx = withPipelineFlowScope(ctx, handlerFlowID)
	return pc.executeNodeContractHandler(ctx, node, handler, triggerCtx, false, true)
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
	deferCommittedDispatchOption ...bool,
) (contractHandlerExecutionResult, error) {
	deferCommittedDispatch := len(deferCommittedDispatchOption) > 0 && deferCommittedDispatchOption[0]
	if !node.Valid() {
		return contractHandlerExecutionResult{}, nil
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
	if exactDelivery {
		if targetFlowID := strings.TrimSpace(stampedOwner.Route().FlowID); targetFlowID != "" {
			flowID = targetFlowID
		}
		entityID = stampedOwner.Route().EntityID
		if err := prepareStampedSelectOrCreateState(source, flowID, handler, triggerCtx.Event, stampedOwner, &triggerCtx.State); err != nil {
			return contractHandlerExecutionResult{}, err
		}
	}
	if !exactDelivery && handler.SelectEntity != nil && !handler.SelectEntity.Empty() {
		selected, err := pc.selectHandlerEntityForFlow(ctx, flowID, node.Key(), handler, triggerCtx.Event)
		if err != nil {
			return contractHandlerExecutionResult{}, err
		}
		entityID = selected.EntityID
		triggerCtx.Event = selected.Event
		triggerCtx.State = selected.State
	}
	if !exactDelivery && handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
		selected, err := pc.selectOrCreateHandlerEntityForFlow(ctx, flowID, node.Key(), handler, triggerCtx.Event)
		if err != nil {
			return contractHandlerExecutionResult{}, err
		}
		entityID = selected.EntityID
		triggerCtx.Event = selected.Event
		triggerCtx.State = selected.State
	}
	originalEntityID := entityID
	originalStateEntityID := strings.TrimSpace(triggerCtx.State.EntityID)
	var targetOwnership []events.DeliveryTargetOwnership
	if exactDelivery {
		targetOwnership = append(targetOwnership, stampedOwner)
	}
	resolvedEntityID, resolvedEvent, err := resolveHandlerEntityIDForFlow(source, flowID, handler, entityID, triggerCtx.Event, &triggerCtx.State, targetOwnership...)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	entityID, triggerCtx.Event = resolvedEntityID, resolvedEvent
	if !handler.CreateEntity && entityID != "" && originalStateEntityID != "" && originalStateEntityID != entityID {
		stateRoute, err := canonicalHandlerRoute(
			source,
			flowID,
			firstNonEmptyString(triggerCtx.State.Control.FlowPath, triggerCtx.Event.FlowInstance()),
			triggerCtx.Event,
		)
		if err != nil {
			return contractHandlerExecutionResult{}, err
		}
		currentState, err := pc.currentWorkflowState(ctx, stateRoute, identity.NormalizeEntityID(entityID))
		if err != nil {
			return contractHandlerExecutionResult{}, err
		}
		triggerCtx.State = currentState
		if strings.TrimSpace(triggerCtx.State.EntityID) == "" {
			triggerCtx.State.EntityID = entityID
		}
	}
	if !handler.CreateEntity && entityID != "" && originalEntityID != "" && originalEntityID != entityID && strings.TrimSpace(triggerCtx.State.EntityID) == "" {
		triggerCtx.State.EntityID = entityID
	}
	if handler.Join == nil && terminalStateHandlerRejected(pc, flowID, triggerCtx.State, handler) {
		outcome := &handlerExecutionOutcome{
			Status:          HandlerOutcomeTerminalReject,
			GuardsEvaluated: []string{"not_in_terminal_state"},
		}
		plan := handlerExecutionPlanFromNodeHandler(node, strings.TrimSpace(string(triggerCtx.Event.Type())), handler)
		return contractHandlerExecutionResult{
			Transition:      workflowTransitionFromHandlerOutcome(triggerCtx.State, node, strings.TrimSpace(string(triggerCtx.Event.Type())), outcome),
			Plan:            plan,
			Outcome:         outcome,
			GuardsEvaluated: append([]string{}, outcome.GuardsEvaluated...),
			PreviewMetadata: cloneStringAnyMap(triggerCtx.State.Metadata),
			Handled:         true,
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
		return contractHandlerExecutionResult{}, fmt.Errorf("build runtime engine: %w", err)
	}
	workflowVersion := ""
	if source != nil {
		workflowVersion = source.WorkflowVersion()
	}
	stateSnapshot, err := handlerExecutionStateSnapshot(handler, entityID, triggerCtx.State, flowID, workflowVersion)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	statePath := firstNonEmptyString(triggerCtx.State.Control.FlowPath, triggerCtx.Event.FlowInstance())
	if exactDelivery {
		statePath = stampedOwner.Route().FlowInstance
	}
	stateRoute, err := canonicalHandlerRoute(
		source,
		flowID,
		statePath,
		triggerCtx.Event,
	)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	producerSource, err := workflowNodeProducerSource(ctx, source, node, flowID, entityID, triggerCtx.Event.RoutingSource())
	if err != nil {
		return contractHandlerExecutionResult{}, fmt.Errorf("admit workflow node producer source: %w", err)
	}
	joinDeclaration, err := workflowJoinDeclarationForExecution(source, triggerCtx.Event, node, handlerEventKey, handler)
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	result, err := exec.Execute(ctx, runtimeengine.ExecutionRequest{
		EntityID:               identity.NormalizeEntityID(entityID),
		Node:                   node,
		Route:                  stateRoute,
		Event:                  triggerCtx.Event,
		ProducerSource:         producerSource,
		HandlerEventKey:        handlerEventKey,
		JoinDeclaration:        joinDeclaration,
		ChainDepth:             triggerCtx.Event.ChainDepth(),
		Handler:                handler,
		Preview:                preview,
		State:                  stateSnapshot,
		InitialFieldValues:     initialFieldValues,
		DeferCommittedDispatch: deferCommittedDispatch,
	})
	if !preview {
		logComputeModuleReplayEvidence(ctx, pc.bus, node.Key(), triggerCtx.Event, result.ComputeModuleTraces)
		logLoopExecution(ctx, pc.bus, node.Key(), triggerCtx.Event, result.LoopTrace)
	}
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	if handler.CreateEntity && result.StateMutation.StateCarrier.Fields == nil {
		result.StateMutation.StateCarrier.Fields = cloneStringAnyMap(stateSnapshot.StateCarrier.Fields)
	}
	previewMetadata := previewMetadataAfterExecution(stateSnapshot, result.StateMutation)
	initialValuesMaterialized := map[string]any(nil)
	if handler.CreateEntity {
		initialValuesMaterialized = workflowEntitySchemaInitialValues(source, flowID)
	}
	emissions := &pipelineEmissionPlan{}
	if deferCommittedDispatch {
		emissions.appendIntents(result.EmitIntents)
		immediateActivities := make([]runtimeengine.ActivityIntent, 0, len(result.ActivityIntents))
		for _, intent := range result.ActivityIntents {
			if intent.Normalized().ApprovalDecision == "" {
				immediateActivities = append(immediateActivities, intent)
			}
		}
		activityEmissions, err := activityRequestEmitIntents(immediateActivities)
		if err != nil {
			return contractHandlerExecutionResult{}, err
		}
		emissions.appendIntents(activityEmissions)
	}
	if !preview {
		pc.recordInterceptedEmitDeadLetters(ctx, triggerCtx.Event, node.Key(), handlerOutcomeFromExecutionResult(result), emissionPlanWhen(deferCommittedDispatch, emissions))
	}
	handled := runtimeengine.IsHandledOutcome(result.Status)
	if result.Status == runtimeengine.OutcomeUnknown {
		return contractHandlerExecutionResult{Handled: handled, Emissions: emissions.immutableEvents()}, nil
	}
	outcome := handlerOutcomeFromExecutionResult(result)
	plan := handlerExecutionPlanFromNodeHandler(node, strings.TrimSpace(string(triggerCtx.Event.Type())), handler)
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
		Transition:                workflowTransitionFromHandlerOutcome(triggerCtx.State, node, strings.TrimSpace(string(triggerCtx.Event.Type())), outcome),
		Plan:                      plan,
		Outcome:                   outcome,
		GuardsEvaluated:           append([]string{}, outcome.GuardsEvaluated...),
		PreviewMetadata:           previewMetadata,
		InitialValuesMaterialized: initialValuesMaterialized,
		Emissions:                 emissions.immutableEvents(),
		Handled:                   handled,
	}, nil
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
	entityID = strings.TrimSpace(entityID)
	if handler.CreateEntity {
		sourceEntityID := strings.TrimSpace(evt.EntityID())
		stampedEntityID := ""
		if len(targetOwnership) > 0 && targetOwnership[0].MaterializingEntity() {
			stampedEntityID = targetOwnership[0].Route().EntityID
		}
		instanceID := canonicalHandlerInstanceID(flowID, evt)
		instance := deriveFlowInstanceIdentity(source, flowID, instanceID)
		if source != nil && strings.TrimSpace(flowID) == strings.TrimSpace(source.WorkflowName()) {
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
			state.EntityID = entityID
			state.Stage = NormalizeWorkflowStateID(workflowInitialStateForFlow(source, flowID))
			state.Status = ""
			state.Metadata = workflowCreateEntityFields(source, flowID)
			state.Control = workflowStateControlFromIdentity(instance)
		}
		envelope := events.EnvelopeForFlowInstance(evt.NormalizedEnvelope(), instance.InstancePath)
		resolved, err := events.ResolveEnvelope(evt, envelope)
		if err != nil {
			return "", evt, fmt.Errorf("carry created workflow instance route: %w", err)
		}
		return entityID, resolved, nil
	}
	var err error
	entityID, evt, err = ensureHandlerEntityID(source, flowID, handler, entityID, evt)
	if err != nil {
		return "", evt, err
	}
	if handlerExecutionEntityRequirement(source, flowID, handler).materializes() {
		statePath := ""
		if state != nil {
			statePath = state.Control.FlowPath
		}
		route, routeErr := canonicalHandlerRoute(source, flowID, statePath, evt)
		if routeErr != nil {
			return "", evt, routeErr
		}
		if err := prepareHandlerMaterializationState(source, flowID, handler, route, entityID, state); err != nil {
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

func workflowStateControlFromIdentity(instance FlowInstanceIdentity) runtimeengine.StateControl {
	return runtimeengine.StateControl{
		FlowPath: instance.InstancePath, StorageRef: instance.InstancePath, InstanceID: instance.InstanceID,
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
		RuleID:           strings.TrimSpace(result.RuleID),
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

func terminalStateHandlerRejected(pc *PipelineCoordinator, flowID string, state WorkflowState, _ runtimecontracts.SystemNodeEventHandler) bool {
	if pc == nil {
		return false
	}
	currentState := strings.TrimSpace(string(state.Stage))
	if currentState == "" {
		return false
	}
	source := pc.SemanticSource()
	if source != nil {
		for _, candidateFlowID := range terminalStateFlowCandidates(source, flowID, state) {
			if terminalStageContains(source.FlowTerminalStages(candidateFlowID), currentState) {
				return true
			}
			if stageSetContains(source.FlowStates(candidateFlowID), currentState) {
				return false
			}
		}
	}
	workflow := pc.WorkflowDefinition()
	if workflow != nil {
		if stage, ok := workflow.Stage(state.Stage); ok {
			return stage.Terminal
		}
	}
	return false
}

func terminalStateFlowCandidates(source semanticview.Source, flowID string, state WorkflowState) []string {
	seen := map[string]struct{}{}
	out := []string{}
	add := func(candidate string) {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		out = append(out, candidate)
	}
	add(flowIDForWorkflowState(source, state))
	add(flowID)
	return out
}

func flowIDForWorkflowState(source semanticview.Source, state WorkflowState) string {
	if source == nil {
		return ""
	}
	flowPath := strings.Trim(strings.TrimSpace(state.Control.FlowPath), "/")
	if flowPath == "" {
		return ""
	}
	bestID := ""
	bestLen := -1
	for _, scope := range source.FlowScopes() {
		path := strings.Trim(strings.TrimSpace(scope.Path), "/")
		if path == "" {
			continue
		}
		if flowPath != path && !strings.HasPrefix(flowPath, path+"/") {
			continue
		}
		if len(path) > bestLen {
			bestLen = len(path)
			bestID = strings.TrimSpace(scope.ID)
		}
	}
	return bestID
}

func terminalStageContains(stages []string, current string) bool {
	return stageSetContains(stages, current)
}

func stageSetContains(stages []string, current string) bool {
	current = strings.TrimSpace(current)
	if current == "" {
		return false
	}
	for _, stage := range stages {
		if strings.EqualFold(strings.TrimSpace(stage), current) {
			return true
		}
	}
	return false
}

func workflowTransitionFromHandlerOutcome(state WorkflowState, node identity.ExecutableNode, eventType string, outcome *handlerExecutionOutcome) WorkflowTransition {
	target := strings.TrimSpace(string(state.Stage))
	if outcome != nil && strings.TrimSpace(outcome.AdvancesTo) != "" {
		target = strings.TrimSpace(outcome.AdvancesTo)
	}
	transition := WorkflowTransition{
		Name:    node.Key() + ":" + strings.TrimSpace(eventType),
		From:    []WorkflowStateID{NormalizeWorkflowStateID(string(state.Stage))},
		To:      NormalizeWorkflowStateID(target),
		Trigger: strings.TrimSpace(eventType),
		Node:    node,
	}
	if outcome != nil {
		transition.DataAccumulation = outcome.DataAccumulation
	}
	return transition
}
