package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
	runtimecorrelation "swarm/internal/runtime/correlation"
	runtimeengine "swarm/internal/runtime/engine"
	"swarm/internal/runtime/semanticview"
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
	Transition      WorkflowTransition
	Plan            handlerExecutionPlan
	Outcome         *handlerExecutionOutcome
	GuardsEvaluated []string
	Handled         bool
}

func (pc *PipelineCoordinator) executeAuthoritativeNodeHandler(ctx context.Context, evt events.Event, triggerCtx workflowTriggerContext) (contractHandlerExecutionResult, error) {
	source := pc.SemanticSource()
	if pc == nil || source == nil {
		return contractHandlerExecutionResult{}, nil
	}
	trigger := strings.TrimSpace(string(evt.Type))
	if trigger == "" {
		return contractHandlerExecutionResult{}, nil
	}
	owners := source.RuntimeEventOwners(trigger)
	if len(owners) == 0 && !isAccumulationTimeoutEvent(events.EventType(trigger)) {
		return contractHandlerExecutionResult{}, nil
	}
	var (
		nodeID  string
		handler runtimecontracts.SystemNodeEventHandler
		matched bool
	)
	for _, owner := range owners {
		candidate, ok := source.NodeEventHandler(owner, trigger)
		if !ok {
			continue
		}
		if matched {
			return contractHandlerExecutionResult{}, nil
		}
		nodeID = strings.TrimSpace(owner)
		handler = candidate
		matched = true
	}
	if !matched && isAccumulationTimeoutEvent(events.EventType(trigger)) {
		payload := parsePayloadMap(evt.Payload)
		hintNodeID := strings.TrimSpace(asString(payload["node_id"]))
		if hintNodeID != "" {
			timeoutHandler, ok := findAccumulationTimeoutHandlerForNode(source, hintNodeID, trigger)
			if ok {
				nodeID = hintNodeID
				handler = timeoutHandler
				matched = true
			}
		}
		if !matched {
			timeoutNodeID, timeoutHandler, ok := findAccumulationTimeoutHandler(source, trigger)
			if ok {
				nodeID = timeoutNodeID
				handler = timeoutHandler
				matched = true
			}
		}
	}
	if !matched {
		return contractHandlerExecutionResult{}, nil
	}
	return pc.executeNodeContractHandler(ctx, nodeID, handler, triggerCtx, false)
}

func isAccumulationTimeoutEvent(eventType events.EventType) bool {
	eventName := strings.TrimSpace(string(eventType))
	return strings.HasSuffix(eventName, ".timeout") || strings.EqualFold(eventName, "accumulate.timeout")
}

func findAccumulationTimeoutHandler(source interface {
	NodeEntries() map[string]runtimecontracts.SystemNodeContract
	NodeEventHandlers(nodeID string) map[string]runtimecontracts.SystemNodeEventHandler
}, trigger string) (string, runtimecontracts.SystemNodeEventHandler, bool) {
	trigger = strings.TrimSpace(trigger)
	if source == nil || trigger == "" {
		return "", runtimecontracts.SystemNodeEventHandler{}, false
	}
	var (
		nodeID  string
		handler runtimecontracts.SystemNodeEventHandler
		matched bool
	)
	for candidateNodeID, node := range source.NodeEntries() {
		if !containsString(node.SubscribesTo, trigger) {
			continue
		}
		for _, candidate := range source.NodeEventHandlers(candidateNodeID) {
			if candidate.Accumulate == nil {
				continue
			}
			if candidate.Accumulate.Completion.Mode != runtimecontracts.AccumulateModeTimeout && candidate.Accumulate.OnTimeout == nil {
				continue
			}
			if matched {
				return "", runtimecontracts.SystemNodeEventHandler{}, false
			}
			nodeID = strings.TrimSpace(candidateNodeID)
			handler = candidate
			matched = true
			break
		}
	}
	if !matched {
		return "", runtimecontracts.SystemNodeEventHandler{}, false
	}
	return nodeID, handler, true
}

func findAccumulationTimeoutHandlerForNode(source interface {
	NodeEntries() map[string]runtimecontracts.SystemNodeContract
	NodeEventHandlers(nodeID string) map[string]runtimecontracts.SystemNodeEventHandler
}, nodeID, trigger string) (runtimecontracts.SystemNodeEventHandler, bool) {
	nodeID = strings.TrimSpace(nodeID)
	trigger = strings.TrimSpace(trigger)
	if source == nil || nodeID == "" || trigger == "" {
		return runtimecontracts.SystemNodeEventHandler{}, false
	}
	node, ok := source.NodeEntries()[nodeID]
	if !ok || !containsString(node.SubscribesTo, trigger) {
		return runtimecontracts.SystemNodeEventHandler{}, false
	}
	for _, candidate := range source.NodeEventHandlers(nodeID) {
		if candidate.Accumulate == nil {
			continue
		}
		if candidate.Accumulate.Completion.Mode != runtimecontracts.AccumulateModeTimeout && candidate.Accumulate.OnTimeout == nil {
			continue
		}
		return candidate, true
	}
	return runtimecontracts.SystemNodeEventHandler{}, false
}

func (pc *PipelineCoordinator) executeNodeContractHandler(
	ctx context.Context,
	nodeID string,
	handler runtimecontracts.SystemNodeEventHandler,
	triggerCtx workflowTriggerContext,
	preview bool,
) (contractHandlerExecutionResult, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return contractHandlerExecutionResult{}, nil
	}
	flowID := workflowNodeFlowID(pc.SemanticSource(), nodeID)
	entityID := strings.TrimSpace(firstNonEmptyString(
		workflowEventEntityID(triggerCtx.Event),
		triggerCtx.State.EntityID,
	))
	originalEntityID := entityID
	originalStateEntityID := strings.TrimSpace(triggerCtx.State.EntityID)
	source := pc.SemanticSource()
	entityID, triggerCtx.Event = resolveHandlerEntityIDForFlow(source, flowID, handler, entityID, triggerCtx.Event, &triggerCtx.State)
	if !handler.CreateEntity && entityID != "" && originalStateEntityID != "" && originalStateEntityID != entityID {
		triggerCtx.State = pc.currentWorkflowState(ctx, entityID)
		if strings.TrimSpace(triggerCtx.State.EntityID) == "" {
			triggerCtx.State.EntityID = entityID
		}
	}
	if !handler.CreateEntity && entityID != "" && originalEntityID != "" && originalEntityID != entityID && strings.TrimSpace(triggerCtx.State.EntityID) == "" {
		triggerCtx.State.EntityID = entityID
	}
	if terminalStateHandlerRejected(pc, triggerCtx.State, handler) {
		outcome := &handlerExecutionOutcome{
			Status:          HandlerOutcomeTerminalReject,
			GuardsEvaluated: []string{"not_in_terminal_state"},
		}
		plan := handlerExecutionPlanFromNodeHandler(nodeID, strings.TrimSpace(string(triggerCtx.Event.Type)), handler)
		return contractHandlerExecutionResult{
			Transition:      workflowTransitionFromHandlerOutcome(triggerCtx.State, nodeID, strings.TrimSpace(string(triggerCtx.Event.Type)), outcome),
			Plan:            plan,
			Outcome:         outcome,
			GuardsEvaluated: append([]string{}, outcome.GuardsEvaluated...),
			Handled:         true,
		}, nil
	}
	var (
		parentEventCollector *[]events.Event
		collectLocally       bool
		collectedIntents     *[]runtimeengine.EmitIntent
	)
	ctx, parentEventCollector, collectedIntents, collectLocally = pipelineCollectorExecutionContext(ctx)
	ctx = withPipelineFlowScope(ctx, flowID)
	ctx = runtimecorrelation.WithInboundEvent(ctx, triggerCtx.Event)
	ctx = runtimecorrelation.WithHandlerID(ctx, strings.TrimSpace(nodeID)+":"+strings.TrimSpace(string(triggerCtx.Event.Type)))
	deps := coordinatorEngineDependencies(pc)
	if collectLocally {
		deps.Outbox = noOpEngineOutbox{}
	}
	exec, err := runtimeengine.NewExecutor(deps, newCoordinatorEngineEvaluator(pc))
	if err != nil {
		return contractHandlerExecutionResult{}, fmt.Errorf("build runtime engine: %w", err)
	}
	result, err := exec.Execute(ctx, runtimeengine.ExecutionRequest{
		EntityID:   identity.NormalizeEntityID(entityID),
		NodeID:     identity.NormalizeNodeID(nodeID),
		FlowID:     identity.NormalizeFlowID(flowID),
		Event:      triggerCtx.Event,
		ChainDepth: triggerCtx.Event.ChainDepth,
		Handler:    handler,
		Preview:    preview,
		State:      handlerExecutionStateSnapshot(handler, entityID, triggerCtx.State),
	})
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	if !preview {
		pc.recordInterceptedEmitDeadLetters(ctx, triggerCtx.Event, nodeID, handlerOutcomeFromExecutionResult(result))
	}
	if err := pc.reconcileAccumulationTimeoutSchedule(ctx, entityID, nodeID, handler, triggerCtx.Event, result.StateMutation.StateBuckets, result.Status == runtimeengine.OutcomeWaiting); err != nil {
		return contractHandlerExecutionResult{}, err
	}
	if collectLocally {
		flushCollectedPipelineEmitIntents(parentEventCollector, collectedIntents)
	}
	if result.Status == runtimeengine.OutcomeUnknown {
		return contractHandlerExecutionResult{Handled: false}, nil
	}
	outcome := handlerOutcomeFromExecutionResult(result)
	plan := handlerExecutionPlanFromNodeHandler(nodeID, strings.TrimSpace(string(triggerCtx.Event.Type)), handler)
	plan.AdvancesTo = firstNonEmptyString(outcome.AdvancesTo, plan.AdvancesTo)
	if len(outcome.Emits) > 0 {
		plan.EmitEvents = append([]string{}, outcome.Emits...)
		plan.Emits = strings.TrimSpace(outcome.Emits[0])
	}
	if outcome.SetsGate != "" {
		plan.SetsGate = outcome.SetsGate
	}
	plan.DataAccumulation = outcome.DataAccumulation
	return contractHandlerExecutionResult{
		Transition:      workflowTransitionFromHandlerOutcome(triggerCtx.State, nodeID, strings.TrimSpace(string(triggerCtx.Event.Type)), outcome),
		Plan:            plan,
		Outcome:         outcome,
		GuardsEvaluated: append([]string{}, outcome.GuardsEvaluated...),
		Handled:         true,
	}, nil
}

func resolveHandlerEntityIDForFlow(
	source semanticview.Source,
	flowID string,
	handler runtimecontracts.SystemNodeEventHandler,
	entityID string,
	evt events.Event,
	state *WorkflowState,
) (string, events.Event) {
	entityID = strings.TrimSpace(entityID)
	if handler.CreateEntity {
		sourceEntityID := strings.TrimSpace(firstNonEmptyString(entityID, evt.EntityID()))
		instanceID := uuid.NewString()
		newEntityID := instanceID
		flowPath := ""
		if strings.TrimSpace(flowID) != "" {
			flowPath = strings.TrimSpace(DeriveFlowInstancePath(source, flowID, instanceID))
			if flowPath != "" {
				newEntityID = FlowInstanceEntityID(flowPath)
			}
		}
		subjectID := ""
		if state != nil && state.Metadata != nil {
			subjectID = strings.TrimSpace(asString(state.Metadata["subject_id"]))
		}
		if subjectID == "" {
			subjectID = sourceEntityID
		}
		if subjectID == "" {
			subjectID = newEntityID
		}
		entityID = newEntityID
		if state != nil {
			state.EntityID = entityID
			state.Stage = ""
			state.Status = ""
			state.Metadata = map[string]any{"subject_id": subjectID}
			if flowPath != "" {
				state.Metadata["flow_path"] = flowPath
				state.Metadata["storage_ref"] = flowPath
			}
			state.Metadata["instance_id"] = instanceID
			if sourceEntityID != "" {
				state.Metadata["parent_entity_id"] = sourceEntityID
			}
		}
		return entityID, evt
	}
	entityID, evt = ensureHandlerEntityID(source, handler, entityID, evt)
	if flowID != "" && state != nil {
		currentFlowPath := strings.Trim(strings.TrimSpace(flowID), "/")
		if source != nil {
			if resolved := strings.Trim(strings.TrimSpace(source.FlowPath(flowID)), "/"); resolved != "" {
				currentFlowPath = resolved
			}
		}
		inboundFlowPath := strings.Trim(strings.TrimSpace(asString(state.Metadata["flow_path"])), "/")
		if currentFlowPath != "" && inboundFlowPath != "" && inboundFlowPath != currentFlowPath &&
			strings.HasPrefix(inboundFlowPath, currentFlowPath+"/") {
			remainder := strings.Trim(strings.TrimPrefix(inboundFlowPath, currentFlowPath+"/"), "/")
			if strings.Contains(remainder, "/") {
				if parentEntityID := strings.TrimSpace(asString(state.Metadata["parent_entity_id"])); parentEntityID != "" {
					entityID = parentEntityID
					state.EntityID = parentEntityID
				}
			}
		}
	}
	if strings.TrimSpace(flowID) == "" && state != nil {
		subjectID := strings.TrimSpace(asString(state.Metadata["subject_id"]))
		if subjectID != "" && subjectID != entityID {
			entityID = subjectID
			state.EntityID = subjectID
		}
	}
	if state != nil && strings.TrimSpace(state.EntityID) == "" {
		state.EntityID = entityID
	}
	return entityID, evt
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
			if eventType := strings.TrimSpace(string(intent.Event.Type)); eventType != "" {
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

func terminalStateHandlerRejected(pc *PipelineCoordinator, state WorkflowState, _ runtimecontracts.SystemNodeEventHandler) bool {
	if pc == nil {
		return false
	}
	currentState := strings.TrimSpace(string(state.Stage))
	if currentState == "" {
		return false
	}
	workflow := pc.WorkflowDefinition()
	if workflow != nil {
		if stage, ok := workflow.Stage(state.Stage); ok {
			return stage.Terminal
		}
	}
	source := pc.SemanticSource()
	if source == nil {
		return false
	}
	for _, terminal := range source.WorkflowTerminalStages() {
		if strings.EqualFold(strings.TrimSpace(terminal), currentState) {
			return true
		}
	}
	return false
}

func workflowTransitionFromHandlerOutcome(state WorkflowState, nodeID, eventType string, outcome *handlerExecutionOutcome) WorkflowTransition {
	target := strings.TrimSpace(string(state.Stage))
	if outcome != nil && strings.TrimSpace(outcome.AdvancesTo) != "" {
		target = strings.TrimSpace(outcome.AdvancesTo)
	}
	transition := WorkflowTransition{
		Name:    strings.TrimSpace(nodeID) + ":" + strings.TrimSpace(eventType),
		From:    []WorkflowStateID{NormalizeWorkflowStateID(string(state.Stage))},
		To:      NormalizeWorkflowStateID(target),
		Trigger: strings.TrimSpace(eventType),
		Node:    strings.TrimSpace(nodeID),
	}
	if outcome != nil {
		transition.DataAccumulation = outcome.DataAccumulation
	}
	return transition
}
