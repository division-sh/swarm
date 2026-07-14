package pipeline

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
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
	owners := source.RuntimeEventOwners(trigger)
	if len(owners) == 0 && !isJoinLifecycleEvent(events.EventType(trigger)) {
		return contractHandlerExecutionResult{}, nil
	}
	var (
		nodeID          string
		handler         runtimecontracts.SystemNodeEventHandler
		handlerEventKey string
		matched         bool
	)
	for _, owner := range owners {
		resolved := workflowNodeEventHandlerResolutionForDelivery(source, owner, evt)
		if !resolved.Matched {
			continue
		}
		if matched {
			return contractHandlerExecutionResult{}, nil
		}
		nodeID = strings.TrimSpace(owner)
		handler = resolved.Handler
		handlerEventKey = resolved.HandlerEventKey
		matched = true
	}
	if !matched && isJoinLifecycleEvent(events.EventType(trigger)) {
		if ref, _, ok := timeridentity.ParseJoinRef(parsePayloadMap(evt.Payload())); ok {
			joinHandler, ok := findJoinHandlerForRef(source, ref)
			if ok {
				nodeID = ref.NodeID
				handler = joinHandler
				handlerEventKey = ref.HandlerEvent
				matched = true
			}
		}
	}
	if !matched {
		return contractHandlerExecutionResult{}, nil
	}
	if strings.TrimSpace(triggerCtx.HandlerEventKey) == "" {
		triggerCtx.HandlerEventKey = handlerEventKey
	}
	return pc.executeNodeContractHandler(ctx, nodeID, handler, triggerCtx, false)
}

func isJoinLifecycleEvent(eventType events.EventType) bool {
	eventName := strings.TrimSpace(string(eventType))
	return eventName == joinTimeoutEvent || eventName == joinCompleteEvent
}

func findJoinHandlerForRef(source interface {
	NodeEntries() map[string]runtimecontracts.SystemNodeContract
	NodeEventHandlers(nodeID string) map[string]runtimecontracts.SystemNodeEventHandler
}, ref timeridentity.JoinRef) (runtimecontracts.SystemNodeEventHandler, bool) {
	ref = ref.Normalize()
	if source == nil || !ref.Valid() {
		return runtimecontracts.SystemNodeEventHandler{}, false
	}
	if _, ok := source.NodeEntries()[ref.NodeID]; !ok {
		return runtimecontracts.SystemNodeEventHandler{}, false
	}
	handler, ok := source.NodeEventHandlers(ref.NodeID)[ref.HandlerEvent]
	return handler, ok && handler.Join != nil && handler.Join.EffectiveID() == ref.JoinID && strings.TrimSpace(handler.Join.Stage) == ref.Stage
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
	source := pc.SemanticSource()
	if handler.SelectEntity != nil && !handler.SelectEntity.Empty() {
		selected, err := pc.selectHandlerEntityForFlow(ctx, flowID, nodeID, handler, triggerCtx.Event)
		if err != nil {
			return contractHandlerExecutionResult{}, err
		}
		entityID = selected.EntityID
		triggerCtx.Event = selected.Event
		triggerCtx.State = selected.State
	}
	if handler.SelectOrCreateEntity != nil && !handler.SelectOrCreateEntity.Empty() {
		selected, err := pc.selectOrCreateHandlerEntityForFlow(ctx, flowID, nodeID, handler, triggerCtx.Event)
		if err != nil {
			return contractHandlerExecutionResult{}, err
		}
		entityID = selected.EntityID
		triggerCtx.Event = selected.Event
		triggerCtx.State = selected.State
	}
	originalEntityID := entityID
	originalStateEntityID := strings.TrimSpace(triggerCtx.State.EntityID)
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
	if handler.Join == nil && terminalStateHandlerRejected(pc, flowID, triggerCtx.State, handler) {
		outcome := &handlerExecutionOutcome{
			Status:          HandlerOutcomeTerminalReject,
			GuardsEvaluated: []string{"not_in_terminal_state"},
		}
		plan := handlerExecutionPlanFromNodeHandler(nodeID, strings.TrimSpace(string(triggerCtx.Event.Type())), handler)
		return contractHandlerExecutionResult{
			Transition:      workflowTransitionFromHandlerOutcome(triggerCtx.State, nodeID, strings.TrimSpace(string(triggerCtx.Event.Type())), outcome),
			Plan:            plan,
			Outcome:         outcome,
			GuardsEvaluated: append([]string{}, outcome.GuardsEvaluated...),
			PreviewMetadata: cloneStringAnyMap(triggerCtx.State.Metadata),
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
	ctx = runtimecorrelation.WithHandlerID(ctx, strings.TrimSpace(nodeID)+":"+strings.TrimSpace(string(triggerCtx.Event.Type())))
	if handler.CreateEntity {
		ctx = withWorkflowCreateEntityInitialValues(ctx, workflowEntitySchemaInitialValues(source, flowID))
	}
	handlerEventKey := strings.TrimSpace(triggerCtx.HandlerEventKey)
	if handlerEventKey == "" {
		handlerEventKey = workflowNodeHandlerEventKeyForExecution(source, nodeID, triggerCtx.Event)
	}
	deps := coordinatorEngineDependencies(pc)
	if collectLocally {
		deps.Outbox = noOpEngineOutbox{}
	}
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
	producerRoute := actionResultProducerRoute(source, flowID, entityID, triggerCtx.Event, stateSnapshot, triggerCtx.Event.TargetRoute())
	result, err := exec.Execute(ctx, runtimeengine.ExecutionRequest{
		EntityID:        identity.NormalizeEntityID(entityID),
		NodeID:          identity.NormalizeNodeID(nodeID),
		FlowID:          identity.NormalizeFlowID(flowID),
		Event:           triggerCtx.Event,
		ProducerRoute:   producerRoute,
		HandlerEventKey: handlerEventKey,
		ChainDepth:      triggerCtx.Event.ChainDepth(),
		Handler:         handler,
		Preview:         preview,
		State:           stateSnapshot,
	})
	if !preview {
		logComputeModuleReplayEvidence(ctx, pc.bus, nodeID, triggerCtx.Event, result.ComputeModuleTraces)
		logLoopExecution(ctx, pc.bus, nodeID, triggerCtx.Event, result.LoopTrace)
	}
	if err != nil {
		return contractHandlerExecutionResult{}, err
	}
	if handler.CreateEntity && result.StateMutation.StateCarrier.Metadata == nil {
		result.StateMutation.StateCarrier.Metadata = cloneStringAnyMap(stateSnapshot.StateCarrier.Metadata)
	}
	previewMetadata := previewMetadataAfterExecution(stateSnapshot, result.StateMutation)
	initialValuesMaterialized := map[string]any(nil)
	if handler.CreateEntity {
		initialValuesMaterialized = workflowEntitySchemaInitialValues(source, flowID)
	}
	if !preview {
		pc.recordInterceptedEmitDeadLetters(ctx, triggerCtx.Event, nodeID, handlerOutcomeFromExecutionResult(result))
	}
	if collectLocally {
		flushCollectedPipelineEmitIntents(parentEventCollector, collectedIntents)
	}
	if result.Status == runtimeengine.OutcomeUnknown {
		return contractHandlerExecutionResult{Handled: false}, nil
	}
	outcome := handlerOutcomeFromExecutionResult(result)
	plan := handlerExecutionPlanFromNodeHandler(nodeID, strings.TrimSpace(string(triggerCtx.Event.Type())), handler)
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
		Transition:                workflowTransitionFromHandlerOutcome(triggerCtx.State, nodeID, strings.TrimSpace(string(triggerCtx.Event.Type())), outcome),
		Plan:                      plan,
		Outcome:                   outcome,
		GuardsEvaluated:           append([]string{}, outcome.GuardsEvaluated...),
		PreviewMetadata:           previewMetadata,
		InitialValuesMaterialized: initialValuesMaterialized,
		Handled:                   true,
	}, nil
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
) (string, events.Event) {
	entityID = strings.TrimSpace(entityID)
	if handler.CreateEntity {
		sourceEntityID := strings.TrimSpace(firstNonEmptyString(entityID, evt.EntityID()))
		instanceID := canonicalHandlerInstanceID(flowID, evt)
		instance := deriveFlowInstanceIdentity(source, flowID, instanceID)
		instance.ParentEntityID = sourceEntityID
		entityID = instance.EntityID
		if state != nil {
			state.EntityID = entityID
			state.Stage = NormalizeWorkflowStateID(workflowInitialStateForFlow(source, flowID))
			state.Status = ""
			state.Metadata = workflowCreateEntityMetadata(source, flowID, instance)
		}
		return entityID, evt
	}
	entityID, evt = ensureHandlerEntityID(source, flowID, handler, entityID, evt)
	if state != nil && handlerMaterializesEntity(source, flowID, handler) {
		state.Metadata = workflowMaterializeEntityMetadata(source, flowID, state.Metadata)
	}
	if state != nil && strings.TrimSpace(state.EntityID) == "" {
		state.EntityID = entityID
	}
	return entityID, evt
}

func canonicalHandlerInstanceID(flowID string, evt events.Event) string {
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

func workflowCreateEntityMetadata(source semanticview.Source, flowID string, instance FlowInstanceIdentity) map[string]any {
	metadata := workflowEntitySchemaInitialValues(source, flowID)
	if metadata == nil {
		metadata = map[string]any{}
	}
	if contract, ok := workflowEntityContract(source, flowID); ok {
		if entityType := strings.TrimSpace(contract.EntityType); entityType != "" {
			metadata["entity_type"] = entityType
		}
	}
	if instance.InstancePath != "" {
		metadata["flow_path"] = instance.InstancePath
		metadata["storage_ref"] = instance.InstancePath
	}
	if instance.InstanceID != "" {
		metadata["instance_id"] = instance.InstanceID
	}
	if instance.ParentEntityID != "" {
		metadata["parent_entity_id"] = instance.ParentEntityID
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func previewMetadataAfterExecution(snapshot runtimeengine.StateSnapshot, mutation runtimeengine.StateMutation) map[string]any {
	carrier := snapshot.StateCarrier
	if mutation.StateCarrier.Metadata != nil {
		carrier.Metadata = cloneStringAnyMap(mutation.StateCarrier.Metadata)
	}
	if len(mutation.StateCarrier.Gates) > 0 {
		carrier.Gates = workflowCloneBoolMap(mutation.StateCarrier.Gates)
	}
	return carrier.PersistedMetadata()
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
	if source == nil || len(state.Metadata) == 0 {
		return ""
	}
	flowPath := strings.Trim(strings.TrimSpace(asString(state.Metadata["flow_path"])), "/")
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
