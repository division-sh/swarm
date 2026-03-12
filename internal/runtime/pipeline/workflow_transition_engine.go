package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
)

type workflowTransitionOutcome struct {
	Transition       WorkflowTransition
	PreviousState    WorkflowState
	CurrentState     WorkflowState
	GuardsEvaluated  []string
	ActionsExecuted  []string
	TriggerEventID   string
	TriggerEventType string
}

type workflowTriggerContext struct {
	Event           events.Event
	State           WorkflowState
	ValidationState *validationPipelineState
}

type workflowTransitionShadowComparison struct {
	FlatTransition    WorkflowTransition
	DerivedTransition WorkflowTransition
	Matched           bool
	Reason            string
}

type handlerExecutionPlan struct {
	NodeID           string
	EventType        string
	Guard            string
	GuardSpec        *runtimecontracts.GuardSpec
	Action           string
	Template         string
	InstanceIDFrom   string
	ConfigFrom       *runtimecontracts.ConfigFromSpec
	CompletionRule   string
	AdvancesTo       string
	SetsGate         string
	ClearGates       bool
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
	Emits            string
	EmitEvents       []string
	Rules            []runtimecontracts.HandlerRuleEntry
	OnComplete       *runtimecontracts.HandlerRuleEntry
	ExecutionOrder   []string
}

type handlerExecutionPlanShadowComparison struct {
	FlatPlan    handlerExecutionPlan
	DerivedPlan handlerExecutionPlan
	Matched     bool
	Reason      string
}

type handlerExecutionPlanSafetyComparison struct {
	FlatPlan    handlerExecutionPlan
	DerivedPlan handlerExecutionPlan
	Safe        bool
	Reason      string
}

type workflowRuleMatch struct {
	RuleID           string
	AdvancesTo       string
	Emits            []string
	DataAccumulation runtimecontracts.WorkflowDataAccumulation
}

func workflowHookContextFromTrigger(triggerCtx workflowTriggerContext) WorkflowHookContext {
	payload := parsePayloadMap(triggerCtx.Event.Payload)
	verticalID := workflowEventEntityIDWithPayload(triggerCtx.Event, payload)
	return WorkflowHookContext{
		Event:      triggerCtx.Event,
		VerticalID: verticalID,
		Payload:    payload,
		State:      triggerCtx.State,
	}
}

func workflowEventEntityID(evt events.Event) string {
	return workflowEventEntityIDWithPayload(evt, parsePayloadMap(evt.Payload))
}

func workflowEventEntityIDWithPayload(evt events.Event, payload map[string]any) string {
	return strings.TrimSpace(firstNonEmptyString(
		asString(payload["entity_id"]),
		asString(payload["vertical_id"]),
		evt.EntityID(),
	))
}

func (pc *FactoryPipelineCoordinator) applyWorkflowEventTransition(ctx context.Context, evt events.Event) (workflowTransitionOutcome, bool) {
	if pc == nil {
		return workflowTransitionOutcome{}, false
	}
	verticalID := workflowEventEntityID(evt)
	if verticalID == "" {
		return workflowTransitionOutcome{}, false
	}

	previousState := pc.currentWorkflowState(ctx, verticalID)
	triggerCtx := workflowTriggerContext{
		Event:           evt,
		State:           previousState,
		ValidationState: pc.validationStateSnapshot(verticalID),
	}
	if result, err := pc.executeDerivedContractHandler(ctx, triggerCtx, false); err != nil {
		runtimeWarn(runtimeWorkflowID, "handler engine failed event=%s entity=%s: %v", strings.TrimSpace(string(evt.Type)), verticalID, err)
		return workflowTransitionOutcome{}, false
	} else if result.Handled {
		pc.reconcileWorkflowEventTimers(ctx, verticalID, string(evt.Type))
		nextState := pc.currentWorkflowState(ctx, verticalID)
		return workflowTransitionOutcome{
			Transition:       result.Transition,
			PreviousState:    previousState,
			CurrentState:     nextState,
			GuardsEvaluated:  result.GuardsEvaluated,
			ActionsExecuted:  append([]string{}, result.Outcome.ActionsExecuted...),
			TriggerEventID:   strings.TrimSpace(evt.ID),
			TriggerEventType: strings.TrimSpace(string(evt.Type)),
		}, true
	}
	transition, guardsEvaluated, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		return workflowTransitionOutcome{}, false
	}

	actionsExecuted := []string{}
	pc.applyWorkflowDataAccumulation(ctx, verticalID, transition, evt)
	actionsExecuted = pc.executeWorkflowTransitionActions(ctx, triggerCtx, transition, true)
	pc.updateVerticalStage(ctx, verticalID, string(transition.To), string(evt.Type))
	nextState := pc.currentWorkflowState(ctx, verticalID)
	actionsExecuted = append(actionsExecuted, pc.executeWorkflowTransitionActions(ctx, workflowTriggerContext{
		Event:           evt,
		State:           nextState,
		ValidationState: pc.validationStateSnapshot(verticalID),
	}, transition, false)...)
	pc.reconcileWorkflowEventTimers(ctx, verticalID, string(evt.Type))

	return workflowTransitionOutcome{
		Transition:       transition,
		PreviousState:    previousState,
		CurrentState:     nextState,
		GuardsEvaluated:  guardsEvaluated,
		ActionsExecuted:  actionsExecuted,
		TriggerEventID:   strings.TrimSpace(evt.ID),
		TriggerEventType: strings.TrimSpace(string(evt.Type)),
	}, true
}

func (pc *FactoryPipelineCoordinator) applyWorkflowDataAccumulation(
	ctx context.Context,
	verticalID string,
	transition WorkflowTransition,
	evt events.Event,
) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return
	}
	writes := transition.DataAccumulation.Writes
	if len(writes) == 0 {
		return
	}
	allowedFields := workflowEntitySchemaFields(pc.ContractBundle())
	if len(allowedFields) == 0 {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if len(payload) == 0 {
		return
	}
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		entityProjection := workflowMutableStateBucket(instance, workflowStateBucketEntityProjection)
		for _, write := range writes {
			targetField := strings.TrimSpace(write.Target())
			if targetField == "" {
				continue
			}
			if _, ok := allowedFields[targetField]; !ok {
				continue
			}
			if write.HasLiteralValue() {
				entityProjection[targetField] = write.Value
				continue
			}
			sourceField := strings.TrimSpace(write.Source())
			if sourceField == "" {
				continue
			}
			if value, ok := payload[sourceField]; ok {
				entityProjection[targetField] = value
			}
		}
		if len(entityProjection) > 0 {
			workflowSetStateBucket(instance, workflowStateBucketEntityProjection, entityProjection)
		}
		metadata := workflowMutableMetadata(instance)
		metadata["last_data_accumulation_event"] = strings.TrimSpace(string(evt.Type))
		if source := strings.TrimSpace(transition.DataAccumulation.SourceEvent); source != "" {
			metadata["last_data_accumulation_source"] = source
		}
	})
}

func (pc *FactoryPipelineCoordinator) resolveWorkflowTransitionByEvent(
	triggerCtx workflowTriggerContext,
) (WorkflowTransition, []string, bool) {
	trigger := strings.TrimSpace(string(triggerCtx.Event.Type))
	if trigger == "" {
		return WorkflowTransition{}, nil, false
	}
	var guardsEvaluated []string
	workflow := pc.WorkflowDefinition()
	if workflow == nil {
		return WorkflowTransition{}, nil, false
	}
	transition, ok := workflow.TransitionByTrigger(triggerCtx.State, trigger, func(candidate WorkflowTransition) bool {
		passed, evaluated := pc.evaluateWorkflowTransitionGuards(triggerCtx, candidate)
		if passed {
			guardsEvaluated = evaluated
		}
		return passed
	})
	if !ok {
		return WorkflowTransition{}, nil, false
	}
	return transition, guardsEvaluated, true
}

func handlerExecutionPlanFromDerivedSemantic(
	derived runtimecontracts.HandlerTransitionSemantic,
) handlerExecutionPlan {
	plan := handlerExecutionPlan{
		NodeID:           strings.TrimSpace(derived.NodeID),
		EventType:        strings.TrimSpace(derived.EventType),
		Guard:            handlerGuardID(derived.Guard),
		GuardSpec:        derived.Guard,
		Action:           strings.TrimSpace(derived.Action),
		Template:         strings.TrimSpace(derived.Template),
		InstanceIDFrom:   strings.TrimSpace(derived.InstanceIDFrom),
		ConfigFrom:       derived.ConfigFrom,
		CompletionRule:   strings.TrimSpace(derived.CompletionRule),
		AdvancesTo:       strings.TrimSpace(derived.AdvancesTo),
		SetsGate:         gateSpecString(derived.SetsGate),
		ClearGates:       len(derived.ClearGates) > 0,
		DataAccumulation: derived.DataAccumulation,
		Emits:            strings.TrimSpace(derived.Emits.First()),
		EmitEvents:       derived.Emits.Values(),
		Rules:            append([]runtimecontracts.HandlerRuleEntry(nil), derived.Rules...),
		OnComplete:       derived.OnComplete,
	}
	plan.ExecutionOrder = handlerExecutionOrderForPlan(plan)
	return plan
}

func handlerExecutionPlanFromNodeHandler(nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) handlerExecutionPlan {
	plan := handlerExecutionPlan{
		NodeID:           strings.TrimSpace(nodeID),
		EventType:        strings.TrimSpace(eventType),
		Guard:            handlerGuardID(handler.Guard),
		GuardSpec:        handler.Guard,
		Action:           strings.TrimSpace(handler.Action),
		Template:         strings.TrimSpace(handler.Template),
		InstanceIDFrom:   strings.TrimSpace(handler.InstanceIDFrom),
		ConfigFrom:       handler.ConfigFrom,
		CompletionRule:   strings.TrimSpace(handler.CompletionRule),
		AdvancesTo:       strings.TrimSpace(handler.AdvancesTo),
		SetsGate:         gateSpecString(handler.SetsGate),
		ClearGates:       len(handler.ClearGates) > 0,
		DataAccumulation: handler.DataAccumulation,
		Emits:            strings.TrimSpace(handler.Emits.First()),
		EmitEvents:       handler.Emits.Values(),
		Rules:            append([]runtimecontracts.HandlerRuleEntry(nil), handler.Rules...),
		OnComplete:       handler.OnComplete,
	}
	plan.ExecutionOrder = handlerExecutionOrderForPlan(plan)
	return plan
}

func handlerExecutionOrderForPlan(plan handlerExecutionPlan) []string {
	steps := make([]string, 0, 10)
	if handlerPlanHasGuard(plan) {
		steps = append(steps, "guard")
	}
	if plan.DataAccumulation.HasWrites() || strings.TrimSpace(plan.DataAccumulation.SourceEvent) != "" {
		steps = append(steps, "accumulate")
	}
	if plan.Action != "" {
		steps = append(steps, "compute")
	}
	if plan.OnComplete != nil || plan.CompletionRule != "" {
		steps = append(steps, "on_complete")
	}
	if plan.AdvancesTo != "" {
		steps = append(steps, "advances_to")
	}
	if plan.SetsGate != "" {
		steps = append(steps, "sets_gate")
	}
	if plan.DataAccumulation.HasWrites() || strings.TrimSpace(plan.DataAccumulation.SourceEvent) != "" {
		steps = append(steps, "data_accumulation")
	}
	if plan.Emits != "" {
		steps = append(steps, "emits")
	}
	if len(plan.Rules) > 0 {
		steps = append(steps, "rules")
	}
	if plan.Action != "" {
		steps = append(steps, "action_hook")
	}
	return steps
}

func handlerPlanHasGuard(plan handlerExecutionPlan) bool {
	return strings.TrimSpace(plan.Guard) != "" || plan.GuardSpec != nil
}

func workflowTransitionFromHandlerPlan(state WorkflowState, plan handlerExecutionPlan) WorkflowTransition {
	to := strings.TrimSpace(plan.AdvancesTo)
	if to == "" {
		to = strings.TrimSpace(string(state.Stage))
	}
	return WorkflowTransition{
		Name:             strings.TrimSpace(plan.NodeID) + ":" + strings.TrimSpace(plan.EventType),
		From:             []PipelineStage{NormalizePipelineStage(string(state.Stage))},
		To:               NormalizePipelineStage(to),
		Trigger:          strings.TrimSpace(plan.EventType),
		Node:             strings.TrimSpace(plan.NodeID),
		DataAccumulation: plan.DataAccumulation,
	}
}

func (pc *FactoryPipelineCoordinator) executeContractHandlerFirstPlan(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	transition WorkflowTransition,
	plan handlerExecutionPlan,
) []string {
	if pc == nil {
		return nil
	}
	verticalID := workflowEventEntityID(triggerCtx.Event)
	if verticalID == "" {
		return nil
	}
	actionsExecuted := pc.executeHandlerPlanActions(ctx, triggerCtx, plan)
	pc.applyWorkflowGateMutation(ctx, verticalID, strings.TrimSpace(plan.NodeID), strings.TrimSpace(plan.SetsGate), plan.ClearGates)
	pc.applyWorkflowDataAccumulation(ctx, verticalID, transition, triggerCtx.Event)
	if strings.TrimSpace(plan.AdvancesTo) != "" {
		pc.updateVerticalStage(ctx, verticalID, plan.AdvancesTo, string(triggerCtx.Event.Type))
	}
	for _, emitEvent := range plan.EmitEvents {
		emitEvent = strings.TrimSpace(emitEvent)
		if emitEvent == "" {
			continue
		}
		pc.publish(ctx, emitEvent, verticalID, pc.handlerEmitPayload(ctx, triggerCtx, emitEvent))
	}
	return actionsExecuted
}

func (pc *FactoryPipelineCoordinator) executeNodeHandlerPlan(ctx context.Context, nodeID string, evt events.Event) bool {
	if pc == nil || pc.ContractBundle() == nil {
		return false
	}
	nodeID = strings.TrimSpace(nodeID)
	eventType := strings.TrimSpace(string(evt.Type))
	if nodeID == "" || eventType == "" {
		return false
	}
	handler, ok := pc.ContractBundle().NodeEventHandler(nodeID, eventType)
	if !ok {
		return false
	}
	verticalID := workflowEventEntityID(evt)
	triggerCtx := workflowTriggerContext{
		Event:           evt,
		State:           pc.currentWorkflowState(ctx, verticalID),
		ValidationState: pc.validationStateSnapshot(verticalID),
	}
	result, err := pc.executeNodeContractHandler(ctx, nodeID, handler, triggerCtx, false)
	if err != nil {
		runtimeWarn(runtimeWorkflowID, "node handler execution failed node=%s event=%s: %v", nodeID, eventType, err)
		return false
	}
	if !result.Handled {
		return false
	}
	pc.reconcileWorkflowEventTimers(ctx, verticalID, eventType)
	return true
}

func (pc *FactoryPipelineCoordinator) executeHandlerPlanActions(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	plan handlerExecutionPlan,
) []string {
	action := strings.TrimSpace(plan.Action)
	if action == "" {
		return nil
	}
	switch action {
	case "record_evidence":
		if pc.recordWorkflowEvidence(ctx, workflowEventEntityID(triggerCtx.Event), strings.TrimSpace(plan.NodeID), parsePayloadMap(triggerCtx.Event.Payload)) {
			return []string{action}
		}
		return nil
	case "create_flow_instance":
		if pc.createFlowInstance(ctx, triggerCtx, plan) {
			return []string{action}
		}
		return nil
	default:
		return nil
	}
}

func (pc *FactoryPipelineCoordinator) createFlowInstance(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	plan handlerExecutionPlan,
) bool {
	if pc == nil {
		return false
	}
	templateID := strings.TrimSpace(plan.Template)
	instanceID := strings.TrimSpace(pc.resolveFlowInstanceID(triggerCtx, plan.InstanceIDFrom))
	if templateID == "" || instanceID == "" {
		return false
	}
	bundle := pc.ContractBundle()
	if bundle == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return false
	}
	schema, ok := bundle.FlowSchemas[templateID]
	if !ok {
		return false
	}
	if mode := strings.TrimSpace(schema.Mode); mode != "" && !strings.EqualFold(mode, "template") {
		return false
	}
	initialState := strings.TrimSpace(bundle.FlowInitialStage(templateID))
	if initialState == "" {
		initialState = strings.TrimSpace(schema.InitialState)
	}
	if initialState == "" {
		return false
	}
	config := pc.resolveFlowInstanceConfig(triggerCtx, plan.ConfigFrom)
	verticalID := strings.TrimSpace(firstNonEmptyString(
		workflowEventEntityID(triggerCtx.Event),
		instanceID,
	))
	flowPath := workflowInstancePath(templateID, instanceID)

	existing, found, err := pc.workflowStore.Load(ctx, flowPath)
	if err != nil {
		return false
	}
	if found {
		if strings.TrimSpace(existing.WorkflowName) != templateID {
			return false
		}
		return true
	}
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      instanceID,
		WorkflowName:    templateID,
		WorkflowVersion: strings.TrimSpace(bundle.WorkflowVersion()),
		CurrentStage:    initialState,
		EnteredStageAt:  time.Now().UTC(),
		Metadata: map[string]any{
			"template_id":               templateID,
			"instance_id":               instanceID,
			"vertical_id":               verticalID,
			"flow_path":                 flowPath,
			"flow_mode":                 strings.TrimSpace(schema.Mode),
			"instance_config":           cloneMapFromAny(config),
			"trigger_event_id":          strings.TrimSpace(triggerCtx.Event.ID),
			"trigger_event_type":        strings.TrimSpace(string(triggerCtx.Event.Type)),
			"auto_emit_on_create_event": strings.TrimSpace(schema.AutoEmitOnCreate.Event),
		},
	}); err != nil {
		return false
	}

	if pc.instanceActivator != nil {
		if err := pc.instanceActivator(ctx, FlowInstanceActivationRequest{
			ContractBundle: bundle,
			TemplateID:     templateID,
			InstanceID:     instanceID,
			VerticalID:     verticalID,
			FlowPath:       flowPath,
			InitialState:   initialState,
			Config:         config,
			TriggerEvent:   triggerCtx.Event,
		}); err != nil {
			return false
		}
	}

	autoEmitEvent := strings.TrimSpace(schema.AutoEmitOnCreate.Event)
	if autoEmitEvent == "" {
		return true
	}
	payload := map[string]any{
		"vertical_id": verticalID,
		"instance_id": instanceID,
		"flow_path":   flowPath,
	}
	for key, value := range config {
		if strings.TrimSpace(key) == "" {
			continue
		}
		payload[key] = value
	}
	pc.publish(ctx, workflowInstanceEventType(templateID, instanceID, autoEmitEvent), verticalID, payload)
	_ = pc.workflowStore.Mutate(ctx, flowPath, func(instance *WorkflowInstance) {
		metadata := workflowMutableMetadata(instance)
		metadata["auto_emit_on_create_delivered"] = true
	})
	return true
}

func (pc *FactoryPipelineCoordinator) resolveFlowInstanceID(triggerCtx workflowTriggerContext, ref string) string {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return ""
	}
	hookCtx := workflowHookContextFromTrigger(triggerCtx)
	if value, ok := workflowExpressionResolveValueRef(ref, hookCtx, nil); ok {
		return strings.TrimSpace(asString(value))
	}
	return strings.TrimSpace(ref)
}

func (pc *FactoryPipelineCoordinator) resolveFlowInstanceConfig(
	triggerCtx workflowTriggerContext,
	spec *runtimecontracts.ConfigFromSpec,
) map[string]any {
	if spec == nil {
		return map[string]any{}
	}
	hookCtx := workflowHookContextFromTrigger(triggerCtx)
	out := make(map[string]any, len(spec.PolicyKeys)+len(spec.Bindings))
	for _, key := range spec.PolicyKeys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if resolved, found := workflowExpressionResolveValueRef("policy."+key, hookCtx, nil); found {
			out[key] = resolved
		}
	}
	for key, value := range spec.Bindings {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if resolved, found := workflowExpressionResolveValueRef(value, hookCtx, nil); found {
			out[key] = resolved
			continue
		}
		out[key] = value
	}
	return out
}

func workflowInstancePath(templateID, instanceID string) string {
	templateID = strings.TrimSpace(templateID)
	instanceID = strings.TrimSpace(instanceID)
	if templateID == "" || instanceID == "" {
		return ""
	}
	return templateID + "/" + instanceID
}

func workflowInstanceEventType(templateID, instanceID, localEvent string) string {
	localEvent = strings.TrimSpace(localEvent)
	if localEvent == "" {
		return ""
	}
	path := workflowInstancePath(templateID, instanceID)
	if path == "" {
		return localEvent
	}
	return path + "/" + localEvent
}

func (pc *FactoryPipelineCoordinator) recordWorkflowEvidence(ctx context.Context, verticalID string, nodeID string, payload map[string]any) bool {
	if pc == nil {
		return false
	}
	verticalID = strings.TrimSpace(firstNonEmptyString(verticalID, asString(payload["vertical_id"])))
	nodeID = strings.TrimSpace(nodeID)
	if verticalID == "" || nodeID == "" {
		return false
	}
	field := workflowEvidenceAccumulatorField(pc.ContractBundle(), nodeID)
	if field == "" {
		return false
	}
	if pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return true
	}
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		bucket := workflowMutableStateBucket(instance, nodeID)
		if _, ok := workflowSystemNodeStateSchemaFields(pc.ContractBundle(), nodeID)["vertical_id"]; ok && strings.TrimSpace(asString(bucket["vertical_id"])) == "" {
			bucket["vertical_id"] = verticalID
		}
		existing, _ := asArray(bucket[field])
		entries := make([]any, 0, len(existing)+1)
		entries = append(entries, existing...)
		entries = append(entries, cloneStringAnyMap(payload))
		bucket[field] = entries
		workflowSetStateBucket(instance, nodeID, bucket)
	})
	return true
}

func workflowEvidenceAccumulatorField(bundle *runtimecontracts.WorkflowContractBundle, nodeID string) string {
	fields := workflowSystemNodeStateSchemaFields(bundle, nodeID)
	if len(fields) == 0 {
		return ""
	}
	if _, ok := fields["build_evidence"]; ok {
		return "build_evidence"
	}
	if _, ok := fields["evidence"]; ok {
		return "evidence"
	}
	candidates := make([]string, 0, len(fields))
	for field := range fields {
		field = strings.TrimSpace(field)
		if field == "" || field == "vertical_id" {
			continue
		}
		if strings.Contains(field, "evidence") {
			candidates = append(candidates, field)
		}
	}
	sort.Strings(candidates)
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0]
}

func (pc *FactoryPipelineCoordinator) applyWorkflowGateMutation(
	ctx context.Context,
	verticalID string,
	nodeID string,
	setGate string,
	clearGates bool,
) {
	if pc == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	nodeID = strings.TrimSpace(nodeID)
	setGate = strings.TrimSpace(setGate)
	if verticalID == "" {
		return
	}
	if clearGates || setGate != "" {
		pc.mutateValidationState(ctx, verticalID, func(st *validationPipelineState) {
			if clearGates {
				st.G1Research = false
				st.G2Spec = false
				st.G3CTO = false
				st.G4Brand = false
			}
			switch setGate {
			case "g1_research":
				st.G1Research = true
			case "g2_spec":
				st.G2Spec = true
			case "g3_cto":
				st.G3CTO = true
			case "g4_brand":
				st.G4Brand = true
			}
		})
	}
	if pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		metadata := workflowMutableMetadata(instance)
		if nodeID != "" {
			bucket := workflowMutableStateBucket(instance, nodeID)
			gateState, _ := asObject(bucket["gate_state"])
			if gateState == nil {
				gateState = map[string]any{}
			}
			if clearGates {
				for _, gate := range []string{"g1_research", "g2_spec", "g3_cto", "g4_brand"} {
					gateState[gate] = false
					metadata[gate] = false
				}
			}
			if setGate != "" {
				gateState[setGate] = true
				metadata[setGate] = true
			}
			if len(gateState) > 0 {
				bucket["gate_state"] = gateState
			}
			workflowSetStateBucket(instance, nodeID, bucket)
		}
	})
}

func (pc *FactoryPipelineCoordinator) handlerEmitPayload(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	emitEvent string,
) map[string]any {
	payload := cloneStringAnyMap(parsePayloadMap(triggerCtx.Event.Payload))
	if payload == nil {
		payload = map[string]any{}
	}
	verticalID := workflowEventEntityIDWithPayload(triggerCtx.Event, payload)
	if verticalID != "" && strings.TrimSpace(asString(payload["vertical_id"])) == "" {
		payload["vertical_id"] = verticalID
	}
	switch strings.TrimSpace(emitEvent) {
	case "validation.started":
		return payloadMap(pc.payloadFactory.BuildValidationStartedPayload(ctx, verticalID, parsePayloadMap(triggerCtx.Event.Payload), nil))
	case "validation.package_ready", "mailbox.review_requested":
		snap := pc.payloadFactory.ValidationContext(verticalID)
		return payloadMap(pc.payloadFactory.BuildValidationPackageReadyPayload(ctx, verticalID, snap))
	case "brand.requested":
		snap := pc.payloadFactory.ValidationContext(verticalID)
		return payloadMap(pc.payloadFactory.BuildBrandRequestedPayload(ctx, verticalID, snap.Scoring, snap.Research))
	case "cto.spec_review_requested":
		return payloadMap(pc.payloadFactory.BuildCTOSpecReviewRequestedPayload(ctx, verticalID, parsePayloadMap(triggerCtx.Event.Payload)))
	case "research.additional_requested":
		snap := pc.payloadFactory.ValidationContext(verticalID)
		return payloadMap(pc.payloadFactory.BuildValidationMoreDataPayload(ctx, verticalID, parsePayloadMap(triggerCtx.Event.Payload), snap))
	case "spec.revision_requested":
		return payloadMap(pc.payloadFactory.BuildSpecRevisionRequestedPayload(ctx, verticalID, strings.TrimSpace(string(triggerCtx.Event.Type)), parsePayloadMap(triggerCtx.Event.Payload)))
	case "vertical.killed":
		return payloadMap(pc.payloadFactory.BuildVerticalKilledPayload(ctx, verticalID, strings.TrimSpace(string(triggerCtx.Event.Type)), parsePayloadMap(triggerCtx.Event.Payload)))
	case "opco.spinup_requested":
		return payloadMap(pc.OpcoSpinupRequestedPayload(ctx, verticalID, parsePayloadMap(triggerCtx.Event.Payload)))
	default:
		return payload
	}
}

func handlerGuardID(spec *runtimecontracts.GuardSpec) string {
	if spec == nil {
		return ""
	}
	return strings.TrimSpace(spec.ID)
}

func equalWorkflowDataWrites(left []runtimecontracts.WorkflowDataWrite, right []runtimecontracts.WorkflowDataWrite) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if strings.TrimSpace(left[i].Source()) != strings.TrimSpace(right[i].Source()) {
			return false
		}
		if strings.TrimSpace(left[i].Target()) != strings.TrimSpace(right[i].Target()) {
			return false
		}
		if !reflect.DeepEqual(left[i].Value, right[i].Value) {
			return false
		}
	}
	return true
}

func workflowTransitionSemanticParity(flat WorkflowTransition, derived WorkflowTransition) (bool, string) {
	if NormalizePipelineStage(string(flat.To)) != NormalizePipelineStage(string(derived.To)) {
		return false, "target_mismatch"
	}
	if strings.TrimSpace(flat.Trigger) != strings.TrimSpace(derived.Trigger) {
		return false, "trigger_mismatch"
	}
	if strings.TrimSpace(flat.Node) != strings.TrimSpace(derived.Node) {
		return false, "node_mismatch"
	}
	if len(flat.GuardIDs) > 0 && len(derived.GuardIDs) > 0 && strings.TrimSpace(flat.GuardIDs[0]) != strings.TrimSpace(derived.GuardIDs[0]) {
		return false, "guard_mismatch"
	}
	if len(flat.Actions) > 0 && len(derived.Actions) > 0 {
		flatAction := strings.TrimSpace(flat.Actions[0].Name)
		derivedAction := strings.TrimSpace(derived.Actions[0].Name)
		flatEmit := strings.TrimSpace(flat.Actions[0].Emits)
		derivedEmit := strings.TrimSpace(derived.Actions[0].Emits)
		if flatEmit != "" && derivedEmit != "" && flatEmit == derivedEmit {
			return true, "emit_match"
		}
		if flatAction != "" && derivedAction != "" && !workflowTransitionActionAliasesMatch(flatAction, derivedAction) {
			return false, "action_mismatch"
		}
	}
	return true, "match"
}

func workflowTransitionActionAliasesMatch(flatAction string, derivedAction string) bool {
	flatAction = strings.TrimSpace(flatAction)
	derivedAction = strings.TrimSpace(derivedAction)
	if flatAction == derivedAction {
		return true
	}
	switch {
	case flatAction == "increment_revision_count" && derivedAction == "revision_loop":
		return true
	default:
		return false
	}
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowTransitionGuards(
	triggerCtx workflowTriggerContext,
	transition WorkflowTransition,
) (bool, []string) {
	evaluated := make([]string, 0, len(transition.GuardIDs))
	for _, guardID := range transition.GuardIDs {
		guardID = strings.TrimSpace(guardID)
		if guardID == "" {
			continue
		}
		evaluated = append(evaluated, guardID)
		if !pc.evaluateWorkflowGuard(triggerCtx, guardID) {
			return false, evaluated
		}
	}
	return true, evaluated
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowGuard(triggerCtx workflowTriggerContext, guardID string) bool {
	guardID = strings.TrimSpace(guardID)
	if guardID == "" {
		return true
	}
	entry, ok := pc.resolveWorkflowGuard(guardID)
	if !ok {
		return false
	}
	return pc.evaluateWorkflowGuardEntry(triggerCtx, entry)
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowGuardSpec(triggerCtx workflowTriggerContext, guardSpec *runtimecontracts.GuardSpec) (bool, []string) {
	if guardSpec == nil {
		return true, nil
	}
	if len(guardSpec.Checks) > 0 {
		evaluated := make([]string, 0, len(guardSpec.Checks))
		for _, check := range guardSpec.Checks {
			passed, ids := pc.evaluateWorkflowGuardCheck(triggerCtx, check.ID, check.Check, guardSpec.PolicyRef)
			evaluated = append(evaluated, ids...)
			if !passed {
				return false, evaluated
			}
		}
		return true, evaluated
	}
	return pc.evaluateWorkflowGuardCheck(triggerCtx, guardSpec.ID, guardSpec.Check, guardSpec.PolicyRef)
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowGuardCheck(
	triggerCtx workflowTriggerContext,
	guardID string,
	check string,
	policyRef string,
) (bool, []string) {
	guardID = strings.TrimSpace(guardID)
	check = strings.TrimSpace(check)
	policyRef = strings.TrimSpace(policyRef)
	if check != "" {
		entry := runtimecontracts.GuardActionEntry{
			ID:        guardID,
			Check:     check,
			PolicyRef: policyRef,
		}
		passed := pc.evaluateWorkflowGuardEntry(triggerCtx, entry)
		if guardID != "" {
			return passed, []string{guardID}
		}
		return passed, []string{check}
	}
	if guardID == "" {
		return true, nil
	}
	if entry, ok := pc.resolveWorkflowGuard(guardID); ok {
		return pc.evaluateWorkflowGuardEntry(triggerCtx, entry), []string{guardID}
	}
	passed, err := pc.evaluateWorkflowExpressionBool(triggerCtx, guardID)
	if err != nil {
		return false, []string{guardID}
	}
	return passed, []string{guardID}
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowGuardEntry(triggerCtx workflowTriggerContext, entry runtimecontracts.GuardActionEntry) bool {
	if check := strings.TrimSpace(entry.Check); check != "" {
		if passed, err := pc.evaluateWorkflowExpressionBool(triggerCtx, check); err == nil {
			return passed
		}
	}
	hookCtx := workflowHookContextFromTrigger(triggerCtx)
	if passed, handled := pc.evaluateWorkflowPlatformBuiltinGuard(triggerCtx, hookCtx, entry); handled {
		return passed
	}
	return false
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowPlatformBuiltinGuard(
	triggerCtx workflowTriggerContext,
	hookCtx WorkflowHookContext,
	entry runtimecontracts.GuardActionEntry,
) (bool, bool) {
	key := firstNonEmptyString(entry.PlatformBuiltin, entry.ID)
	handler, ok := lookupWorkflowBuiltinGuard(key)
	if !ok {
		return false, false
	}
	exec := &handlerEngineExecution{
		ctx:         context.Background(),
		scope:       &handlerEngineContext{coordinator: pc},
		state:       &triggerCtx.State,
		event:       triggerCtx.Event,
		payload:     cloneStringAnyMap(hookCtx.Payload),
		entityID:    strings.TrimSpace(hookCtx.VerticalID),
		policy:      policyDocumentToMap(pc.ContractBundle().MergedPolicy),
		accumulated: map[string]any{},
		fanOut:      map[string]any{},
	}
	if len(exec.policy) == 0 && pc.ContractBundle() != nil {
		exec.policy = policyDocumentToMap(pc.ContractBundle().Policy)
	}
	passed, handled, err := handler(exec, strings.TrimSpace(entry.PolicyRef))
	if err != nil {
		return false, true
	}
	return passed, handled
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowExpressionBool(triggerCtx workflowTriggerContext, expression string) (bool, error) {
	return pc.evaluateWorkflowExpressionBoolWithVars(triggerCtx, expression, nil)
}

func (pc *FactoryPipelineCoordinator) evaluateWorkflowExpressionBoolWithVars(triggerCtx workflowTriggerContext, expression string, extraVars map[string]any) (bool, error) {
	if pc == nil || pc.expressionEval == nil {
		return false, fmt.Errorf("workflow expression evaluator unavailable")
	}
	hookCtx := workflowHookContextFromTrigger(triggerCtx)
	expression = pc.rewriteWorkflowExpressionRuntimeCounts(expression, hookCtx, extraVars)
	entity := cloneStringAnyMap(hookCtx.State.Metadata)
	if entity == nil {
		entity = map[string]any{}
	}
	if _, ok := entity["revision_count"]; !ok {
		entity["revision_count"] = asInt(hookCtx.State.Metadata["revision_count"])
	}
	gates := map[string]any{}
	if triggerCtx.ValidationState != nil {
		gates["g1_research"] = triggerCtx.ValidationState.G1Research
		gates["g2_spec"] = triggerCtx.ValidationState.G2Spec
		gates["g3_cto"] = triggerCtx.ValidationState.G3CTO
		gates["g4_brand"] = triggerCtx.ValidationState.G4Brand
		if _, ok := entity["revision_count"]; !ok {
			entity["revision_count"] = triggerCtx.ValidationState.RevisionCount
		}
	}
	for _, gate := range []string{"g1_research", "g2_spec", "g3_cto", "g4_brand", "g_product_spec", "g_tech_spec", "g_qa_passed"} {
		if value, ok := entity[gate]; ok {
			gates[gate] = value
		}
	}
	if len(gates) > 0 {
		entity["gates"] = gates
	}
	if stage := strings.TrimSpace(string(hookCtx.State.Stage)); stage != "" {
		entity["stage"] = stage
	}
	if status := strings.TrimSpace(hookCtx.State.Status); status != "" {
		entity["status"] = status
	}
	policy := map[string]any{}
	if bundle := pc.ContractBundle(); bundle != nil {
		policy = policyDocumentToMap(bundle.MergedPolicy)
		if len(policy) == 0 {
			policy = policyDocumentToMap(bundle.Policy)
		}
	}
	return pc.expressionEval.EvalBool(expression, workflowExpressionContext{
		Entity:  entity,
		Payload: hookCtx.Payload,
		Policy:  policy,
		Vars:    cloneStringAnyMap(extraVars),
	})
}

func (pc *FactoryPipelineCoordinator) rewriteWorkflowExpressionRuntimeCounts(
	expression string,
	hookCtx WorkflowHookContext,
	extraVars map[string]any,
) string {
	expression = strings.TrimSpace(expression)
	if expression == "" || pc == nil {
		return expression
	}
	expression = workflowExpressionStageRangeCountPattern.ReplaceAllStringFunc(expression, func(token string) string {
		match := workflowExpressionStageRangeCountPattern.FindStringSubmatch(token)
		if len(match) != 3 {
			return pc.rewriteWorkflowExpressionRuntimeQueries(token, hookCtx, extraVars)
		}
		count := pc.countVerticalsInStageRange(context.Background(), match[1], match[2])
		if count < 0 {
			return pc.rewriteWorkflowExpressionRuntimeQueries(token, hookCtx, extraVars)
		}
		return pc.rewriteWorkflowExpressionRuntimeQueries(strconv.Itoa(count), hookCtx, extraVars)
	})
	return pc.rewriteWorkflowExpressionRuntimeQueries(expression, hookCtx, extraVars)
}

func (pc *FactoryPipelineCoordinator) rewriteWorkflowExpressionRuntimeQueries(
	expression string,
	hookCtx WorkflowHookContext,
	extraVars map[string]any,
) string {
	if expression == "" || pc == nil {
		return expression
	}
	return workflowExpressionQueryEntitiesCountPattern.ReplaceAllStringFunc(expression, func(token string) string {
		match := workflowExpressionQueryEntitiesCountPattern.FindStringSubmatch(token)
		if len(match) != 2 {
			return token
		}
		name, ok := workflowExpressionResolveStringRef(match[1], hookCtx, extraVars)
		if !ok || strings.TrimSpace(name) == "" {
			return token
		}
		count, ok := pc.countWorkflowEntitiesByName(context.Background(), name)
		if !ok {
			return token
		}
		return strconv.Itoa(count)
	})
}

func (pc *FactoryPipelineCoordinator) countWorkflowEntitiesByName(ctx context.Context, name string) (int, bool) {
	if pc == nil || pc.db == nil {
		return 0, true
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return 0, true
	}
	var count int
	if err := dbQueryRowContext(ctx, pc.db, `
		SELECT COUNT(*)
		FROM verticals
		WHERE lower(trim(name)) = lower(trim($1))
	`, name).Scan(&count); err != nil {
		return 0, false
	}
	return count, true
}

func workflowExpressionResolveStringRef(ref string, hookCtx WorkflowHookContext, extraVars map[string]any) (string, bool) {
	value, ok := workflowExpressionResolveValueRef(ref, hookCtx, extraVars)
	if !ok {
		return "", false
	}
	text := strings.TrimSpace(asString(value))
	return text, text != ""
}

func workflowExpressionResolveValueRef(ref string, hookCtx WorkflowHookContext, extraVars map[string]any) (any, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return nil, false
	}
	if unquoted, err := strconv.Unquote(ref); err == nil {
		return strings.TrimSpace(unquoted), true
	}
	switch {
	case strings.HasPrefix(ref, "payload."):
		return workflowExpressionLookupPath(hookCtx.Payload, strings.TrimPrefix(ref, "payload."))
	case strings.HasPrefix(ref, "entity."):
		return workflowExpressionLookupPath(hookCtx.State.Metadata, strings.TrimPrefix(ref, "entity."))
	case strings.HasPrefix(ref, "vars."):
		return workflowExpressionLookupPath(extraVars, strings.TrimPrefix(ref, "vars."))
	default:
		if value, ok := workflowExpressionLookupPath(hookCtx.Payload, ref); ok {
			return value, true
		}
		if value, ok := workflowExpressionLookupPath(hookCtx.State.Metadata, ref); ok {
			return value, true
		}
		return workflowExpressionLookupPath(extraVars, ref)
	}
}

func workflowExpressionLookupStringPath(source map[string]any, path string) (string, bool) {
	value, ok := workflowExpressionLookupPath(source, path)
	if !ok {
		return "", false
	}
	text := strings.TrimSpace(asString(value))
	return text, text != ""
}

func workflowExpressionLookupPath(source map[string]any, path string) (any, bool) {
	source = cloneStringAnyMap(source)
	path = strings.TrimSpace(path)
	if source == nil || path == "" {
		return nil, false
	}
	current := any(source)
	for _, segment := range strings.Split(path, ".") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			return nil, false
		}
		object, ok := asObject(current)
		if !ok {
			return nil, false
		}
		current = object[segment]
	}
	return current, current != nil
}

func (pc *FactoryPipelineCoordinator) matchWorkflowRules(triggerCtx workflowTriggerContext, rules []runtimecontracts.HandlerRuleEntry) (workflowRuleMatch, bool) {
	return pc.matchWorkflowRulesWithVars(triggerCtx, rules, nil)
}

func (pc *FactoryPipelineCoordinator) matchWorkflowRulesWithVars(triggerCtx workflowTriggerContext, rules []runtimecontracts.HandlerRuleEntry, extraVars map[string]any) (workflowRuleMatch, bool) {
	if len(rules) == 0 {
		return workflowRuleMatch{}, false
	}
	var fallback workflowRuleMatch
	var hasFallback bool
	for i, rule := range rules {
		key := strings.TrimSpace(rule.ID)
		if key == "" {
			key = strings.TrimSpace(rule.Description)
		}
		if key == "" {
			key = fmt.Sprintf("rule_%d", i)
		}
		condition := strings.TrimSpace(rule.Condition)
		match := workflowRuleMatch{
			RuleID:           key,
			AdvancesTo:       strings.TrimSpace(rule.AdvancesTo),
			Emits:            rule.Emits.Values(),
			DataAccumulation: rule.DataAccumulation,
		}
		if strings.EqualFold(condition, "else") {
			fallback = match
			hasFallback = true
			continue
		}
		if condition == "" {
			continue
		}
		passed, err := pc.evaluateWorkflowExpressionBoolWithVars(triggerCtx, condition, extraVars)
		if err != nil {
			continue
		}
		if passed {
			return match, true
		}
	}
	if hasFallback {
		return fallback, true
	}
	return workflowRuleMatch{}, false
}

func (pc *FactoryPipelineCoordinator) executeWorkflowTransitionActions(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	transition WorkflowTransition,
	preStageUpdate bool,
) []string {
	if pc == nil {
		return nil
	}
	executed := make([]string, 0, len(transition.Actions))
	for _, action := range transition.Actions {
		actionID := strings.TrimSpace(action.Name)
		if actionID == "" {
			continue
		}
		if workflowActionRunsPreStageUpdate(actionID) != preStageUpdate {
			continue
		}
		if pc.executeWorkflowAction(ctx, triggerCtx, actionID) {
			executed = append(executed, actionID)
		}
	}
	return executed
}

func workflowActionRunsPreStageUpdate(actionID string) bool {
	switch strings.TrimSpace(actionID) {
	case "increment_revision_count",
		"emit_vertical_shortlisted",
		"emit_vertical_marginal",
		"emit_vertical_rejected":
		return true
	default:
		return false
	}
}

func (pc *FactoryPipelineCoordinator) executeWorkflowAction(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	actionID string,
) bool {
	actionID = strings.TrimSpace(actionID)
	if actionID == "" {
		return false
	}
	entry, ok := pc.resolveWorkflowAction(actionID)
	if !ok {
		return false
	}
	hookCtx := workflowHookContextFromTrigger(triggerCtx)
	if executed, handled := pc.executeWorkflowPlatformBuiltinAction(ctx, hookCtx, entry); handled {
		return executed
	}
	if emitEvent := strings.TrimSpace(entry.Emits); emitEvent != "" {
		payload := pc.workflowActionEmitPayload(ctx, triggerCtx, emitEvent)
		pc.publish(ctx, emitEvent, hookCtx.VerticalID, payload)
		return true
	}
	return false
}

func (pc *FactoryPipelineCoordinator) executeWorkflowPlatformBuiltinAction(
	ctx context.Context,
	hookCtx WorkflowHookContext,
	entry runtimecontracts.GuardActionEntry,
) (bool, bool) {
	handler, ok := lookupWorkflowBuiltinAction(firstNonEmptyString(entry.PlatformBuiltin, entry.ID))
	if !ok {
		return false, false
	}
	executed, err := handler(ctx, pc, hookCtx, strings.TrimSpace(entry.PolicyRef))
	if err != nil {
		return false, true
	}
	return executed, true
}

func (pc *FactoryPipelineCoordinator) workflowActionEmitPayload(
	ctx context.Context,
	triggerCtx workflowTriggerContext,
	eventType string,
) map[string]any {
	eventType = strings.TrimSpace(eventType)
	entityID := workflowEventEntityID(triggerCtx.Event)
	switch eventType {
	case "validation.started":
		if pc.payloadFactory != nil {
			return payloadMap(pc.payloadFactory.BuildValidationStartedPayload(ctx, entityID, parsePayloadMap(triggerCtx.Event.Payload), nil))
		}
	case "vertical.shortlisted":
		if pc.payloadFactory != nil {
			payload := parsePayloadMap(triggerCtx.Event.Payload)
			return payloadMap(pc.payloadFactory.BuildVerticalShortlistedPayload(
				entityID,
				workflowPayloadFloat(payload, "composite_score"),
				workflowPayloadFloat(payload, "viability_score"),
				payload,
			))
		}
	case "vertical.marginal":
		if pc.payloadFactory != nil {
			return payloadMap(pc.payloadFactory.BuildVerticalMarginalPayload(
				entityID,
				scoringCompositeFromPayload(parsePayloadMap(triggerCtx.Event.Payload)),
			))
		}
	case "vertical.rejected":
		if pc.payloadFactory != nil {
			return payloadMap(pc.payloadFactory.BuildVerticalRejectedPayload(
				entityID,
				scoringCompositeFromPayload(parsePayloadMap(triggerCtx.Event.Payload)),
			))
		}
	case "opco.spinup_requested":
		pc.PersistWorkflowMetadata(ctx, entityID, func(metadata map[string]any) {
			metadata["opco_spinup_emitted"] = true
		})
		return pc.handlerEmitPayload(ctx, triggerCtx, eventType)
	}
	return pc.handlerEmitPayload(ctx, triggerCtx, eventType)
}

func (pc *FactoryPipelineCoordinator) currentWorkflowState(ctx context.Context, verticalID string) WorkflowState {
	verticalID = strings.TrimSpace(verticalID)
	state := workflowStateForVertical(verticalID, "", pc.validationStateSnapshot(verticalID))
	if verticalID == "" {
		return state
	}
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
		if err == nil && ok {
			state = workflowStateFromInstance(instance, state)
			return state
		}
	}
	if pc.db != nil {
		var stage string
		if err := dbQueryRowContext(ctx, pc.db, `
			SELECT COALESCE(stage, '')
			FROM verticals
			WHERE id = $1::uuid
		`, verticalID).Scan(&stage); err == nil {
			state.Stage = NormalizePipelineStage(stage)
		}
	}
	return state
}

func (pc *FactoryPipelineCoordinator) resolveWorkflowGuard(guardID string) (runtimecontracts.GuardActionEntry, bool) {
	if pc == nil || pc.GuardRegistry() == nil {
		return runtimecontracts.GuardActionEntry{}, false
	}
	entry, ok := pc.GuardRegistry().Guard(guardID)
	if !ok || !pc.GuardRegistry().IsExecutable(guardID) {
		return runtimecontracts.GuardActionEntry{}, false
	}
	return entry, true
}

func (pc *FactoryPipelineCoordinator) resolveWorkflowAction(actionID string) (runtimecontracts.GuardActionEntry, bool) {
	if pc == nil || pc.ActionRegistry() == nil {
		return runtimecontracts.GuardActionEntry{}, false
	}
	entry, ok := pc.ActionRegistry().Action(actionID)
	if !ok || !pc.ActionRegistry().IsExecutable(actionID) {
		return runtimecontracts.GuardActionEntry{}, false
	}
	return entry, true
}

func workflowStateFromInstance(instance WorkflowInstance, fallback WorkflowState) WorkflowState {
	out := fallback
	out.Stage = NormalizePipelineStage(instance.CurrentStage)
	if metadata := workflowMetadataSnapshot(instance); len(metadata) > 0 {
		out.Metadata = metadata
	}
	if validationState, ok := workflowValidationProjectionBucket(instance); ok {
		if out.Metadata == nil {
			out.Metadata = map[string]any{}
		}
		if gateState, ok := asObject(validationState["gate_state"]); ok {
			for _, gate := range []string{"g1_research", "g2_spec", "g3_cto", "g4_brand"} {
				if _, exists := out.Metadata[gate]; !exists {
					out.Metadata[gate] = gateState[gate]
				}
			}
		}
		if _, exists := out.Metadata["revision_count"]; !exists {
			out.Metadata["revision_count"] = validationState["revision_count"]
		}
	}
	if status := strings.TrimSpace(asString(out.Metadata["status"])); status != "" {
		out.Status = status
	}
	if out.Metadata == nil {
		out.Metadata = map[string]any{}
	}
	return out
}

func (pc *FactoryPipelineCoordinator) PersistWorkflowMetadata(ctx context.Context, verticalID string, mutate func(metadata map[string]any)) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || mutate == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return
	}
	_ = pc.workflowStore.Mutate(ctx, verticalID, func(instance *WorkflowInstance) {
		mutate(workflowMutableMetadata(instance))
	})
}

func (pc *FactoryPipelineCoordinator) validationStateSnapshot(verticalID string) *validationPipelineState {
	if pc == nil {
		return nil
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return nil
	}
	pc.mu.Lock()
	st := pc.validationGate.states[verticalID]
	pc.mu.Unlock()
	if st != nil {
		return cloneValidationPipelineState(st)
	}
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		if instance, ok, err := pc.workflowStore.Load(context.Background(), verticalID); err == nil && ok {
			if restored, ok := restoreValidationStateFromInstance(instance); ok {
				return restored
			}
		}
	}
	return nil
}

func (pc *FactoryPipelineCoordinator) mutateValidationState(
	ctx context.Context,
	verticalID string,
	mutate func(*validationPipelineState),
) *validationPipelineState {
	if pc == nil || mutate == nil {
		return nil
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return nil
	}
	pc.mu.Lock()
	if st := pc.validationGate.states[verticalID]; st != nil {
		mutate(st)
		snapshot := cloneValidationPipelineState(st)
		pc.mu.Unlock()
		return snapshot
	}
	pc.mu.Unlock()

	var restored *validationPipelineState
	if pc.workflowStore != nil && pc.workflowStore.Enabled() {
		if instance, ok, err := pc.workflowStore.Load(ctx, verticalID); err == nil && ok {
			if st, ok := restoreValidationStateFromInstance(instance); ok {
				restored = st
			}
		}
	}
	if restored == nil {
		return nil
	}

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if st := pc.validationGate.states[verticalID]; st != nil {
		mutate(st)
		return cloneValidationPipelineState(st)
	}
	mutate(restored)
	pc.validationGate.states[verticalID] = restored
	return cloneValidationPipelineState(restored)
}

func cloneValidationPipelineState(st *validationPipelineState) *validationPipelineState {
	if st == nil {
		return nil
	}
	copyState := *st
	copyState.ResearchPayload = cloneRaw(st.ResearchPayload)
	copyState.SpecPayload = cloneRaw(st.SpecPayload)
	copyState.CTOPayload = cloneRaw(st.CTOPayload)
	copyState.BrandPayload = cloneRaw(st.BrandPayload)
	copyState.ScoringPayload = cloneRaw(st.ScoringPayload)
	return &copyState
}

func (pc *FactoryPipelineCoordinator) OpcoSpinupRequestedPayload(
	ctx context.Context,
	verticalID string,
	approvalPayload map[string]any,
) map[string]any {
	snap := pc.payloadFactory.ValidationContext(verticalID)
	name, geography := pc.payloadFactory.identityForPayload(ctx, verticalID)
	founderDirectives := strings.TrimSpace(asString(approvalPayload["founder_directives"]))
	brandChoice := strings.TrimSpace(asString(approvalPayload["brand_choice"]))
	brandPayload := cloneStringAnyMap(snap.Brand)
	if brandPayload == nil {
		brandPayload = map[string]any{}
	}
	if brandChoice != "" && strings.TrimSpace(asString(brandPayload["choice"])) == "" {
		brandPayload["choice"] = brandChoice
	}
	mandate := map[string]any{
		"vertical_id":        verticalID,
		"vertical_name":      name,
		"geography":          geography,
		"founder_directives": founderDirectives,
		"founder_notes":      founderDirectives,
		"business_brief":     cloneStringAnyMap(snap.Research),
		"mvp_spec":           cloneStringAnyMap(snap.Spec),
		"brand":              cloneStringAnyMap(brandPayload),
		"cto_feasibility":    cloneStringAnyMap(snap.CTONotes),
	}
	if launchTargets, ok := normalizePayloadObject(approvalPayload["launch_targets"]); ok {
		mandate["launch_targets"] = launchTargets
	}
	out := map[string]any{
		"vertical_id":        verticalID,
		"vertical_name":      name,
		"geography":          geography,
		"mandate":            mandate,
		"brand":              cloneStringAnyMap(brandPayload),
		"founder_directives": founderDirectives,
	}
	if techStack := firstNonEmptyString(
		asString(snap.CTONotes["tech_stack"]),
		asString(snap.CTONotes["recommended_stack"]),
		asString(snap.CTONotes["stack"]),
	); techStack != "" {
		out["tech_stack"] = techStack
	}
	return out
}

func normalizePayloadObject(raw any) (map[string]any, bool) {
	switch typed := raw.(type) {
	case map[string]any:
		return cloneStringAnyMap(typed), true
	case json.RawMessage:
		var decoded map[string]any
		if err := json.Unmarshal(typed, &decoded); err == nil && len(decoded) > 0 {
			return decoded, true
		}
	case []byte:
		var decoded map[string]any
		if err := json.Unmarshal(typed, &decoded); err == nil && len(decoded) > 0 {
			return decoded, true
		}
	}
	return nil, false
}

func truthyMetadataFlag(raw any) bool {
	switch typed := raw.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	default:
		return false
	}
}

func payloadSliceLen(raw any) int {
	switch typed := raw.(type) {
	case []any:
		return len(typed)
	case []string:
		return len(typed)
	default:
		return 0
	}
}

func workflowPayloadFloat(payload map[string]any, key string) float64 {
	if payload == nil {
		return 0
	}
	value, ok := asFloat64(payload[strings.TrimSpace(key)])
	if !ok {
		return 0
	}
	return value
}

func workflowPayloadDimensionScore(payload map[string]any, dimension string) int {
	rawDimensions, ok := asObject(payload["dimensions"])
	if !ok {
		return 0
	}
	rawResult, ok := asObject(rawDimensions[strings.TrimSpace(dimension)])
	if !ok {
		return 0
	}
	return asInt(rawResult["score"])
}

func workflowMarginalPromotionEligible(pc *FactoryPipelineCoordinator, payload map[string]any) bool {
	threshold := 2
	if pc != nil {
		threshold = pc.ContractPolicyInt("marginal_tier1_dimensions_above_70", threshold)
	}
	count := 0
	for _, dim := range []string{"icp_crispness", "distribution_leverage", "time_to_value", "operational_drag"} {
		if workflowPayloadDimensionScore(payload, dim) >= 70 {
			count++
		}
	}
	return count >= threshold
}

func workflowGateMetadataFlag(triggerCtx workflowTriggerContext, key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	if triggerCtx.ValidationState != nil {
		switch key {
		case "g1_research":
			if triggerCtx.ValidationState.G1Research {
				return true
			}
		case "g2_spec":
			if triggerCtx.ValidationState.G2Spec {
				return true
			}
		case "g3_cto":
			if triggerCtx.ValidationState.G3CTO {
				return true
			}
		case "g4_brand":
			if triggerCtx.ValidationState.G4Brand {
				return true
			}
		}
	}
	return truthyMetadataFlag(triggerCtx.State.Metadata[key])
}

func (pc *FactoryPipelineCoordinator) ContractPolicyFloat(key string, fallback float64) float64 {
	if pc == nil || pc.ContractBundle() == nil {
		return fallback
	}
	key = strings.TrimSpace(key)
	if pv, ok := pc.ContractBundle().MergedPolicy.Values[key]; ok {
		if value, ok := asFloat64(pv.Value); ok {
			return value
		}
	}
	if pv, ok := pc.ContractBundle().Policy.Values[key]; ok {
		if value, ok := asFloat64(pv.Value); ok {
			return value
		}
	}
	return fallback
}

func (pc *FactoryPipelineCoordinator) ContractPolicyInt(key string, fallback int) int {
	if pc == nil || pc.ContractBundle() == nil {
		return fallback
	}
	key = strings.TrimSpace(key)
	if pv, ok := pc.ContractBundle().MergedPolicy.Values[key]; ok {
		if got := asInt(pv.Value); got != 0 {
			return got
		}
	}
	if pv, ok := pc.ContractBundle().Policy.Values[key]; ok {
		if got := asInt(pv.Value); got != 0 {
			return got
		}
	}
	return fallback
}

func scoringCompositeFromPayload(payload map[string]any) scoringComposite {
	out := scoringComposite{
		Result:         strings.TrimSpace(asString(payload["result"])),
		Reason:         strings.TrimSpace(asString(payload["reason"])),
		CompositeScore: asFloat(payload["composite_score"]),
		ViabilityScore: asFloat(payload["viability_score"]),
		MarketScore:    asFloat(payload["market_score"]),
		Rubric:         strings.TrimSpace(asString(payload["rubric"])),
		Partial:        truthyMetadataFlag(payload["partial"]),
		Dimensions:     map[string]scoreDimensionResult{},
	}
	rawDimensions, ok := asObject(payload["dimensions"])
	if !ok {
		return out
	}
	for dim, rawResult := range rawDimensions {
		resultMap, ok := asObject(rawResult)
		if !ok {
			continue
		}
		out.Dimensions[dim] = scoreDimensionResult{
			Score:      asInt(resultMap["score"]),
			Evidence:   strings.TrimSpace(asString(resultMap["evidence"])),
			Confidence: strings.TrimSpace(asString(resultMap["confidence"])),
		}
	}
	return out
}

func (pc *FactoryPipelineCoordinator) PipelineHasCapacity(ctx context.Context, limit int) bool {
	if pc == nil || pc.db == nil || limit <= 0 {
		return true
	}
	count := pc.countVerticalsInStageRange(ctx, "researching", "ready_for_review")
	if count < 0 {
		return true
	}
	return count < limit
}

func (pc *FactoryPipelineCoordinator) countVerticalsInStageRange(ctx context.Context, startStage, endStage string) int {
	if pc == nil || pc.db == nil {
		return 0
	}
	stages := pc.workflowStageRange(startStage, endStage)
	if len(stages) == 0 {
		return -1
	}
	placeholders := make([]string, 0, len(stages))
	args := make([]any, 0, len(stages))
	for idx, stage := range stages {
		placeholders = append(placeholders, fmt.Sprintf("$%d", idx+1))
		args = append(args, stage)
	}
	query := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM verticals
		WHERE stage IN (%s)
	`, strings.Join(placeholders, ", "))
	var count int
	if err := dbQueryRowContext(ctx, pc.db, query, args...).Scan(&count); err != nil {
		return -1
	}
	return count
}

func (pc *FactoryPipelineCoordinator) workflowStageRange(startStage, endStage string) []string {
	if pc == nil || pc.ContractBundle() == nil {
		return nil
	}
	startStage = strings.TrimSpace(string(NormalizePipelineStage(startStage)))
	endStage = strings.TrimSpace(string(NormalizePipelineStage(endStage)))
	if startStage == "" || endStage == "" {
		return nil
	}
	stages := pc.ContractBundle().WorkflowStages()
	startIdx := -1
	endIdx := -1
	for idx, stage := range stages {
		stageID := strings.TrimSpace(string(NormalizePipelineStage(stage.ID)))
		if stageID == startStage && startIdx < 0 {
			startIdx = idx
		}
		if stageID == endStage {
			endIdx = idx
		}
	}
	if startIdx < 0 || endIdx < 0 || startIdx > endIdx {
		return nil
	}
	out := make([]string, 0, endIdx-startIdx+1)
	for _, stage := range stages[startIdx : endIdx+1] {
		stageID := strings.TrimSpace(string(NormalizePipelineStage(stage.ID)))
		if stageID != "" {
			out = append(out, stageID)
		}
	}
	return out
}
