package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func (pc *PipelineCoordinator) handleWorkflowStageTimerFire(ctx context.Context, evt events.Event) (bool, bool, error) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() || pc.workflowTimers == nil {
		return false, false, nil
	}
	activation, occurrence, recognized, err := pc.workflowTimers.AuthorizeAcceptedEvent(ctx, evt)
	if err != nil || !recognized {
		return recognized, false, err
	}
	source := pc.SemanticSource()
	if source == nil {
		return true, false, fmt.Errorf("workflow timer event requires semantic source")
	}
	timer, ok, err := pc.workflowTimers.workflowTimerDeclarationForActivation(ctx, activation)
	if err != nil {
		return true, false, err
	}
	if !ok {
		return true, false, fmt.Errorf("workflow timer declaration %s is unavailable", activation.Ref.DeclarationKey)
	}
	if err := validateWorkflowTimerTopology(source, timer); err != nil {
		return true, false, err
	}
	if !timer.StageOwned {
		return true, true, nil
	}

	entityID := strings.TrimSpace(activation.EntityID)
	if entityID == "" {
		return true, false, fmt.Errorf("stage timer %s fired without entity_id", timer.ID)
	}
	route := activation.Route
	if !route.Valid() {
		return true, false, fmt.Errorf("workflow timer activation is missing its canonical route")
	}
	nextStage := strings.TrimSpace(timer.AdvancesTo)
	if nextStage == "" {
		instance, found, err := pc.workflowStore.Load(ctx, route)
		if err != nil {
			return true, false, err
		}
		if !found || strings.TrimSpace(instance.CurrentState) != strings.TrimSpace(timer.Stage) {
			return true, false, nil
		}
		current, err := workflowLoopGenerationCurrent(&instance, activation.Ref.Generation, timer.Stage)
		if err != nil {
			return true, false, err
		}
		if !current {
			return true, false, nil
		}
		return true, true, nil
	}

	if pc.workflowStore.engineMutations == nil {
		return true, false, fmt.Errorf("workflow timer transition requires the selected workflow engine mutation owner")
	}
	instance, found, err := pc.workflowStore.Load(ctx, route)
	if err != nil || !found {
		return true, false, err
	}
	currentStage := strings.TrimSpace(instance.CurrentState)
	if currentStage != strings.TrimSpace(timer.Stage) {
		return true, false, nil
	}
	if current, generationErr := workflowLoopGenerationCurrent(&instance, activation.Ref.Generation, timer.Stage); generationErr != nil {
		return true, false, generationErr
	} else if !current {
		return true, false, nil
	}
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return true, false, err
	}
	if generation := activation.Ref.Generation.Normalize(); generation.Valid() {
		loopActivation, found, err := loopruntime.Load(carrier.StateBuckets, generation.FlowID, generation.LoopID)
		if err != nil {
			return true, false, err
		}
		if !found || !loopActivation.Generation().Equal(generation) {
			return true, false, fmt.Errorf("timer %s loop generation is no longer authoritative", timer.ID)
		}
		if err := loopActivation.AdvanceWithin(nextStage, evt.ID(), evt.CreatedAt()); err != nil {
			return true, false, err
		}
		if err := loopruntime.Store(carrier.StateBuckets, loopActivation); err != nil {
			return true, false, err
		}
	}
	address := runtimeengine.StateAddress{
		FlowID: identity.NormalizeFlowID(instance.WorkflowName), Route: route,
		EntityID: identity.NormalizeEntityID(entityID),
	}
	prepared, err := (pipelineEngineStateRepo{coordinator: pc}).prepareMutation(ctx, address, runtimeengine.StateMutation{
		NextState: nextStage, TriggerEventID: evt.ID(), TriggerEventType: string(evt.Type()),
		TriggeredAt: evt.CreatedAt(), StateCarrier: carrier,
	})
	if err != nil {
		return true, false, err
	}
	effect, err := (pipelineWorkflowLifecycleOwner{coordinator: pc}).AcceptedEventEffect(route, address.EntityID, evt, currentStage, nextStage)
	if err != nil {
		return true, false, err
	}
	lifecycle, err := pc.prepareWorkflowLifecycleMutation(ctx, &prepared.instance, []runtimeworkflowlifecycle.Effect{effect}, true)
	if err != nil {
		return true, false, err
	}
	state, err := prepared.record()
	if err != nil {
		return true, false, err
	}
	committed, err := pc.workflowStore.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{State: state, Lifecycle: lifecycle.Commit})
	if err != nil {
		return true, false, err
	}
	if err := pc.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle); err != nil {
		return true, true, err
	}
	pc.notifyTestEntityStateUpdated(entityID, nextStage)
	if err := pc.maybeDeactivateTerminalFlowInstance(ctx, route, identity.NormalizeEntityID(entityID), nextStage); err != nil {
		return true, true, err
	}
	if lateBy := evt.CreatedAt().Sub(occurrence.DueAt); lateBy > time.Minute {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_fired_late", evt.ID(), string(evt.Type()), runtimeWorkflowID, entityID, map[string]any{
			"activation_id": activation.Ref.ActivationID,
			"timer_id":      timer.ID,
			"stage":         timer.Stage,
			"late_by":       lateBy.String(),
		}, nil)
	}
	return true, true, nil
}

func workflowTimerLifecycleMatches(trigger timeridentity.Trigger, stage, sourceEvent string) bool {
	return trigger.MatchesStage(stage) || trigger.MatchesEvent(sourceEvent)
}

func workflowTimerShouldCancelOnTransition(timer runtimecontracts.WorkflowTimerContract, currentStage, nextStage, sourceEvent string) bool {
	if timer.StageOwned {
		stage := strings.TrimSpace(timer.Stage)
		target := strings.TrimSpace(nextStage)
		return stage != "" && target != "" && strings.TrimSpace(currentStage) == stage && target != stage
	}
	cancelTrigger, ok := workflowTimerCancelTrigger(timer)
	return ok && workflowTimerLifecycleMatches(cancelTrigger, nextStage, sourceEvent)
}

func workflowTimerShouldStartOnTransition(timer runtimecontracts.WorkflowTimerContract, currentStage, nextStage, sourceEvent string) bool {
	if timer.StageOwned {
		stage := strings.TrimSpace(timer.Stage)
		return stage != "" && strings.TrimSpace(currentStage) != strings.TrimSpace(nextStage) && stage == strings.TrimSpace(nextStage)
	}
	startTrigger, ok := workflowTimerStartTrigger(timer)
	return ok && workflowTimerLifecycleMatches(startTrigger, nextStage, sourceEvent)
}

func workflowTimerDuration(timer runtimecontracts.WorkflowTimerContract, policy map[string]any) time.Duration {
	if delay := workflowTimerRenderedDelay(timer.Delay, policy); delay != "" {
		if parsed, ok := timeridentity.ParseDelayDuration(delay); ok {
			return parsed
		}
	}
	return 0
}

func workflowTimerRenderedDelay(delay string, policy map[string]any) string {
	delay = strings.TrimSpace(delay)
	if delay == "" || !strings.Contains(delay, "{{") {
		return delay
	}
	return workflowExpressionPolicyPlaceholder.ReplaceAllStringFunc(delay, func(token string) string {
		match := workflowExpressionPolicyPlaceholder.FindStringSubmatch(token)
		if len(match) != 2 {
			return token
		}
		value, ok := workflowExpressionPolicyValue(policy, strings.TrimSpace(match[1]))
		if !ok || value == nil {
			return token
		}
		return fmt.Sprint(value)
	})
}

func workflowTimerPolicy(source semanticview.Source, flowID string) map[string]any {
	if source == nil {
		return nil
	}
	return policyDocumentToMap(source.ResolvedPolicyForFlow(strings.TrimSpace(flowID)))
}

func workflowTimerConnectedToLoop(source semanticview.Source, timer runtimecontracts.WorkflowTimerContract) bool {
	if source == nil {
		return false
	}
	for _, plan := range semanticview.WorkflowLoops(source) {
		if !loopFlowIDMatches(source, plan.FlowID, timer.OwningFlowID()) {
			continue
		}
		for _, stage := range plan.RegionStages {
			if strings.TrimSpace(timer.Stage) == strings.TrimSpace(stage) {
				return true
			}
			if trigger, err := timeridentity.ParseStartTrigger(timer.StartOn); err == nil && trigger.MatchesStage(stage) {
				return true
			}
		}
		for _, operation := range plan.Operations {
			if strings.TrimSpace(timer.Event) == strings.TrimSpace(operation.HandlerEvent) {
				return true
			}
		}
	}
	return false
}

func workflowTimerLeavesBoundedLoop(source semanticview.Source, timer runtimecontracts.WorkflowTimerContract) bool {
	target := strings.TrimSpace(timer.AdvancesTo)
	if source == nil || target == "" {
		return false
	}
	for _, plan := range semanticview.WorkflowLoops(source) {
		if !loopFlowIDMatches(source, plan.FlowID, timer.OwningFlowID()) || !workflowTimerConnectedToPlan(timer, plan) {
			continue
		}
		if !containsLoopStage(plan.RegionStages, target) {
			return true
		}
	}
	return false
}

func workflowTimerConnectedToPlan(timer runtimecontracts.WorkflowTimerContract, plan runtimecontracts.WorkflowLoopPlan) bool {
	for _, stage := range plan.RegionStages {
		if strings.TrimSpace(timer.Stage) == strings.TrimSpace(stage) {
			return true
		}
		if trigger, err := timeridentity.ParseStartTrigger(timer.StartOn); err == nil && trigger.MatchesStage(stage) {
			return true
		}
	}
	for _, operation := range plan.Operations {
		if strings.TrimSpace(timer.Event) == strings.TrimSpace(operation.HandlerEvent) {
			return true
		}
	}
	return false
}

func containsLoopStage(stages []string, stage string) bool {
	for _, candidate := range stages {
		if strings.TrimSpace(candidate) == strings.TrimSpace(stage) {
			return true
		}
	}
	return false
}

func workflowTimerStartTrigger(timer runtimecontracts.WorkflowTimerContract) (timeridentity.Trigger, bool) {
	trigger, err := timeridentity.ParseStartTrigger(timer.StartOn)
	return trigger, err == nil && trigger.Valid()
}

func workflowTimerCancelTrigger(timer runtimecontracts.WorkflowTimerContract) (timeridentity.Trigger, bool) {
	trigger, err := timeridentity.ParseCancelTrigger(timer.CancelOn)
	return trigger, err == nil && trigger.Valid()
}
