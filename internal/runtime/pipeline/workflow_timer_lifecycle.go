package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// ArmFlowInstanceInitialStageLifecycle materializes all stage-owned durable
// intents in the flow-instance creation mutation.
func (pc *PipelineCoordinator) ArmFlowInstanceInitialStageLifecycle(ctx context.Context, entityID string) error {
	return pc.armWorkflowCurrentStageLifecycle(ctx, strings.TrimSpace(entityID), "")
}

func workflowTimerInitialCause(instance WorkflowInstance, stage string) (workflowTimerCause, error) {
	occurredAt := instance.CreatedAt
	if occurredAt.IsZero() {
		occurredAt = instance.EnteredStageAt
	}
	cause := workflowTimerCause{
		Kind:       workflowTimerCauseInitial,
		EventType:  "state:" + strings.TrimSpace(stage),
		OccurredAt: occurredAt,
		ToState:    strings.TrimSpace(stage),
	}
	cause = cause.normalized()
	if err := cause.validateForActivation(); err != nil {
		return workflowTimerCause{}, err
	}
	return cause, nil
}

func (pc *PipelineCoordinator) handleWorkflowStageTimerFire(ctx context.Context, evt events.Event) (bool, bool, error) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() || pc.workflowTimers == nil {
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
	timer, ok := source.WorkflowTimerByID(activation.Ref.Declaration)
	if !ok {
		return true, false, fmt.Errorf("workflow timer declaration %s is unavailable", activation.Ref.Declaration)
	}
	if err := validateWorkflowTimerTopology(source, timer); err != nil {
		return true, false, err
	}
	if !timer.StageOwned {
		return true, true, nil
	}

	entityID := workflowEventEntityID(evt)
	if entityID == "" {
		return true, false, fmt.Errorf("stage timer %s fired without entity_id", timer.ID)
	}
	applied := false
	err = pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		currentStage := ""
		nextStage := strings.TrimSpace(timer.AdvancesTo)
		if err := pc.workflowStore.MutateE(txctx, entityID, func(instance *WorkflowInstance) error {
			currentStage = strings.TrimSpace(instance.CurrentState)
			if currentStage != strings.TrimSpace(timer.Stage) {
				return nil
			}
			if current, generationErr := workflowLoopGenerationCurrent(instance, activation.Ref.Generation, timer.Stage); generationErr != nil {
				return generationErr
			} else if !current {
				return nil
			}
			if nextStage != "" {
				if generation := activation.Ref.Generation.Normalize(); generation.Valid() {
					carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
					if err != nil {
						return err
					}
					loopActivation, found, err := loopruntime.Load(carrier.StateBuckets, generation.FlowID, generation.LoopID)
					if err != nil {
						return err
					}
					if !found || !loopActivation.Generation().Equal(generation) {
						return fmt.Errorf("timer %s loop generation is no longer authoritative", timer.ID)
					}
					if err := loopActivation.AdvanceWithin(nextStage, evt.ID(), evt.CreatedAt()); err != nil {
						return err
					}
					if err := loopruntime.Store(carrier.StateBuckets, loopActivation); err != nil {
						return err
					}
					instance.StateBuckets = carrier.PersistedStateBuckets()
				}
				instance.CurrentState = nextStage
				instance.EnteredStageAt = evt.CreatedAt().UTC()
				instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(
					pc.WorkflowDefinition(), currentStage, nextStage, evt.ID(), string(evt.Type()), evt.CreatedAt(),
				))
			}
			applied = true
			return nil
		}); err != nil {
			return err
		}
		if !applied || nextStage == "" {
			return nil
		}
		cause := workflowTimerCause{
			Kind:         workflowTimerCauseTransition,
			EventID:      evt.ID(),
			EventType:    strings.TrimSpace(string(evt.Type())),
			OccurredAt:   evt.CreatedAt(),
			TransitionID: workflowTransitionIdentity(pc.WorkflowDefinition(), currentStage, nextStage, string(evt.Type())),
			FromState:    currentStage,
			ToState:      nextStage,
		}
		if err := pc.workflowTimers.Reconcile(txctx, entityID, currentStage, nextStage, cause); err != nil {
			return err
		}
		if err := pc.applyWorkflowJoinIntents(txctx, entityID, currentStage, nextStage); err != nil {
			return err
		}
		if err := pc.applyWorkflowGateIntents(txctx, entityID, currentStage, nextStage, string(evt.Type())); err != nil {
			return err
		}
		return pc.maybeDeactivateTerminalFlowInstance(txctx, entityID, nextStage)
	})
	if err != nil {
		return true, applied, err
	}
	if lateBy := evt.CreatedAt().Sub(occurrence.DueAt); applied && lateBy > time.Minute {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_fired_late", evt.ID(), string(evt.Type()), runtimeWorkflowID, entityID, map[string]any{
			"activation_id": activation.Ref.ActivationID,
			"timer_id":      timer.ID,
			"stage":         timer.Stage,
			"late_by":       lateBy.String(),
		}, nil)
	}
	return true, applied, nil
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

func scheduleWithRunIDFromContext(ctx context.Context, sc Schedule) Schedule {
	if strings.TrimSpace(sc.RunID) == "" {
		sc.RunID = runtimecorrelation.RunIDFromContext(ctx)
	}
	if strings.TrimSpace(sc.RunID) == "" {
		if inbound, ok := runtimecorrelation.InboundEventFromContext(ctx); ok {
			sc.RunID = strings.TrimSpace(inbound.RunID())
		}
	}
	sc.NormalizeRunID()
	return sc
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
		if !loopFlowIDMatches(source, plan.FlowID, timer.FlowID) {
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
		if !loopFlowIDMatches(source, plan.FlowID, timer.FlowID) || !workflowTimerConnectedToPlan(timer, plan) {
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
