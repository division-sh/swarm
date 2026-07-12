package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeaccumulator "github.com/division-sh/swarm/internal/runtime/accumulator"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (pc *PipelineCoordinator) applyWorkflowTimerIntents(ctx context.Context, entityID, currentStage, nextStage, sourceEvent string) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	currentStage = strings.TrimSpace(currentStage)
	nextStage = strings.TrimSpace(nextStage)
	sourceEvent = strings.TrimSpace(sourceEvent)
	if entityID == "" || nextStage == "" || currentStage == nextStage {
		return nil
	}
	now := time.Now().UTC()
	toSchedule := make([]Schedule, 0, 2)
	toCancel := make([]Schedule, 0, 2)
	if err := pc.workflowStore.MutateE(ctx, entityID, func(instance *WorkflowInstance) error {
		if instance.TimerState == nil {
			instance.TimerState = []WorkflowTimerState{}
		}
		for i := range instance.TimerState {
			timerState := &instance.TimerState[i]
			if timerState.Cancelled || timerState.Fired {
				continue
			}
			timer, ok := source.WorkflowTimerByID(timerState.TimerID)
			if ok && timer.Recurring && workflowTimerConnectedToLoop(source, timer) {
				return fmt.Errorf("recurring timer %s is connected to a bounded loop", timer.ID)
			}
			if ok && workflowTimerLeavesBoundedLoop(source, timer) {
				return fmt.Errorf("timer %s cannot advance directly outside its bounded loop", timer.ID)
			}
			if !ok || !workflowTimerShouldCancelOnTransition(timer, currentStage, nextStage, sourceEvent) {
				continue
			}
			timerState.Cancelled = true
			toCancel = append(toCancel, workflowTimerSchedule(timer, entityID, instance.StorageRef, timerState.FiresAt, workflowTimerPolicy(source, timer.FlowID), timerState.Generation))
		}
		generation, _, err := workflowLoopGenerationForStage(source, instance, nextStage)
		if err != nil {
			return err
		}
		for _, timer := range source.WorkflowTimers() {
			if !workflowTimerShouldStartOnTransition(timer, nextStage, sourceEvent) {
				continue
			}
			if timer.Recurring && workflowTimerConnectedToLoop(source, timer) {
				return fmt.Errorf("recurring timer %s is connected to a bounded loop", timer.ID)
			}
			if workflowTimerLeavesBoundedLoop(source, timer) {
				return fmt.Errorf("timer %s cannot advance directly outside its bounded loop", timer.ID)
			}
			if workflowTimerStateActiveForGeneration(instance.TimerState, timer.ID, generation) {
				continue
			}
			fireAt, ok := workflowTimerFireAt(timer, now, workflowTimerPolicy(source, timer.FlowID))
			if !ok {
				continue
			}
			handle := timeridentity.WorkflowTimerHandle(timer.ID)
			handle.Generation = generation
			instance.TimerState = append(instance.TimerState, WorkflowTimerState{
				TimerID:    strings.TrimSpace(timer.ID),
				TaskID:     handle.TaskID(),
				EventType:  strings.TrimSpace(timer.Event),
				CreatedAt:  now,
				FiresAt:    fireAt,
				StartedBy:  "state:" + nextStage,
				Recurring:  timer.Recurring,
				Generation: generation,
			})
			toSchedule = append(toSchedule, workflowTimerSchedule(timer, entityID, instance.StorageRef, fireAt, workflowTimerPolicy(source, timer.FlowID), generation))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, sc := range toCancel {
		if err := pc.persistWorkflowTimerCancellation(ctx, scheduleWithRunIDFromContext(ctx, sc)); err != nil {
			return err
		}
	}
	for _, sc := range toSchedule {
		if err := pc.persistWorkflowTimerSchedule(ctx, scheduleWithRunIDFromContext(ctx, sc)); err != nil {
			return err
		}
	}
	return nil
}

func (pc *PipelineCoordinator) armWorkflowCurrentStageTimers(ctx context.Context, entityID, sourceEvent string) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return nil
	}
	instance, ok, err := pc.workflowStore.Load(ctx, entityID)
	if err != nil || !ok {
		return err
	}
	stage := strings.TrimSpace(instance.CurrentState)
	if stage == "" {
		return nil
	}
	if strings.TrimSpace(sourceEvent) == "" {
		sourceEvent = "state:" + stage
	}
	return pc.applyWorkflowTimerIntents(ctx, entityID, "", stage, sourceEvent)
}

func (pc *PipelineCoordinator) ArmFlowInstanceInitialStageTimers(ctx context.Context, entityID string) error {
	return pc.armWorkflowCurrentStageTimers(ctx, strings.TrimSpace(entityID), "")
}

func (pc *PipelineCoordinator) reconcileWorkflowStageTimers(ctx context.Context, entityID, currentStage, nextStage, sourceEvent string) error {
	if err := pc.applyWorkflowTimerIntents(ctx, entityID, currentStage, nextStage, sourceEvent); err != nil {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_projection_failed", "", sourceEvent, runtimeWorkflowID, entityID, map[string]any{
			"stage":         strings.TrimSpace(nextStage),
			"current_stage": strings.TrimSpace(currentStage),
			"source_event":  strings.TrimSpace(sourceEvent),
		}, err)
		return err
	}
	return nil
}

func (pc *PipelineCoordinator) handleWorkflowStageTimerFire(ctx context.Context, evt events.Event) (bool, bool, error) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return false, false, nil
	}
	source := pc.SemanticSource()
	if source == nil {
		return false, false, nil
	}
	timerID := ""
	if handle, ok := timeridentity.ParseTimerHandle(parsePayloadMap(evt.Payload())); ok && handle.Kind == timeridentity.TimerHandleWorkflowTimer {
		timerID = strings.TrimSpace(handle.TimerID)
	}
	if timerID == "" {
		timerID = strings.TrimSpace(evt.TaskID())
	}
	if timerID == "" {
		return false, false, nil
	}
	timer, ok := source.WorkflowTimerByID(timerID)
	if !ok || !timer.StageOwned {
		return false, false, nil
	}
	if timer.Recurring && workflowTimerConnectedToLoop(source, timer) {
		return true, false, fmt.Errorf("recurring timer %s is connected to a bounded loop", timer.ID)
	}
	if workflowTimerLeavesBoundedLoop(source, timer) {
		return true, false, fmt.Errorf("timer %s cannot advance directly outside its bounded loop", timer.ID)
	}
	entityID := workflowEventEntityID(evt)
	if entityID == "" {
		return true, false, fmt.Errorf("stage timer %s fired without entity_id", timerID)
	}
	fired := false
	var lateBy time.Duration
	err := pc.workflowStore.RunPipelineMutation(ctx, func(txctx context.Context) error {
		currentStage := ""
		nextStage := strings.TrimSpace(timer.AdvancesTo)
		if err := pc.workflowStore.MutateE(txctx, entityID, func(instance *WorkflowInstance) error {
			currentStage = strings.TrimSpace(instance.CurrentState)
			if currentStage != strings.TrimSpace(timer.Stage) {
				return nil
			}
			generation := attemptgeneration.Generation{}
			if handle, ok := timeridentity.ParseTimerHandle(parsePayloadMap(evt.Payload())); ok {
				generation = handle.Generation
			}
			if current, generationErr := workflowLoopGenerationCurrent(instance, generation, timer.Stage); generationErr != nil {
				return generationErr
			} else if !current {
				for i := range instance.TimerState {
					state := &instance.TimerState[i]
					if state.TimerID == timerID && state.Generation.Equal(generation) && !state.Fired {
						state.Cancelled = true
					}
				}
				return nil
			}
			for i := range instance.TimerState {
				state := &instance.TimerState[i]
				if strings.TrimSpace(state.TimerID) != timerID || state.Cancelled || state.Fired || (generation.Valid() && !state.Generation.Equal(generation)) {
					continue
				}
				state.Fired = true
				if !state.FiresAt.IsZero() {
					firedAt := evt.CreatedAt()
					if firedAt.IsZero() {
						firedAt = time.Now().UTC()
					}
					if firedAt.After(state.FiresAt) {
						lateBy = firedAt.Sub(state.FiresAt)
					}
				}
				if nextStage != "" {
					if generation.Valid() {
						carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
						if err != nil {
							return err
						}
						activation, ok, err := loopruntime.Load(carrier.StateBuckets, generation.FlowID, generation.LoopID)
						if err != nil {
							return err
						}
						if !ok || !activation.Generation().Equal(generation) {
							return fmt.Errorf("timer %s loop generation is no longer authoritative", timerID)
						}
						firedAt := evt.CreatedAt()
						if firedAt.IsZero() {
							firedAt = time.Now().UTC()
						}
						if err := activation.AdvanceWithin(nextStage, evt.ID(), firedAt); err != nil {
							return err
						}
						if err := loopruntime.Store(carrier.StateBuckets, activation); err != nil {
							return err
						}
						instance.StateBuckets = carrier.PersistedStateBuckets()
					}
					instance.CurrentState = nextStage
					instance.EnteredStageAt = time.Now().UTC()
					instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(pc.WorkflowDefinition(), currentStage, nextStage, "timer:"+timerID))
				}
				fired = true
				return nil
			}
			return nil
		}); err != nil {
			return err
		}
		if fired && nextStage != "" {
			if err := pc.applyWorkflowTimerIntents(txctx, entityID, currentStage, nextStage, "timer:"+timerID); err != nil {
				return err
			}
			if err := pc.applyWorkflowJoinIntents(txctx, entityID, currentStage, nextStage); err != nil {
				return err
			}
			if err := pc.applyWorkflowGateIntents(txctx, entityID, currentStage, nextStage, "timer:"+timerID); err != nil {
				return err
			}
			return pc.maybeDeactivateTerminalFlowInstance(txctx, entityID, nextStage)
		}
		return nil
	})
	if err != nil {
		return true, fired, err
	}
	if fired && lateBy > time.Minute {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_fired_late", strings.TrimSpace(evt.ID()), strings.TrimSpace(string(evt.Type())), runtimeWorkflowID, entityID, map[string]any{
			"timer_id": strings.TrimSpace(timerID),
			"stage":    strings.TrimSpace(timer.Stage),
			"late_by":  lateBy.String(),
		}, nil)
	}
	return true, fired, nil
}

func (pc *PipelineCoordinator) reconcileWorkflowEventTimers(ctx context.Context, entityID, sourceEvent string) {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return
	}
	source := pc.SemanticSource()
	if source == nil {
		return
	}
	entityID = strings.TrimSpace(entityID)
	sourceEvent = strings.TrimSpace(sourceEvent)
	if entityID == "" || sourceEvent == "" {
		return
	}
	if _, ok, err := pc.workflowStore.Load(ctx, entityID); err != nil {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_event_timer_load_failed", "", sourceEvent, runtimeWorkflowID, entityID, map[string]any{
			"source_event": sourceEvent,
		}, err)
		return
	} else if !ok {
		return
	}
	now := time.Now().UTC()
	toSchedule := make([]Schedule, 0, 1)
	toCancel := make([]Schedule, 0, 1)
	if err := pc.workflowStore.MutateE(ctx, entityID, func(instance *WorkflowInstance) error {
		if instance.TimerState == nil {
			instance.TimerState = []WorkflowTimerState{}
		}
		for i := range instance.TimerState {
			timerState := &instance.TimerState[i]
			if timerState.Cancelled || timerState.Fired {
				continue
			}
			timer, ok := source.WorkflowTimerByID(timerState.TimerID)
			if ok && timer.Recurring && workflowTimerConnectedToLoop(source, timer) {
				return fmt.Errorf("recurring timer %s is connected to a bounded loop", timer.ID)
			}
			if ok && workflowTimerLeavesBoundedLoop(source, timer) {
				return fmt.Errorf("timer %s cannot advance directly outside its bounded loop", timer.ID)
			}
			if !ok || timer.StageOwned {
				continue
			}
			cancelTrigger, ok := workflowTimerCancelTrigger(timer)
			if !ok || !workflowTimerLifecycleMatches(cancelTrigger, "", sourceEvent) {
				continue
			}
			timerState.Cancelled = true
			toCancel = append(toCancel, workflowTimerSchedule(timer, entityID, instance.StorageRef, timerState.FiresAt, workflowTimerPolicy(source, timer.FlowID), timerState.Generation))
		}
		generation, _, err := workflowLoopGenerationForStage(source, instance, instance.CurrentState)
		if err != nil {
			return err
		}
		for _, timer := range source.WorkflowTimers() {
			if timer.StageOwned {
				continue
			}
			startTrigger, ok := workflowTimerStartTrigger(timer)
			if !ok || !workflowTimerLifecycleMatches(startTrigger, "", sourceEvent) {
				continue
			}
			if timer.Recurring && workflowTimerConnectedToLoop(source, timer) {
				return fmt.Errorf("recurring timer %s is connected to a bounded loop", timer.ID)
			}
			if workflowTimerLeavesBoundedLoop(source, timer) {
				return fmt.Errorf("timer %s cannot advance directly outside its bounded loop", timer.ID)
			}
			if workflowTimerStateActiveForGeneration(instance.TimerState, timer.ID, generation) {
				continue
			}
			fireAt, ok := workflowTimerFireAt(timer, now, workflowTimerPolicy(source, timer.FlowID))
			if !ok {
				continue
			}
			handle := timeridentity.WorkflowTimerHandle(timer.ID)
			handle.Generation = generation
			instance.TimerState = append(instance.TimerState, WorkflowTimerState{
				TimerID:    strings.TrimSpace(timer.ID),
				TaskID:     handle.TaskID(),
				EventType:  strings.TrimSpace(timer.Event),
				CreatedAt:  now,
				FiresAt:    fireAt,
				StartedBy:  "event:" + sourceEvent,
				Recurring:  timer.Recurring,
				Generation: generation,
			})
			toSchedule = append(toSchedule, workflowTimerSchedule(timer, entityID, instance.StorageRef, fireAt, workflowTimerPolicy(source, timer.FlowID), generation))
		}
		return nil
	}); err != nil {
		pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_event_timer_projection_failed", "", sourceEvent, runtimeWorkflowID, entityID, map[string]any{
			"source_event": sourceEvent,
		}, err)
		return
	}
	for _, sc := range toCancel {
		pc.cancelWorkflowTimerSchedule(ctx, scheduleWithRunIDFromContext(ctx, sc))
	}
	for _, sc := range toSchedule {
		pc.registerWorkflowTimerSchedule(ctx, scheduleWithRunIDFromContext(ctx, sc))
	}
}

func (pc *PipelineCoordinator) reconcileAccumulationTimeoutSchedule(
	ctx context.Context,
	entityID, nodeID string,
	handler runtimecontracts.SystemNodeEventHandler,
	evt Event,
	handlerEventKey string,
	stateBuckets map[string]any,
	waiting bool,
) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	spec := handler.Accumulate
	if spec == nil || spec.TimeoutMS <= 0 {
		return nil
	}
	effectiveSpec, err := pc.effectiveAccumulationTimeoutSpec(ctx, nodeID, handlerEventKey, spec)
	if err != nil {
		return err
	}
	spec = effectiveSpec
	entityID = strings.TrimSpace(entityID)
	nodeID = strings.TrimSpace(nodeID)
	if entityID == "" || nodeID == "" {
		return nil
	}
	generation := attemptgeneration.Generation{}
	if resolved, found, resolveErr := workflowLoopGenerationFromBuckets(pc.SemanticSource(), stateBuckets); resolveErr != nil {
		return resolveErr
	} else if found {
		generation = resolved
	}
	bucketRef, ok := accumulationTimeoutBucketRef(evt, nodeID, handlerEventKey, spec, generation)
	if !ok {
		return nil
	}
	sc := accumulationTimeoutSchedule(entityID, evt.FlowInstance(), bucketRef, time.Time{}, spec.TimeoutMS)
	if runID := strings.TrimSpace(evt.RunID()); runID != "" {
		sc.RunID = runID
	}
	if isAccumulationTimeoutEvent(evt.Type()) || !waiting {
		return pc.persistWorkflowTimerCancellation(ctx, sc)
	}
	startedAt, ok := accumulationTimeoutStartedAt(stateBuckets, bucketRef)
	if !ok {
		return nil
	}
	sc.At = startedAt.Add(time.Duration(spec.TimeoutMS) * time.Millisecond)
	return pc.persistWorkflowTimerSchedule(ctx, sc)
}

func (pc *PipelineCoordinator) effectiveAccumulationTimeoutSpec(ctx context.Context, nodeID, handlerEventKey string, spec *runtimecontracts.AccumulateSpec) (*runtimecontracts.AccumulateSpec, error) {
	if spec == nil {
		return nil, nil
	}
	if pc == nil || pc.SemanticSource() == nil || pipelineFlowScope(ctx) == "" {
		return spec, nil
	}
	return runtimeaccumulator.EffectiveSpecForHandler(pc.SemanticSource(), pipelineFlowScope(ctx), nodeID, handlerEventKey, spec)
}

func workflowTimerLifecycleMatches(trigger timeridentity.Trigger, stage, sourceEvent string) bool {
	return trigger.MatchesStage(stage) || trigger.MatchesEvent(sourceEvent)
}

func workflowTimerShouldCancelOnTransition(timer runtimecontracts.WorkflowTimerContract, currentStage, nextStage, sourceEvent string) bool {
	if timer.StageOwned {
		stage := strings.TrimSpace(timer.Stage)
		return stage != "" && strings.TrimSpace(currentStage) == stage && strings.TrimSpace(nextStage) != stage
	}
	cancelTrigger, ok := workflowTimerCancelTrigger(timer)
	return ok && workflowTimerLifecycleMatches(cancelTrigger, nextStage, sourceEvent)
}

func workflowTimerShouldStartOnTransition(timer runtimecontracts.WorkflowTimerContract, nextStage, sourceEvent string) bool {
	if timer.StageOwned {
		return strings.TrimSpace(timer.Stage) != "" && strings.TrimSpace(timer.Stage) == strings.TrimSpace(nextStage)
	}
	startTrigger, ok := workflowTimerStartTrigger(timer)
	return ok && workflowTimerLifecycleMatches(startTrigger, nextStage, sourceEvent)
}

func accumulationTimeoutBucketRef(evt Event, nodeID, handlerEventKey string, spec *runtimecontracts.AccumulateSpec, generations ...attemptgeneration.Generation) (timeridentity.AccumulatorBucketRef, bool) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return timeridentity.AccumulatorBucketRef{}, false
	}
	if !isAccumulationTimeoutEvent(evt.Type()) {
		eventType := strings.TrimSpace(handlerEventKey)
		if eventType == "" {
			eventType = strings.TrimSpace(string(evt.Type()))
		}
		generation := attemptgeneration.Generation{}
		if len(generations) > 0 {
			generation = generations[0]
		}
		bucket := timeridentity.NewAccumulatorBucketRefForGeneration(nodeID, eventType, "", generation)
		if spec != nil && strings.TrimSpace(spec.Window) != "" {
			window, ok := accumulationTimeoutWindowValue(parsePayloadMap(evt.Payload()), spec.Window)
			if !ok {
				return timeridentity.AccumulatorBucketRef{}, false
			}
			bucket = timeridentity.NewAccumulatorBucketRefForGeneration(nodeID, eventType, window, generation)
		}
		return bucket, bucket.Valid()
	}
	bucket, ok := timeridentity.ParseAccumulatorBucketRef(parsePayloadMap(evt.Payload()))
	if !ok || strings.TrimSpace(bucket.NodeID) != nodeID {
		return timeridentity.AccumulatorBucketRef{}, false
	}
	return bucket, true
}

func accumulationTimeoutWindowValue(payload map[string]any, window string) (string, bool) {
	window = strings.TrimSpace(window)
	if window == "" {
		return "", false
	}
	if strings.HasPrefix(window, "payload.") {
		window = strings.TrimPrefix(window, "payload.")
	}
	if strings.Contains(window, ".") || window == "" {
		return "", false
	}
	value := strings.TrimSpace(asString(payload[window]))
	return value, value != ""
}

func accumulationTimeoutStartedAt(stateBuckets map[string]any, bucketRef timeridentity.AccumulatorBucketRef) (time.Time, bool) {
	bucketRef = bucketRef.Normalize()
	nodeBucket, _ := stateBuckets[bucketRef.NodeID].(map[string]any)
	accumulators, _ := nodeBucket["handler_accumulators"].(map[string]any)
	raw, ok := accumulators[bucketRef.Key()].(map[string]any)
	if !ok {
		return time.Time{}, false
	}
	startedAt := strings.TrimSpace(asString(raw["started_at"]))
	if startedAt == "" {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func accumulationTimeoutSchedule(entityID, flowInstance string, bucketRef timeridentity.AccumulatorBucketRef, fireAt time.Time, timeoutMS int) Schedule {
	handle := timeridentity.AccumulationTimeoutHandle(bucketRef)
	return Schedule{
		AgentID:      runtimeWorkflowID,
		EventType:    "accumulate.timeout",
		Mode:         "once",
		At:           fireAt,
		EntityID:     strings.TrimSpace(entityID),
		FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"),
		TaskID:       handle.TaskID(),
		Payload:      mustJSON(accumulationTimeoutPayload(handle, timeoutMS)),
	}
}

func accumulationTimeoutPayload(handle timeridentity.TimerHandle, timeoutMS int) map[string]any {
	payload := handle.PayloadMetadata()
	if payload == nil {
		payload = map[string]any{}
	}
	payload["timeout_ms"] = timeoutMS
	if generation := handle.Generation.Normalize(); generation.Valid() {
		payload[generation.RevisionField] = generation.RevisionID
	}
	return payload
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

func workflowTimerStateActive(items []WorkflowTimerState, timerID string) bool {
	return workflowTimerStateActiveForGeneration(items, timerID, attemptgeneration.Generation{})
}

func workflowTimerStateActiveForGeneration(items []WorkflowTimerState, timerID string, generation attemptgeneration.Generation) bool {
	timerID = strings.TrimSpace(timerID)
	if timerID == "" {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.TimerID) == timerID && !item.Cancelled && !item.Fired && (!generation.Valid() || item.Generation.Equal(generation)) {
			return true
		}
	}
	return false
}

func workflowTimerFireAt(timer runtimecontracts.WorkflowTimerContract, now time.Time, policy map[string]any) (time.Time, bool) {
	interval := workflowTimerDuration(timer, policy)
	if interval <= 0 {
		return time.Time{}, false
	}
	return now.Add(interval), true
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
		key := strings.TrimSpace(match[1])
		value, ok := workflowExpressionPolicyValue(policy, key)
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

func workflowTimerSchedule(timer runtimecontracts.WorkflowTimerContract, entityID, flowInstance string, fireAt time.Time, policy map[string]any, generations ...attemptgeneration.Generation) Schedule {
	handle := timeridentity.WorkflowTimerHandle(timer.ID)
	if len(generations) > 0 {
		handle.Generation = generations[0].Normalize()
	}
	payload := handle.PayloadMetadata()
	if generation := handle.Generation.Normalize(); generation.Valid() {
		payload[generation.RevisionField] = generation.RevisionID
	}
	sc := Schedule{
		AgentID:      strings.TrimSpace(timer.Owner),
		EventType:    strings.TrimSpace(timer.Event),
		Mode:         "once",
		At:           fireAt,
		EntityID:     strings.TrimSpace(entityID),
		FlowInstance: strings.Trim(strings.TrimSpace(flowInstance), "/"),
		TaskID:       handle.TaskID(),
		Payload:      mustJSON(payload),
	}
	if timer.Recurring {
		if cronSpec, ok := workflowTimerRecurringSpec(timer, policy); ok {
			sc.Mode = "cron"
			sc.Cron = cronSpec
			sc.At = time.Time{}
		}
	}
	return sc
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

func workflowTimerRecurringSpec(timer runtimecontracts.WorkflowTimerContract, policy map[string]any) (string, bool) {
	if delay := workflowTimerRenderedDelay(timer.Delay, policy); delay != "" {
		if interval, ok := timeridentity.ParseDelayDuration(delay); ok {
			return "@every " + interval.String(), true
		}
	}
	return "", false
}

func (pc *PipelineCoordinator) registerWorkflowTimerSchedule(ctx context.Context, sc Schedule) {
	if pc == nil {
		return
	}
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.UpsertSchedule(ctx, sc); err != nil {
			pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_persist_failed", "", sc.EventType, sc.AgentID, sc.EffectiveEntityID(), map[string]any{
				"task_id": strings.TrimSpace(sc.TaskID),
				"mode":    strings.TrimSpace(sc.Mode),
			}, err)
			return
		}
	}
	if pc.timerScheduler != nil {
		register := func(registerCtx context.Context) {
			if _, err := ClaimAndRegisterSchedule(registerCtx, pc.timerScheduleStore, pc.timerScheduler, sc); err != nil {
				pc.logRuntimeWarn(registerCtx, runtimeWorkflowID, "workflow_timer_register_failed", "", sc.EventType, sc.AgentID, sc.EffectiveEntityID(), map[string]any{
					"task_id": strings.TrimSpace(sc.TaskID),
					"mode":    strings.TrimSpace(sc.Mode),
				}, err)
			}
		}
		if !queuePipelinePostCommitAction(ctx, func() { register(withoutSQLTxContext(ctx)) }) {
			register(ctx)
		}
	}
}

func (pc *PipelineCoordinator) cancelWorkflowTimerSchedule(ctx context.Context, sc Schedule) {
	if pc == nil {
		return
	}
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.CancelScheduleExactTerminal(ctx, sc); err != nil {
			pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_cancel_persist_failed", "", sc.EventType, sc.AgentID, sc.EffectiveEntityID(), map[string]any{
				"task_id": strings.TrimSpace(sc.TaskID),
				"mode":    strings.TrimSpace(sc.Mode),
			}, err)
			if !TerminalTransitionApplied(err) {
				return
			}
		}
	}
	if pc.timerScheduler != nil {
		cancel := func() {
			if err := pc.timerScheduler.CancelExact(sc); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_cancel_failed", "", sc.EventType, sc.AgentID, sc.EffectiveEntityID(), map[string]any{
					"task_id": strings.TrimSpace(sc.TaskID),
					"mode":    strings.TrimSpace(sc.Mode),
				}, err)
			}
		}
		if !queuePipelinePostCommitAction(ctx, cancel) {
			cancel()
		}
	}
}

func (pc *PipelineCoordinator) persistWorkflowTimerSchedule(ctx context.Context, sc Schedule) error {
	if pc == nil {
		return nil
	}
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.UpsertSchedule(ctx, sc); err != nil {
			pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_persist_failed", "", sc.EventType, sc.AgentID, sc.EffectiveEntityID(), map[string]any{
				"task_id": strings.TrimSpace(sc.TaskID),
				"mode":    strings.TrimSpace(sc.Mode),
			}, err)
			return err
		}
	}
	if pc.timerScheduler != nil {
		register := func(registerCtx context.Context) {
			if _, err := ClaimAndRegisterSchedule(registerCtx, pc.timerScheduleStore, pc.timerScheduler, sc); err != nil {
				pc.logRuntimeWarn(registerCtx, runtimeWorkflowID, "workflow_timer_register_failed", "", sc.EventType, sc.AgentID, sc.EffectiveEntityID(), map[string]any{
					"task_id": strings.TrimSpace(sc.TaskID),
					"mode":    strings.TrimSpace(sc.Mode),
				}, err)
			}
		}
		if !queuePipelinePostCommitAction(ctx, func() { register(withoutSQLTxContext(ctx)) }) {
			register(ctx)
		}
	}
	return nil
}

func (pc *PipelineCoordinator) persistWorkflowTimerCancellation(ctx context.Context, sc Schedule) error {
	if pc == nil {
		return nil
	}
	if pc.timerScheduleStore != nil {
		if err := pc.timerScheduleStore.CancelScheduleExactTerminal(ctx, sc); err != nil {
			pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_cancel_persist_failed", "", sc.EventType, sc.AgentID, sc.EffectiveEntityID(), map[string]any{
				"task_id": strings.TrimSpace(sc.TaskID),
				"mode":    strings.TrimSpace(sc.Mode),
			}, err)
			if !TerminalTransitionApplied(err) {
				return err
			}
		}
	}
	if pc.timerScheduler != nil {
		cancel := func() {
			if err := pc.timerScheduler.CancelExact(sc); err != nil {
				pc.logRuntimeWarn(ctx, runtimeWorkflowID, "workflow_timer_cancel_failed", "", sc.EventType, sc.AgentID, sc.EffectiveEntityID(), map[string]any{
					"task_id": strings.TrimSpace(sc.TaskID),
					"mode":    strings.TrimSpace(sc.Mode),
				}, err)
			}
		}
		if !queuePipelinePostCommitAction(ctx, cancel) {
			cancel()
		}
	}
	return nil
}
