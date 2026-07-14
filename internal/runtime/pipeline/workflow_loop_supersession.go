package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
)

func supersedePriorLoopGenerationArtifacts(instance *WorkflowInstance, previousBuckets map[string]any, nextCarrier *runtimeengine.StateCarrier) error {
	if instance == nil || nextCarrier == nil {
		return nil
	}
	previous, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, previousBuckets)
	if err != nil {
		return fmt.Errorf("decode prior loop state: %w", err)
	}
	priorLoops, err := loopruntime.List(previous.StateBuckets)
	if err != nil {
		return fmt.Errorf("list prior loop state: %w", err)
	}
	for _, prior := range priorLoops {
		next, found, err := loopruntime.Load(nextCarrier.StateBuckets, prior.FlowID, prior.LoopID)
		if err != nil {
			return err
		}
		if !found || next.Generation().Equal(prior.Generation()) && next.Status == prior.Status {
			continue
		}
		priorGeneration := prior.Generation()
		for i := range instance.TimerState {
			state := &instance.TimerState[i]
			if state.Generation.Equal(priorGeneration) && !state.Fired {
				state.Cancelled = true
			}
		}
		joins, err := joinruntime.List(nextCarrier.StateBuckets)
		if err != nil {
			return fmt.Errorf("list joins for loop supersession: %w", err)
		}
		for _, activation := range joins {
			if !activation.Generation.Equal(priorGeneration) || !activation.CloseForStageExit() {
				continue
			}
			activation.TimerCancelled = true
			if err := joinruntime.Store(nextCarrier.StateBuckets, activation); err != nil {
				return fmt.Errorf("supersede join %s: %w", activation.Key(), err)
			}
		}
	}
	return nil
}

func (pc *PipelineCoordinator) reconcileSupersededLoopSchedules(ctx context.Context, entityID string) error {
	if pc == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	instance, ok, err := pc.workflowStore.Load(ctx, strings.TrimSpace(entityID))
	if err != nil || !ok {
		return err
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Metadata, instance.StateBuckets)
	if err != nil {
		return fmt.Errorf("decode current loop state: %w", err)
	}
	loops, err := loopruntime.List(carrier.StateBuckets)
	if err != nil {
		return fmt.Errorf("list current loop state: %w", err)
	}
	current := make([]attemptgeneration.Generation, 0, len(loops))
	for _, activation := range loops {
		if generation := activation.Generation(); generation.Valid() && activation.Status == loopruntime.StatusOpen {
			current = append(current, generation)
		}
	}
	if pc.decisionCards != nil {
		store, ok := pc.decisionCards.(decisioncard.ProposedEffectStore)
		if !ok || store == nil {
			return fmt.Errorf("proposed-effect continuation store is required for loop supersession")
		}
		runID := firstNonEmptyString(runtimecorrelation.RunIDFromContext(ctx), asString(instance.Metadata["run_id"]))
		if err := store.SupersedeProposedEffectsForLoopGenerations(ctx, runID, entityID, current, "loop_generation_superseded", time.Now().UTC()); err != nil {
			return err
		}
	}
	source := pc.SemanticSource()
	for _, state := range instance.TimerState {
		if !state.Cancelled || !state.Generation.Valid() {
			continue
		}
		if source == nil {
			return fmt.Errorf("loop schedule reconciliation requires semantic source")
		}
		timer, found := source.WorkflowTimerByID(state.TimerID)
		if !found {
			continue
		}
		schedule := workflowTimerSchedule(timer, entityID, instance.StorageRef, state.FiresAt, workflowTimerPolicy(source, timer.FlowID), state.Generation)
		if err := pc.persistWorkflowTimerCancellation(ctx, scheduleWithRunIDFromContext(ctx, schedule)); err != nil {
			return err
		}
	}
	return nil
}
