package pipeline

import (
	"context"
	"fmt"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func (pc *PipelineCoordinator) persistWorkflowStateForTest(ctx context.Context, entityID, nextState, sourceEvent string) error {
	if _, ok := PipelineSQLTxFromContext(ctx); !ok {
		return pc.workflowStore.runPipelineMutation(ctx, func(txctx context.Context) error {
			return pc.persistWorkflowStateForTest(txctx, entityID, nextState, sourceEvent)
		})
	}
	inbound, ok := runtimecorrelation.InboundEventFromContext(ctx)
	if !ok {
		return fmt.Errorf("test transition requires an inbound event")
	}
	var currentState string
	if err := pc.workflowStore.mutateE(ctx, entityID, func(instance *WorkflowInstance) error {
		currentState = strings.TrimSpace(instance.CurrentState)
		instance.CurrentState = strings.TrimSpace(nextState)
		instance.EnteredStageAt = inbound.CreatedAt()
		instance.TransitionHistory = append(instance.TransitionHistory, workflowTransitionRecord(
			pc.WorkflowDefinition(), currentState, nextState, inbound.ID(), sourceEvent, inbound.CreatedAt(),
		))
		return nil
	}); err != nil {
		return err
	}
	return pc.applyAcceptedWorkflowEvent(ctx, entityID, inbound, currentState, nextState)
}

func (pc *PipelineCoordinator) applyWorkflowGateForTest(ctx context.Context, entityID, _ string, setGate string, clearAll bool) error {
	return pc.workflowStore.mutate(ctx, entityID, func(instance *WorkflowInstance) {
		metadata := cloneStringAnyMap(instance.Metadata)
		gates := payloadMap(metadata["gates"])
		if clearAll {
			clear(gates)
		}
		if setGate = strings.TrimSpace(setGate); setGate != "" {
			gates[setGate] = true
		}
		if len(gates) == 0 {
			delete(metadata, "gates")
		} else {
			metadata["gates"] = gates
		}
		instance.Metadata = metadata
	})
}

func fireWorkflowTimerTestWakeup(ctx context.Context, pc *PipelineCoordinator, activation WorkflowTimerActivation) (WorkflowTimerFireOutcome, error) {
	wakeup, err := newWorkflowTimerWakeup(activation)
	if err != nil {
		return "", err
	}
	return fireTypedWorkflowTimerTestWakeup(ctx, pc, wakeup)
}

func fireTypedWorkflowTimerTestWakeup(ctx context.Context, pc *PipelineCoordinator, wakeup WorkflowTimerWakeup) (WorkflowTimerFireOutcome, error) {
	outcome, _, err := pc.workflowTimers.fireWakeup(ctx, wakeup)
	return outcome, err
}

func applyTestInitialEntryEffect(ctx context.Context, pc *PipelineCoordinator, entityID string) error {
	if _, ok := PipelineSQLTxFromContext(ctx); !ok {
		return pc.workflowStore.runPipelineMutation(ctx, func(txctx context.Context) error {
			return applyTestInitialEntryEffect(txctx, pc, entityID)
		})
	}
	instance, found, err := pc.workflowStore.Load(ctx, entityID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("test workflow instance %s is missing", entityID)
	}
	effect, err := runtimeworkflowlifecycle.NewInitialEntry(entityID, instance.CurrentState, instance.EnteredStageAt)
	if err != nil {
		return err
	}
	return pipelineWorkflowLifecycleOwner{coordinator: pc}.ApplyWorkflowLifecycleEffects(ctx, []runtimeworkflowlifecycle.Effect{effect})
}
