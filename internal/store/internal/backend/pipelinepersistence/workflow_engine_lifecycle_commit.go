package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func commitWorkflowEngineLifecycle(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	decisions workflowDecisionLifecycleTxOwner,
	genericSchedules GenericScheduleTxOwner,
	postgres bool,
	plan runtimepipeline.WorkflowLifecycleMutationPlan,
) (runtimepipeline.CommittedWorkflowLifecycleMutation, error) {
	result := runtimepipeline.CommittedWorkflowLifecycleMutation{}
	for index, mutation := range plan.Timers {
		ref, changed, err := commitWorkflowEngineTimerMutation(ctx, tx, postgres, mutation)
		if err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("commit workflow engine timer mutation %d: %w", index, err)
		}
		switch mutation.Kind {
		case runtimepipeline.WorkflowTimerMutationInsert:
			result.Wakeups = append(result.Wakeups, ref)
		case runtimepipeline.WorkflowTimerMutationCancel:
			if changed {
				result.Cancellations = append(result.Cancellations, ref)
			}
		}
	}
	if len(plan.Schedules) > 0 && genericSchedules == nil {
		return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("workflow lifecycle schedule mutations require the generic schedule transaction owner")
	}
	for index, mutation := range plan.Schedules {
		switch mutation.Kind {
		case runtimepipeline.WorkflowScheduleMutationUpsert:
			admitted, err := genericSchedules.AdmitTx(ctx, tx, mutation.Command)
			if err != nil {
				return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("commit workflow engine schedule admission %d: %w", index, err)
			}
			result.GenericScheduleActivations = append(result.GenericScheduleActivations, admitted.Activation)
		case runtimepipeline.WorkflowScheduleMutationCancel:
			cancelled, err := genericSchedules.CancelAdmissionTx(ctx, tx, mutation.Command, mutation.CancelCause, mutation.CancelledAt)
			if err != nil {
				return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("commit workflow engine schedule cancellation %d: %w", index, err)
			}
			if cancelled.Activation.ID != "" {
				result.GenericScheduleCancellations = append(result.GenericScheduleCancellations, cancelled.Activation)
			}
		}
	}
	for index, mutation := range plan.GateCards {
		if err := commitWorkflowEngineGateCardMutation(ctx, tx, story, decisions, mutation); err != nil {
			return runtimepipeline.CommittedWorkflowLifecycleMutation{}, fmt.Errorf("commit workflow engine gate card mutation %d: %w", index, err)
		}
	}
	return result, result.Validate()
}

type workflowDecisionLifecycleTxOwner interface {
	InsertTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, decisioncard.Card) error
	InsertProposedEffectTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, decisioncard.Card, decisioncard.ProposedEffectContinuation) error
	LoadByActivationTx(context.Context, *sql.Tx, string, string, string) (decisioncard.Card, error)
	SupersedeStageTx(context.Context, runtimeauthoractivity.Mutation, *sql.Tx, string, string, string, string, time.Time) (bool, error)
}

func commitWorkflowEngineGateCardMutation(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, decisions workflowDecisionLifecycleTxOwner, mutation runtimepipeline.WorkflowGateCardMutation) error {
	if decisions == nil {
		return fmt.Errorf("workflow gate card decision owner is required")
	}
	switch mutation.Kind {
	case runtimepipeline.WorkflowGateCardMutationCreate:
		return decisions.InsertTx(ctx, story, tx, mutation.Card)
	case runtimepipeline.WorkflowGateCardMutationSupersede:
		persisted, err := decisions.LoadByActivationTx(ctx, tx, mutation.Card.RunID, mutation.EntityID, mutation.ActivationID)
		if err != nil {
			return err
		}
		if !sameWorkflowEngineGateCard(persisted, mutation.Card) {
			return fmt.Errorf("workflow gate card changed before supersession")
		}
		changed, err := decisions.SupersedeStageTx(ctx, story, tx, mutation.Card.RunID, mutation.EntityID, mutation.ActivationID, mutation.Reason, mutation.OccurredAt)
		if err != nil {
			return err
		}
		if !changed {
			return fmt.Errorf("workflow gate card supersession changed no card")
		}
		return nil
	default:
		return fmt.Errorf("workflow gate card mutation kind %q is unsupported", mutation.Kind)
	}
}

func sameWorkflowEngineGateCard(left, right decisioncard.Card) bool {
	return left.CardID == right.CardID && left.RunID == right.RunID && left.Status == right.Status &&
		left.CardContentHash == right.CardContentHash && left.EffectContentHash == right.EffectContentHash &&
		left.DecisionSchemaHash == right.DecisionSchemaHash && left.BundleHash == right.BundleHash &&
		left.Anchor.Kind() == right.Anchor.Kind() && left.Anchor.SemanticValue().Equal(right.Anchor.SemanticValue())
}
