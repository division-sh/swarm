package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

// Recover only the original entry's committed references. Reading replay must
// not recreate missing declarations, rearm terminal work, or rotate sessions.
func recoverInstanceRestartLifecycleTx(ctx context.Context, tx *sql.Tx, attempt *mutationprotocol.Attempt, store eventCommitTxStore, postgres bool, plan pipeline.WorkflowLifecycleMutationPlan) (pipeline.CommittedWorkflowLifecycleMutation, error) {
	var result pipeline.CommittedWorkflowLifecycleMutation
	if plan.TurnTermination != nil || plan.RequestCompletionCandidate {
		return result, fmt.Errorf("instance restart replay requires initial-entry lifecycle evidence")
	}
	for _, timer := range plan.Timers {
		if timer.Kind != pipeline.WorkflowTimerMutationInsert {
			return result, fmt.Errorf("instance restart replay cannot repeat timer cancellation")
		}
		actual, found, err := loadWorkflowEngineTimerActivation(ctx, tx, postgres, timer.Activation.Ref)
		if err != nil {
			return result, err
		}
		if !found || !sameWorkflowEngineTimerCancellationTarget(actual, timer.Activation) {
			return result, fmt.Errorf("instance restart lost its exact committed initial timer")
		}
		if actual.Status == timer.Activation.Status {
			result.Wakeups = append(result.Wakeups, actual.Ref)
		}
	}
	for _, schedule := range plan.Schedules {
		if schedule.Kind != pipeline.WorkflowScheduleMutationUpsert || store.genericScheduleTxOwner() == nil {
			return result, fmt.Errorf("instance restart replay requires its original schedule owner")
		}
		activation, err := store.genericScheduleTxOwner().ReadAdmissionTx(ctx, attempt, schedule.Command)
		if err != nil {
			return result, err
		}
		result.GenericScheduleActivations = append(result.GenericScheduleActivations, activation)
	}
	for _, gate := range plan.GateCards {
		if gate.Kind != pipeline.WorkflowGateCardMutationCreate || store.workflowDecisionLifecycleOwner() == nil {
			return result, fmt.Errorf("instance restart replay requires its original gate-card owner")
		}
		anchor, err := gate.Card.Anchor.StageGate()
		if err != nil {
			return result, err
		}
		actual, err := store.workflowDecisionLifecycleOwner().LoadByActivationTx(ctx, attempt, gate.Card.RunID, gate.EntityID, anchor.StageActivationID)
		if err != nil {
			return result, err
		}
		expected := gate.Card
		expected.Status = actual.Status
		if !sameWorkflowEngineGateCard(actual, expected) {
			return result, fmt.Errorf("instance restart lost its exact committed gate card")
		}
		result.ChannelCardsChanged = true
	}
	return result, result.Validate()
}
