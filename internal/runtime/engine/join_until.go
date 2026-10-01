package engine

import (
	"fmt"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

func currentJoinEntry(frame *executionFrame, ref timeridentity.JoinRef) (bool, error) {
	entry, found, err := workflowlifecycle.LoadStageEntry(frame.state.State.StateCarrier.Bookkeeping)
	if err != nil || !found {
		return false, fmt.Errorf("join execution lacks valid lifecycle entry evidence: %v", err)
	}
	current, err := loopruntime.GenerationCurrent(frame.state.State.StateCarrier.StateBuckets, ref.Generation(), "")
	return current && entry == ref.StageEntry() && frame.state.State.CurrentState == entry.Stage, err
}

func (e *Executor) stepJoinUntil(frame *executionFrame) error {
	context := events.DeliveryContextFromContext(frame.ctx)
	if err := context.Validate(); err != nil {
		return err
	}
	matched := false
	for _, receipt := range context.Joins {
		for _, plan := range frame.req.Handler.JoinUntilPlans {
			declaration, err := timeridentity.NewJoinRef(plan.Node, plan.HandlerEvent, plan.Spec.Stage, plan.Spec.EffectiveID())
			if err != nil {
				return err
			}
			if !receipt.Ref.Declaration().Equal(declaration) {
				continue
			}
			matched = true
			if err := e.closeJoinUntil(frame, plan, receipt); err != nil {
				return err
			}
		}
	}
	if !matched {
		return fmt.Errorf("until execution is missing its exact publication receipt")
	}
	frame.result.Status = OutcomeWaiting
	return nil
}

func (e *Executor) closeJoinUntil(frame *executionFrame, plan runtimecontracts.WorkflowJoinPlan, receipt events.JoinAdmissionReceipt) error {
	if receipt.Disposition == events.JoinAdmissionEarly {
		return e.joinArrivalFailure(frame, failures.ClassEarlyArrival, "join_not_armed", &plan.Spec, "", "")
	}
	address := frame.req.StateAddress()
	entry := receipt.Ref.StageEntry()
	if err := entry.RequireOwner(address.FlowInstance.RunID, address.FlowInstance.Route.ScopeKey, address.FlowInstance.Route.InstanceID, address.FlowInstance.Route.InstancePath, address.EntityID.String(), plan.Spec.Stage); err != nil {
		return err
	}
	activation, found, err := joinruntime.Load(frame.state.State.StateCarrier.StateBuckets, plan.Node, joinruntime.ActivationKey(receipt.Ref))
	if err != nil {
		return err
	}
	if !found || !activation.JoinRef().Equal(receipt.Ref) {
		return fmt.Errorf("until receipt has no exact retained join arm")
	}
	current, err := currentJoinEntry(frame, receipt.Ref)
	if err != nil {
		return err
	}
	if activation.Status != joinruntime.StatusOpen || !current {
		return e.joinArrivalFailure(frame, failures.ClassStaleArrival, "join_stage_closed", &plan.Spec, entry.Key(), "")
	}
	activation.Close(joinruntime.CloseReasonUntil, true, false)
	handle, err := timeridentity.JoinCompleteHandle(receipt.Ref)
	if err != nil {
		return err
	}
	activation, err = activation.WithTimerHandle(handle, e.emitNow())
	if err != nil {
		return err
	}
	return e.storeJoinActivation(frame, activation)
}
