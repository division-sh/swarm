package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// The sealed frame consumes fixed-cut event/delivery membership separately from
// child correspondence. Neither retained metadata nor a fired timer rearms work.
func prepareRunForkBarrierTransfer(ctx context.Context, tx *sql.Tx, snapshot *runForkRevisionSnapshot, barrier fanoutbarrier.Barrier, childRunID string, selected contracts.FanOutPlanRef, generation *loopruntime.ForkChildReference, at time.Time) (genericschedule.JoinContinuation, events.DeliveryRoute, error) {
	if snapshot == nil || snapshot.Revision <= 0 || snapshot.RunID != barrier.Registration.IntentKey.RunID {
		return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, fmt.Errorf("published barrier transfer requires its exact bound source cut")
	}
	if barrier.TransferredOccurrence != nil {
		source, err := genericschedule.NewTransferredFanOutBarrierOccurrence(barrier)
		if err != nil {
			return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, err
		}
		original, found, err := runForkTransferredJoinEvent(snapshot, source)
		if err != nil {
			return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, err
		}
		child, err := projectRunForkBarrierContinuationCommand(barrier, childRunID, selected, generation, source.Command.Due.Absolute, at)
		if err != nil {
			return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, err
		}
		if !found {
			var route events.DeliveryRoute
			if err := requireRunForkTransferredJoinCause(ctx, tx, source, make(map[string]bool), &route); err != nil {
				return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, err
			}
			continuation, err := genericschedule.ProjectPendingJoinContinuation(source, child)
			return continuation, route, err
		}
		route, err := requireRunForkPublishedBarrierDelivery(snapshot, barrier, original, true)
		if err != nil {
			return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, err
		}
		continuation, err := genericschedule.ProjectTransferredJoinContinuation(source, original, child)
		return continuation, route, err
	}
	activation, err := runForkBarrierCapturedActivation(snapshot, barrier.ScheduleActivationID)
	if err != nil {
		return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, err
	}
	original, route, err := runForkPublishedFanOutBarrierEvidence(snapshot, barrier, activation)
	if err != nil {
		return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, err
	}
	child, err := projectRunForkBarrierContinuationCommand(barrier, childRunID, selected, generation, activation.Command.Due.Absolute, at)
	if err != nil {
		return genericschedule.JoinContinuation{}, events.DeliveryRoute{}, err
	}
	continuation, err := genericschedule.ProjectPublishedJoinContinuation(activation, original.Event(), child)
	return continuation, route, err
}

func projectRunForkBarrierContinuationCommand(source fanoutbarrier.Barrier, childRunID string, selected contracts.FanOutPlanRef, generation *loopruntime.ForkChildReference, due, at time.Time) (genericschedule.AdmissionCommand, error) {
	child, err := runfork.ProjectFanOutBarrierRegistration(source.Registration, childRunID, selected, generation, at)
	if err != nil {
		return genericschedule.AdmissionCommand{}, err
	}
	if source.Summary == nil {
		return genericschedule.AdmissionCommand{}, fmt.Errorf("published barrier lacks its frozen summary")
	}
	return genericschedule.FanOutBarrierAdmission(child, *source.Summary, due)
}

func runForkBarrierCapturedActivation(snapshot *runForkRevisionSnapshot, id string) (genericschedule.Activation, error) {
	var captured *runForkRevisionTimer
	for i := range snapshot.Timers {
		if snapshot.Timers[i].TimerID != id {
			continue
		}
		if captured != nil {
			return genericschedule.Activation{}, fmt.Errorf("published barrier repeats its captured activation")
		}
		captured = &snapshot.Timers[i]
	}
	if id == "" || captured == nil {
		return genericschedule.Activation{}, fmt.Errorf("published barrier lacks its captured activation")
	}
	return projectRunForkGenericActivation(*captured)
}
