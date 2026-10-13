package runforkpersistence

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/fanoutbarrier"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func loadRunForkPendingBarrierSchedules(snapshot *runForkRevisionSnapshot, plan runfork.RunForkPlan) (map[string]genericschedule.Activation, error) {
	pending := make(map[string]struct{})
	for _, obligation := range plan.FanOutObligations {
		barrier := obligation.Barrier
		if barrier == nil || barrier.Status != fanoutbarrier.StatusClosedPending {
			continue
		}
		if barrier.Registration.IntentKey.RunID != plan.SourceRunID {
			return nil, fmt.Errorf("pending fork barrier belongs to another source run")
		}
		if barrier.TransferredOccurrence != nil {
			continue
		}
		pending[barrier.ScheduleActivationID] = struct{}{}
	}
	if len(pending) == 0 {
		return nil, nil
	}
	if snapshot == nil || snapshot.RunID != plan.SourceRunID || snapshot.Revision != plan.ForkPoint.Revision {
		return nil, fmt.Errorf("fork barrier requires its exact captured cut")
	}
	if _, err := validateRunForkBarrierSchedules(snapshot, plan.FanOutObligations); err != nil {
		return nil, err
	}
	schedules := make(map[string]genericschedule.Activation, len(pending))
	for _, timer := range snapshot.Timers {
		if _, retained := pending[timer.TimerID]; !retained {
			continue
		}
		activation, err := projectRunForkGenericActivation(timer)
		if err != nil {
			return nil, err
		}
		if activation.Status != genericschedule.StatusActive && activation.Status != genericschedule.StatusFired {
			return nil, fmt.Errorf("pending fork barrier has no active or published occurrence")
		}
		schedules[timer.TimerID] = activation
	}
	if len(schedules) != len(pending) {
		return nil, fmt.Errorf("pending fork barrier lacks its exact fixed-cut activation")
	}
	return schedules, nil
}

// Only a complete fixed-revision relation grants historical materialization.
// This does not grant selected execution of the copied pending schedule.
func validateRunForkBarrierSchedules(snapshot *runForkRevisionSnapshot, obligations []runfork.RunForkFanOutObligation) (map[string]struct{}, error) {
	timers := make(map[string]runForkRevisionTimer, len(snapshot.Timers))
	for _, timer := range snapshot.Timers {
		if _, duplicate := timers[timer.TimerID]; duplicate {
			return nil, fmt.Errorf("duplicate fixed-revision timer %s", timer.TimerID)
		}
		timers[timer.TimerID] = timer
	}
	owned := make(map[string]struct{})
	for _, obligation := range obligations {
		barrier := obligation.Barrier
		if barrier == nil {
			continue
		}
		if err := validateRunForkBarrierIntent(*barrier, obligation); err != nil {
			return nil, err
		}
		if barrier.TransferredOccurrence != nil {
			if err := requireRunForkTransferredBarrierHistory(snapshot, *barrier, timers); err != nil {
				return nil, err
			}
			continue
		}
		if barrier.ScheduleActivationID == "" {
			continue
		}
		id := barrier.ScheduleActivationID
		if _, duplicate := owned[id]; duplicate {
			return nil, fmt.Errorf("fixed-revision schedule %s has multiple barrier owners", id)
		}
		timer, found := timers[id]
		if !found {
			return nil, fmt.Errorf("fixed-revision barrier schedule %s is missing", id)
		}
		activation, err := projectRunForkGenericActivation(timer)
		if err != nil {
			return nil, fmt.Errorf("fixed-revision barrier schedule %s: %w", id, err)
		}
		if barrier.Status == fanoutbarrier.StatusClosedPending && activation.Status == genericschedule.StatusFired {
			if _, _, err := runForkPublishedFanOutBarrierEvidence(snapshot, *barrier, activation); err != nil {
				return nil, fmt.Errorf("fixed-revision published barrier %s: %w", id, err)
			}
		} else if err := genericschedule.ValidateFanOutBarrierScheduleRelation(*barrier, activation); err != nil {
			return nil, fmt.Errorf("fixed-revision barrier schedule %s: %w", id, err)
		}
		owned[id] = struct{}{}
	}
	return owned, nil
}

func validateRunForkBarrierIntent(barrier fanoutbarrier.Barrier, obligation runfork.RunForkFanOutObligation) error {
	if barrier.Registration.PlanRef != obligation.Intent.Request.PlanRef || barrier.Registration.IntentKey != obligation.Intent.Request.Key {
		return fmt.Errorf("fixed-revision barrier contradicts its fan-out intent")
	}
	if barrier.Summary != nil && barrier.Summary.Total != obligation.Intent.Request.Cardinality {
		return fmt.Errorf("fixed-revision barrier summary contradicts its fan-out cardinality")
	}
	return nil
}

func requireRunForkTransferredBarrierHistory(snapshot *runForkRevisionSnapshot, barrier fanoutbarrier.Barrier, timers map[string]runForkRevisionTimer) error {
	occurrence, err := genericschedule.NewTransferredFanOutBarrierOccurrence(barrier)
	if err != nil {
		return err
	}
	if snapshot == nil || snapshot.RunID != occurrence.Command.RunID || snapshot.Revision <= 0 {
		return fmt.Errorf("transferred barrier requires its exact historical owner and cut")
	}
	scope, err := occurrence.Command.ScopeKey()
	if err != nil {
		return err
	}
	for _, timer := range timers {
		if timer.ScheduleScope == scope && timer.ScheduleKey == occurrence.Command.ScheduleKey {
			return fmt.Errorf("transferred barrier cannot also own an executable schedule")
		}
	}
	event, found, err := runForkTransferredJoinEvent(snapshot, occurrence)
	if err != nil {
		return err
	}
	if !found {
		if barrier.Status == fanoutbarrier.StatusFired || barrier.Status == fanoutbarrier.StatusOutcomeDeadLettered {
			return fmt.Errorf("settled transferred barrier lacks its retained publication")
		}
		return nil
	}
	_, err = requireRunForkPublishedBarrierDelivery(snapshot, barrier, event, barrier.Status == fanoutbarrier.StatusClosedPending)
	return err
}

// Transport projection only: the existing activation and barrier owners validate
// the command, immutable hash, due coordinates, status, and ownership relation.
func projectRunForkGenericActivation(timer runForkRevisionTimer) (genericschedule.Activation, error) {
	if len(timer.ClockSuspension) != 0 && string(timer.ClockSuspension) != "null" {
		return genericschedule.Activation{}, fmt.Errorf("workflow join activation cannot carry clock suspension")
	}
	if timer.DueBasisKind != string(genericschedule.DueAbsolute) || timer.DueBasisAbsolute == nil ||
		timer.DueBasisDuration != "" || timer.DueBasisCron != "" || timer.Recurring || timer.RecurrenceInterval != "" ||
		timer.TaskType != "timer" || timer.OwnerKind != string(genericschedule.OwnerSystem) || timer.OwnerNode != "" ||
		timer.AgentNameOwner != "" || timer.AgentNameSource != "" || timer.AgentRoutePresence != "" ||
		timer.AgentFlowScopeKey != "" || timer.AgentFlowInstanceID != "" || timer.FlowScopeKey != "" || timer.FlowInstanceID != "" {
		return genericschedule.Activation{}, fmt.Errorf("workflow join activation has incompatible schedule storage coordinates")
	}
	payload, err := canonicaljson.Decode(timer.FirePayload)
	if err != nil {
		return genericschedule.Activation{}, err
	}
	var source events.RoutingSource
	if err := json.Unmarshal(timer.RoutingSource, &source); err != nil {
		return genericschedule.Activation{}, err
	}
	command := genericschedule.AdmissionCommand{
		ScheduleKey: timer.ScheduleKey, RunID: timer.RunID, EntityID: timer.EntityID, FlowInstance: timer.FlowInstance,
		OwnerKind: genericschedule.OwnerKind(timer.OwnerKind), OwnerID: timer.OwnerAgent,
		EventType: timer.FireEvent, Payload: payload, RoutingSource: source, ExecutionMode: executionmode.Mode(timer.ExecutionMode),
		ReplyContext: timer.ReplyContextID, TaskID: timer.TaskID, Due: genericschedule.AbsoluteDue(*timer.DueBasisAbsolute),
	}
	scope, err := command.ScopeKey()
	if err != nil || scope != timer.ScheduleScope || timer.TimerName != timer.ScheduleKey {
		return genericschedule.Activation{}, fmt.Errorf("workflow join activation storage identity contradicts admitted command")
	}
	value := func(at *time.Time) time.Time {
		if at == nil {
			return time.Time{}
		}
		return *at
	}
	activation := genericschedule.Activation{
		ID: timer.TimerID, Command: command, ImmutableHash: timer.ImmutableHash,
		AdmittedAt: timer.CreatedAt, InitialDueAt: value(timer.InitialFireAt), CurrentDueAt: timer.FireAt,
		CurrentEventID: timer.OccurrenceEventID, CurrentEventAdmittedAt: value(timer.OccurrenceAdmittedAt),
		Status: genericschedule.Status(timer.Status), CancelCause: timer.CancelCause, CancelledAt: value(timer.CancelledAt),
		FiredAt: value(timer.FiredAt), AcceptedAt: value(timer.AcceptedAt), FailedAt: value(timer.FailedAt),
		Failure: genericschedule.Failure{Code: timer.FailureCode, Message: timer.FailureMessage},
	}
	activation.ForkJoinOrigin, err = genericschedule.DecodeForkJoinOrigin(genericschedule.ForkJoinOrigin{
		SourceActivationID: timer.SourceTimerID, SourceRunID: timer.ForkedFromRunID,
		PointKind: timer.ForkedFromPointKind, PointRevision: timer.ForkedFromPointRevision,
		PointEventID: timer.ForkedFromEventID, SourceAdmittedAt: value(timer.SourceArmedAt), Owner: timer.ReconstructionOwner,
	})
	if err != nil {
		return genericschedule.Activation{}, err
	}
	return activation, activation.Validate()
}
