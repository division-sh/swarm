package runforkpersistence

import (
	"context"
	"database/sql"
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

func loadRunForkPendingBarrierSchedules(ctx context.Context, tx *sql.Tx, plan runfork.RunForkPlan) (map[string]genericschedule.Activation, error) {
	pending := make(map[string]struct{})
	for _, obligation := range plan.FanOutObligations {
		barrier := obligation.Barrier
		if barrier == nil || barrier.Status != fanoutbarrier.StatusClosedPending {
			continue
		}
		if barrier.Registration.IntentKey.RunID != plan.SourceRunID {
			return nil, fmt.Errorf("pending fork barrier belongs to another source run")
		}
		pending[barrier.ScheduleActivationID] = struct{}{}
	}
	if len(pending) == 0 {
		return nil, nil
	}
	snapshot, err := loadRunForkPointSnapshot(ctx, tx, plan.SourceRunID, plan.ForkPoint)
	if err != nil {
		return nil, err
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
		if activation.Status != genericschedule.StatusActive {
			return nil, fmt.Errorf("published fork barrier occurrence requires historical event continuation, not rearming")
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
		if barrier.Registration.PlanRef != obligation.Intent.Request.PlanRef || barrier.Registration.IntentKey != obligation.Intent.Request.Key {
			return nil, fmt.Errorf("fixed-revision barrier contradicts its fan-out intent")
		}
		if barrier.Summary != nil && barrier.Summary.Total != obligation.Intent.Request.Cardinality {
			return nil, fmt.Errorf("fixed-revision barrier summary contradicts its fan-out cardinality")
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
		if err := genericschedule.ValidateFanOutBarrierScheduleRelation(*barrier, activation); err != nil {
			return nil, fmt.Errorf("fixed-revision barrier schedule %s: %w", id, err)
		}
		owned[id] = struct{}{}
	}
	return owned, nil
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
	return activation, activation.Validate()
}
