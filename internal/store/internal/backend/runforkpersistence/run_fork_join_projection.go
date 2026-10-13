package runforkpersistence

import (
	"fmt"
	"slices"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

type runForkArrivalJoinScheduleProjection struct {
	source  genericschedule.Activation
	command genericschedule.AdmissionCommand
}

// Projection retains source lifecycle evidence separately from the child
// command. In particular, a published source occurrence is not a new child arm.
func prepareRunForkArrivalJoinSchedules(plan runfork.RunForkPlan, childRunID string) ([]runForkArrivalJoinScheduleProjection, error) {
	byKey := make(map[runForkJoinScheduleKey]genericschedule.Activation, len(plan.JoinSchedules))
	for _, schedule := range plan.JoinSchedules {
		if err := schedule.Validate(); err != nil {
			return nil, err
		}
		if schedule.Command.RunID != plan.SourceRunID {
			return nil, fmt.Errorf("retained arrival schedule belongs to another source run")
		}
		if schedule.Status != genericschedule.StatusFired {
			if err := requireRunForkArrivalRestorationEvidence(plan, schedule); err != nil {
				return nil, err
			}
		}
		scope, err := schedule.Command.ScopeKey()
		if err != nil {
			return nil, err
		}
		key := runForkJoinScheduleKey{scope, schedule.Command.ScheduleKey}
		if _, duplicate := byKey[key]; duplicate {
			return nil, fmt.Errorf("retained arrival schedule repeats its scoped identity")
		}
		byKey[key] = schedule
	}
	var projected []runForkArrivalJoinScheduleProjection
	used := make(map[string]struct{}, len(plan.JoinSchedules))
	for _, entity := range plan.Entities {
		buckets, err := joinruntime.PersistedBuckets(entity.Accumulator)
		if err != nil {
			return nil, err
		}
		joins, err := joinruntime.List(buckets)
		if err != nil {
			return nil, err
		}
		for _, join := range joins {
			if err := requireRunForkJoinSourceOwner(plan.SourceRunID, entity, join); err != nil {
				return nil, err
			}
			keys, err := runForkArrivalArmScheduleKeys(join)
			if err != nil {
				return nil, err
			}
			for _, key := range keys {
				if join.TransferredPublication != nil && key.schedule == join.TimerTaskID() {
					continue
				}
				source, found := byKey[key]
				if !found {
					return nil, fmt.Errorf("retained arrival arm lacks its exact source schedule")
				}
				if _, duplicate := used[source.ID]; duplicate {
					return nil, fmt.Errorf("retained arrival schedule has multiple source owners")
				}
				command, err := projectRunForkArrivalJoinSchedule(plan, childRunID, join, source)
				if err != nil {
					return nil, err
				}
				used[source.ID] = struct{}{}
				projected = append(projected, runForkArrivalJoinScheduleProjection{source, command})
			}
		}
	}
	if len(used) != len(plan.JoinSchedules) {
		return nil, fmt.Errorf("retained arrival schedule has no exact source arm")
	}
	sort.Slice(projected, func(i, j int) bool { return projected[i].source.ID < projected[j].source.ID })
	return projected, nil
}

// Preparation reserves an occurrence; only the complete fixed-cut event owner
// can establish that it is still unpublished. Source stamps grant no execution.
func requireRunForkArrivalRestorationEvidence(plan runfork.RunForkPlan, source genericschedule.Activation) error {
	if source.Command.RunID != plan.SourceRunID {
		return fmt.Errorf("arrival restoration source belongs to another run")
	}
	if err := source.ValidateForkJoinRestorationSource(); err != nil {
		return err
	}
	if source.CurrentEventID == "" {
		return nil
	}
	history, complete := plan.HistoricalEventIDs(plan.ForkPoint.Revision)
	if !complete || slices.Contains(history, source.CurrentEventID) {
		return fmt.Errorf("prepared arrival requires complete fixed-cut evidence of no committed publication")
	}
	return nil
}

func projectRunForkArrivalJoinSchedule(plan runfork.RunForkPlan, childRunID string, join joinruntime.Activation, source genericschedule.Activation) (genericschedule.AdmissionCommand, error) {
	if err := genericschedule.ValidateWorkflowJoinScheduleRelation(join, source); err != nil {
		return genericschedule.AdmissionCommand{}, err
	}
	return projectRunForkArrivalJoinCommand(plan, childRunID, join, source.Command)
}

func projectRunForkArrivalJoinCommand(plan runfork.RunForkPlan, childRunID string, join joinruntime.Activation, source genericschedule.AdmissionCommand) (genericschedule.AdmissionCommand, error) {
	ref, err := projectConstructionReturnAtCut(plan, childRunID, join.JoinRef())
	if err != nil {
		return genericschedule.AdmissionCommand{}, err
	}
	child, err := join.WithForkReference(ref)
	if err != nil {
		return genericschedule.AdmissionCommand{}, err
	}
	if source.TaskID != join.TimerTaskID() {
		handle, err := timeridentity.JoinTimeoutHandle(ref)
		if err != nil {
			return genericschedule.AdmissionCommand{}, err
		}
		child, err = child.WithTimerHandle(handle, child.DeadlineAt)
		if err != nil {
			return genericschedule.AdmissionCommand{}, err
		}
	}
	return genericschedule.WorkflowJoinAdmission(child, source.ExecutionMode)
}
