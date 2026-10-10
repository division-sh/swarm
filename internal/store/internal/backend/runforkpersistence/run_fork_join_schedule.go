package runforkpersistence

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

// These are retained semantic rows, not fresh execution authority. The generic
// timer blocker remains until child materialization and recovery prove them.
func loadRunForkArrivalJoinSchedules(snapshot *runForkRevisionSnapshot, entities []runfork.RunForkEntityState) ([]genericschedule.Activation, error) {
	timers, err := indexRunForkJoinSchedules(snapshot)
	if err != nil {
		return nil, err
	}
	var schedules []genericschedule.Activation
	owned := make(map[string]struct{})
	for _, entity := range entities {
		buckets, err := joinruntime.PersistedBuckets(entity.Accumulator)
		if err != nil {
			return nil, err
		}
		joins, err := joinruntime.List(buckets)
		if err != nil {
			return nil, err
		}
		for _, join := range joins {
			if err := requireRunForkJoinSourceOwner(snapshot.RunID, entity, join); err != nil {
				return nil, err
			}
			rows, err := loadRunForkArrivalArmSchedules(join, timers, owned)
			if err != nil {
				return nil, err
			}
			schedules = append(schedules, rows...)
		}
	}
	sort.Slice(schedules, func(i, j int) bool { return schedules[i].ID < schedules[j].ID })
	return schedules, nil
}

func indexRunForkJoinSchedules(snapshot *runForkRevisionSnapshot) (map[string]runForkRevisionTimer, error) {
	timers := make(map[string]runForkRevisionTimer)
	for _, timer := range snapshot.Timers {
		if timer.RunID != snapshot.RunID {
			continue
		}
		if _, duplicate := timers[timer.ScheduleKey]; duplicate && timer.ScheduleKey != "" {
			return nil, fmt.Errorf("fixed-revision join schedule repeats key %s", timer.ScheduleKey)
		}
		if timer.ScheduleKey != "" {
			timers[timer.ScheduleKey] = timer
		}
	}
	return timers, nil
}

func loadRunForkArrivalArmSchedules(join joinruntime.Activation, timers map[string]runForkRevisionTimer, owned map[string]struct{}) ([]genericschedule.Activation, error) {
	if join.FireAt.IsZero() {
		return nil, nil
	}
	keys := []string{join.TimerTaskID()}
	if join.TimerHandle().Kind() == timeridentity.TimerHandleJoinComplete && !join.DeadlineAt.IsZero() {
		handle, err := timeridentity.JoinTimeoutHandle(join.JoinRef())
		if err != nil {
			return nil, err
		}
		keys = append(keys, handle.TaskID())
	}
	var schedules []genericschedule.Activation
	for _, key := range keys {
		timer, found := timers[key]
		if !found {
			return nil, fmt.Errorf("fixed-revision arrival arm lacks schedule %s", key)
		}
		if _, duplicate := owned[timer.TimerID]; duplicate {
			return nil, fmt.Errorf("fixed-revision schedule %s has multiple arrival owners", timer.TimerID)
		}
		activation, err := projectRunForkGenericActivation(timer)
		if err != nil {
			return nil, err
		}
		if err := genericschedule.ValidateWorkflowJoinScheduleRelation(join, activation); err != nil {
			return nil, err
		}
		owned[activation.ID] = struct{}{}
		schedules = append(schedules, activation)
	}
	return schedules, nil
}

func requireRunForkJoinSourceOwner(runID string, entity runfork.RunForkEntityState, join joinruntime.Activation) error {
	entry := join.JoinRef().StageEntry()
	metadata := entity.MaterializationMetadata
	if metadata == nil || metadata.Owner != runfork.RunForkMaterializedEntitySnapshotMetadataOwner ||
		metadata.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance {
		return fmt.Errorf("fixed-revision arrival arm lacks its exact materialized source owner")
	}
	receipt, err := pipeline.DecodeStoredFlowConstructionReceipt(metadata.InitialMaterialization,
		runID, entity.EntityID, metadata.FlowInstance, metadata.FlowTemplate)
	if err != nil {
		return err
	}
	owner := receipt.Identity
	if err := entry.RequireOwner(runID, owner.ScopeKey, owner.InstanceID, owner.InstancePath, owner.EntityID, join.Stage()); err != nil {
		return err
	}
	if join.JoinRef().FlowPath() != owner.TemplateID {
		return fmt.Errorf("fixed-revision arrival declaration contradicts its materialized source flow")
	}
	return nil
}
