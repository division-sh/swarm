package runforkpersistence

import (
	"fmt"
	"sort"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storegenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
)

func loadRunForkOneShotSchedules(snapshot *runForkRevisionSnapshot) ([]genericschedule.Activation, error) {
	var schedules []genericschedule.Activation
	for _, timer := range snapshot.Timers {
		if timer.RunID != snapshot.RunID || timer.OwnerKind != string(genericschedule.OwnerAgent) ||
			(timer.TaskType != "timer" && timer.TaskType != "scheduled_task") {
			continue
		}
		activation, err := storegenericschedule.DecodeSnapshot(timer.TimerSnapshot)
		if err != nil {
			return nil, err
		}
		if err := activation.ValidateForkOneShotRestorationSource(); err != nil {
			return nil, err
		}
		schedules = append(schedules, activation)
	}
	sort.Slice(schedules, func(i, j int) bool { return schedules[i].ID < schedules[j].ID })
	return schedules, nil
}

func runForkPlanTimerRecordInventory(plan runfork.RunForkPlan) (runForkTimerHistoryInventory, error) {
	inventory, err := runForkTimerRecordInventory(plan.SourceRunID, plan.WorkflowTimers, plan.JoinSchedules, plan.TransferredJoins...)
	if err != nil {
		return inventory, err
	}
	return includeRunForkOneShotInventory(inventory, plan.OneShotSchedules)
}

func includeRunForkOneShotInventory(inventory runForkTimerHistoryInventory, schedules []genericschedule.Activation) (runForkTimerHistoryInventory, error) {
	if len(schedules) == 0 {
		return inventory, nil
	}
	seen := make(map[string]bool)
	for _, ids := range [][]string{inventory.WorkflowTimerIDs, inventory.ArrivalScheduleIDs, inventory.TransferredPublicationIDs} {
		for _, id := range ids {
			seen[id] = true
		}
	}
	var evidence []runForkArrivalScheduleRecord
	for _, schedule := range schedules {
		if seen[schedule.ID] || schedule.Command.RunID != inventory.SourceRunID {
			return inventory, fmt.Errorf("one-shot inventory repeats an identity or includes a foreign owner")
		}
		if err := schedule.ValidateForkOneShotRestorationSource(); err != nil {
			return inventory, err
		}
		seen[schedule.ID] = true
		digest, err := schedule.EvidenceDigest()
		if err != nil {
			return inventory, err
		}
		evidence = append(evidence, runForkArrivalScheduleRecord{schedule.ID, digest})
		inventory.OneShotScheduleIDs = append(inventory.OneShotScheduleIDs, schedule.ID)
	}
	var unresolved []string
	for _, id := range inventory.UnresolvedTimerIDs {
		if !seen[id] {
			unresolved = append(unresolved, id)
		}
	}
	inventory.UnresolvedTimerIDs = unresolved
	sort.Strings(inventory.OneShotScheduleIDs)
	sort.Slice(evidence, func(i, j int) bool { return evidence[i].ID < evidence[j].ID })
	digest, err := canonicaljson.Hash(struct {
		Existing string
		OneShots []runForkArrivalScheduleRecord
	}{inventory.RecordDigest, evidence})
	if err != nil {
		return inventory, err
	}
	inventory.RecordDigest = digest
	return inventory, nil
}

func includeRunForkOneShotAppliedCertificate(existing, childRunID string, schedules []genericschedule.Activation) (string, error) {
	if len(schedules) == 0 {
		return existing, nil
	}
	var rows []runForkArrivalScheduleRecord
	for _, schedule := range schedules {
		if schedule.Command.RunID != childRunID || schedule.ForkOneShotOrigin == nil {
			return "", fmt.Errorf("one-shot readback certificate lacks exact child lineage")
		}
		digest, err := schedule.EvidenceDigest()
		if err != nil {
			return "", err
		}
		rows = append(rows, runForkArrivalScheduleRecord{schedule.ID, digest})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })
	digest, err := canonicaljson.Hash(struct {
		Existing string
		OneShots []runForkArrivalScheduleRecord
	}{existing, rows})
	if err != nil {
		return "", err
	}
	return runForkTimerAppliedPrefix + digest, nil
}
