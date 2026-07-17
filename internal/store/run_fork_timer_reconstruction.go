package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

type runForkTimerReconstructionPlan struct {
	Required bool
	Rows     []runForkTimerReconstructionRow
}

type runForkTimerReconstructionRow struct {
	TimerID            string
	ForkTimerID        string
	TimerName          string
	EntityID           string
	FlowInstance       string
	FireEvent          string
	FirePayload        []byte
	FireAt             time.Time
	Recurring          bool
	RecurrenceCron     string
	RecurrenceInterval string
	OwnerNode          string
	OwnerAgent         string
	TaskType           string
	Status             string
	FiredAt            sql.NullTime
	CreatedAt          time.Time
}

type timerReconstructionQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func runForkPlanHasTimerBlocker(plan RunForkPlan) bool {
	for _, blocker := range plan.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == RunForkBlockerTimerHistoryUnproven {
			return true
		}
	}
	return false
}

func runForkSelectedContractExecutionPlanBlockersWithTimerResolution(plan RunForkPlan, allowedSourceEventIDs []string, timerResolved bool) []RunForkUnsupportedBlocker {
	admission := RunForkSelectedContractReplayResumeAdmission(plan)
	if timerResolved {
		admission = runForkReplayResumeAdmissionWithTimerReconstruction(admission, runForkTimerReconstructionPlan{Required: true})
	}
	return runForkSelectedContractExecutionPlanBlockersFromAdmission(plan, admission, allowedSourceEventIDs)
}

func runForkReplayResumeAdmissionWithTimerReconstruction(admission RunForkReplayResumeAdmission, reconstruction runForkTimerReconstructionPlan) RunForkReplayResumeAdmission {
	if !reconstruction.Required {
		return admission
	}
	filteredBlockers := make([]RunForkUnsupportedBlocker, 0, len(admission.UnsupportedBlockers))
	for _, blocker := range admission.UnsupportedBlockers {
		if strings.TrimSpace(blocker.Code) == RunForkBlockerTimerHistoryUnproven {
			continue
		}
		filteredBlockers = append(filteredBlockers, blocker)
	}
	admission.UnsupportedBlockers = filteredBlockers
	for i := range admission.Dispositions {
		if strings.TrimSpace(admission.Dispositions[i].Fact) != RunForkReplayResumeFactTimerHistory {
			continue
		}
		admission.Dispositions[i].Disposition = RunForkReplayResumeDispositionReconstruct
		admission.Dispositions[i].BlockerCode = ""
		admission.Dispositions[i].Classification = RunForkHistoricalReplayAdmissionReconstructedForkState
		admission.Dispositions[i].Message = fmt.Sprintf("%s reconstructs %d active fork-local timer(s) under the fork run_id", RunForkHistoricalReplayTimerReconstructionOwner, len(reconstruction.Rows))
	}
	admission.BoundedReplaySupported = len(admission.UnsupportedBlockers) == 0
	return admission
}

func (s *PostgresStore) planRunForkSelectedContractTimerReconstruction(ctx context.Context, plan RunForkPlan) (runForkTimerReconstructionPlan, error) {
	if !runForkPlanHasTimerBlocker(plan) {
		return runForkTimerReconstructionPlan{}, nil
	}
	facts := loadRunForkSourceFactsFromRevision(plan.historicalSnapshot, plan.Entities)
	rows, err := loadRunForkReconstructableSourceTimersFromRevision(plan.historicalSnapshot, facts)
	if err != nil {
		return runForkTimerReconstructionPlan{}, err
	}
	if len(rows) == 0 {
		return runForkTimerReconstructionPlan{}, runForkReplayResumeError(RunForkBlockerTimerHistoryUnproven, RunForkReplayResumeFactTimerHistory, "selected-contract timer reconstruction blocked: no reconstructable active source timers")
	}
	return runForkTimerReconstructionPlan{Required: true, Rows: rows}, nil
}

func loadRunForkReconstructableSourceTimersFromRevision(snapshot *runForkRevisionSnapshot, facts runForkSourceFacts) ([]runForkTimerReconstructionRow, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("selected-contract timer reconstruction requires a revision snapshot")
	}
	if len(facts.EntityIDs) == 0 && len(facts.FlowInstances) == 0 {
		return nil, nil
	}
	entityIDs := stringSliceSet(facts.EntityIDs)
	flowInstances := stringSliceSet(facts.FlowInstances)
	out := []runForkTimerReconstructionRow{}
	for _, timer := range snapshot.Timers {
		_, entityRelevant := entityIDs[strings.TrimSpace(timer.EntityID)]
		_, flowRelevant := flowInstances[strings.TrimSpace(timer.FlowInstance)]
		if (!entityRelevant || strings.TrimSpace(timer.EntityID) == "") && (!flowRelevant || strings.TrimSpace(timer.FlowInstance) == "") {
			continue
		}
		row := runForkTimerReconstructionRow{
			TimerID:            timer.TimerID,
			TimerName:          timer.TimerName,
			EntityID:           timer.EntityID,
			FlowInstance:       timer.FlowInstance,
			FireEvent:          timer.FireEvent,
			FirePayload:        append([]byte(nil), timer.FirePayload...),
			FireAt:             timer.FireAt,
			Recurring:          timer.Recurring,
			RecurrenceCron:     timer.RecurrenceCron,
			RecurrenceInterval: timer.RecurrenceInterval,
			OwnerNode:          timer.OwnerNode,
			OwnerAgent:         timer.OwnerAgent,
			TaskType:           timer.TaskType,
			Status:             timer.Status,
			CreatedAt:          timer.CreatedAt,
		}
		if timer.FiredAt != nil {
			row.FiredAt = sql.NullTime{Time: *timer.FiredAt, Valid: true}
		}
		normalized, err := validateRunForkReconstructableSourceTimer(row)
		if err != nil {
			return nil, err
		}
		out = append(out, normalized)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TimerID < out[j].TimerID })
	return out, nil
}

func validateRunForkReconstructableSourceTimer(row runForkTimerReconstructionRow) (runForkTimerReconstructionRow, error) {
	workflowTimer := timeridentity.IsWorkflowTimerActivationTaskID(row.TimerName)
	if strings.TrimSpace(row.Status) != "active" || (row.FiredAt.Valid && !(workflowTimer && row.Recurring)) {
		return runForkTimerReconstructionRow{}, runForkReplayResumeError(RunForkBlockerTimerHistoryUnproven, RunForkReplayResumeFactTimerHistory, "selected-contract timer reconstruction blocked: source timer history is not active-at-fork only")
	}
	if strings.TrimSpace(row.OwnerAgent) == "" || strings.TrimSpace(row.FireEvent) == "" {
		return runForkTimerReconstructionRow{}, runForkReplayResumeError(RunForkBlockerTimerHistoryUnproven, RunForkReplayResumeFactTimerHistory, "selected-contract timer reconstruction blocked: source timer lacks executable owner/event identity")
	}
	if len(row.FirePayload) == 0 || string(row.FirePayload) == "null" {
		row.FirePayload = []byte("{}")
	}
	if !json.Valid(row.FirePayload) {
		return runForkTimerReconstructionRow{}, runForkReplayResumeError(RunForkBlockerTimerHistoryUnproven, RunForkReplayResumeFactTimerHistory, "selected-contract timer reconstruction blocked: source timer payload is invalid JSON")
	}
	if workflowTimer {
		ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(row.TimerName)
		if !ok || ref.ActivationID != strings.TrimSpace(row.TimerID) || strings.TrimSpace(row.OwnerNode) != "" {
			return runForkTimerReconstructionRow{}, runForkReplayResumeError(RunForkBlockerTimerHistoryUnproven, RunForkReplayResumeFactTimerHistory, "selected-contract timer reconstruction blocked: workflow timer activation identity is invalid")
		}
		if strings.TrimSpace(row.RecurrenceCron) != "" {
			return runForkTimerReconstructionRow{}, runForkReplayResumeError(RunForkBlockerTimerHistoryUnproven, RunForkReplayResumeFactTimerHistory, "selected-contract timer reconstruction blocked: workflow timer recurrence must use a persisted interval")
		}
	}
	return row, nil
}

func remintRunForkWorkflowTimer(row runForkTimerReconstructionRow, sourceRunID, forkRunID, forkEventID string) (runForkTimerReconstructionRow, error) {
	ref, ok := timeridentity.ParseWorkflowTimerActivationTaskID(row.TimerName)
	if !ok || ref.ActivationID != strings.TrimSpace(row.TimerID) {
		return row, fmt.Errorf("fork workflow timer activation identity is invalid")
	}
	var interval time.Duration
	if strings.TrimSpace(row.RecurrenceInterval) != "" {
		var parsed bool
		interval, parsed = timeridentity.ParseDelayDuration(row.RecurrenceInterval)
		if !parsed {
			return row, fmt.Errorf("fork workflow timer recurrence interval %q is invalid", row.RecurrenceInterval)
		}
	}
	source := runtimepipeline.WorkflowTimerActivation{
		Ref: ref, RunID: sourceRunID, EntityID: row.EntityID, FlowInstance: row.FlowInstance,
		OwnerAgent: row.OwnerAgent, EventType: row.FireEvent, Payload: append([]byte(nil), row.FirePayload...),
		FireAt: row.FireAt, Recurring: row.Recurring, RecurrenceInterval: interval,
		Status: row.Status, CreatedAt: row.CreatedAt,
	}
	if row.FiredAt.Valid {
		source.FiredAt = row.FiredAt.Time
	}
	forked, err := runtimepipeline.RemintWorkflowTimerActivationForFork(source, runtimepipeline.WorkflowTimerForkLineage{
		ForkRunID: forkRunID, ForkEventID: forkEventID,
		ReconstructionOwner: RunForkHistoricalReplayTimerReconstructionOwner,
	})
	if err != nil {
		return row, err
	}
	row.ForkTimerID = forked.Ref.ActivationID
	row.TimerName = forked.Ref.TaskID()
	row.FireAt = forked.FireAt
	row.FiredAt = sql.NullTime{}
	row.CreatedAt = forked.CreatedAt
	row.Status = forked.Status
	return row, nil
}

func insertRunForkSelectedContractTimerReconstructions(ctx context.Context, tx *sql.Tx, forkRunID, sourceRunID, forkEventID string, reconstruction runForkTimerReconstructionPlan, now time.Time) error {
	if !reconstruction.Required {
		return nil
	}
	for _, row := range reconstruction.Rows {
		var err error
		if timeridentity.IsWorkflowTimerActivationTaskID(row.TimerName) {
			row, err = remintRunForkWorkflowTimer(row, sourceRunID, forkRunID, forkEventID)
		} else {
			row, err = forkGenericAttemptGenerationTimer(row, forkRunID)
			row.CreatedAt = now
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO timers (
				timer_id, run_id, source_timer_id, forked_from_run_id, forked_from_event_id, reconstruction_owner,
				timer_name, entity_id, flow_instance, fire_event, fire_payload,
				fire_at, recurring, recurrence_cron, recurrence_interval,
				owner_node, owner_agent, task_type, status, created_at
			)
			VALUES (
				COALESCE(NULLIF($1,'')::uuid, gen_random_uuid()), $2::uuid, $3::uuid, $4::uuid, $5::uuid, $6,
				$7, NULLIF($8,'')::uuid, NULLIF($9,''), $10, $11::jsonb,
				$12, $13, NULLIF($14,''), NULLIF($15,''),
				NULLIF($16,''), NULLIF($17,''), $18, 'active', $19
			)
			ON CONFLICT DO NOTHING
		`,
			row.ForkTimerID, forkRunID, row.TimerID, sourceRunID, forkEventID, RunForkHistoricalReplayTimerReconstructionOwner,
			row.TimerName, row.EntityID, row.FlowInstance, row.FireEvent, string(row.FirePayload),
			row.FireAt, row.Recurring, row.RecurrenceCron, row.RecurrenceInterval,
			row.OwnerNode, row.OwnerAgent, row.TaskType, row.CreatedAt,
		); err != nil {
			return fmt.Errorf("insert selected-contract reconstructed timer: %w", err)
		}
	}
	return nil
}

func runForkSelectedContractTimerReconstructionComplete(ctx context.Context, tx *sql.Tx, lineage runForkActivationLineage, plan RunForkPlan) (bool, error) {
	if !runForkPlanHasTimerBlocker(plan) {
		return false, nil
	}
	facts := loadRunForkSourceFactsFromRevision(plan.historicalSnapshot, plan.Entities)
	expected, err := loadRunForkReconstructableSourceTimersFromRevision(plan.historicalSnapshot, facts)
	if err != nil {
		return false, err
	}
	if len(expected) == 0 {
		return false, nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT source_timer_id::text
		FROM timers
		WHERE run_id = $1::uuid
		  AND forked_from_run_id = $2::uuid
		  AND forked_from_event_id = $3::uuid
		  AND reconstruction_owner = $4
		  AND status = 'active'
	`, lineage.ForkRunID, lineage.SourceRunID, lineage.ForkEventID, RunForkHistoricalReplayTimerReconstructionOwner)
	if err != nil {
		return false, fmt.Errorf("load fork reconstructed timers: %w", err)
	}
	defer rows.Close()
	actual := map[string]struct{}{}
	for rows.Next() {
		var timerID string
		if err := rows.Scan(&timerID); err != nil {
			return false, fmt.Errorf("scan fork reconstructed timer: %w", err)
		}
		actual[timerID] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate fork reconstructed timers: %w", err)
	}
	if len(actual) != len(expected) {
		return false, nil
	}
	for _, timer := range expected {
		if _, ok := actual[timer.TimerID]; !ok {
			return false, nil
		}
	}
	return true, nil
}
