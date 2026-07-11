package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
)

type runForkTimerReconstructionPlan struct {
	Required bool
	Rows     []runForkTimerReconstructionRow
}

type runForkTimerReconstructionRow struct {
	TimerID            string
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

func (s *PostgresStore) planRunForkSelectedContractTimerReconstruction(ctx context.Context, catalog schemaColumnCatalog, plan RunForkPlan) (runForkTimerReconstructionPlan, error) {
	if !runForkPlanHasTimerBlocker(plan) {
		return runForkTimerReconstructionPlan{}, nil
	}
	if !catalog.hasColumns("timers", "run_id", "source_timer_id", "forked_from_run_id", "forked_from_event_id", "reconstruction_owner") {
		return runForkTimerReconstructionPlan{}, fmt.Errorf("selected-contract timer reconstruction requires run-scoped timer lineage columns")
	}
	facts, err := s.loadRunForkSourceFacts(ctx, plan.SourceRunID, runForkEventCursor{
		EventID:   plan.ForkPoint.EventID,
		EventName: plan.ForkPoint.EventName,
		CreatedAt: plan.ForkPoint.Timestamp,
	}, plan.Entities)
	if err != nil {
		return runForkTimerReconstructionPlan{}, err
	}
	rows, err := loadRunForkReconstructableSourceTimers(ctx, s.DB, plan.SourceRunID, plan.ForkPoint.Timestamp, facts)
	if err != nil {
		return runForkTimerReconstructionPlan{}, err
	}
	if len(rows) == 0 {
		return runForkTimerReconstructionPlan{}, runForkReplayResumeError(RunForkBlockerTimerHistoryUnproven, RunForkReplayResumeFactTimerHistory, "selected-contract timer reconstruction blocked: no reconstructable active source timers")
	}
	return runForkTimerReconstructionPlan{Required: true, Rows: rows}, nil
}

func loadRunForkReconstructableSourceTimers(ctx context.Context, q timerReconstructionQueryer, sourceRunID string, at time.Time, facts runForkSourceFacts) ([]runForkTimerReconstructionRow, error) {
	if len(facts.EntityIDs) == 0 && len(facts.FlowInstances) == 0 {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, `
		SELECT
			timer_id::text,
			timer_name,
			COALESCE(entity_id::text, ''),
			COALESCE(flow_instance, ''),
			fire_event,
			fire_payload,
			fire_at,
			recurring,
			COALESCE(recurrence_cron, ''),
			COALESCE(recurrence_interval, ''),
			COALESCE(owner_node, ''),
			COALESCE(owner_agent, ''),
			task_type,
			status,
			fired_at,
			created_at
		FROM timers
		WHERE run_id = $1::uuid
		  AND created_at <= $2::timestamptz
		  AND (
				(entity_id IS NOT NULL AND entity_id::text = ANY($3::text[]))
				OR
				(COALESCE(flow_instance, '') <> '' AND flow_instance = ANY($4::text[]))
		  )
		ORDER BY created_at ASC, timer_id ASC
	`, sourceRunID, at, pq.Array(facts.EntityIDs), pq.Array(facts.FlowInstances))
	if err != nil {
		return nil, fmt.Errorf("load source timers for reconstruction: %w", err)
	}
	defer rows.Close()

	out := []runForkTimerReconstructionRow{}
	for rows.Next() {
		var row runForkTimerReconstructionRow
		if err := rows.Scan(
			&row.TimerID,
			&row.TimerName,
			&row.EntityID,
			&row.FlowInstance,
			&row.FireEvent,
			&row.FirePayload,
			&row.FireAt,
			&row.Recurring,
			&row.RecurrenceCron,
			&row.RecurrenceInterval,
			&row.OwnerNode,
			&row.OwnerAgent,
			&row.TaskType,
			&row.Status,
			&row.FiredAt,
			&row.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan source timer for reconstruction: %w", err)
		}
		normalized, err := validateRunForkReconstructableSourceTimer(row)
		if err != nil {
			return nil, err
		}
		out = append(out, normalized)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source timers for reconstruction: %w", err)
	}
	return out, nil
}

func validateRunForkReconstructableSourceTimer(row runForkTimerReconstructionRow) (runForkTimerReconstructionRow, error) {
	if strings.TrimSpace(row.Status) != "active" || row.FiredAt.Valid {
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
	return row, nil
}

func insertRunForkSelectedContractTimerReconstructions(ctx context.Context, tx *sql.Tx, forkRunID, sourceRunID, forkEventID string, reconstruction runForkTimerReconstructionPlan, now time.Time) error {
	if !reconstruction.Required {
		return nil
	}
	for _, row := range reconstruction.Rows {
		var err error
		row, err = forkAttemptGenerationTimer(row, forkRunID)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO timers (
				run_id, source_timer_id, forked_from_run_id, forked_from_event_id, reconstruction_owner,
				timer_name, entity_id, flow_instance, fire_event, fire_payload,
				fire_at, recurring, recurrence_cron, recurrence_interval,
				owner_node, owner_agent, task_type, status, created_at
			)
			VALUES (
				$1::uuid, $2::uuid, $3::uuid, $4::uuid, $5,
				$6, NULLIF($7,'')::uuid, NULLIF($8,''), $9, $10::jsonb,
				$11, $12, NULLIF($13,''), NULLIF($14,''),
				NULLIF($15,''), NULLIF($16,''), $17, 'active', $18
			)
			ON CONFLICT DO NOTHING
		`,
			forkRunID, row.TimerID, sourceRunID, forkEventID, RunForkHistoricalReplayTimerReconstructionOwner,
			row.TimerName, row.EntityID, row.FlowInstance, row.FireEvent, string(row.FirePayload),
			row.FireAt, row.Recurring, row.RecurrenceCron, row.RecurrenceInterval,
			row.OwnerNode, row.OwnerAgent, row.TaskType, now,
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
	var sourceCount, forkCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE run_id = $1::uuid
		  AND created_at <= $2::timestamptz
		  AND status = 'active'
		  AND fired_at IS NULL
		  AND (
				(entity_id IS NOT NULL AND entity_id::text = ANY($3::text[]))
				OR
				(COALESCE(flow_instance, '') <> '' AND flow_instance = ANY($4::text[]))
		  )
	`, lineage.SourceRunID, lineage.ForkEventTime, pq.Array(lineage.EntityIDs), pq.Array(lineage.FlowInstances)).Scan(&sourceCount); err != nil {
		return false, fmt.Errorf("count source timers for reconstruction proof: %w", err)
	}
	if sourceCount == 0 {
		return false, nil
	}
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT source_timer_id)
		FROM timers
		WHERE run_id = $1::uuid
		  AND forked_from_run_id = $2::uuid
		  AND forked_from_event_id = $3::uuid
		  AND reconstruction_owner = $4
		  AND status = 'active'
	`, lineage.ForkRunID, lineage.SourceRunID, lineage.ForkEventID, RunForkHistoricalReplayTimerReconstructionOwner).Scan(&forkCount); err != nil {
		return false, fmt.Errorf("count fork reconstructed timers: %w", err)
	}
	return forkCount == sourceCount, nil
}
