package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func (s *SQLiteRuntimeStore) LoadRunLifecycleSnapshot(ctx context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	if s == nil || s.DB == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("sqlite runtime store is required")
	}
	snap, err := loadSQLiteRunLifecycleSnapshot(ctx, s.DB, runID)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return projectBusRunLifecycleSnapshot(snap), nil
}

func sqliteDecisionProposedEffectsSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM decision_cards c
			LEFT JOIN proposed_effect_continuations p ON p.card_id = c.card_id
			WHERE c.run_id = ?
			  AND c.anchor_kind = 'proposed_effect'
			  AND (
				p.card_id IS NULL
				OR p.run_id <> c.run_id
				OR (
					c.status = 'decided'
					AND (
						p.state NOT IN ('request_released', 'outcome_dispatched')
						OR p.decision_event_id IS NULL
						OR p.route_event_id IS NULL
						OR c.decision_event_id IS NULL
						OR c.decision_event_id <> p.decision_event_id
						OR p.route_event_id <> p.decision_event_id
						OR NOT EXISTS (
							SELECT 1 FROM events e
							WHERE e.run_id = c.run_id
							  AND (
								(p.verdict = 'approve' AND e.event_id = p.request_event_id AND e.event_name = 'platform.activity_requested')
								OR (p.verdict = 'revise' AND e.source_event_id = p.decision_event_id AND e.event_name = json_extract(p.effect, '$.revision_event'))
								OR (p.verdict = 'reject' AND e.source_event_id = p.decision_event_id AND e.event_name = json_extract(p.effect, '$.rejected_event'))
							  )
						)
					)
				)
				OR (c.status = 'superseded' AND p.state <> 'superseded')
				OR c.status NOT IN ('decided', 'superseded')
			  )
		)
	`, runID).Scan(&unresolved); err != nil {
		return false, fmt.Errorf("check sqlite normal run proposed-effect settlement: %w", err)
	}
	if unresolved {
		return false, nil
	}
	return normalRunCompletionProposedEffectOutcomeEventsPersistedTx(ctx, tx, runID, false)
}

func sqliteDecisionHumanTasksSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM decision_cards c
			LEFT JOIN human_task_continuations h ON h.card_id = c.card_id
			WHERE c.run_id = ?
			  AND c.anchor_kind = 'human_task'
			  AND (
				h.card_id IS NULL
				OR h.run_id <> c.run_id
				OR h.state <> 'outcome_dispatched'
				OR h.outcome_event_id IS NULL
				OR c.status NOT IN ('decided', 'expired')
				OR (c.status = 'decided' AND (c.decision_event_id IS NULL OR c.decision_event_id <> h.outcome_event_id))
				OR NOT EXISTS (
					SELECT 1 FROM events e
					WHERE e.run_id = c.run_id
					  AND e.source_event_id = h.outcome_event_id
					  AND e.event_name = CASE
						WHEN c.status = 'expired' THEN 'human_task.expired'
						WHEN c.verdict = 'approve' THEN 'human_task.approved'
						WHEN c.verdict = 'reject' THEN 'human_task.rejected'
						ELSE ''
					  END
				)
			  )
		)
	`, runID).Scan(&unresolved); err != nil {
		return false, fmt.Errorf("check sqlite normal run human-task settlement: %w", err)
	}
	if unresolved {
		return false, nil
	}
	return normalRunCompletionHumanTaskOutcomeEventsPersistedTx(ctx, tx, runID, false)
}

func sqliteDecisionGatesSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM entity_state es, json_each(COALESCE(json_extract(es.accumulator, '$.stage_gates'), '{}')) gate
			WHERE es.run_id = ?
			  AND COALESCE(json_extract(gate.value, '$.status'), '') IN ('open', 'decision_committed')
		)
	`, runID).Scan(&active); err != nil {
		return false, fmt.Errorf("check sqlite normal run gate obligations: %w", err)
	}
	return !active, nil
}

func sqliteLoadStandaloneRuntimePlatformRunRecord(ctx context.Context, q rowQueryer, eventID string) (standaloneRuntimePlatformRunRecord, bool, error) {
	eventID = sanitizeOptionalUUID(eventID)
	if q == nil || eventID == "" {
		return standaloneRuntimePlatformRunRecord{}, false, nil
	}
	durable, found, err := loadSQLiteEventIdentity(ctx, q, eventID)
	if err != nil || !found {
		return standaloneRuntimePlatformRunRecord{}, found, err
	}
	admitted, err := decodeEventRecord(durable)
	if err != nil {
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("decode sqlite standalone runtime platform event: %w", err)
	}
	event := admitted.Event()
	rec := standaloneRuntimePlatformRunRecord{
		RunID: event.RunID(), EventID: event.ID(), EventClass: string(event.AdmissionClass()),
		EventType: string(event.Type()), ProducedBy: event.SourceAgent(), ProducedByType: string(event.ProducerType()),
		SourceEventID: event.ParentEventID(),
	}
	snapshot, err := loadSQLiteRunLifecycleSnapshot(ctx, q, rec.RunID)
	switch {
	case errors.Is(err, runtimerunlifecycle.ErrRunNotFound):
		return standaloneRuntimePlatformRunRecord{}, false, nil
	case err != nil:
		return standaloneRuntimePlatformRunRecord{}, false, fmt.Errorf("load sqlite standalone runtime platform run candidate: %w", err)
	default:
		rec.RunStatus = string(snapshot.State)
		rec.Origin = snapshot.Origin
		return rec, true, nil
	}
}
