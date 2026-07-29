package store

import (
	"context"
	"database/sql"
	"fmt"
)

func postgresDecisionProposedEffectsSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM decision_cards c
			LEFT JOIN proposed_effect_continuations p ON p.card_id = c.card_id
			WHERE c.run_id = $1::uuid
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
								OR (p.verdict = 'revise' AND e.source_event_id = p.decision_event_id AND e.event_name = p.effect->>'revision_event')
								OR (p.verdict = 'reject' AND e.source_event_id = p.decision_event_id AND e.event_name = p.effect->>'rejected_event')
							  )
						)
					)
				)
				OR (c.status = 'superseded' AND p.state <> 'superseded')
				OR c.status NOT IN ('decided', 'superseded')
			  )
		)
	`, runID).Scan(&unresolved); err != nil {
		return false, fmt.Errorf("check normal run proposed-effect settlement: %w", err)
	}
	if unresolved {
		return false, nil
	}
	return normalRunCompletionProposedEffectOutcomeEventsPersistedTx(ctx, tx, runID, true)
}

func postgresDecisionHumanTasksSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var unresolved bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM decision_cards c
			LEFT JOIN human_task_continuations h ON h.card_id = c.card_id
			WHERE c.run_id = $1::uuid
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
		return false, fmt.Errorf("check normal run human-task settlement: %w", err)
	}
	if unresolved {
		return false, nil
	}
	return normalRunCompletionHumanTaskOutcomeEventsPersistedTx(ctx, tx, runID, true)
}

func postgresDecisionGatesSettledTx(ctx context.Context, tx *sql.Tx, runID string) (bool, error) {
	var active bool
	if err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM entity_state es
			CROSS JOIN LATERAL jsonb_each(COALESCE(es.accumulator->'stage_gates', '{}'::jsonb)) gate
			WHERE es.run_id = $1::uuid
			  AND COALESCE(gate.value->>'status', '') IN ('open', 'decision_committed')
		)
	`, runID).Scan(&active); err != nil {
		return false, fmt.Errorf("check normal run gate obligations: %w", err)
	}
	return !active, nil
}
