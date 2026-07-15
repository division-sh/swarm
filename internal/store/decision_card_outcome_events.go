package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
)

type decisionCardOutcomeEvent struct {
	eventID       string
	runID         string
	eventName     string
	sourceEventID string
}

func requireDecisionCardOutcomeEvent(ctx context.Context, tx *sql.Tx, expected decisionCardOutcomeEvent, postgres bool) error {
	persisted, err := decisionCardOutcomeEventPersisted(ctx, tx, expected, postgres)
	if err != nil {
		return err
	}
	if !persisted {
		return fmt.Errorf("decision-card outcome event is not persisted in the active pipeline mutation")
	}
	return nil
}

func decisionCardOutcomeEventPersisted(ctx context.Context, tx *sql.Tx, expected decisionCardOutcomeEvent, postgres bool) (bool, error) {
	expected.eventID = strings.TrimSpace(expected.eventID)
	expected.runID = strings.TrimSpace(expected.runID)
	expected.eventName = strings.TrimSpace(expected.eventName)
	expected.sourceEventID = strings.TrimSpace(expected.sourceEventID)
	if tx == nil || expected.eventID == "" || expected.runID == "" || expected.eventName == "" {
		return false, fmt.Errorf("decision-card outcome event identity is incomplete")
	}
	query := `SELECT COUNT(*) FROM events
		WHERE event_id = ? AND run_id = ? AND event_name = ? AND COALESCE(CAST(source_event_id AS TEXT), '') = ?`
	if postgres {
		query = `SELECT COUNT(*) FROM events
			WHERE event_id = $1::uuid AND run_id = $2::uuid AND event_name = $3 AND COALESCE(source_event_id::text, '') = $4`
	}
	var count int
	if err := tx.QueryRowContext(ctx, query, expected.eventID, expected.runID, expected.eventName, expected.sourceEventID).Scan(&count); err != nil {
		return false, fmt.Errorf("verify decision-card outcome event: %w", err)
	}
	return count == 1, nil
}

func normalRunCompletionHumanTaskOutcomeEventsPersistedTx(ctx context.Context, tx *sql.Tx, runID string, postgres bool) (bool, error) {
	query := `
		SELECT c.card_id, c.run_id, c.status, COALESCE(c.verdict, ''), h.outcome_event_id
		FROM decision_cards c
		INNER JOIN human_task_continuations h ON h.card_id = c.card_id
		WHERE c.run_id = ?
		  AND c.anchor_kind = 'human_task'
		  AND h.state = 'outcome_dispatched'`
	if postgres {
		query = `
			SELECT c.card_id::text, c.run_id::text, c.status, COALESCE(c.verdict, ''), h.outcome_event_id::text
			FROM decision_cards c
			INNER JOIN human_task_continuations h ON h.card_id = c.card_id
			WHERE c.run_id = $1::uuid
			  AND c.anchor_kind = 'human_task'
			  AND h.state = 'outcome_dispatched'`
	}
	rows, err := tx.QueryContext(ctx, query, strings.TrimSpace(runID))
	if err != nil {
		return false, fmt.Errorf("list normal run human-task outcome events: %w", err)
	}
	var expected []decisionCardOutcomeEvent
	for rows.Next() {
		var cardID, eventRunID, status, verdict, outcomeEventID string
		if err := rows.Scan(&cardID, &eventRunID, &status, &verdict, &outcomeEventID); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("scan normal run human-task outcome event: %w", err)
		}
		eventName := ""
		switch {
		case status == string(decisioncard.StatusExpired):
			eventName = "human_task.expired"
		case status == string(decisioncard.StatusDecided) && verdict == "approve":
			eventName = "human_task.approved"
		case status == string(decisioncard.StatusDecided) && verdict == "reject":
			eventName = "human_task.rejected"
		default:
			_ = rows.Close()
			return false, fmt.Errorf("human-task outcome identity is inconsistent")
		}
		expected = append(expected, decisionCardOutcomeEvent{
			eventID: decisioncard.HumanTaskOutcomeEventID(cardID, outcomeEventID), runID: eventRunID,
			eventName: eventName, sourceEventID: outcomeEventID,
		})
	}
	if err := rows.Close(); err != nil {
		return false, fmt.Errorf("close normal run human-task outcome events: %w", err)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate normal run human-task outcome events: %w", err)
	}
	return decisionCardOutcomeEventsPersisted(ctx, tx, expected, postgres)
}

func normalRunCompletionProposedEffectOutcomeEventsPersistedTx(ctx context.Context, tx *sql.Tx, runID string, postgres bool) (bool, error) {
	query := `
		SELECT c.card_id, c.run_id, p.decision_event_id, p.verdict,
		       COALESCE(json_extract(p.effect, '$.revision_event'), ''),
		       COALESCE(json_extract(p.effect, '$.rejected_event'), '')
		FROM decision_cards c
		INNER JOIN proposed_effect_continuations p ON p.card_id = c.card_id
		WHERE c.run_id = ?
		  AND c.anchor_kind = 'proposed_effect'
		  AND c.status = 'decided'
		  AND p.state = 'outcome_dispatched'
		  AND p.verdict IN ('revise', 'reject')`
	if postgres {
		query = `
			SELECT c.card_id::text, c.run_id::text, p.decision_event_id::text, p.verdict,
			       COALESCE(p.effect->>'revision_event', ''),
			       COALESCE(p.effect->>'rejected_event', '')
			FROM decision_cards c
			INNER JOIN proposed_effect_continuations p ON p.card_id = c.card_id
			WHERE c.run_id = $1::uuid
			  AND c.anchor_kind = 'proposed_effect'
			  AND c.status = 'decided'
			  AND p.state = 'outcome_dispatched'
			  AND p.verdict IN ('revise', 'reject')`
	}
	rows, err := tx.QueryContext(ctx, query, strings.TrimSpace(runID))
	if err != nil {
		return false, fmt.Errorf("list normal run proposed-effect outcome events: %w", err)
	}
	var expected []decisionCardOutcomeEvent
	for rows.Next() {
		var cardID, eventRunID, decisionEventID, verdict, revisionEvent, rejectedEvent string
		if err := rows.Scan(&cardID, &eventRunID, &decisionEventID, &verdict, &revisionEvent, &rejectedEvent); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("scan normal run proposed-effect outcome event: %w", err)
		}
		eventName := rejectedEvent
		if verdict == "revise" {
			eventName = revisionEvent
		}
		expected = append(expected, decisionCardOutcomeEvent{
			eventID: decisioncard.ProposedEffectOutcomeEventID(cardID, decisionEventID, verdict), runID: eventRunID,
			eventName: eventName, sourceEventID: decisionEventID,
		})
	}
	if err := rows.Close(); err != nil {
		return false, fmt.Errorf("close normal run proposed-effect outcome events: %w", err)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate normal run proposed-effect outcome events: %w", err)
	}
	return decisionCardOutcomeEventsPersisted(ctx, tx, expected, postgres)
}

func decisionCardOutcomeEventsPersisted(ctx context.Context, tx *sql.Tx, expected []decisionCardOutcomeEvent, postgres bool) (bool, error) {
	for _, event := range expected {
		persisted, err := decisionCardOutcomeEventPersisted(ctx, tx, event, postgres)
		if err != nil || !persisted {
			return persisted, err
		}
	}
	return true, nil
}
