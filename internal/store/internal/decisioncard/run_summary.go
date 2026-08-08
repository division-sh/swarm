package decisionstore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	runtimedecision "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
)

type SummaryDialect string

const (
	SummaryDialectPostgres SummaryDialect = "postgres"
	SummaryDialectSQLite   SummaryDialect = "sqlite"
)

type SummaryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// ReadRunSummary is the decision owner's selected-store projection of every
// decision-card continuation and gate activation that can block run completion.
func ReadRunSummary(ctx context.Context, queryer SummaryQueryer, dialect SummaryDialect, runID string) (runtimedecision.RunSummary, error) {
	runID = strings.TrimSpace(runID)
	if queryer == nil || runID == "" {
		return runtimedecision.RunSummary{}, fmt.Errorf("decision run summary requires selected store and run_id")
	}
	summary := runtimedecision.RunSummary{RunID: runID}
	var err error
	summary.UnresolvedHumanTasks, err = unresolvedHumanTasks(ctx, queryer, dialect, runID)
	if err != nil {
		return runtimedecision.RunSummary{}, err
	}
	summary.UnresolvedEffects, err = unresolvedProposedEffects(ctx, queryer, dialect, runID)
	if err != nil {
		return runtimedecision.RunSummary{}, err
	}
	open, malformed, err := summarizeGates(ctx, queryer, dialect, runID)
	if err != nil {
		return runtimedecision.RunSummary{}, err
	}
	summary.OpenGateObligations = open
	summary.MalformedObligations = malformed
	return summary, summary.Validate()
}

func unresolvedHumanTasks(ctx context.Context, q SummaryQueryer, dialect SummaryDialect, runID string) (int, error) {
	query := `
		SELECT COUNT(*)
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
		  )`
	args := []any{runID}
	switch dialect {
	case SummaryDialectSQLite:
	case SummaryDialectPostgres:
		query = `
			SELECT COUNT(*)
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
			  )`
		args = []any{runID}
	default:
		return 0, fmt.Errorf("decision run summary requires selected store dialect")
	}
	var unresolved int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&unresolved); err != nil {
		return 0, fmt.Errorf("summarize human-task continuations: %w", err)
	}
	exact, err := humanOutcomeEvents(ctx, q, dialect, runID)
	if err != nil {
		return 0, err
	}
	return unresolved + exact, nil
}

func unresolvedProposedEffects(ctx context.Context, q SummaryQueryer, dialect SummaryDialect, runID string) (int, error) {
	query := `
		SELECT COUNT(*)
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
		  )`
	args := []any{runID}
	switch dialect {
	case SummaryDialectSQLite:
	case SummaryDialectPostgres:
		query = `
			SELECT COUNT(*)
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
			  )`
	default:
		return 0, fmt.Errorf("decision run summary requires selected store dialect")
	}
	var unresolved int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&unresolved); err != nil {
		return 0, fmt.Errorf("summarize proposed-effect continuations: %w", err)
	}
	exact, err := proposedEffectOutcomeEvents(ctx, q, dialect, runID)
	if err != nil {
		return 0, err
	}
	return unresolved + exact, nil
}

func humanOutcomeEvents(ctx context.Context, q SummaryQueryer, dialect SummaryDialect, runID string) (int, error) {
	query := `
		SELECT c.card_id, c.run_id, c.status, COALESCE(c.verdict, ''), h.outcome_event_id
		FROM decision_cards c
		INNER JOIN human_task_continuations h ON h.card_id = c.card_id
		WHERE c.run_id = ? AND c.anchor_kind = 'human_task' AND h.state = 'outcome_dispatched'`
	if dialect == SummaryDialectPostgres {
		query = `
			SELECT c.card_id::text, c.run_id::text, c.status, COALESCE(c.verdict, ''), h.outcome_event_id::text
			FROM decision_cards c
			INNER JOIN human_task_continuations h ON h.card_id = c.card_id
			WHERE c.run_id = $1::uuid AND c.anchor_kind = 'human_task' AND h.state = 'outcome_dispatched'`
	}
	rows, err := q.QueryContext(ctx, query, runID)
	if err != nil {
		return 0, fmt.Errorf("list human-task outcome identities: %w", err)
	}
	var expected []expectedOutcomeEvent
	for rows.Next() {
		var cardID, eventRunID, status, verdict, sourceID string
		if err := rows.Scan(&cardID, &eventRunID, &status, &verdict, &sourceID); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan human-task outcome identity: %w", err)
		}
		eventName := ""
		switch {
		case status == string(runtimedecision.StatusExpired):
			eventName = "human_task.expired"
		case status == string(runtimedecision.StatusDecided) && verdict == "approve":
			eventName = "human_task.approved"
		case status == string(runtimedecision.StatusDecided) && verdict == "reject":
			eventName = "human_task.rejected"
		default:
			_ = rows.Close()
			return 0, fmt.Errorf("human-task outcome identity is inconsistent")
		}
		expected = append(expected, expectedOutcomeEvent{
			id: runtimedecision.HumanTaskOutcomeEventID(cardID, sourceID), runID: eventRunID,
			name: eventName, sourceID: sourceID,
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate human-task outcome identities: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close human-task outcome identities: %w", err)
	}
	unresolved := 0
	for _, event := range expected {
		ok, err := exactOutcomeEvent(ctx, q, dialect, event.id, event.runID, event.name, event.sourceID)
		if err != nil {
			return 0, err
		}
		if !ok {
			unresolved++
		}
	}
	return unresolved, nil
}

func proposedEffectOutcomeEvents(ctx context.Context, q SummaryQueryer, dialect SummaryDialect, runID string) (int, error) {
	query := `
		SELECT c.card_id, c.run_id, p.decision_event_id, p.verdict,
		       COALESCE(json_extract(p.effect, '$.revision_event'), ''),
		       COALESCE(json_extract(p.effect, '$.rejected_event'), '')
		FROM decision_cards c
		INNER JOIN proposed_effect_continuations p ON p.card_id = c.card_id
		WHERE c.run_id = ? AND c.anchor_kind = 'proposed_effect'
		  AND c.status = 'decided' AND p.state = 'outcome_dispatched' AND p.verdict IN ('revise', 'reject')`
	if dialect == SummaryDialectPostgres {
		query = `
			SELECT c.card_id::text, c.run_id::text, p.decision_event_id::text, p.verdict,
			       COALESCE(p.effect->>'revision_event', ''), COALESCE(p.effect->>'rejected_event', '')
			FROM decision_cards c
			INNER JOIN proposed_effect_continuations p ON p.card_id = c.card_id
			WHERE c.run_id = $1::uuid AND c.anchor_kind = 'proposed_effect'
			  AND c.status = 'decided' AND p.state = 'outcome_dispatched' AND p.verdict IN ('revise', 'reject')`
	}
	rows, err := q.QueryContext(ctx, query, runID)
	if err != nil {
		return 0, fmt.Errorf("list proposed-effect outcome identities: %w", err)
	}
	var expected []expectedOutcomeEvent
	for rows.Next() {
		var cardID, eventRunID, sourceID, verdict, revisionEvent, rejectedEvent string
		if err := rows.Scan(&cardID, &eventRunID, &sourceID, &verdict, &revisionEvent, &rejectedEvent); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan proposed-effect outcome identity: %w", err)
		}
		eventName := rejectedEvent
		if verdict == "revise" {
			eventName = revisionEvent
		}
		expected = append(expected, expectedOutcomeEvent{
			id: runtimedecision.ProposedEffectOutcomeEventID(cardID, sourceID, verdict), runID: eventRunID,
			name: eventName, sourceID: sourceID,
		})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate proposed-effect outcome identities: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("close proposed-effect outcome identities: %w", err)
	}
	unresolved := 0
	for _, event := range expected {
		ok, err := exactOutcomeEvent(ctx, q, dialect, event.id, event.runID, event.name, event.sourceID)
		if err != nil {
			return 0, err
		}
		if !ok {
			unresolved++
		}
	}
	return unresolved, nil
}

type expectedOutcomeEvent struct {
	id       string
	runID    string
	name     string
	sourceID string
}

func exactOutcomeEvent(ctx context.Context, q SummaryQueryer, dialect SummaryDialect, eventID, runID, eventName, sourceID string) (bool, error) {
	query := `SELECT COUNT(*) FROM events WHERE event_id = ? AND run_id = ? AND event_name = ? AND COALESCE(CAST(source_event_id AS TEXT), '') = ?`
	if dialect == SummaryDialectPostgres {
		query = `SELECT COUNT(*) FROM events WHERE event_id = $1::uuid AND run_id = $2::uuid AND event_name = $3 AND COALESCE(source_event_id::text, '') = $4`
	}
	var count int
	if err := q.QueryRowContext(ctx, query, eventID, runID, eventName, sourceID).Scan(&count); err != nil {
		return false, fmt.Errorf("verify exact decision outcome event: %w", err)
	}
	return count == 1, nil
}

func summarizeGates(ctx context.Context, q SummaryQueryer, dialect SummaryDialect, runID string) (int, int, error) {
	query := `SELECT accumulator FROM entity_state WHERE run_id = ? ORDER BY entity_id`
	if dialect == SummaryDialectPostgres {
		query = `SELECT accumulator::text FROM entity_state WHERE run_id = $1::uuid ORDER BY entity_id FOR SHARE`
	}
	rows, err := q.QueryContext(ctx, query, runID)
	if err != nil {
		return 0, 0, fmt.Errorf("list decision gate state: %w", err)
	}
	open, malformed := 0, 0
	var activations []gateruntime.Activation
	for rows.Next() {
		var raw any
		if err := rows.Scan(&raw); err != nil {
			return 0, 0, fmt.Errorf("scan decision gate state: %w", err)
		}
		document, err := jsonBytes(raw)
		if err != nil {
			malformed++
			continue
		}
		var accumulator map[string]json.RawMessage
		if err := json.Unmarshal(document, &accumulator); err != nil || accumulator == nil {
			malformed++
			continue
		}
		bucketRaw, exists := accumulator[gateruntime.BucketKey]
		if !exists {
			continue
		}
		var bucket map[string]json.RawMessage
		if len(bucketRaw) == 0 || bytes.Equal(bytes.TrimSpace(bucketRaw), []byte("null")) || json.Unmarshal(bucketRaw, &bucket) != nil || bucket == nil {
			malformed++
			continue
		}
		for key, activationRaw := range bucket {
			var activation gateruntime.Activation
			decoder := json.NewDecoder(bytes.NewReader(activationRaw))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&activation); err != nil || decoder.Decode(&struct{}{}) != io.EOF || activation.Validate() != nil || key != activation.Key() {
				malformed++
				continue
			}
			activations = append(activations, activation)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, 0, fmt.Errorf("iterate decision gate state: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, 0, fmt.Errorf("close decision gate state: %w", err)
	}
	for _, activation := range activations {
		if strings.TrimSpace(activation.DecisionEventID) != "" {
			persisted, err := exactGateDecision(ctx, q, dialect, runID, activation)
			if err != nil {
				return 0, 0, err
			}
			if !persisted {
				malformed++
				continue
			}
		}
		switch activation.Status {
		case gateruntime.StatusOpen, gateruntime.StatusDecisionCommitted:
			open++
		case gateruntime.StatusRouted, gateruntime.StatusSuperseded:
		default:
			malformed++
		}
	}
	return open, malformed, nil
}

func exactGateDecision(
	ctx context.Context,
	q SummaryQueryer,
	dialect SummaryDialect,
	runID string,
	activation gateruntime.Activation,
) (bool, error) {
	query := `
		SELECT COUNT(*)
		FROM decision_cards c
		INNER JOIN events e
		  ON CAST(e.event_id AS TEXT) = CAST(c.decision_event_id AS TEXT)
		 AND e.run_id = c.run_id
		WHERE c.card_id = ?
		  AND c.run_id = ?
		  AND c.status IN ('decided', 'superseded')
		  AND CAST(c.decision_event_id AS TEXT) = ?`
	if dialect == SummaryDialectPostgres {
		query = `
			SELECT COUNT(*)
			FROM decision_cards c
			INNER JOIN events e ON e.event_id = c.decision_event_id AND e.run_id = c.run_id
			WHERE c.card_id = $1::uuid
			  AND c.run_id = $2::uuid
			  AND c.status IN ('decided', 'superseded')
			  AND c.decision_event_id = $3::uuid`
	}
	var count int
	if err := q.QueryRowContext(
		ctx, query, activation.CardID, runID, activation.DecisionEventID,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("verify exact gate decision event: %w", err)
	}
	return count == 1, nil
}

func jsonBytes(raw any) ([]byte, error) {
	switch value := raw.(type) {
	case string:
		return []byte(value), nil
	case []byte:
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported JSON value %T", raw)
	}
}
