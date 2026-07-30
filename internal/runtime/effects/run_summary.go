package effects

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type SummaryDialect string

const (
	SummaryDialectPostgres SummaryDialect = "postgres"
	SummaryDialectSQLite   SummaryDialect = "sqlite"
)

type SummaryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

// RunSummary is the external-effect owner's validated view of attempts and
// subordinate budget reservations that still carry durable work for one run.
type RunSummary struct {
	RunID              string
	ActiveAttempts     int
	OrphanReservations int
	MalformedBindings  int
}

func (s RunSummary) Validate() error {
	if strings.TrimSpace(s.RunID) == "" {
		return fmt.Errorf("external-effect run summary requires run_id")
	}
	if s.ActiveAttempts < 0 || s.OrphanReservations < 0 || s.MalformedBindings < 0 {
		return fmt.Errorf("external-effect run summary counts cannot be negative")
	}
	return nil
}

func (s RunSummary) BlocksCompletion() bool {
	return s.ActiveAttempts > 0 || s.OrphanReservations > 0 || s.MalformedBindings > 0
}

type runEffectAttempt struct {
	id                  string
	ordinal             int
	state               State
	targetKind          string
	targetID            string
	targetOrdinal       int
	reservations        int
	invalidReservations int
}

type runEffectOperation struct {
	id             string
	lineageRunID   string
	authorityRunID string
	targetKind     string
	targetID       string
	targetOrdinal  int
	state          State
	attempts       []runEffectAttempt
}

// ReadRunSummary is the external-effect owner's selected-store projection.
// It validates the logical operation against its latest attempt and keeps
// historical terminal retries from masquerading as current work.
func ReadRunSummary(ctx context.Context, queryer SummaryQueryer, dialect SummaryDialect, runID string) (RunSummary, error) {
	runID = strings.TrimSpace(runID)
	if queryer == nil || runID == "" {
		return RunSummary{}, fmt.Errorf("external-effect run summary requires selected store and run_id")
	}
	query := `
		SELECT o.operation_id,
		       COALESCE(json_extract(o.lineage, '$.run_id'), ''),
		       COALESCE(json_extract(o.authority_evidence, '$.usage_target.run_id'), ''),
		       COALESCE(json_extract(o.authority_evidence, '$.usage_target.kind'), ''),
		       COALESCE(json_extract(o.authority_evidence, '$.usage_target.id'), ''),
		       COALESCE(json_extract(o.authority_evidence, '$.usage_target.ordinal'), 0),
		       o.state,
		       COALESCE(CAST(a.attempt_id AS TEXT), ''),
		       COALESCE(a.attempt_ordinal, 0),
		       COALESCE(a.state, ''),
		       COALESCE(a.usage_target_kind, ''),
		       COALESCE(CAST(a.usage_target_id AS TEXT), ''),
		       COALESCE(a.target_ordinal, 0),
		       CASE WHEN a.attempt_id IS NULL THEN 0 ELSE
		         (SELECT COUNT(*) FROM runtime_effect_budget_reservations r WHERE r.attempt_id = a.attempt_id)
		       END,
		       CASE WHEN a.attempt_id IS NULL THEN 0 ELSE
		         (SELECT COUNT(*) FROM runtime_effect_budget_reservations r
		          WHERE r.attempt_id = a.attempt_id
		            AND r.scope_kind = 'entity'
		            AND r.scope_key <> COALESCE(json_extract(o.authority_evidence, '$.usage_target.entity_id'), ''))
		       END
		FROM runtime_external_effect_operations o
		LEFT JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
		WHERE COALESCE(json_extract(o.lineage, '$.run_id'), '') = ?
		   OR COALESCE(json_extract(o.authority_evidence, '$.usage_target.run_id'), '') = ?
		ORDER BY o.operation_id, a.attempt_ordinal`
	args := []any{runID, runID}
	switch dialect {
	case SummaryDialectSQLite:
	case SummaryDialectPostgres:
		query = `
			SELECT o.operation_id::text,
			       COALESCE(o.lineage->>'run_id', ''),
			       COALESCE(o.authority_evidence #>> '{usage_target,run_id}', ''),
			       COALESCE(o.authority_evidence #>> '{usage_target,kind}', ''),
			       COALESCE(o.authority_evidence #>> '{usage_target,id}', ''),
			       COALESCE((o.authority_evidence #>> '{usage_target,ordinal}')::integer, 0),
			       o.state,
			       COALESCE(a.attempt_id::text, ''),
			       COALESCE(a.attempt_ordinal, 0),
			       COALESCE(a.state, ''),
			       COALESCE(a.usage_target_kind, ''),
			       COALESCE(a.usage_target_id::text, ''),
			       COALESCE(a.target_ordinal, 0),
			       CASE WHEN a.attempt_id IS NULL THEN 0 ELSE
			         (SELECT COUNT(*) FROM runtime_effect_budget_reservations r WHERE r.attempt_id = a.attempt_id)
			       END,
			       CASE WHEN a.attempt_id IS NULL THEN 0 ELSE
			         (SELECT COUNT(*) FROM runtime_effect_budget_reservations r
			          WHERE r.attempt_id = a.attempt_id
			            AND r.scope_kind = 'entity'
			            AND r.scope_key <> COALESCE(o.authority_evidence #>> '{usage_target,entity_id}', ''))
			       END
			FROM runtime_external_effect_operations o
			LEFT JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
			WHERE COALESCE(o.lineage->>'run_id', '') = $1
			   OR COALESCE(o.authority_evidence #>> '{usage_target,run_id}', '') = $1
			ORDER BY o.operation_id, a.attempt_ordinal
			FOR SHARE OF o`
		args = []any{runID}
	default:
		return RunSummary{}, fmt.Errorf("external-effect run summary requires selected store dialect")
	}
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return RunSummary{}, fmt.Errorf("read external-effect run summary: %w", err)
	}
	defer rows.Close()
	var operations []runEffectOperation
	for rows.Next() {
		var operationID, lineageRunID, authorityRunID, operationState string
		var attempt runEffectAttempt
		var attemptState string
		var targetKind, targetID string
		var targetOrdinal int
		if err := rows.Scan(
			&operationID, &lineageRunID, &authorityRunID, &targetKind, &targetID, &targetOrdinal, &operationState,
			&attempt.id, &attempt.ordinal, &attemptState, &attempt.targetKind, &attempt.targetID, &attempt.targetOrdinal,
			&attempt.reservations, &attempt.invalidReservations,
		); err != nil {
			return RunSummary{}, fmt.Errorf("scan external-effect run summary: %w", err)
		}
		if len(operations) == 0 || operations[len(operations)-1].id != operationID {
			operations = append(operations, runEffectOperation{
				id: operationID, lineageRunID: strings.TrimSpace(lineageRunID),
				authorityRunID: strings.TrimSpace(authorityRunID),
				targetKind:     strings.TrimSpace(targetKind), targetID: strings.TrimSpace(targetID),
				targetOrdinal: targetOrdinal, state: State(strings.TrimSpace(operationState)),
			})
		}
		if strings.TrimSpace(attempt.id) != "" {
			attempt.state = State(strings.TrimSpace(attemptState))
			operations[len(operations)-1].attempts = append(operations[len(operations)-1].attempts, attempt)
		}
	}
	if err := rows.Err(); err != nil {
		return RunSummary{}, fmt.Errorf("iterate external-effect run summary: %w", err)
	}

	summary := RunSummary{RunID: runID}
	for _, operation := range operations {
		if !exactRunBinding(operation.lineageRunID, operation.authorityRunID, runID) {
			summary.MalformedBindings++
			continue
		}
		if operation.state == StatePrepared {
			if len(operation.attempts) != 0 {
				summary.MalformedBindings++
			} else {
				summary.ActiveAttempts++
			}
			continue
		}
		if len(operation.attempts) == 0 {
			summary.MalformedBindings++
			continue
		}
		for _, historical := range operation.attempts[:len(operation.attempts)-1] {
			if historical.ordinal <= 0 || !terminalEffectState(historical.state) ||
				!attemptTargetMatches(operation, historical) || historical.invalidReservations != 0 {
				summary.MalformedBindings++
			}
			summary.OrphanReservations += historical.reservations
		}
		latest := operation.attempts[len(operation.attempts)-1]
		if latest.ordinal <= 0 || latest.state != operation.state ||
			!attemptTargetMatches(operation, latest) || latest.invalidReservations != 0 {
			summary.MalformedBindings++
			continue
		}
		switch {
		case activeEffectState(latest.state):
			summary.ActiveAttempts++
		case terminalEffectState(latest.state):
			summary.OrphanReservations += latest.reservations
		default:
			summary.MalformedBindings++
		}
	}
	return summary, summary.Validate()
}

func attemptTargetMatches(operation runEffectOperation, attempt runEffectAttempt) bool {
	if operation.targetKind == "" && operation.targetID == "" && operation.targetOrdinal == 0 {
		return strings.TrimSpace(attempt.targetKind) == "" &&
			strings.TrimSpace(attempt.targetID) == "" &&
			attempt.targetOrdinal == 0
	}
	return operation.targetKind != "" &&
		operation.targetKind == strings.TrimSpace(attempt.targetKind) &&
		operation.targetID != "" &&
		operation.targetID == strings.TrimSpace(attempt.targetID) &&
		operation.targetOrdinal == attempt.targetOrdinal
}

func exactRunBinding(lineageRunID, authorityRunID, runID string) bool {
	if lineageRunID == "" && authorityRunID == "" {
		return false
	}
	if lineageRunID != "" && lineageRunID != runID {
		return false
	}
	if authorityRunID != "" && authorityRunID != runID {
		return false
	}
	return true
}

func activeEffectState(state State) bool {
	switch state {
	case StateAuthorized, StateLaunched, StateResponseObserved:
		return true
	default:
		return false
	}
}

func terminalEffectState(state State) bool {
	switch state {
	case StateSettled, StateTerminalFailure, StateOutcomeUncertain:
		return true
	default:
		return false
	}
}
