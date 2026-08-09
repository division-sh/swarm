package effectstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

type SummaryDialect string

const (
	SummaryDialectPostgres SummaryDialect = "postgres"
	SummaryDialectSQLite   SummaryDialect = "sqlite"
)

type SummaryQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type runEffectAttempt struct {
	id                  string
	ordinal             int
	state               runtimeeffects.State
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
	state          runtimeeffects.State
	attempts       []runEffectAttempt
}

// ReadRunSummary is the external-effect owner's selected-store projection.
// It validates the logical operation against its latest attempt and keeps
// historical terminal retries from masquerading as current work.
func ReadRunSummary(ctx context.Context, queryer SummaryQueryer, dialect SummaryDialect, runID string) (runtimeeffects.RunSummary, error) {
	runID = strings.TrimSpace(runID)
	if queryer == nil || runID == "" {
		return runtimeeffects.RunSummary{}, fmt.Errorf("external-effect run summary requires selected store and run_id")
	}
	query := `
		SELECT o.operation_id,
		       json_extract(o.lineage, '$.run_id'),
		       json_extract(o.authority_evidence, '$.usage_target.run_id'),
		       json_extract(o.authority_evidence, '$.usage_target.kind'),
		       json_extract(o.authority_evidence, '$.usage_target.id'),
		       json_extract(o.authority_evidence, '$.usage_target.ordinal'),
		       o.state,
		       CAST(a.attempt_id AS TEXT),
		       a.attempt_ordinal,
		       a.state,
		       a.usage_target_kind,
		       CAST(a.usage_target_id AS TEXT),
		       a.target_ordinal,
		       (SELECT COUNT(*) FROM runtime_effect_budget_reservations r WHERE r.attempt_id = a.attempt_id),
		       (SELECT COUNT(*) FROM runtime_effect_budget_reservations r
		        WHERE r.attempt_id = a.attempt_id
		          AND r.scope_kind = 'entity'
		          AND (json_extract(o.authority_evidence, '$.usage_target.entity_id') IS NULL
		               OR r.scope_key <> json_extract(o.authority_evidence, '$.usage_target.entity_id')))
		FROM runtime_external_effect_operations o
		LEFT JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
		WHERE json_extract(o.lineage, '$.run_id') = ?
		   OR json_extract(o.authority_evidence, '$.usage_target.run_id') = ?
		ORDER BY o.operation_id, a.attempt_ordinal`
	args := []any{runID, runID}
	switch dialect {
	case SummaryDialectSQLite:
	case SummaryDialectPostgres:
		query = `
			SELECT o.operation_id::text,
			       o.lineage->>'run_id',
			       o.authority_evidence #>> '{usage_target,run_id}',
			       o.authority_evidence #>> '{usage_target,kind}',
			       o.authority_evidence #>> '{usage_target,id}',
			       (o.authority_evidence #>> '{usage_target,ordinal}')::integer,
			       o.state,
			       a.attempt_id::text,
			       a.attempt_ordinal,
			       a.state,
			       a.usage_target_kind,
			       a.usage_target_id::text,
			       a.target_ordinal,
			       (SELECT COUNT(*) FROM runtime_effect_budget_reservations r WHERE r.attempt_id = a.attempt_id),
			       (SELECT COUNT(*) FROM runtime_effect_budget_reservations r
			        WHERE r.attempt_id = a.attempt_id
			          AND r.scope_kind = 'entity'
			          AND (o.authority_evidence #>> '{usage_target,entity_id}' IS NULL
			               OR r.scope_key <> (o.authority_evidence #>> '{usage_target,entity_id}')))
			FROM runtime_external_effect_operations o
			LEFT JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
			WHERE o.lineage->>'run_id' = $1
			   OR o.authority_evidence #>> '{usage_target,run_id}' = $1
			ORDER BY o.operation_id, a.attempt_ordinal
			FOR SHARE OF o`
		args = []any{runID}
	default:
		return runtimeeffects.RunSummary{}, fmt.Errorf("external-effect run summary requires selected store dialect")
	}
	rows, err := queryer.QueryContext(ctx, query, args...)
	if err != nil {
		return runtimeeffects.RunSummary{}, fmt.Errorf("read external-effect run summary: %w", err)
	}
	defer rows.Close()
	var operations []runEffectOperation
	for rows.Next() {
		var operationID, operationState string
		var lineageRunID, authorityRunID, targetKind, targetID sql.NullString
		var targetOrdinal sql.NullInt64
		var attempt runEffectAttempt
		var attemptID, attemptState, attemptTargetKind, attemptTargetID sql.NullString
		var attemptOrdinal, attemptTargetOrdinal sql.NullInt64
		if err := rows.Scan(
			&operationID, &lineageRunID, &authorityRunID, &targetKind, &targetID, &targetOrdinal, &operationState,
			&attemptID, &attemptOrdinal, &attemptState, &attemptTargetKind, &attemptTargetID, &attemptTargetOrdinal,
			&attempt.reservations, &attempt.invalidReservations,
		); err != nil {
			return runtimeeffects.RunSummary{}, fmt.Errorf("scan external-effect run summary: %w", err)
		}
		if len(operations) == 0 || operations[len(operations)-1].id != operationID {
			operations = append(operations, runEffectOperation{
				id: operationID, lineageRunID: strings.TrimSpace(lineageRunID.String),
				authorityRunID: strings.TrimSpace(authorityRunID.String),
				targetKind:     strings.TrimSpace(targetKind.String), targetID: strings.TrimSpace(targetID.String),
				targetOrdinal: int(targetOrdinal.Int64), state: runtimeeffects.State(strings.TrimSpace(operationState)),
			})
		}
		if attemptID.Valid {
			attempt.id = strings.TrimSpace(attemptID.String)
			attempt.ordinal = int(attemptOrdinal.Int64)
			attempt.state = runtimeeffects.State(strings.TrimSpace(attemptState.String))
			attempt.targetKind = strings.TrimSpace(attemptTargetKind.String)
			attempt.targetID = strings.TrimSpace(attemptTargetID.String)
			attempt.targetOrdinal = int(attemptTargetOrdinal.Int64)
			operations[len(operations)-1].attempts = append(operations[len(operations)-1].attempts, attempt)
		}
	}
	if err := rows.Err(); err != nil {
		return runtimeeffects.RunSummary{}, fmt.Errorf("iterate external-effect run summary: %w", err)
	}

	summary := runtimeeffects.RunSummary{RunID: runID}
	for _, operation := range operations {
		if !exactRunBinding(operation.lineageRunID, operation.authorityRunID, runID) {
			summary.MalformedBindings++
			continue
		}
		if operation.state == runtimeeffects.StatePrepared {
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

func activeEffectState(state runtimeeffects.State) bool {
	switch state {
	case runtimeeffects.StateAuthorized, runtimeeffects.StateLaunched, runtimeeffects.StateResponseObserved:
		return true
	default:
		return false
	}
}

func terminalEffectState(state runtimeeffects.State) bool {
	switch state {
	case runtimeeffects.StateSettled, runtimeeffects.StateTerminalFailure, runtimeeffects.StateOutcomeUncertain:
		return true
	default:
		return false
	}
}
