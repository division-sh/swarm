package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	runtimedecision "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeentity "github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runtimetimers "github.com/division-sh/swarm/internal/runtime/timerobligation"
)

type runCompletionOwnerSummaries struct {
	Delivery  runtimedelivery.RunSummary
	Pipeline  runtimepipelineobligation.RunSummary
	Timers    runtimetimers.RunSummary
	Sessions  runtimesessions.RunSummary
	Decisions runtimedecision.RunSummary
	Effects   runtimeeffects.RunSummary
	Entities  runtimeentity.RunSummary
}

func (s runCompletionOwnerSummaries) validate() error {
	if err := s.Delivery.Validate(); err != nil {
		return err
	}
	if err := s.Pipeline.Validate(); err != nil {
		return err
	}
	if err := s.Timers.Validate(); err != nil {
		return err
	}
	if err := s.Sessions.Validate(); err != nil {
		return err
	}
	if err := s.Decisions.Validate(); err != nil {
		return err
	}
	if err := s.Effects.Validate(); err != nil {
		return err
	}
	if err := s.Entities.Validate(); err != nil {
		return err
	}
	runID := strings.TrimSpace(s.Delivery.RunID)
	for owner, candidate := range map[string]string{
		"pipeline": s.Pipeline.RunID,
		"timer":    s.Timers.RunID,
		"session":  s.Sessions.RunID,
		"decision": s.Decisions.RunID,
		"effect":   s.Effects.RunID,
		"entity":   s.Entities.RunID,
	} {
		if strings.TrimSpace(candidate) != runID {
			return fmt.Errorf("%s run summary identity does not match delivery run %s", owner, runID)
		}
	}
	return nil
}

func (s runCompletionOwnerSummaries) blocksCompletion() bool {
	return !s.Delivery.Settled() ||
		s.Pipeline.BlocksCompletion() ||
		s.Timers.BlocksCompletion() ||
		s.Sessions.BlocksCompletion() ||
		s.Decisions.BlocksCompletion() ||
		s.Effects.BlocksCompletion() ||
		!s.Entities.ReadyForCompletion()
}

func loadPostgresRunCompletionOwnerSummaries(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	selectedNow time.Time,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runCompletionOwnerSummaries, error) {
	delivery, err := postgresDeliveryAdapter.SummarizeRun(ctx, tx, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize delivery obligations: %w", err)
	}
	pipeline, err := summarizePipelineRun(ctx, tx, runID, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize pipeline obligations: %w", err)
	}
	timers, err := summarizeTimerRun(ctx, tx, runID, selectedNow, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	sessions, err := summarizeSessionRun(ctx, tx, runID, selectedNow, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	decisions, err := summarizeDecisionRun(ctx, tx, runID, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	effects, err := summarizeExternalEffectRun(ctx, tx, runID, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	entities, err := summarizeEntityRun(ctx, tx, runID, catalog, true)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	out := runCompletionOwnerSummaries{
		Delivery: delivery, Pipeline: pipeline, Timers: timers, Sessions: sessions,
		Decisions: decisions, Effects: effects, Entities: entities,
	}
	return out, out.validate()
}

func loadSQLiteRunCompletionOwnerSummaries(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	selectedNow time.Time,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runCompletionOwnerSummaries, error) {
	delivery, err := sqliteDeliveryAdapter.SummarizeRun(ctx, tx, runID)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize sqlite delivery obligations: %w", err)
	}
	pipeline, err := summarizePipelineRun(ctx, tx, runID, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, fmt.Errorf("summarize sqlite pipeline obligations: %w", err)
	}
	timers, err := summarizeTimerRun(ctx, tx, runID, selectedNow, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	sessions, err := summarizeSessionRun(ctx, tx, runID, selectedNow, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	decisions, err := summarizeDecisionRun(ctx, tx, runID, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	effects, err := summarizeExternalEffectRun(ctx, tx, runID, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	entities, err := summarizeEntityRun(ctx, tx, runID, catalog, false)
	if err != nil {
		return runCompletionOwnerSummaries{}, err
	}
	out := runCompletionOwnerSummaries{
		Delivery: delivery, Pipeline: pipeline, Timers: timers, Sessions: sessions,
		Decisions: decisions, Effects: effects, Entities: entities,
	}
	return out, out.validate()
}

func summarizeTimerRun(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time, postgres bool) (runtimetimers.RunSummary, error) {
	scope, err := runtimetimers.Run(runID)
	if err != nil {
		return runtimetimers.RunSummary{}, err
	}
	dialect := runtimetimers.DialectSQLite
	if postgres {
		dialect = runtimetimers.DialectPostgres
	}
	snapshot, err := runtimetimers.Read(ctx, tx, dialect, scope, selectedNow.UTC())
	if err != nil {
		return runtimetimers.RunSummary{}, err
	}
	obligations, ok := snapshot.Run(runID)
	if !ok {
		return runtimetimers.RunSummary{}, fmt.Errorf("timer owner omitted requested run %s", runID)
	}
	summary := obligations.Summary(snapshot.ObservedAt)
	return summary, summary.Validate()
}

func summarizeSessionRun(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time, postgres bool) (runtimesessions.RunSummary, error) {
	query := `SELECT lease_expires_at FROM agent_sessions WHERE run_id = ? AND status = 'active' AND lease_holder IS NOT NULL AND lease_expires_at IS NOT NULL`
	if postgres {
		query = `SELECT lease_expires_at FROM agent_sessions WHERE run_id = $1::uuid AND status = 'active' AND lease_holder IS NOT NULL AND lease_expires_at IS NOT NULL`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return runtimesessions.RunSummary{}, fmt.Errorf("summarize session obligations: %w", err)
	}
	defer rows.Close()
	summary := runtimesessions.RunSummary{RunID: runID, ObservedAt: selectedNow.UTC()}
	for rows.Next() {
		var raw any
		if err := rows.Scan(&raw); err != nil {
			return runtimesessions.RunSummary{}, fmt.Errorf("scan session obligation: %w", err)
		}
		expiresAt, ok, err := sqliteTimeValue(raw)
		if err != nil || !ok {
			return runtimesessions.RunSummary{}, fmt.Errorf("decode session obligation expiry: %w", err)
		}
		expiresAt = expiresAt.UTC()
		if !expiresAt.After(summary.ObservedAt) {
			continue
		}
		summary.ActiveLeases++
		if summary.NextExpiry.IsZero() || expiresAt.Before(summary.NextExpiry) {
			summary.NextExpiry = expiresAt
		}
	}
	if err := rows.Err(); err != nil {
		return runtimesessions.RunSummary{}, fmt.Errorf("read session obligations: %w", err)
	}
	return summary, summary.Validate()
}

func postgresRunSessionNextWakeTx(ctx context.Context, tx *sql.Tx, runID string, selectedNow time.Time) (*time.Time, error) {
	summary, err := summarizeSessionRun(ctx, tx, runID, selectedNow, true)
	if err != nil {
		return nil, err
	}
	return optionalWake(summary.NextExpiry), nil
}

func summarizeDecisionRun(ctx context.Context, tx *sql.Tx, runID string, postgres bool) (runtimedecision.RunSummary, error) {
	summary := runtimedecision.RunSummary{RunID: runID}
	humanSettled, err := decisionHumanTasksSettled(ctx, tx, runID, postgres)
	if err != nil {
		return runtimedecision.RunSummary{}, err
	}
	if !humanSettled {
		summary.UnresolvedHumanTasks = 1
	}
	effectsSettled, err := decisionProposedEffectsSettled(ctx, tx, runID, postgres)
	if err != nil {
		return runtimedecision.RunSummary{}, err
	}
	if !effectsSettled {
		summary.UnresolvedEffects = 1
	}
	gatesSettled, err := decisionGatesSettled(ctx, tx, runID, postgres)
	if err != nil {
		return runtimedecision.RunSummary{}, err
	}
	if !gatesSettled {
		summary.OpenGateObligations = 1
	}
	return summary, summary.Validate()
}

func decisionHumanTasksSettled(ctx context.Context, tx *sql.Tx, runID string, postgres bool) (bool, error) {
	if postgres {
		return postgresDecisionHumanTasksSettledTx(ctx, tx, runID)
	}
	return sqliteDecisionHumanTasksSettledTx(ctx, tx, runID)
}

func decisionProposedEffectsSettled(ctx context.Context, tx *sql.Tx, runID string, postgres bool) (bool, error) {
	if postgres {
		return postgresDecisionProposedEffectsSettledTx(ctx, tx, runID)
	}
	return sqliteDecisionProposedEffectsSettledTx(ctx, tx, runID)
}

func decisionGatesSettled(ctx context.Context, tx *sql.Tx, runID string, postgres bool) (bool, error) {
	if postgres {
		return postgresDecisionGatesSettledTx(ctx, tx, runID)
	}
	return sqliteDecisionGatesSettledTx(ctx, tx, runID)
}

func summarizeExternalEffectRun(ctx context.Context, tx *sql.Tx, runID string, postgres bool) (runtimeeffects.RunSummary, error) {
	query := `
		SELECT COALESCE(json_extract(o.lineage, '$.run_id'), ''),
		       COALESCE(json_extract(o.authority_evidence, '$.usage_target.run_id'), ''),
		       a.state,
		       EXISTS (SELECT 1 FROM runtime_effect_budget_reservations r WHERE r.attempt_id = a.attempt_id)
		FROM runtime_external_effect_operations o
		JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
		WHERE COALESCE(json_extract(o.lineage, '$.run_id'), '') = ?
		   OR COALESCE(json_extract(o.authority_evidence, '$.usage_target.run_id'), '') = ?
	`
	if postgres {
		query = `
			SELECT COALESCE(o.lineage->>'run_id', ''),
			       COALESCE(o.authority_evidence #>> '{usage_target,run_id}', ''),
			       a.state,
			       EXISTS (SELECT 1 FROM runtime_effect_budget_reservations r WHERE r.attempt_id = a.attempt_id)
			FROM runtime_external_effect_operations o
			JOIN runtime_external_effect_attempts a ON a.operation_id = o.operation_id
			WHERE COALESCE(o.lineage->>'run_id', '') = $1
			   OR COALESCE(o.authority_evidence #>> '{usage_target,run_id}', '') = $1
			FOR SHARE OF o, a
		`
	}
	rows, err := tx.QueryContext(ctx, query, runID, runID)
	if postgres {
		rows, err = tx.QueryContext(ctx, query, runID)
	}
	if err != nil {
		return runtimeeffects.RunSummary{}, fmt.Errorf("summarize external-effect obligations: %w", err)
	}
	defer rows.Close()
	summary := runtimeeffects.RunSummary{RunID: runID}
	for rows.Next() {
		var lineageRunID, authorityRunID, state string
		var hasReservation bool
		if err := rows.Scan(&lineageRunID, &authorityRunID, &state, &hasReservation); err != nil {
			return runtimeeffects.RunSummary{}, fmt.Errorf("scan external-effect obligation: %w", err)
		}
		lineageRunID = strings.TrimSpace(lineageRunID)
		authorityRunID = strings.TrimSpace(authorityRunID)
		if lineageRunID != "" && authorityRunID != "" && lineageRunID != authorityRunID {
			summary.MalformedBindings++
			continue
		}
		switch strings.TrimSpace(state) {
		case string(runtimeeffects.StatePrepared),
			string(runtimeeffects.StateAuthorized),
			string(runtimeeffects.StateLaunched),
			string(runtimeeffects.StateResponseObserved):
			summary.ActiveAttempts++
		case string(runtimeeffects.StateSettled),
			string(runtimeeffects.StateTerminalFailure),
			string(runtimeeffects.StateOutcomeUncertain):
			if hasReservation {
				summary.OrphanReservations++
			}
		default:
			summary.MalformedBindings++
		}
	}
	if err := rows.Err(); err != nil {
		return runtimeeffects.RunSummary{}, fmt.Errorf("read external-effect obligations: %w", err)
	}
	return summary, summary.Validate()
}

func summarizeEntityRun(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
	catalog runtimerunlifecycle.TerminalCatalog,
	postgres bool,
) (runtimeentity.RunSummary, error) {
	query := `
		SELECT LOWER(COALESCE(es.current_state, '')),
		       COALESCE(es.flow_instance, ''),
		       COALESCE(fi.flow_template, '')
		FROM entity_state es
		LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
		WHERE es.run_id = ?
	`
	if postgres {
		query = `
			SELECT LOWER(COALESCE(es.current_state, '')),
			       COALESCE(es.flow_instance, ''),
			       COALESCE(fi.flow_template, '')
			FROM entity_state es
			LEFT JOIN flow_instances fi ON fi.instance_id = es.flow_instance
			WHERE es.run_id = $1::uuid
			FOR SHARE OF es
		`
	}
	rows, err := tx.QueryContext(ctx, query, runID)
	if err != nil {
		return runtimeentity.RunSummary{}, fmt.Errorf("summarize entity terminality: %w", err)
	}
	defer rows.Close()
	summary := runtimeentity.RunSummary{RunID: runID}
	for rows.Next() {
		var state, flowInstance, flowTemplate string
		if err := rows.Scan(&state, &flowInstance, &flowTemplate); err != nil {
			return runtimeentity.RunSummary{}, fmt.Errorf("scan entity terminality: %w", err)
		}
		summary.Total++
		terminal, known := catalog.Terminal(flowTemplate, flowInstance, state)
		switch {
		case !known:
			summary.Malformed++
		case terminal:
			summary.Terminal++
		default:
			summary.Nonterminal++
		}
	}
	if err := rows.Err(); err != nil {
		return runtimeentity.RunSummary{}, fmt.Errorf("read entity terminality: %w", err)
	}
	return summary, summary.Validate()
}
