package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"swarm/internal/runtime/budgetspend"
)

var _ budgetspend.Store = (*PostgresStore)(nil)
var _ budgetspend.Store = (*SQLiteRuntimeStore)(nil)

func (s *PostgresStore) RecordSpend(ctx context.Context, rec budgetspend.SpendRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("postgres budget spend store is required")
	}
	rec = normalizeBudgetSpendRecord(rec)
	if err := validateBudgetSpendEntity(rec.EntityID); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `
			INSERT INTO spend_ledger (
				entity_id, flow_instance, agent_id, model, model_alias, backend_profile, provider, transport, resolved_model,
				input_tokens, output_tokens, cost_usd, invocation_type, usage_accounting, created_at
			) VALUES (
				NULLIF($1,'')::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
			)
		`, rec.EntityID, rec.FlowInstance, rec.AgentID, rec.Model, rec.ModelAlias, rec.BackendProfile, rec.Provider, rec.Transport, rec.ResolvedModel, rec.InputTokens, rec.OutputTokens, rec.CostUSD, rec.InvocationType, rec.UsageAccounting, rec.RecordedAt)
	if err != nil {
		return fmt.Errorf("record postgres spend: %w", err)
	}
	return nil
}

func (s *PostgresStore) ResolveFlowInstance(ctx context.Context, runID string, entityID string) (string, error) {
	if s == nil || s.DB == nil {
		return "", fmt.Errorf("postgres budget spend store is required")
	}
	runID, entityID, err := validateBudgetSpendIdentity(runID, entityID)
	if err != nil {
		return "", err
	}
	var flowInstance string
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
	`, runID, entityID).Scan(&flowInstance); err != nil {
		return "", err
	}
	return strings.TrimSpace(flowInstance), nil
}

func (s *PostgresStore) ListActiveEntityIDs(ctx context.Context, runID string, terminalStates []string) ([]string, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("postgres budget spend store is required")
	}
	runID, err := validateBudgetRunID(runID)
	if err != nil {
		return nil, err
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT entity_id::text
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND NOT (current_state = ANY($2::text[]))
		ORDER BY created_at ASC
	`, runID, pq.Array(normalizeBudgetTerminalStates(terminalStates)))
	if err != nil {
		return nil, fmt.Errorf("list postgres active budget entities: %w", err)
	}
	defer rows.Close()
	return scanBudgetEntityIDs(rows)
}

func (s *PostgresStore) SumSpendUSD(ctx context.Context, query budgetspend.SpendQuery) (float64, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("postgres budget spend store is required")
	}
	query = normalizeBudgetSpendQuery(query)
	var spent float64
	var err error
	switch query.Scope {
	case budgetspend.ScopeSystem:
		err = s.DB.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE created_at >= $1
		`, query.Since).Scan(&spent)
	case budgetspend.ScopeGlobal:
		err = s.DB.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id IS NULL
			  AND created_at >= $1
		`, query.Since).Scan(&spent)
	case budgetspend.ScopeEntity:
		if err := validateBudgetSpendEntityRequired(query.EntityID); err != nil {
			return 0, err
		}
		err = s.DB.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id = $1::uuid
			  AND created_at >= $2
		`, query.EntityID, query.Since).Scan(&spent)
	default:
		return 0, fmt.Errorf("unsupported budget spend scope %q", query.Scope)
	}
	if err != nil {
		return 0, fmt.Errorf("sum postgres spend: %w", err)
	}
	return spent, nil
}

func (s *SQLiteRuntimeStore) RecordSpend(ctx context.Context, rec budgetspend.SpendRecord) error {
	if s == nil || s.DB == nil {
		return fmt.Errorf("sqlite budget spend store is required")
	}
	rec = normalizeBudgetSpendRecord(rec)
	if err := validateBudgetSpendEntity(rec.EntityID); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx, `
			INSERT INTO spend_ledger (
				entity_id, flow_instance, agent_id, model, model_alias, backend_profile, provider, transport, resolved_model,
				input_tokens, output_tokens, cost_usd, invocation_type, usage_accounting, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, sqliteNullUUID(rec.EntityID), rec.FlowInstance, rec.AgentID, rec.Model, rec.ModelAlias, rec.BackendProfile, rec.Provider, rec.Transport, rec.ResolvedModel, rec.InputTokens, rec.OutputTokens, rec.CostUSD, rec.InvocationType, rec.UsageAccounting, rec.RecordedAt.UTC())
	if err != nil {
		return fmt.Errorf("record sqlite spend: %w", err)
	}
	return nil
}

func (s *SQLiteRuntimeStore) ResolveFlowInstance(ctx context.Context, runID string, entityID string) (string, error) {
	if s == nil || s.DB == nil {
		return "", fmt.Errorf("sqlite budget spend store is required")
	}
	runID, entityID, err := validateBudgetSpendIdentity(runID, entityID)
	if err != nil {
		return "", err
	}
	var flowInstance string
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM entity_state
		WHERE run_id = ?
		  AND entity_id = ?
	`, runID, entityID).Scan(&flowInstance); err != nil {
		return "", err
	}
	return strings.TrimSpace(flowInstance), nil
}

func (s *SQLiteRuntimeStore) ListActiveEntityIDs(ctx context.Context, runID string, terminalStates []string) ([]string, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("sqlite budget spend store is required")
	}
	runID, err := validateBudgetRunID(runID)
	if err != nil {
		return nil, err
	}
	args := []any{runID}
	query := `
		SELECT entity_id
		FROM entity_state
		WHERE run_id = ?
	`
	states := normalizeBudgetTerminalStates(terminalStates)
	if len(states) > 0 {
		placeholders := make([]string, 0, len(states))
		for _, state := range states {
			placeholders = append(placeholders, "?")
			args = append(args, state)
		}
		query += " AND current_state NOT IN (" + strings.Join(placeholders, ", ") + ")"
	}
	query += " ORDER BY created_at ASC"
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite active budget entities: %w", err)
	}
	defer rows.Close()
	return scanBudgetEntityIDs(rows)
}

func (s *SQLiteRuntimeStore) SumSpendUSD(ctx context.Context, query budgetspend.SpendQuery) (float64, error) {
	if s == nil || s.DB == nil {
		return 0, fmt.Errorf("sqlite budget spend store is required")
	}
	query = normalizeBudgetSpendQuery(query)
	var spent float64
	var err error
	switch query.Scope {
	case budgetspend.ScopeSystem:
		err = s.DB.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE created_at >= ?
		`, query.Since.UTC()).Scan(&spent)
	case budgetspend.ScopeGlobal:
		err = s.DB.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id IS NULL
			  AND created_at >= ?
		`, query.Since.UTC()).Scan(&spent)
	case budgetspend.ScopeEntity:
		if err := validateBudgetSpendEntityRequired(query.EntityID); err != nil {
			return 0, err
		}
		err = s.DB.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id = ?
			  AND created_at >= ?
		`, query.EntityID, query.Since.UTC()).Scan(&spent)
	default:
		return 0, fmt.Errorf("unsupported budget spend scope %q", query.Scope)
	}
	if err != nil {
		return 0, fmt.Errorf("sum sqlite spend: %w", err)
	}
	return spent, nil
}

func normalizeBudgetSpendRecord(rec budgetspend.SpendRecord) budgetspend.SpendRecord {
	rec.NormalizeEntityID()
	rec.FlowInstance = strings.TrimSpace(rec.FlowInstance)
	rec.AgentID = strings.TrimSpace(rec.AgentID)
	rec.Model = strings.TrimSpace(rec.Model)
	rec.ModelAlias = strings.TrimSpace(rec.ModelAlias)
	rec.BackendProfile = strings.TrimSpace(rec.BackendProfile)
	rec.Provider = strings.TrimSpace(rec.Provider)
	rec.Transport = strings.TrimSpace(rec.Transport)
	rec.ResolvedModel = strings.TrimSpace(rec.ResolvedModel)
	if rec.ModelAlias == "" {
		rec.ModelAlias = "unknown"
	}
	if rec.BackendProfile == "" {
		rec.BackendProfile = "unknown"
	}
	if rec.Provider == "" {
		rec.Provider = "unknown"
	}
	if rec.Transport == "" {
		rec.Transport = "unknown"
	}
	if rec.ResolvedModel == "" {
		rec.ResolvedModel = rec.Model
	}
	rec.InvocationType = strings.TrimSpace(strings.ToLower(rec.InvocationType))
	rec.UsageAccounting = strings.TrimSpace(strings.ToLower(rec.UsageAccounting))
	if rec.RecordedAt.IsZero() {
		rec.RecordedAt = time.Now().UTC()
	} else {
		rec.RecordedAt = rec.RecordedAt.UTC()
	}
	return rec
}

func normalizeBudgetSpendQuery(query budgetspend.SpendQuery) budgetspend.SpendQuery {
	query.EntityID = strings.TrimSpace(query.EntityID)
	if query.Since.IsZero() {
		query.Since = time.Now().UTC()
	} else {
		query.Since = query.Since.UTC()
	}
	return query
}

func normalizeBudgetTerminalStates(states []string) []string {
	out := make([]string, 0, len(states))
	seen := map[string]struct{}{}
	for _, state := range states {
		state = strings.TrimSpace(state)
		if state == "" {
			continue
		}
		if _, ok := seen[state]; ok {
			continue
		}
		seen[state] = struct{}{}
		out = append(out, state)
	}
	return out
}

func scanBudgetEntityIDs(rows *sql.Rows) ([]string, error) {
	out := make([]string, 0)
	for rows.Next() {
		var entityID string
		if err := rows.Scan(&entityID); err != nil {
			return nil, fmt.Errorf("scan active budget entity: %w", err)
		}
		entityID = strings.TrimSpace(entityID)
		if entityID != "" {
			out = append(out, entityID)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read active budget entities: %w", err)
	}
	return out, nil
}

func validateBudgetRunID(runID string) (string, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", fmt.Errorf("budget spend run_id is required")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return "", fmt.Errorf("budget spend run_id must be uuid: %w", err)
	}
	return runID, nil
}

func validateBudgetSpendEntity(entityID string) error {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return nil
	}
	_, err := uuid.Parse(entityID)
	if err != nil {
		return fmt.Errorf("budget spend entity_id must be uuid: %w", err)
	}
	return nil
}

func validateBudgetSpendEntityRequired(entityID string) error {
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return fmt.Errorf("budget spend entity_id is required")
	}
	return validateBudgetSpendEntity(entityID)
}

func validateBudgetSpendIdentity(runID string, entityID string) (string, string, error) {
	runID, err := validateBudgetRunID(runID)
	if err != nil {
		return "", "", err
	}
	entityID = strings.TrimSpace(entityID)
	if err := validateBudgetSpendEntityRequired(entityID); err != nil {
		return "", "", err
	}
	return runID, entityID, nil
}
