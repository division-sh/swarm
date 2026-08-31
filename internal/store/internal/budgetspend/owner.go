package budgetspendstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/budgetspend"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

type BudgetPostgresOwner struct {
	backend *postgresbackend.Backend
}

type BudgetSQLiteOwner struct {
	backend *sqlitebackend.Backend
}

func NewPostgres(backend *postgresbackend.Backend) (*BudgetPostgresOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("postgres budget spend backend is required")
	}
	return &BudgetPostgresOwner{backend: backend}, nil
}

func NewSQLite(backend *sqlitebackend.Backend) (*BudgetSQLiteOwner, error) {
	if backend == nil || !backend.Valid() {
		return nil, fmt.Errorf("sqlite budget spend backend is required")
	}
	return &BudgetSQLiteOwner{backend: backend}, nil
}

var _ budgetspend.Store = (*BudgetPostgresOwner)(nil)
var _ budgetspend.Store = (*BudgetSQLiteOwner)(nil)

func (s *BudgetPostgresOwner) RecordSpend(ctx context.Context, rec budgetspend.SpendRecord) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("postgres budget spend store is required")
	}
	rec = normalizeBudgetSpendRecord(rec)
	if !rec.ExecutionMode.Valid() {
		return fmt.Errorf("record postgres spend: execution_mode must be live or mock")
	}
	if err := validateBudgetSpendEntity(rec.EntityID); err != nil {
		return err
	}
	identityFields, err := budgetSpendAgentIdentityFields(rec)
	if err != nil {
		return err
	}
	return s.backend.RunTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if rec.EntityID != "" {
			runID, err := runtimecurrentstate.RequireRunID(txctx)
			if err != nil {
				return err
			}
			if err := storerunstate.RequirePostgresActiveTx(txctx, tx, runID); err != nil {
				return err
			}
			var exists bool
			if err := tx.QueryRowContext(txctx, `SELECT EXISTS (SELECT 1 FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid)`, runID, rec.EntityID).Scan(&exists); err != nil {
				return fmt.Errorf("resolve postgres spend entity: %w", err)
			}
			if !exists {
				return fmt.Errorf("budget spend entity does not belong to canonical run")
			}
		}
		_, err := tx.ExecContext(txctx, `
			INSERT INTO spend_ledger (
				execution_mode, run_id, entity_id, flow_instance, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, agent_flow_scope_key, agent_flow_instance_id,
				model, model_alias, backend_profile, provider, transport, resolved_model,
				input_tokens, output_tokens, cost_usd, invocation_type, usage_accounting, created_at
			) VALUES (
				$1, $2::uuid, NULLIF($3,'')::uuid, $4, $5, $6, $7, $8, $9, $10,
				$11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
			)
		`, rec.ExecutionMode, identityFields.RunID, rec.EntityID, identityFields.FlowInstancePath, identityFields.AgentID,
			identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence,
			identityFields.FlowScopeKey, identityFields.FlowInstanceID,
			rec.Model, rec.ModelAlias, rec.BackendProfile, rec.Provider, rec.Transport, rec.ResolvedModel,
			rec.InputTokens, rec.OutputTokens, rec.CostUSD, rec.InvocationType, rec.UsageAccounting, rec.RecordedAt)
		if err != nil {
			return fmt.Errorf("record postgres spend: %w", err)
		}
		return nil
	})
}

func (s *BudgetPostgresOwner) ResolveFlowInstance(ctx context.Context, runID string, entityID string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("postgres budget spend store is required")
	}
	runID, entityID, err := validateBudgetSpendIdentity(runID, entityID)
	if err != nil {
		return "", err
	}
	var flowInstance string
	if err := s.backend.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
	`, runID, entityID).Scan(&flowInstance); err != nil {
		return "", err
	}
	return strings.TrimSpace(flowInstance), nil
}

func (s *BudgetPostgresOwner) ListBudgetProjectionTargets(ctx context.Context, terminalStates []string) ([]budgetspend.ProjectionTarget, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("postgres budget spend store is required")
	}
	rows, err := s.backend.QueryContext(ctx, `
		SELECT es.run_id::text, es.entity_id::text
		FROM entity_state es
		JOIN runs run ON run.run_id = es.run_id
		WHERE run.status IN (`+storerunstate.ActiveStateSQLValues+`)
		  AND NOT (es.current_state = ANY($1::text[]))
		ORDER BY es.run_id::text ASC, es.created_at ASC, es.entity_id::text ASC
	`, pq.Array(normalizeBudgetTerminalStates(terminalStates)))
	if err != nil {
		return nil, fmt.Errorf("list postgres budget projection targets: %w", err)
	}
	defer rows.Close()
	return scanBudgetProjectionTargets(rows)
}

func (s *BudgetPostgresOwner) SumSpendUSD(ctx context.Context, query budgetspend.SpendQuery) (float64, error) {
	if s == nil || s.backend == nil {
		return 0, fmt.Errorf("postgres budget spend store is required")
	}
	query = normalizeBudgetSpendQuery(query)
	var spent float64
	var err error
	switch query.Scope {
	case budgetspend.ScopeSystem:
		err = s.backend.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE created_at >= $1
			  AND ($2::boolean = FALSE OR execution_mode = 'live')
		`, query.Since, query.LiveOnly).Scan(&spent)
	case budgetspend.ScopeGlobal:
		err = s.backend.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id IS NULL
			  AND created_at >= $1
			  AND ($2::boolean = FALSE OR execution_mode = 'live')
		`, query.Since, query.LiveOnly).Scan(&spent)
	case budgetspend.ScopeEntity:
		if err := validateBudgetSpendEntityRequired(query.EntityID); err != nil {
			return 0, err
		}
		err = s.backend.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id = $1::uuid
			  AND created_at >= $2
			  AND ($3::boolean = FALSE OR execution_mode = 'live')
		`, query.EntityID, query.Since, query.LiveOnly).Scan(&spent)
	default:
		return 0, fmt.Errorf("unsupported budget spend scope %q", query.Scope)
	}
	if err != nil {
		return 0, fmt.Errorf("sum postgres spend: %w", err)
	}
	return spent, nil
}

func (s *BudgetSQLiteOwner) RecordSpend(ctx context.Context, rec budgetspend.SpendRecord) error {
	if s == nil || s.backend == nil {
		return fmt.Errorf("sqlite budget spend store is required")
	}
	rec = normalizeBudgetSpendRecord(rec)
	if !rec.ExecutionMode.Valid() {
		return fmt.Errorf("record sqlite spend: execution_mode must be live or mock")
	}
	if err := validateBudgetSpendEntity(rec.EntityID); err != nil {
		return err
	}
	identityFields, err := budgetSpendAgentIdentityFields(rec)
	if err != nil {
		return err
	}
	if err := s.backend.RunTransaction(ctx, "sqlite budget spend record", func(txctx context.Context, tx *sql.Tx) error {
		if rec.EntityID != "" {
			runID, err := runtimecurrentstate.RequireRunID(txctx)
			if err != nil {
				return err
			}
			if err := storerunstate.RequireSQLiteActiveTx(txctx, tx, runID); err != nil {
				return err
			}
			var exists bool
			if err := tx.QueryRowContext(txctx, `SELECT EXISTS (SELECT 1 FROM entity_state WHERE run_id = ? AND entity_id = ?)`, runID, rec.EntityID).Scan(&exists); err != nil {
				return fmt.Errorf("resolve sqlite spend entity: %w", err)
			}
			if !exists {
				return fmt.Errorf("budget spend entity does not belong to canonical run")
			}
		}
		_, err := tx.ExecContext(txctx, `
			INSERT INTO spend_ledger (
				execution_mode, run_id, entity_id, flow_instance, agent_id, agent_name_owner, agent_name_source,
				agent_route_presence, agent_flow_scope_key, agent_flow_instance_id,
				model, model_alias, backend_profile, provider, transport, resolved_model,
				input_tokens, output_tokens, cost_usd, invocation_type, usage_accounting, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, rec.ExecutionMode, identityFields.RunID, nullUUID(rec.EntityID), identityFields.FlowInstancePath, identityFields.AgentID,
			identityFields.NameOwner, identityFields.NameSource, identityFields.RoutePresence,
			identityFields.FlowScopeKey, identityFields.FlowInstanceID,
			rec.Model, rec.ModelAlias, rec.BackendProfile, rec.Provider, rec.Transport, rec.ResolvedModel,
			rec.InputTokens, rec.OutputTokens, rec.CostUSD, rec.InvocationType, rec.UsageAccounting, rec.RecordedAt.UTC())
		return err
	}); err != nil {
		return fmt.Errorf("record sqlite spend: %w", err)
	}
	return nil
}

func (s *BudgetSQLiteOwner) ResolveFlowInstance(ctx context.Context, runID string, entityID string) (string, error) {
	if s == nil || s.backend == nil {
		return "", fmt.Errorf("sqlite budget spend store is required")
	}
	runID, entityID, err := validateBudgetSpendIdentity(runID, entityID)
	if err != nil {
		return "", err
	}
	var flowInstance string
	if err := s.backend.QueryRowContext(ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM entity_state
		WHERE run_id = ?
		  AND entity_id = ?
	`, runID, entityID).Scan(&flowInstance); err != nil {
		return "", err
	}
	return strings.TrimSpace(flowInstance), nil
}

func (s *BudgetSQLiteOwner) ListBudgetProjectionTargets(ctx context.Context, terminalStates []string) ([]budgetspend.ProjectionTarget, error) {
	if s == nil || s.backend == nil {
		return nil, fmt.Errorf("sqlite budget spend store is required")
	}
	args := make([]any, 0, len(terminalStates))
	query := `
		SELECT es.run_id, es.entity_id
		FROM entity_state es
		JOIN runs run ON run.run_id = es.run_id
		WHERE run.status IN (` + storerunstate.ActiveStateSQLValues + `)
	`
	states := normalizeBudgetTerminalStates(terminalStates)
	if len(states) > 0 {
		placeholders := make([]string, 0, len(states))
		for _, state := range states {
			placeholders = append(placeholders, "?")
			args = append(args, state)
		}
		query += " AND es.current_state NOT IN (" + strings.Join(placeholders, ", ") + ")"
	}
	query += " ORDER BY es.run_id ASC, es.created_at ASC, es.entity_id ASC"
	rows, err := s.backend.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list sqlite budget projection targets: %w", err)
	}
	defer rows.Close()
	return scanBudgetProjectionTargets(rows)
}

func (s *BudgetSQLiteOwner) SumSpendUSD(ctx context.Context, query budgetspend.SpendQuery) (float64, error) {
	if s == nil || s.backend == nil {
		return 0, fmt.Errorf("sqlite budget spend store is required")
	}
	query = normalizeBudgetSpendQuery(query)
	var spent float64
	var err error
	switch query.Scope {
	case budgetspend.ScopeSystem:
		err = s.backend.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE created_at >= ?
			  AND (? = 0 OR execution_mode = 'live')
		`, query.Since.UTC(), query.LiveOnly).Scan(&spent)
	case budgetspend.ScopeGlobal:
		err = s.backend.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id IS NULL
			  AND created_at >= ?
			  AND (? = 0 OR execution_mode = 'live')
		`, query.Since.UTC(), query.LiveOnly).Scan(&spent)
	case budgetspend.ScopeEntity:
		if err := validateBudgetSpendEntityRequired(query.EntityID); err != nil {
			return 0, err
		}
		err = s.backend.QueryRowContext(ctx, `
			SELECT COALESCE(SUM(cost_usd), 0)
			FROM spend_ledger
			WHERE entity_id = ?
			  AND created_at >= ?
			  AND (? = 0 OR execution_mode = 'live')
		`, query.EntityID, query.Since.UTC(), query.LiveOnly).Scan(&spent)
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
	rec.AgentIdentity = rec.AgentIdentity.Normalize()
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

func budgetSpendAgentIdentityFields(rec budgetspend.SpendRecord) (fields runtimeagentidentity.StorageFields, err error) {
	fields, err = rec.AgentIdentity.StorageFields()
	if err != nil {
		return fields, fmt.Errorf("budget spend concrete agent identity: %w", err)
	}
	if fields.AgentID != rec.AgentID || fields.FlowInstancePath != rec.FlowInstance {
		return fields, fmt.Errorf("budget spend display fields do not match concrete agent identity")
	}
	return fields, nil
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

func scanBudgetProjectionTargets(rows *sql.Rows) ([]budgetspend.ProjectionTarget, error) {
	out := make([]budgetspend.ProjectionTarget, 0)
	for rows.Next() {
		var target budgetspend.ProjectionTarget
		if err := rows.Scan(&target.RunID, &target.EntityID); err != nil {
			return nil, fmt.Errorf("scan budget projection target: %w", err)
		}
		target.RunID = strings.TrimSpace(target.RunID)
		target.EntityID = strings.TrimSpace(target.EntityID)
		if target.RunID != "" && target.EntityID != "" {
			out = append(out, target)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read budget projection targets: %w", err)
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

func nullUUID(raw string) any {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	return raw
}
