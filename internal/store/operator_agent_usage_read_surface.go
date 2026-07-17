package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

const (
	AgentUsageAccountingExact     = "exact"
	AgentUsageAccountingEstimated = "estimated"
)

// OperatorAgentUsageReadStore is the backend-neutral owner for the public
// per-agent usage read surface over canonical spend_ledger facts.
type OperatorAgentUsageReadStore interface {
	LoadOperatorAgentUsage(context.Context, string, OperatorAgentUsageOptions) (OperatorAgentUsage, error)
}

var _ OperatorAgentUsageReadStore = (*PostgresStore)(nil)
var _ OperatorAgentUsageReadStore = (*SQLiteRuntimeStore)(nil)

type OperatorAgentUsageOptions struct {
	Since *time.Time
	Until *time.Time
}

type OperatorAgentUsage struct {
	AgentID   string                         `json:"agent_id"`
	Window    OperatorAgentUsageWindow       `json:"window"`
	Usage     OperatorAgentUsageByAccounting `json:"usage"`
	Breakdown []OperatorAgentUsageBreakdown  `json:"breakdown"`
}

type OperatorAgentUsageWindow struct {
	Since *time.Time `json:"since,omitempty"`
	Until *time.Time `json:"until,omitempty"`
}

type OperatorAgentUsageByAccounting struct {
	Exact     OperatorAgentUsageTotals `json:"exact"`
	Estimated OperatorAgentUsageTotals `json:"estimated"`
}

type OperatorAgentUsageTotals struct {
	LedgerEntries    int     `json:"ledger_entries"`
	InputTokens      int64   `json:"input_tokens"`
	OutputTokens     int64   `json:"output_tokens"`
	EstimatedCostUSD float64 `json:"estimated_cost_usd"`
}

type OperatorAgentUsageBreakdown struct {
	ExecutionMode   string                   `json:"execution_mode"`
	UsageAccounting string                   `json:"usage_accounting"`
	InvocationType  string                   `json:"invocation_type"`
	Model           string                   `json:"model"`
	ModelAlias      string                   `json:"model_alias"`
	BackendProfile  string                   `json:"backend_profile"`
	Provider        string                   `json:"provider"`
	Transport       string                   `json:"transport"`
	ResolvedModel   string                   `json:"resolved_model"`
	CostDisplay     string                   `json:"cost_display"`
	Totals          OperatorAgentUsageTotals `json:"totals"`
}

func (s *PostgresStore) LoadOperatorAgentUsage(ctx context.Context, agentID string, opts OperatorAgentUsageOptions) (OperatorAgentUsage, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentUsage{}, ErrAgentNotFound
	}
	if err := validateOperatorAgentUsageWindow(opts); err != nil {
		return OperatorAgentUsage{}, err
	}
	if err := s.requireAgentUsageAccess(); err != nil {
		return OperatorAgentUsage{}, err
	}
	if err := s.ensureAgentUsageAgentExists(ctx, agentID); err != nil {
		return OperatorAgentUsage{}, err
	}
	breakdown, err := s.loadAgentUsageBreakdown(ctx, agentID, opts)
	if err != nil {
		return OperatorAgentUsage{}, err
	}
	return buildOperatorAgentUsage(agentID, opts, breakdown)
}

func (s *SQLiteRuntimeStore) LoadOperatorAgentUsage(ctx context.Context, agentID string, opts OperatorAgentUsageOptions) (OperatorAgentUsage, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentUsage{}, ErrAgentNotFound
	}
	if err := validateOperatorAgentUsageWindow(opts); err != nil {
		return OperatorAgentUsage{}, err
	}
	if err := s.requireAgentUsageAccess(); err != nil {
		return OperatorAgentUsage{}, err
	}
	if err := s.ensureAgentUsageAgentExists(ctx, agentID); err != nil {
		return OperatorAgentUsage{}, err
	}
	breakdown, err := s.loadAgentUsageBreakdown(ctx, agentID, opts)
	if err != nil {
		return OperatorAgentUsage{}, err
	}
	return buildOperatorAgentUsage(agentID, opts, breakdown)
}

func buildOperatorAgentUsage(agentID string, opts OperatorAgentUsageOptions, breakdown []OperatorAgentUsageBreakdown) (OperatorAgentUsage, error) {
	result := OperatorAgentUsage{
		AgentID:   agentID,
		Window:    OperatorAgentUsageWindow{Since: copyTimePtr(opts.Since), Until: copyTimePtr(opts.Until)},
		Breakdown: breakdown,
	}
	if result.Breakdown == nil {
		result.Breakdown = []OperatorAgentUsageBreakdown{}
	}
	for _, row := range breakdown {
		switch row.UsageAccounting {
		case AgentUsageAccountingExact:
			result.Usage.Exact = addOperatorAgentUsageTotals(result.Usage.Exact, row.Totals)
		case AgentUsageAccountingEstimated:
			result.Usage.Estimated = addOperatorAgentUsageTotals(result.Usage.Estimated, row.Totals)
		default:
			return OperatorAgentUsage{}, fmt.Errorf("agent usage read owner returned unsupported usage_accounting %q", row.UsageAccounting)
		}
	}
	return result, nil
}

func validateOperatorAgentUsageWindow(opts OperatorAgentUsageOptions) error {
	if opts.Since != nil && opts.Until != nil && !opts.Since.Before(*opts.Until) {
		return fmt.Errorf("agent usage window requires since before until")
	}
	return nil
}

func copyTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := value.UTC()
	return &copied
}

func addOperatorAgentUsageTotals(a, b OperatorAgentUsageTotals) OperatorAgentUsageTotals {
	return OperatorAgentUsageTotals{
		LedgerEntries:    a.LedgerEntries + b.LedgerEntries,
		InputTokens:      a.InputTokens + b.InputTokens,
		OutputTokens:     a.OutputTokens + b.OutputTokens,
		EstimatedCostUSD: a.EstimatedCostUSD + b.EstimatedCostUSD,
	}
}

func (s *PostgresStore) requireAgentUsageAccess() error {
	return s.requireCurrentSchema()
}

func (s *SQLiteRuntimeStore) requireAgentUsageAccess() error {
	return s.requireCurrentSchema()
}

func (s *PostgresStore) ensureAgentUsageAgentExists(ctx context.Context, agentID string) error {
	var exists bool
	if err := s.DB.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM agents
			WHERE agent_id = $1
			  AND status NOT IN ('terminated', 'ephemeral')
		)
	`, agentID).Scan(&exists); err != nil {
		return fmt.Errorf("load agent usage agent: %w", err)
	}
	if !exists {
		return ErrAgentNotFound
	}
	return nil
}

func (s *SQLiteRuntimeStore) ensureAgentUsageAgentExists(ctx context.Context, agentID string) error {
	var count int
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM agents
		WHERE agent_id = ?
		  AND status NOT IN ('terminated', 'ephemeral')
	`, agentID).Scan(&count); err != nil {
		return fmt.Errorf("load agent usage agent: %w", err)
	}
	if count == 0 {
		return ErrAgentNotFound
	}
	return nil
}

func (s *PostgresStore) loadAgentUsageBreakdown(ctx context.Context, agentID string, opts OperatorAgentUsageOptions) ([]OperatorAgentUsageBreakdown, error) {
	args := []any{agentID}
	windowClause := strings.Builder{}
	if opts.Since != nil {
		args = append(args, opts.Since.UTC())
		windowClause.WriteString(fmt.Sprintf("\n		  AND created_at >= $%d", len(args)))
	}
	if opts.Until != nil {
		args = append(args, opts.Until.UTC())
		windowClause.WriteString(fmt.Sprintf("\n		  AND created_at < $%d", len(args)))
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		WITH usage_rows AS (
			SELECT
				execution_mode,
				usage_accounting,
				invocation_type,
				model,
				COALESCE(NULLIF(BTRIM(model_alias), ''), 'unknown') AS model_alias,
				COALESCE(NULLIF(BTRIM(backend_profile), ''), 'unknown') AS backend_profile,
				COALESCE(NULLIF(BTRIM(provider), ''), 'unknown') AS provider,
				COALESCE(NULLIF(BTRIM(transport), ''), 'unknown') AS transport,
				COALESCE(NULLIF(BTRIM(resolved_model), ''), model) AS resolved_model,
				input_tokens,
				output_tokens,
				cost_usd
			FROM spend_ledger
			WHERE agent_id = $1
			  %s
		)
		SELECT
			execution_mode,
			usage_accounting,
			invocation_type,
			model,
			model_alias,
			backend_profile,
			provider,
			transport,
			resolved_model,
			COUNT(*)::int,
			COALESCE(SUM(input_tokens), 0)::bigint,
			COALESCE(SUM(output_tokens), 0)::bigint,
			COALESCE(SUM(cost_usd), 0)::float8
		FROM usage_rows
		GROUP BY execution_mode, usage_accounting, invocation_type, model, model_alias, backend_profile, provider, transport, resolved_model
		ORDER BY CASE execution_mode WHEN 'live' THEN 0 ELSE 1 END ASC, CASE usage_accounting WHEN 'exact' THEN 0 WHEN 'estimated' THEN 1 ELSE 2 END ASC, invocation_type ASC, model ASC, model_alias ASC
	`, windowClause.String()), args...)
	if err != nil {
		return nil, fmt.Errorf("load agent usage breakdown: %w", err)
	}
	defer rows.Close()

	var out []OperatorAgentUsageBreakdown
	for rows.Next() {
		var row OperatorAgentUsageBreakdown
		if err := rows.Scan(
			&row.ExecutionMode,
			&row.UsageAccounting,
			&row.InvocationType,
			&row.Model,
			&row.ModelAlias,
			&row.BackendProfile,
			&row.Provider,
			&row.Transport,
			&row.ResolvedModel,
			&row.Totals.LedgerEntries,
			&row.Totals.InputTokens,
			&row.Totals.OutputTokens,
			&row.Totals.EstimatedCostUSD,
		); err != nil {
			return nil, fmt.Errorf("scan agent usage breakdown: %w", err)
		}
		row.UsageAccounting = strings.TrimSpace(row.UsageAccounting)
		row.ExecutionMode = strings.TrimSpace(row.ExecutionMode)
		row.InvocationType = strings.TrimSpace(row.InvocationType)
		row.Model = strings.TrimSpace(row.Model)
		row.ModelAlias = strings.TrimSpace(row.ModelAlias)
		row.BackendProfile = strings.TrimSpace(row.BackendProfile)
		row.Provider = strings.TrimSpace(row.Provider)
		row.Transport = strings.TrimSpace(row.Transport)
		row.ResolvedModel = strings.TrimSpace(row.ResolvedModel)
		row.CostDisplay = operatorUsageCostDisplay(row.ExecutionMode, row.Totals.EstimatedCostUSD)
		if err := validateOperatorAgentUsageBreakdown(row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent usage breakdown: %w", err)
	}
	return out, nil
}

func (s *SQLiteRuntimeStore) loadAgentUsageBreakdown(ctx context.Context, agentID string, opts OperatorAgentUsageOptions) ([]OperatorAgentUsageBreakdown, error) {
	args := []any{agentID}
	windowClause := strings.Builder{}
	if opts.Since != nil {
		args = append(args, opts.Since.UTC())
		windowClause.WriteString("\n		  AND created_at >= ?")
	}
	if opts.Until != nil {
		args = append(args, opts.Until.UTC())
		windowClause.WriteString("\n		  AND created_at < ?")
	}
	rows, err := s.DB.QueryContext(ctx, fmt.Sprintf(`
		WITH usage_rows AS (
			SELECT
				execution_mode,
				usage_accounting,
				invocation_type,
				model,
				COALESCE(NULLIF(trim(model_alias), ''), 'unknown') AS model_alias,
				COALESCE(NULLIF(trim(backend_profile), ''), 'unknown') AS backend_profile,
				COALESCE(NULLIF(trim(provider), ''), 'unknown') AS provider,
				COALESCE(NULLIF(trim(transport), ''), 'unknown') AS transport,
				COALESCE(NULLIF(trim(resolved_model), ''), model) AS resolved_model,
				input_tokens,
				output_tokens,
				cost_usd
			FROM spend_ledger
			WHERE agent_id = ?
			  %s
		)
		SELECT
			execution_mode,
			usage_accounting,
			invocation_type,
			model,
			model_alias,
			backend_profile,
			provider,
			transport,
			resolved_model,
			COUNT(*),
			COALESCE(SUM(input_tokens), 0),
			COALESCE(SUM(output_tokens), 0),
			COALESCE(SUM(cost_usd), 0)
		FROM usage_rows
		GROUP BY execution_mode, usage_accounting, invocation_type, model, model_alias, backend_profile, provider, transport, resolved_model
		ORDER BY CASE execution_mode WHEN 'live' THEN 0 ELSE 1 END ASC, CASE usage_accounting WHEN 'exact' THEN 0 WHEN 'estimated' THEN 1 ELSE 2 END ASC, invocation_type ASC, model ASC, model_alias ASC
	`, windowClause.String()), args...)
	if err != nil {
		return nil, fmt.Errorf("load agent usage breakdown: %w", err)
	}
	defer rows.Close()

	var out []OperatorAgentUsageBreakdown
	for rows.Next() {
		var row OperatorAgentUsageBreakdown
		if err := rows.Scan(
			&row.ExecutionMode,
			&row.UsageAccounting,
			&row.InvocationType,
			&row.Model,
			&row.ModelAlias,
			&row.BackendProfile,
			&row.Provider,
			&row.Transport,
			&row.ResolvedModel,
			&row.Totals.LedgerEntries,
			&row.Totals.InputTokens,
			&row.Totals.OutputTokens,
			&row.Totals.EstimatedCostUSD,
		); err != nil {
			return nil, fmt.Errorf("scan agent usage breakdown: %w", err)
		}
		row.UsageAccounting = strings.TrimSpace(row.UsageAccounting)
		row.ExecutionMode = strings.TrimSpace(row.ExecutionMode)
		row.InvocationType = strings.TrimSpace(row.InvocationType)
		row.Model = strings.TrimSpace(row.Model)
		row.ModelAlias = strings.TrimSpace(row.ModelAlias)
		row.BackendProfile = strings.TrimSpace(row.BackendProfile)
		row.Provider = strings.TrimSpace(row.Provider)
		row.Transport = strings.TrimSpace(row.Transport)
		row.ResolvedModel = strings.TrimSpace(row.ResolvedModel)
		row.CostDisplay = operatorUsageCostDisplay(row.ExecutionMode, row.Totals.EstimatedCostUSD)
		if err := validateOperatorAgentUsageBreakdown(row); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read agent usage breakdown: %w", err)
	}
	return out, nil
}

func validateOperatorAgentUsageBreakdown(row OperatorAgentUsageBreakdown) error {
	if mode, ok := executionmode.Parse(row.ExecutionMode); !ok || string(mode) != row.ExecutionMode {
		return fmt.Errorf("agent usage read owner returned unsupported execution_mode %q", row.ExecutionMode)
	}
	switch row.UsageAccounting {
	case AgentUsageAccountingExact, AgentUsageAccountingEstimated:
	default:
		return fmt.Errorf("agent usage read owner returned unsupported usage_accounting %q", row.UsageAccounting)
	}
	if row.InvocationType == "" {
		return fmt.Errorf("agent usage read owner returned empty invocation_type")
	}
	if row.Model == "" {
		return fmt.Errorf("agent usage read owner returned empty model")
	}
	if row.ModelAlias == "" {
		return fmt.Errorf("agent usage read owner returned empty model_alias")
	}
	if row.BackendProfile == "" {
		return fmt.Errorf("agent usage read owner returned empty backend_profile")
	}
	if row.Provider == "" {
		return fmt.Errorf("agent usage read owner returned empty provider")
	}
	if row.Transport == "" {
		return fmt.Errorf("agent usage read owner returned empty transport")
	}
	if row.ResolvedModel == "" {
		return fmt.Errorf("agent usage read owner returned empty resolved_model")
	}
	if row.Totals.LedgerEntries < 0 || row.Totals.InputTokens < 0 || row.Totals.OutputTokens < 0 || row.Totals.EstimatedCostUSD < 0 {
		return fmt.Errorf("agent usage read owner returned negative totals")
	}
	return nil
}

func operatorUsageCostDisplay(mode string, cost float64) string {
	if strings.TrimSpace(mode) == string(executionmode.Mock) {
		return fmt.Sprintf("~$%.6f (mock estimate)", cost)
	}
	return fmt.Sprintf("$%.6f", cost)
}
