package store

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const (
	AgentUsageAccountingExact     = "exact"
	AgentUsageAccountingEstimated = "estimated"
)

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
	UsageAccounting string                   `json:"usage_accounting"`
	InvocationType  string                   `json:"invocation_type"`
	Model           string                   `json:"model"`
	Totals          OperatorAgentUsageTotals `json:"totals"`
}

func (s *PostgresStore) LoadOperatorAgentUsage(ctx context.Context, agentID string, opts OperatorAgentUsageOptions) (OperatorAgentUsage, error) {
	return NewOperatorAgentConversationReadSurface(s.DB, s, 0).LoadOperatorAgentUsage(ctx, agentID, opts)
}

func (r *OperatorAgentConversationReadSurface) LoadOperatorAgentUsage(ctx context.Context, agentID string, opts OperatorAgentUsageOptions) (OperatorAgentUsage, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return OperatorAgentUsage{}, ErrAgentNotFound
	}
	if err := validateOperatorAgentUsageWindow(opts); err != nil {
		return OperatorAgentUsage{}, err
	}
	if err := r.requireAgentUsageCapabilities(ctx); err != nil {
		return OperatorAgentUsage{}, err
	}
	if err := r.ensureAgentUsageAgentExists(ctx, agentID); err != nil {
		return OperatorAgentUsage{}, err
	}

	breakdown, err := r.loadAgentUsageBreakdown(ctx, agentID, opts)
	if err != nil {
		return OperatorAgentUsage{}, err
	}
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

func (r *OperatorAgentConversationReadSurface) requireAgentUsageCapabilities(ctx context.Context) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("operator agent usage read owner requires postgres store")
	}
	caps, err := r.resolveConversationCapabilities(ctx)
	if err != nil {
		return err
	}
	if caps.Agents != SchemaFlavorCanonical {
		return unsupportedSchemaCapability("agents", caps.Agents)
	}
	catalog, err := loadSchemaColumnCatalog(ctx, r.db)
	if err != nil {
		return err
	}
	required := map[string][]string{
		"agents": {
			"agent_id", "status",
		},
		"spend_ledger": {
			"agent_id", "model", "input_tokens", "output_tokens", "cost_usd",
			"invocation_type", "usage_accounting", "created_at",
		},
	}
	for table, columns := range required {
		if !catalog.hasColumns(table, columns...) {
			return fmt.Errorf("agent usage read owner requires canonical %s columns: %s", table, strings.Join(columns, ", "))
		}
	}
	return nil
}

func (r *OperatorAgentConversationReadSurface) ensureAgentUsageAgentExists(ctx context.Context, agentID string) error {
	var exists bool
	if err := r.db.QueryRowContext(ctx, `
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

func (r *OperatorAgentConversationReadSurface) loadAgentUsageBreakdown(ctx context.Context, agentID string, opts OperatorAgentUsageOptions) ([]OperatorAgentUsageBreakdown, error) {
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
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT
			usage_accounting,
			invocation_type,
			model,
			COUNT(*)::int,
			COALESCE(SUM(input_tokens), 0)::bigint,
			COALESCE(SUM(output_tokens), 0)::bigint,
			COALESCE(SUM(cost_usd), 0)::float8
		FROM spend_ledger
		WHERE agent_id = $1
		  %s
		GROUP BY usage_accounting, invocation_type, model
		ORDER BY CASE usage_accounting WHEN 'exact' THEN 0 WHEN 'estimated' THEN 1 ELSE 2 END ASC, invocation_type ASC, model ASC
	`, windowClause.String()), args...)
	if err != nil {
		return nil, fmt.Errorf("load agent usage breakdown: %w", err)
	}
	defer rows.Close()

	var out []OperatorAgentUsageBreakdown
	for rows.Next() {
		var row OperatorAgentUsageBreakdown
		if err := rows.Scan(
			&row.UsageAccounting,
			&row.InvocationType,
			&row.Model,
			&row.Totals.LedgerEntries,
			&row.Totals.InputTokens,
			&row.Totals.OutputTokens,
			&row.Totals.EstimatedCostUSD,
		); err != nil {
			return nil, fmt.Errorf("scan agent usage breakdown: %w", err)
		}
		row.UsageAccounting = strings.TrimSpace(row.UsageAccounting)
		row.InvocationType = strings.TrimSpace(row.InvocationType)
		row.Model = strings.TrimSpace(row.Model)
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
	if row.Totals.LedgerEntries < 0 || row.Totals.InputTokens < 0 || row.Totals.OutputTokens < 0 || row.Totals.EstimatedCostUSD < 0 {
		return fmt.Errorf("agent usage read owner returned negative totals")
	}
	return nil
}
