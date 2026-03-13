package store

import (
	"context"
	"fmt"

	"empireai/internal/runtime"
)

func (s *PostgresStore) CountActiveInstances(ctx context.Context) (int, error) {
	var n int
	if err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM verticals
		WHERE stage IN ('approved', 'building', 'pre_launch', 'launched', 'operating', 'expanding')
	`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count active instances: %w", err)
	}
	return n, nil
}

func (s *PostgresStore) ListInstanceDigestRows(ctx context.Context, limit int) ([]runtime.InstanceDigestRow, error) {
	if limit <= 0 {
		limit = 10
	}
	const q = `
		WITH latest_metrics AS (
			SELECT DISTINCT ON (vertical_id)
				vertical_id,
				users_total,
				mrr_cents,
				period_end
			FROM vertical_metrics
			ORDER BY vertical_id, period_end DESC
		),
		spend_30d AS (
			SELECT
				vertical_id,
				COALESCE(SUM(amount_cents), 0) AS spend_cents_30d
			FROM spend_ledger
			WHERE created_at >= now() - interval '30 days'
			GROUP BY vertical_id
		)
		SELECT
			v.id::text,
			v.name,
			v.stage,
			COALESCE(m.users_total, 0),
			COALESCE(m.mrr_cents, 0),
			COALESCE(s.spend_cents_30d, 0),
			COALESCE(m.period_end::timestamp, v.updated_at)
		FROM verticals v
		LEFT JOIN latest_metrics m ON m.vertical_id = v.id
		LEFT JOIN spend_30d s ON s.vertical_id = v.id
		WHERE v.stage IN ('approved', 'building', 'pre_launch', 'launched', 'operating', 'expanding')
		ORDER BY COALESCE(m.mrr_cents, 0) DESC, COALESCE(m.users_total, 0) DESC, v.created_at ASC
		LIMIT $1
	`

	rows, err := s.DB.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("list digest rows: %w", err)
	}
	defer rows.Close()

	out := make([]runtime.InstanceDigestRow, 0)
	for rows.Next() {
		var r runtime.InstanceDigestRow
		if err := rows.Scan(
			&r.EntityID,
			&r.Name,
			&r.Stage,
			&r.UsersTotal,
			&r.MRRCents,
			&r.SpendCents30d,
			&r.LastMetricDate,
		); err != nil {
			return nil, fmt.Errorf("scan digest row: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate digest rows: %w", err)
	}
	return out, nil
}
