package store

import (
	"context"
	"fmt"

	"github.com/lib/pq"
	"swarm/internal/runtime"
)

func (s *PostgresStore) CountActiveInstances(ctx context.Context) (int, error) {
	var n int
	err := s.DB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entity_state
		WHERE NOT (current_state = ANY($1::text[]))
	`, pq.Array(runtime.TerminalInstanceStates())).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count active instances: %w", err)
	}
	return n, nil
}

func (s *PostgresStore) ListInstanceDigestRows(ctx context.Context, limit int) ([]runtime.InstanceDigestRow, error) {
	if limit <= 0 {
		limit = 10
	}
	const q = `
		SELECT
			es.entity_id::text,
			COALESCE(NULLIF(es.name, ''), NULLIF(es.fields->>'name', ''), es.entity_id::text),
			es.current_state,
			es.updated_at
		FROM entity_state es
		WHERE NOT (es.current_state = ANY($2::text[]))
		ORDER BY es.updated_at DESC, es.created_at ASC
		LIMIT $1
	`

	rows, err := s.DB.QueryContext(ctx, q, limit, pq.Array(runtime.TerminalInstanceStates()))
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
			&r.UpdatedAt,
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
