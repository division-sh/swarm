package store

import (
	"context"
	"fmt"
)

// ListActiveAgentIDs implements runtime.ActiveAgentLister for broadcast-style events
// such as budget.*.
func (s *PostgresStore) ListActiveAgentIDs(ctx context.Context) ([]string, error) {
	if s == nil || s.DB == nil {
		return nil, fmt.Errorf("db unavailable")
	}
	rows, err := s.DB.QueryContext(ctx, `
		SELECT id
		FROM agents
		WHERE COALESCE(status, '') <> 'terminated'
		ORDER BY id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, 64)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != "" {
			out = append(out, id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
