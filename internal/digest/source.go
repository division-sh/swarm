package digest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"swarm/internal/runtime"
	"swarm/internal/runtime/semanticview"
)

type Source struct {
	db             *sql.DB
	terminalStates []string
}

func NewSource(db *sql.DB, source semanticview.Source) (*Source, error) {
	if source == nil {
		return nil, fmt.Errorf("semantic source is required for digest reads")
	}
	return newSource(db, source.WorkflowTerminalStages())
}

func newSource(db *sql.DB, terminalStates []string) (*Source, error) {
	if db == nil {
		return nil, fmt.Errorf("digest db is required")
	}
	states := normalizeTerminalStates(terminalStates)
	if len(states) == 0 {
		return nil, fmt.Errorf("terminal instance states are required for digest reads")
	}
	return &Source{
		db:             db,
		terminalStates: states,
	}, nil
}

func normalizeTerminalStates(states []string) []string {
	out := make([]string, 0, len(states))
	seen := make(map[string]struct{}, len(states))
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

func (s *Source) CountActiveInstances(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM entity_state
		WHERE NOT (current_state = ANY($1::text[]))
	`, pq.Array(s.terminalStates)).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count active instances: %w", err)
	}
	return n, nil
}

func (s *Source) ListInstanceDigestRows(ctx context.Context, limit int) ([]runtime.InstanceDigestRow, error) {
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

	rows, err := s.db.QueryContext(ctx, q, limit, pq.Array(s.terminalStates))
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
