package authoractivity

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func List(ctx context.Context, db Queryer, dialect Dialect, opts ListOptions) (ListResult, error) {
	if db == nil {
		return ListResult{}, fmt.Errorf("author activity reader is required")
	}
	if opts.AfterSequence < 0 {
		return ListResult{}, fmt.Errorf("author activity cursor must be non-negative")
	}
	if opts.Limit == 0 {
		opts.Limit = 100
	}
	if opts.Limit < 1 || opts.Limit > 500 {
		return ListResult{}, fmt.Errorf("author activity limit must be between 1 and 500")
	}
	if dialect != DialectPostgres && dialect != DialectSQLite {
		return ListResult{}, fmt.Errorf("author activity dialect %q is not supported", dialect)
	}
	where := []string{"sequence > " + bind(dialect, 1)}
	args := []any{opts.AfterSequence}
	filters := []struct {
		column string
		value  string
	}{{"run_id", opts.RunID}, {"entity_id", opts.EntityID}, {"agent_id", opts.AgentID}, {"flow_id", opts.FlowID}}
	for _, filter := range filters {
		value := strings.TrimSpace(filter.value)
		if value == "" {
			continue
		}
		args = append(args, value)
		placeholder := bind(dialect, len(args))
		if dialect == DialectPostgres && (filter.column == "run_id" || filter.column == "entity_id") {
			placeholder += "::uuid"
		}
		where = append(where, filter.column+" = "+placeholder)
	}
	args = append(args, opts.Limit)
	query := occurrenceSelect + " WHERE " + strings.Join(where, " AND ") + " ORDER BY sequence ASC LIMIT " + bind(dialect, len(args))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return ListResult{}, fmt.Errorf("list author activity: %w", err)
	}
	defer rows.Close()
	result := ListResult{Occurrences: make([]Occurrence, 0, opts.Limit)}
	for rows.Next() {
		occurrence, err := scanOccurrence(rows)
		if err != nil {
			return ListResult{}, err
		}
		result.Occurrences = append(result.Occurrences, occurrence)
		result.NextCursor = occurrence.Sequence
	}
	if err := rows.Err(); err != nil {
		return ListResult{}, fmt.Errorf("list author activity: %w", err)
	}
	return result, nil
}

func bind(dialect Dialect, index int) string {
	if dialect == DialectPostgres {
		return fmt.Sprintf("$%d", index)
	}
	return "?"
}
