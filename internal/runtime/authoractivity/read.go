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

type QueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func Head(ctx context.Context, db QueryRower) (int64, error) {
	if db == nil {
		return 0, fmt.Errorf("author activity reader is required")
	}
	var sequence int64
	if err := db.QueryRowContext(ctx, `SELECT last_sequence FROM author_activity_order WHERE singleton_id = 1`).Scan(&sequence); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("read author activity head: %w", err)
	}
	return sequence, nil
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
	if len(opts.BundleHashes) > 0 || opts.IncludeRuntimeScope || strings.TrimSpace(opts.RuntimeInstanceID) != "" {
		runtimeInstanceID := strings.TrimSpace(opts.RuntimeInstanceID)
		if runtimeInstanceID == "" {
			return ListResult{}, fmt.Errorf("author activity scoped read requires runtime_instance_id")
		}
		args = append(args, runtimeInstanceID)
		runtimePlaceholder := bind(dialect, len(args))
		if dialect == DialectPostgres {
			runtimePlaceholder += "::uuid"
		}
		where = append(where, "runtime_instance_id = "+runtimePlaceholder)
		var scopes []string
		bundleHashes := normalizedUniqueStrings(opts.BundleHashes)
		if len(bundleHashes) > 0 {
			placeholders := make([]string, 0, len(bundleHashes))
			for _, bundleHash := range bundleHashes {
				args = append(args, bundleHash)
				placeholders = append(placeholders, bind(dialect, len(args)))
			}
			scopes = append(scopes, "(scope_kind = 'bundle' AND bundle_hash IN ("+strings.Join(placeholders, ", ")+"))")
		}
		if opts.IncludeRuntimeScope {
			scopes = append(scopes, "(scope_kind = 'runtime' AND bundle_hash IS NULL)")
		}
		if len(scopes) == 0 {
			return ListResult{}, fmt.Errorf("author activity scoped read requires bundle hashes or runtime scope")
		}
		where = append(where, "("+strings.Join(scopes, " OR ")+")")
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

func normalizedUniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func bind(dialect Dialect, index int) string {
	if dialect == DialectPostgres {
		return fmt.Sprintf("$%d", index)
	}
	return "?"
}
