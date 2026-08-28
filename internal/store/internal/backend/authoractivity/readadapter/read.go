package authoractivityadapter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/stringsutil"
)

type Dialect uint8

const (
	DialectUnknown Dialect = iota
	DialectPostgres
	DialectSQLite
)

type Queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type QueryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

const occurrenceSelect = `SELECT CAST(occurrence_id AS TEXT), sequence, kind, version, transition, source_owner, source_identity, dedup_key, COALESCE(CAST(run_id AS TEXT), ''), COALESCE(CAST(entity_id AS TEXT), ''), COALESCE(agent_id, ''), COALESCE(flow_id, ''), scope_kind, COALESCE(CAST(runtime_instance_id AS TEXT), ''), COALESCE(bundle_hash, ''), COALESCE(author_safe_summary, ''), projection, failure, occurred_at FROM author_activity_occurrences`

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

func List(ctx context.Context, db Queryer, dialect Dialect, opts runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error) {
	if db == nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("author activity reader is required")
	}
	if opts.AfterSequence < 0 {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("author activity cursor must be non-negative")
	}
	if opts.Limit == 0 {
		opts.Limit = 100
	}
	if opts.Limit < 1 || opts.Limit > 500 {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("author activity limit must be between 1 and 500")
	}
	if dialect != DialectPostgres && dialect != DialectSQLite {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("author activity dialect is not supported")
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
			return runtimeauthoractivity.ListResult{}, fmt.Errorf("author activity scoped read requires runtime_instance_id")
		}
		args = append(args, runtimeInstanceID)
		runtimePlaceholder := bind(dialect, len(args))
		if dialect == DialectPostgres {
			runtimePlaceholder += "::uuid"
		}
		where = append(where, "runtime_instance_id = "+runtimePlaceholder)
		var scopes []string
		bundleHashes := stringsutil.Unique(opts.BundleHashes)
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
			return runtimeauthoractivity.ListResult{}, fmt.Errorf("author activity scoped read requires bundle hashes or runtime scope")
		}
		where = append(where, "("+strings.Join(scopes, " OR ")+")")
	}
	args = append(args, opts.Limit)
	query := occurrenceSelect + " WHERE " + strings.Join(where, " AND ") + " ORDER BY sequence ASC LIMIT " + bind(dialect, len(args))
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("list author activity: %w", err)
	}
	defer rows.Close()
	result := runtimeauthoractivity.ListResult{Occurrences: make([]runtimeauthoractivity.Occurrence, 0, opts.Limit)}
	for rows.Next() {
		occurrence, err := ScanOccurrence(rows)
		if err != nil {
			return runtimeauthoractivity.ListResult{}, err
		}
		result.Occurrences = append(result.Occurrences, occurrence)
		result.NextCursor = occurrence.Sequence
	}
	if err := rows.Err(); err != nil {
		return runtimeauthoractivity.ListResult{}, fmt.Errorf("list author activity: %w", err)
	}
	return result, nil
}

type RowScanner interface {
	Scan(...any) error
}

func ScanOccurrence(row RowScanner) (runtimeauthoractivity.Occurrence, error) {
	var occurrence runtimeauthoractivity.Occurrence
	var projectionRaw []byte
	var failureRaw []byte
	var occurredAtRaw any
	if err := row.Scan(
		&occurrence.OccurrenceID, &occurrence.Sequence, &occurrence.Kind, &occurrence.Version,
		&occurrence.Transition, &occurrence.SourceOwner, &occurrence.SourceIdentity, &occurrence.DedupKey,
		&occurrence.RunID, &occurrence.EntityID, &occurrence.AgentID, &occurrence.FlowID,
		&occurrence.Scope.Kind, &occurrence.Scope.RuntimeInstanceID, &occurrence.Scope.BundleHash,
		&occurrence.AuthorSafeSummary, &projectionRaw, &failureRaw, &occurredAtRaw,
	); err != nil {
		return runtimeauthoractivity.Occurrence{}, err
	}
	if err := json.Unmarshal(projectionRaw, &occurrence.Projection); err != nil {
		return runtimeauthoractivity.Occurrence{}, fmt.Errorf("decode author activity projection: %w", err)
	}
	if len(failureRaw) > 0 && string(failureRaw) != "null" {
		var failure runtimefailures.Envelope
		if err := json.Unmarshal(failureRaw, &failure); err != nil {
			return runtimeauthoractivity.Occurrence{}, fmt.Errorf("decode author activity failure: %w", err)
		}
		if err := runtimefailures.ValidateEnvelope(failure); err != nil {
			return runtimeauthoractivity.Occurrence{}, fmt.Errorf("validate author activity failure: %w", err)
		}
		occurrence.Failure = &failure
	}
	occurredAt, err := decodeTime(occurredAtRaw)
	if err != nil {
		return runtimeauthoractivity.Occurrence{}, fmt.Errorf("decode author activity occurred_at: %w", err)
	}
	occurrence.OccurredAt = occurredAt
	if err := runtimeauthoractivity.ValidateDraft(runtimeauthoractivity.Draft{
		Kind: occurrence.Kind, Version: occurrence.Version, Transition: occurrence.Transition,
		SourceOwner: occurrence.SourceOwner, SourceIdentity: occurrence.SourceIdentity,
		DedupKey: occurrence.DedupKey, OccurredAt: occurrence.OccurredAt, Scope: occurrence.Scope,
		AuthorSafeSummary: occurrence.AuthorSafeSummary, Projection: occurrence.Projection, Failure: occurrence.Failure,
	}); err != nil {
		return runtimeauthoractivity.Occurrence{}, fmt.Errorf("invalid persisted author activity %s: %w", occurrence.OccurrenceID, err)
	}
	return occurrence, nil
}

func decodeTime(raw any) (time.Time, error) {
	switch value := raw.(type) {
	case time.Time:
		return value.UTC(), nil
	case string:
		return parseStoredTime(value)
	case []byte:
		return parseStoredTime(string(value))
	default:
		return time.Time{}, fmt.Errorf("unsupported time value %T", raw)
	}
}

func parseStoredTime(value string) (time.Time, error) {
	var lastErr error
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05 -0700 MST"} {
		parsed, err := time.Parse(layout, strings.TrimSpace(value))
		if err == nil {
			return parsed.UTC(), nil
		}
		lastErr = err
	}
	return time.Time{}, lastErr
}

func bind(dialect Dialect, index int) string {
	if dialect == DialectPostgres {
		return fmt.Sprintf("$%d", index)
	}
	return "?"
}
