package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
)

const hydrationBatchSize = 128

type RowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type Queryer interface {
	RowQueryer
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type Execer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func HydrationBatchSize() int { return hydrationBatchSize }

func Load(ctx context.Context, q RowQueryer, eventID string) (eventrecord.Record, bool, error) {
	var record eventrecord.Record
	var createdAt any
	err := q.QueryRowContext(ctx, selectRecord+` WHERE e.event_id = ?`, strings.TrimSpace(eventID)).Scan(scanTargets(&record, &createdAt)...)
	if err == sql.ErrNoRows {
		return eventrecord.Record{}, false, nil
	}
	if err != nil {
		return eventrecord.Record{}, false, fmt.Errorf("load sqlite event record: %w", err)
	}
	if err := assignCreatedAt(&record, createdAt); err != nil {
		return eventrecord.Record{}, false, eventrecord.Corrupt(record.EventID, err)
	}
	if err := record.Validate(); err != nil {
		return eventrecord.Record{}, false, fmt.Errorf("load sqlite event record: %w", eventrecord.Corrupt(record.EventID, err))
	}
	return record.Clone(), true, nil
}

func LoadMany(ctx context.Context, q Queryer, eventIDs []string) ([]eventrecord.Record, error) {
	ordered, err := normalizeIDs(eventIDs)
	if err != nil {
		return nil, err
	}
	if len(ordered) == 0 {
		return []eventrecord.Record{}, nil
	}
	loaded := make(map[string]eventrecord.Record, len(ordered))
	for start := 0; start < len(ordered); start += hydrationBatchSize {
		end := min(start+hydrationBatchSize, len(ordered))
		args := make([]any, end-start)
		placeholders := make([]string, end-start)
		for i, id := range ordered[start:end] {
			args[i] = id
			placeholders[i] = "?"
		}
		rows, err := q.QueryContext(ctx, selectRecord+` WHERE e.event_id IN (`+strings.Join(placeholders, ", ")+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("hydrate sqlite event record batch: %w", err)
		}
		if err := scanRows(rows, loaded); err != nil {
			return nil, err
		}
	}
	return orderRecords(ordered, loaded)
}

func Insert(ctx context.Context, exec Execer, record eventrecord.Record) (bool, error) {
	if err := record.Validate(); err != nil {
		return false, fmt.Errorf("append sqlite event record: %w", err)
	}
	result, err := exec.ExecContext(ctx, `
		INSERT INTO events (
			event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload,
			execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
			routing_source_kind, routing_source_authority, source_route, target_route, target_set,
			operator_reference_event_id
		) VALUES (?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?, ?, NULLIF(?, ''))
		ON CONFLICT(event_id) DO NOTHING
	`, record.Class, record.EventID, record.RunID, record.EventName, record.TaskID,
		record.EntityID, record.FlowInstance, record.Scope, string(record.Payload), record.ExecutionMode,
		record.ChainDepth, record.ProducedBy, record.ProducedByType, record.SourceEventID, record.CreatedAt.UTC(),
		record.RoutingSourceKind, record.RoutingSourceAuthority, string(record.SourceRoute),
		string(record.TargetRoute), string(record.TargetSet), record.OperatorReferencedEventID)
	if err != nil {
		return false, fmt.Errorf("append sqlite event record: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("append sqlite event record: read affected rows: %w", err)
	}
	return rows == 1, nil
}

const selectRecord = `
	SELECT
		e.event_class, e.event_id, COALESCE(e.run_id, ''), e.event_name, COALESCE(e.task_id, ''),
		COALESCE(e.entity_id, ''), COALESCE(e.flow_instance, ''), e.scope, e.payload, e.execution_mode,
		e.chain_depth, e.produced_by, e.produced_by_type, COALESCE(e.source_event_id, ''), e.created_at,
		e.routing_source_kind, COALESCE(e.routing_source_authority, ''), e.source_route, e.target_route,
		e.target_set, COALESCE(e.operator_reference_event_id, ''),
		COALESCE(sf.source_run_id, ''), COALESCE(sf.source_event_id, ''),
		COALESCE(sf.selection_authority, ''), COALESCE(sf.lineage_owner_count, 0)
	FROM events e
	LEFT JOIN (
		SELECT candidate.*,
			COUNT(*) OVER (PARTITION BY candidate.fork_event_id) AS lineage_owner_count
		FROM (
			SELECT fork_event_id, source_run_id, source_event_id, selection_authority
			FROM run_fork_selected_contract_executions
			UNION ALL
			SELECT fork_event_id, source_run_id, source_event_id, selection_authority
			FROM run_fork_delivery_event_replays
			GROUP BY fork_event_id, source_run_id, source_event_id, selection_authority
		) candidate
	) sf ON sf.fork_event_id = e.event_id`

func scanTargets(record *eventrecord.Record, createdAt *any) []any {
	return []any{
		&record.Class, &record.EventID, &record.RunID, &record.EventName, &record.TaskID,
		&record.EntityID, &record.FlowInstance, &record.Scope, &record.Payload, &record.ExecutionMode,
		&record.ChainDepth, &record.ProducedBy, &record.ProducedByType, &record.SourceEventID,
		createdAt, &record.RoutingSourceKind, &record.RoutingSourceAuthority,
		&record.SourceRoute, &record.TargetRoute, &record.TargetSet, &record.OperatorReferencedEventID,
		&record.SelectedForkSourceRunID, &record.SelectedForkSourceEventID, &record.SelectedForkAuthorityStamp,
		&record.SelectedForkLineageOwners,
	}
}

func scanRows(rows *sql.Rows, loaded map[string]eventrecord.Record) error {
	defer rows.Close()
	for rows.Next() {
		var record eventrecord.Record
		var createdAt any
		if err := rows.Scan(scanTargets(&record, &createdAt)...); err != nil {
			return fmt.Errorf("scan sqlite event record batch: %w", err)
		}
		if err := assignCreatedAt(&record, createdAt); err != nil {
			return eventrecord.Corrupt(record.EventID, err)
		}
		if err := record.Validate(); err != nil {
			return fmt.Errorf("validate sqlite event record batch: %w", eventrecord.Corrupt(record.EventID, err))
		}
		if _, exists := loaded[record.EventID]; exists {
			return fmt.Errorf("event record %s hydrated more than once", record.EventID)
		}
		loaded[record.EventID] = record.Clone()
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read sqlite event record batch: %w", err)
	}
	return rows.Close()
}

func assignCreatedAt(record *eventrecord.Record, raw any) error {
	var parsed time.Time
	switch value := raw.(type) {
	case time.Time:
		parsed = value
	case string:
		var err error
		parsed, err = parseTime(value)
		if err != nil {
			return err
		}
	case []byte:
		var err error
		parsed, err = parseTime(string(value))
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("load sqlite event record: created_at is required")
	}
	if parsed.IsZero() {
		return fmt.Errorf("load sqlite event record: created_at is required")
	}
	record.CreatedAt = parsed.UTC()
	return nil
}

func parseTime(raw string) (time.Time, error) {
	raw = strings.TrimSpace(raw)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999 -0700 MST", "2006-01-02 15:04:05.999999999-07:00", "2006-01-02 15:04:05.999999999+00:00", "2006-01-02 15:04:05.999999999"} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("parse sqlite event record created_at %q", raw)
}

func normalizeIDs(ids []string) ([]string, error) {
	ordered := make([]string, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for i, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			return nil, fmt.Errorf("event record id at index %d is required", i)
		}
		if _, exists := seen[id]; exists {
			return nil, fmt.Errorf("event record id %s is duplicated", id)
		}
		seen[id] = struct{}{}
		ordered[i] = id
	}
	return ordered, nil
}

func orderRecords(ordered []string, loaded map[string]eventrecord.Record) ([]eventrecord.Record, error) {
	out := make([]eventrecord.Record, len(ordered))
	for i, id := range ordered {
		record, ok := loaded[id]
		if !ok {
			return nil, eventrecord.Missing(id)
		}
		out[i] = record.Clone()
	}
	return out, nil
}
