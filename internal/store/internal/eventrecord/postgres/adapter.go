package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
	err := q.QueryRowContext(ctx, selectRecord+` WHERE e.event_id = $1::uuid`, strings.TrimSpace(eventID)).Scan(scanTargets(&record)...)
	if err == sql.ErrNoRows {
		return eventrecord.Record{}, false, nil
	}
	if err != nil {
		return eventrecord.Record{}, false, fmt.Errorf("load event record: %w", err)
	}
	record.CreatedAt = record.CreatedAt.UTC()
	if err := record.Validate(); err != nil {
		return eventrecord.Record{}, false, fmt.Errorf("load event record: %w", eventrecord.Corrupt(record.EventID, err))
	}
	return record.Clone(), true, nil
}

func LoadMany(ctx context.Context, q Queryer, eventIDs []string) ([]eventrecord.Record, error) {
	ordered, err := normalizeIDs(eventIDs)
	if err != nil || len(ordered) == 0 {
		return emptyRecordsOnNil(ordered, err)
	}
	loaded := make(map[string]eventrecord.Record, len(ordered))
	for start := 0; start < len(ordered); start += hydrationBatchSize {
		end := min(start+hydrationBatchSize, len(ordered))
		args := make([]any, end-start)
		placeholders := make([]string, end-start)
		for i, id := range ordered[start:end] {
			args[i] = id
			placeholders[i] = fmt.Sprintf("$%d::uuid", i+1)
		}
		rows, err := q.QueryContext(ctx, selectRecord+` WHERE e.event_id IN (`+strings.Join(placeholders, ", ")+`)`, args...)
		if err != nil {
			return nil, fmt.Errorf("hydrate event record batch: %w", err)
		}
		if err := scanRows(rows, loaded); err != nil {
			return nil, err
		}
	}
	return orderRecords(ordered, loaded)
}

func Insert(ctx context.Context, exec Execer, record eventrecord.Record) (bool, error) {
	if err := record.Validate(); err != nil {
		return false, fmt.Errorf("append event record: %w", err)
	}
	result, err := exec.ExecContext(ctx, `
		INSERT INTO events (
			event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload,
			execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
			routing_source_kind, routing_source_authority, source_route, target_route, target_set,
			operator_reference_event_id
		) VALUES (
			$1, $2::uuid, NULLIF($3,'')::uuid, $4, NULLIF($5,''), NULLIF($6,'')::uuid, NULLIF($7,''), $8, $9::jsonb,
			$10, $11, $12, $13, NULLIF($14,'')::uuid, $15,
			$16, NULLIF($17,''), $18::jsonb, $19::jsonb, $20::jsonb,
			NULLIF($21,'')::uuid
		) ON CONFLICT (event_id) DO NOTHING
	`, record.Class, record.EventID, record.RunID, record.EventName, record.TaskID,
		record.EntityID, record.FlowInstance, record.Scope, string(record.Payload), record.ExecutionMode,
		record.ChainDepth, record.ProducedBy, record.ProducedByType, record.SourceEventID, record.CreatedAt,
		record.RoutingSourceKind, record.RoutingSourceAuthority, string(record.SourceRoute),
		string(record.TargetRoute), string(record.TargetSet), record.OperatorReferencedEventID)
	if err != nil {
		return false, fmt.Errorf("append event record: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("append event record: read affected rows: %w", err)
	}
	return rows == 1, nil
}

// DeleteSelectedForkRunEvents is the event-record portion of the closed
// selected-fork discard operation. Cross-domain cleanup ordering remains owned
// by the caller's named transaction.
func DeleteSelectedForkRunEvents(ctx context.Context, exec Execer, forkRunID string) error {
	forkRunID = strings.TrimSpace(forkRunID)
	if forkRunID == "" {
		return fmt.Errorf("delete selected-fork event records: fork run id is required")
	}
	if _, err := exec.ExecContext(ctx, `DELETE FROM events WHERE run_id = $1::uuid`, forkRunID); err != nil {
		return fmt.Errorf("delete selected-fork event records: %w", err)
	}
	return nil
}

const selectRecord = `
	SELECT
		e.event_class, e.event_id::text, COALESCE(e.run_id::text, ''), e.event_name,
		COALESCE(e.task_id, ''), COALESCE(e.entity_id::text, ''), COALESCE(e.flow_instance, ''),
		e.scope, e.payload, e.execution_mode, e.chain_depth, e.produced_by, e.produced_by_type,
		COALESCE(e.source_event_id::text, ''), e.created_at, e.routing_source_kind,
		COALESCE(e.routing_source_authority, ''), e.source_route, e.target_route, e.target_set,
		COALESCE(e.operator_reference_event_id::text, ''),
		COALESCE(sf.source_run_id::text, ''), COALESCE(sf.source_event_id::text, ''),
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

func scanTargets(record *eventrecord.Record) []any {
	return []any{
		&record.Class, &record.EventID, &record.RunID, &record.EventName, &record.TaskID,
		&record.EntityID, &record.FlowInstance, &record.Scope, &record.Payload, &record.ExecutionMode,
		&record.ChainDepth, &record.ProducedBy, &record.ProducedByType, &record.SourceEventID,
		&record.CreatedAt, &record.RoutingSourceKind, &record.RoutingSourceAuthority,
		&record.SourceRoute, &record.TargetRoute, &record.TargetSet, &record.OperatorReferencedEventID,
		&record.SelectedForkSourceRunID, &record.SelectedForkSourceEventID, &record.SelectedForkAuthorityStamp,
		&record.SelectedForkLineageOwners,
	}
}

func scanRows(rows *sql.Rows, loaded map[string]eventrecord.Record) error {
	defer rows.Close()
	for rows.Next() {
		var record eventrecord.Record
		if err := rows.Scan(scanTargets(&record)...); err != nil {
			return fmt.Errorf("scan event record batch: %w", err)
		}
		record.CreatedAt = record.CreatedAt.UTC()
		if err := record.Validate(); err != nil {
			return fmt.Errorf("validate event record batch: %w", eventrecord.Corrupt(record.EventID, err))
		}
		if _, exists := loaded[record.EventID]; exists {
			return fmt.Errorf("event record %s hydrated more than once", record.EventID)
		}
		loaded[record.EventID] = record.Clone()
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read event record batch: %w", err)
	}
	return rows.Close()
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

func emptyRecordsOnNil(ordered []string, err error) ([]eventrecord.Record, error) {
	if err != nil {
		return nil, err
	}
	return []eventrecord.Record{}, nil
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
