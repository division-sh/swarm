package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
)

// Transferred from B70ab4d333 /888cb4958 under6010346696. The three OR arms
// are one exact handoff predicate, not a delivery-active substitute.
func ReadServedIncompletePipelineHandoffCountForTest(ctx context.Context, selected any, runID string) (int, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return 0, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.ReadIncompletePipelineHandoffCount(ctx, tx, runID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadServedDeliveryStatusCountForTest(ctx context.Context, selected any, eventID, subscriberType, subscriberID string, statuses ...string) (int, error) {
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return 0, err
	}
	var count int
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = delivery.ReadServedDeliveryStatusCount(ctx, tx, string(dialect) == "postgres", eventID, subscriberType, subscriberID, statuses...)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadServedRunDebugSummaryForTest(ctx context.Context, selected any, runID string) (string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return "", err
	}
	dialect, err := eventFixtureDialectForTest(selected)
	if err != nil {
		return "", err
	}
	sections := make([]string, 0, 10)
	for _, scope := range []string{"runs", "entity_state", "flow_instances", "events", "event_deliveries", "settled_delivery_attempts", "event_receipts", "dead_letters", "delivery_agents", "runtime_logs"} {
		section := readServedRunDebugSection(ctx, selected, string(dialect), scope, runID)
		if err := ctx.Err(); err != nil {
			return "", err
		}
		sections = append(sections, section)
	}
	return strings.Join(sections, "\n"), nil
}

func readServedRunDebugSection(ctx context.Context, selected any, backend, scope, runID string) string {
	if scope == "runs" || scope == "event_deliveries" || scope == "settled_delivery_attempts" || scope == "delivery_agents" {
		out, stage := []string{}, ""
		err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			if scope == "runs" {
				switch owner := selected.(type) {
				case *PostgresStore:
					out, stage, err = owner.runLifecyclePostgresOwner.ReadRunDebugStorageTx(ctx, tx, runID)
				case *SQLiteRuntimeStore:
					out, stage, err = owner.runLifecycleSQLiteOwner.ReadRunDebugStorageTx(ctx, tx, runID)
				}
			} else {
				out, stage, err = delivery.ReadRunDeliveryDebugSection(ctx, tx, backend == "postgres", scope, runID)
			}
			return err
		})
		if err != nil {
			return fmt.Sprintf("%s%s: %v", scope, stage, err)
		}
		if len(out) == 0 {
			return scope + ": []"
		}
		return scope + ": " + strings.Join(out, "; ")
	}
	sqlText := servedRunDebugSQL(backend, scope)
	if sqlText == "" {
		return scope + ": unsupported debug query"
	}
	out := []string{}
	stage := ""
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, sqlText, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		columns, err := rows.Columns()
		if err != nil {
			stage = " columns"
			return err
		}
		for rows.Next() {
			values := make([]sql.NullString, len(columns))
			scan := make([]any, len(values))
			for i := range values {
				scan[i] = &values[i]
			}
			if err := rows.Scan(scan...); err != nil {
				stage = " scan"
				return err
			}
			cols := make([]string, len(values))
			for i, value := range values {
				if value.Valid {
					cols[i] = value.String
				}
			}
			out = append(out, fmt.Sprintf("%v", cols))
		}
		if err := rows.Err(); err != nil {
			stage = " rows"
			return err
		}
		return nil
	})
	if err != nil {
		return fmt.Sprintf("%s%s: %v", scope, stage, err)
	}
	if len(out) == 0 {
		return scope + ": []"
	}
	return scope + ": " + strings.Join(out, "; ")
}

func servedRunDebugSQL(backend, scope string) string {
	sqlText := ""
	switch backend {
	case "postgres":
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id::text, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid ORDER BY created_at, entity_id LIMIT 5`
		case "flow_instances":
			sqlText = `SELECT DISTINCT fi.instance_path, fi.flow_template, COALESCE(fi.status, '') FROM flow_instances fi JOIN entity_state es ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path WHERE es.run_id = $1::uuid ORDER BY fi.instance_path LIMIT 5`
		case "events":
			sqlText = `SELECT event_id::text, event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 5`
		case "event_receipts":
			sqlText = `SELECT r.event_id::text, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects::text, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id::text, ''), COALESCE(d.failure->>'class', ''), COALESCE(d.failure->'detail'->>'code', ''), COALESCE(d.failure->'detail'->'attributes'->>'validation_error', '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = $1::uuid ORDER BY d.created_at LIMIT 5`
		case "runtime_logs":
			sqlText = `SELECT payload::text FROM events WHERE run_id = $1::uuid AND event_name = 'platform.runtime_log' ORDER BY created_at LIMIT 8`
		}
	case "sqlite":
		switch scope {
		case "entity_state":
			sqlText = `SELECT entity_id, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = ? ORDER BY created_at, entity_id LIMIT 5`
		case "flow_instances":
			sqlText = `SELECT DISTINCT fi.instance_path, fi.flow_template, COALESCE(fi.status, '') FROM flow_instances fi JOIN entity_state es ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path WHERE es.run_id = ? ORDER BY fi.instance_path LIMIT 5`
		case "events":
			sqlText = `SELECT event_id, event_name, COALESCE(entity_id, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = ? ORDER BY created_at, event_id LIMIT 5`
		case "event_receipts":
			sqlText = `SELECT r.event_id, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id, ''), COALESCE(json_extract(d.failure, '$.class'), ''), COALESCE(json_extract(d.failure, '$.detail.code'), ''), COALESCE(json_extract(d.failure, '$.detail.attributes.validation_error'), '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = ? ORDER BY d.created_at LIMIT 5`
		case "runtime_logs":
			sqlText = `SELECT payload FROM events WHERE run_id = ? AND event_name = 'platform.runtime_log' ORDER BY created_at LIMIT 8`
		}
	}
	return sqlText
}
