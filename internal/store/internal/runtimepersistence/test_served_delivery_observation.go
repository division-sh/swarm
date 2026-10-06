package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
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
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries d WHERE d.run_id=$1 AND
			(d.status IN ('pending','in_progress') OR d.continuation_handoff_at IS NULL OR NOT EXISTS
			(SELECT 1 FROM event_receipts r WHERE r.event_id=d.event_id AND r.subscriber_type='platform' AND r.subscriber_id='pipeline'))`, runID).Scan(&count)
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
	where, args := []string{}, []any{}
	add := func(column, value string) {
		predicate := column + " = ?"
		if string(dialect) == "postgres" {
			predicate = fmt.Sprintf("%s = $%d", column, len(args)+1)
			if column == "event_id" {
				predicate += "::uuid"
			}
		}
		where, args = append(where, predicate), append(args, value)
	}
	add("event_id", eventID)
	if strings.TrimSpace(subscriberType) != "" {
		add("subscriber_type", subscriberType)
	}
	add("subscriber_id", subscriberID)
	// The physical witness historically conjoins status predicates. Do not turn
	// contradictory filters into an OR, or trim the stored lookup values.
	for _, status := range statuses {
		if strings.TrimSpace(status) != "" {
			add("status", status)
		}
	}
	var count int
	err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_deliveries WHERE "+strings.Join(where, " AND "), args...).Scan(&count)
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
	sqlText := ""
	switch backend {
	case "postgres":
		switch scope {
		case "runs":
			sqlText = `SELECT status, completion_revision, COALESCE(completion_due_at::text, ''), bundle_hash FROM runs WHERE run_id = $1::uuid`
		case "entity_state":
			sqlText = `SELECT entity_id::text, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = $1::uuid ORDER BY created_at, entity_id LIMIT 5`
		case "flow_instances":
			sqlText = `SELECT DISTINCT fi.instance_path, fi.flow_template, COALESCE(fi.status, '') FROM flow_instances fi JOIN entity_state es ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path WHERE es.run_id = $1::uuid ORDER BY fi.instance_path LIMIT 5`
		case "events":
			sqlText = `SELECT event_id::text, event_name, COALESCE(entity_id::text, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 5`
		case "event_deliveries":
			sqlText = `SELECT delivery_id::text, event_id::text, subscriber_type, subscriber_id, status, claim_version, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id = $1::uuid ORDER BY created_at, event_id LIMIT 8`
		case "settled_delivery_attempts":
			sqlText = `SELECT o.delivery_id::text, o.claim_version, o.outcome, COALESCE(o.reason_code, '') FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o JOIN event_deliveries d ON d.delivery_id = o.delivery_id WHERE d.run_id = $1::uuid ORDER BY o.settled_at, o.delivery_id LIMIT 8`
		case "event_receipts":
			sqlText = `SELECT r.event_id::text, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects::text, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1::uuid ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id::text, ''), COALESCE(d.failure->>'class', ''), COALESCE(d.failure->'detail'->>'code', ''), COALESCE(d.failure->'detail'->'attributes'->>'validation_error', '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = $1::uuid ORDER BY d.created_at LIMIT 5`
		case "delivery_agents":
			sqlText = `SELECT d.subscriber_id, d.agent_name_owner, d.agent_name_source, d.agent_route_presence, d.agent_flow_scope_key, d.agent_flow_instance_id, d.agent_flow_instance_path, COALESCE(a.status, ''), COALESCE(a.lifecycle_phase, '') FROM event_deliveries d LEFT JOIN agents a ON a.agent_id = d.subscriber_id AND a.agent_name_owner = d.agent_name_owner AND a.agent_name_source = d.agent_name_source AND a.agent_route_presence = d.agent_route_presence AND a.flow_scope_key = d.agent_flow_scope_key AND a.flow_instance_id = d.agent_flow_instance_id AND a.flow_instance = d.agent_flow_instance_path WHERE d.run_id = $1::uuid ORDER BY d.created_at LIMIT 8`
		case "runtime_logs":
			sqlText = `SELECT payload::text FROM events WHERE run_id = $1::uuid AND event_name = 'platform.runtime_log' ORDER BY created_at LIMIT 8`
		}
	case "sqlite":
		switch scope {
		case "runs":
			sqlText = `SELECT status, completion_revision, COALESCE(completion_due_at, ''), bundle_hash FROM runs WHERE run_id = ?`
		case "entity_state":
			sqlText = `SELECT entity_id, COALESCE(flow_instance, ''), COALESCE(current_state, '') FROM entity_state WHERE run_id = ? ORDER BY created_at, entity_id LIMIT 5`
		case "flow_instances":
			sqlText = `SELECT DISTINCT fi.instance_path, fi.flow_template, COALESCE(fi.status, '') FROM flow_instances fi JOIN entity_state es ON es.run_id = fi.run_id AND es.flow_instance = fi.instance_path WHERE es.run_id = ? ORDER BY fi.instance_path LIMIT 5`
		case "events":
			sqlText = `SELECT event_id, event_name, COALESCE(entity_id, ''), COALESCE(flow_instance, '') FROM events WHERE run_id = ? ORDER BY created_at, event_id LIMIT 5`
		case "event_deliveries":
			sqlText = `SELECT delivery_id, event_id, subscriber_type, subscriber_id, status, claim_version, COALESCE(reason_code, '') FROM event_deliveries WHERE run_id = ? ORDER BY created_at, event_id LIMIT 8`
		case "settled_delivery_attempts":
			sqlText = `SELECT o.delivery_id, o.claim_version, o.outcome, COALESCE(o.reason_code, '') FROM (SELECT delivery_id, claim_version, outcome, reason_code, failure, side_effects, duration_ms, completed_at AS settled_at FROM event_delivery_attempts WHERE closure_kind='settled') o JOIN event_deliveries d ON d.delivery_id = o.delivery_id WHERE d.run_id = ? ORDER BY o.settled_at, o.delivery_id LIMIT 8`
		case "event_receipts":
			sqlText = `SELECT r.event_id, r.subscriber_type, r.subscriber_id, r.outcome, COALESCE(r.reason_code, ''), COALESCE(r.side_effects, '') FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = ? ORDER BY r.processed_at, r.event_id LIMIT 8`
		case "dead_letters":
			sqlText = `SELECT d.original_event, COALESCE(d.entity_id, ''), COALESCE(json_extract(d.failure, '$.class'), ''), COALESCE(json_extract(d.failure, '$.detail.code'), ''), COALESCE(json_extract(d.failure, '$.detail.attributes.validation_error'), '') FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = ? ORDER BY d.created_at LIMIT 5`
		case "delivery_agents":
			sqlText = `SELECT d.subscriber_id, d.agent_name_owner, d.agent_name_source, d.agent_route_presence, d.agent_flow_scope_key, d.agent_flow_instance_id, d.agent_flow_instance_path, COALESCE(a.status, ''), COALESCE(a.lifecycle_phase, '') FROM event_deliveries d LEFT JOIN agents a ON a.agent_id = d.subscriber_id AND a.agent_name_owner = d.agent_name_owner AND a.agent_name_source = d.agent_name_source AND a.agent_route_presence = d.agent_route_presence AND a.flow_scope_key = d.agent_flow_scope_key AND a.flow_instance_id = d.agent_flow_instance_id AND a.flow_instance = d.agent_flow_instance_path WHERE d.run_id = ? ORDER BY d.created_at LIMIT 8`
		case "runtime_logs":
			sqlText = `SELECT payload FROM events WHERE run_id = ? AND event_name = 'platform.runtime_log' ORDER BY created_at LIMIT 8`
		}
	}
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
