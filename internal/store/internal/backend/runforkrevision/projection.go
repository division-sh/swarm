package runforkrevision

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

type valueKind uint8

const (
	valueRaw valueKind = iota
	valueJSON
	valueTime
	valueBytesBase64
	valueBool
)

type projectionColumn struct {
	name string
	kind valueKind
}

type projectionSpec struct {
	query   string
	columns []projectionColumn
	build   func(map[string]any) map[string]any
}

type canonicalFact struct {
	key  string
	fact []byte
}

func loadCanonicalProjection(ctx context.Context, q queryer, runID string, family Family) ([]canonicalFact, error) {
	spec, ok := canonicalProjectionSpec(family)
	if !ok {
		return nil, fmt.Errorf("run fork revision owner has no canonical projection for family %q", family)
	}
	rows, err := q.QueryContext(ctx, spec.query, runID)
	if err != nil {
		return nil, fmt.Errorf("query %s projection: %w", family, err)
	}
	defer rows.Close()
	facts := make([]canonicalFact, 0)
	for rows.Next() {
		raw := make([]any, len(spec.columns)+1)
		dest := make([]any, len(raw))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("scan %s projection: %w", family, err)
		}
		key := normalizedText(raw[0])
		if strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("%s projection produced an empty fact key", family)
		}
		values := make(map[string]any, len(spec.columns))
		for i, column := range spec.columns {
			value, err := normalizeProjectionValue(raw[i+1], column.kind)
			if err != nil {
				return nil, fmt.Errorf("normalize %s.%s: %w", family, column.name, err)
			}
			values[column.name] = value
		}
		if spec.build != nil {
			values = spec.build(values)
		}
		encoded, err := json.Marshal(values)
		if err != nil {
			return nil, fmt.Errorf("encode %s projection: %w", family, err)
		}
		facts = append(facts, canonicalFact{key: key, fact: encoded})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read %s projection: %w", family, err)
	}
	sort.Slice(facts, func(i, j int) bool { return facts[i].key < facts[j].key })
	return facts, nil
}

func normalizeProjectionValue(raw any, kind valueKind) (any, error) {
	if raw == nil {
		return nil, nil
	}
	switch kind {
	case valueJSON:
		text := normalizedText(raw)
		if strings.TrimSpace(text) == "" {
			return nil, nil
		}
		var value any
		decoder := json.NewDecoder(strings.NewReader(text))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		return value, nil
	case valueTime:
		if value, ok := raw.(time.Time); ok {
			return value.UTC().Format(time.RFC3339Nano), nil
		}
		text := strings.TrimSpace(normalizedText(raw))
		if text == "" {
			return nil, nil
		}
		for _, layout := range []string{
			time.RFC3339Nano,
			"2006-01-02 15:04:05.999999999 -0700 MST",
			"2006-01-02 15:04:05.999999 -0700 MST",
			"2006-01-02 15:04:05 -0700 MST",
			"2006-01-02 15:04:05.999999999-07:00",
			"2006-01-02 15:04:05.999999999-07",
			"2006-01-02 15:04:05.999999999Z07:00",
			"2006-01-02 15:04:05.999999999",
			"2006-01-02 15:04:05-07:00",
			"2006-01-02 15:04:05Z07:00",
			"2006-01-02 15:04:05",
		} {
			if value, err := time.Parse(layout, text); err == nil {
				return value.UTC().Format(time.RFC3339Nano), nil
			}
		}
		return nil, fmt.Errorf("invalid persisted timestamp %q", text)
	case valueBytesBase64:
		switch value := raw.(type) {
		case []byte:
			return base64.StdEncoding.EncodeToString(value), nil
		case string:
			return base64.StdEncoding.EncodeToString([]byte(value)), nil
		default:
			return nil, fmt.Errorf("unsupported byte value %T", raw)
		}
	case valueBool:
		switch value := raw.(type) {
		case bool:
			return value, nil
		case int64:
			if value == 0 || value == 1 {
				return value == 1, nil
			}
		case []byte:
			return strconv.ParseBool(string(value))
		case string:
			return strconv.ParseBool(value)
		}
		return nil, fmt.Errorf("unsupported boolean value %T", raw)
	default:
		switch value := raw.(type) {
		case []byte:
			return string(value), nil
		case int64, float64, bool, string:
			return value, nil
		case time.Time:
			return value.UTC().Format(time.RFC3339Nano), nil
		default:
			return fmt.Sprint(value), nil
		}
	}
}

func normalizedText(raw any) string {
	switch value := raw.(type) {
	case nil:
		return ""
	case string:
		return value
	case []byte:
		return string(value)
	case int64:
		return strconv.FormatInt(value, 10)
	default:
		return fmt.Sprint(value)
	}
}

func columns(names ...string) []projectionColumn {
	out := make([]projectionColumn, len(names))
	for i, name := range names {
		out[i] = projectionColumn{name: name}
	}
	return out
}

func typedColumns(kinds map[string]valueKind, names ...string) []projectionColumn {
	out := columns(names...)
	for i := range out {
		out[i].kind = kinds[out[i].name]
	}
	return out
}

func canonicalProjectionSpec(family Family) (projectionSpec, bool) {
	var spec projectionSpec
	switch family {
	case FamilyEvents:
		spec = projectionSpec{
			query: `SELECT CAST(e.event_id AS TEXT), CAST(e.event_id AS TEXT), e.event_name,
				CAST(e.entity_id AS TEXT), e.flow_instance, e.routing_source_kind, e.source_route,
				COALESCE(e.routing_source_authority, ''), e.target_route, e.target_set, e.route_settlement, e.scope,
				e.payload_bytes, e.chain_depth, e.produced_by, e.produced_by_type, e.handler_node,
				e.idempotency_key, CAST(e.source_event_id AS TEXT), e.created_at
			FROM events e WHERE e.run_id = $1`,
			columns: typedColumns(map[string]valueKind{
				"source_route": valueJSON, "target_route": valueJSON, "target_set": valueJSON, "route_settlement": valueJSON,
				"payload_base64": valueBytesBase64, "created_at": valueTime,
			}, "event_id", "event_name", "entity_id", "flow_instance", "routing_source_kind", "source_route", "routing_source_authority", "target_route", "target_set", "route_settlement", "scope", "payload_base64", "chain_depth", "produced_by", "produced_by_type", "handler_node", "idempotency_key", "source_event_id", "created_at"),
			build: func(values map[string]any) map[string]any {
				values["routing_source"] = map[string]any{
					"kind": values["routing_source_kind"], "route": values["source_route"], "authority": values["routing_source_authority"],
				}
				delete(values, "routing_source_kind")
				delete(values, "source_route")
				delete(values, "routing_source_authority")
				return values
			},
		}
	case FamilyEntityMutations:
		spec = projectionSpec{
			query:   `SELECT CAST(m.mutation_id AS TEXT), CAST(m.mutation_id AS TEXT), CAST(m.entity_id AS TEXT), m.domain, m.path, m.new_value, CAST(m.caused_by_event AS TEXT), m.created_at FROM entity_mutations m WHERE m.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"new_value": valueJSON, "created_at": valueTime}, "mutation_id", "entity_id", "domain", "path", "new_value", "caused_by_event", "created_at"),
		}
	case FamilyEntityMetadata:
		spec = projectionSpec{
			query:   `SELECT CAST(e.entity_id AS TEXT), CAST(e.entity_id AS TEXT), e.flow_instance, e.entity_type, e.slug, e.name, e.created_at FROM entity_state e WHERE e.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"created_at": valueTime}, "entity_id", "flow_instance", "entity_type", "slug", "name", "created_at"),
		}
	case FamilyEventDeliveries:
		spec = projectionSpec{
			query: `SELECT CAST(d.delivery_id AS TEXT), CAST(d.delivery_id AS TEXT), CAST(d.event_id AS TEXT), CAST(d.run_id AS TEXT), d.route_identity,
				d.subscriber_type, d.subscriber_id, d.agent_name_owner, d.agent_name_source, d.agent_route_presence,
				d.agent_flow_scope_key, CAST(d.agent_flow_instance_id AS TEXT), d.agent_flow_instance_path,
				d.delivery_target_route, d.delivery_context, d.delivery_payload_projection, d.status,
				d.retry_count, d.max_retries, d.next_eligible_at, d.claim_version, current_attempt.lease_expires_at,
				d.reason_code, d.failure, CAST(current_attempt.active_session_id AS TEXT), d.started_at, d.settled_at, d.created_at, d.updated_at
			FROM event_deliveries d
			LEFT JOIN event_delivery_attempts current_attempt ON current_attempt.delivery_id = d.delivery_id AND current_attempt.claim_version = d.current_attempt_version AND current_attempt.open_marker = TRUE
			WHERE d.run_id = $1`,
			columns: typedColumns(map[string]valueKind{
				"delivery_target_ownership": valueJSON, "delivery_context": valueJSON,
				"delivery_payload_projection": valueJSON, "failure": valueJSON,
				"next_eligible_at": valueTime, "claim_expires_at": valueTime, "started_at": valueTime, "settled_at": valueTime, "created_at": valueTime, "updated_at": valueTime,
			}, "delivery_id", "event_id", "run_id", "route_identity", "subscriber_type", "subscriber_id", "agent_name_owner", "agent_name_source", "agent_route_presence", "agent_flow_scope_key", "agent_flow_instance_id", "agent_flow_instance_path", "delivery_target_ownership", "delivery_context", "delivery_payload_projection", "status", "retry_count", "max_retries", "next_eligible_at", "claim_version", "claim_expires_at", "reason_code", "failure", "active_session_id", "started_at", "settled_at", "created_at", "updated_at"),
			build: func(values map[string]any) map[string]any {
				identity := map[string]any{}
				if normalizedText(values["subscriber_type"]) == "agent" {
					identity = map[string]any{
						"name":  map[string]any{"agent_id": values["subscriber_id"], "owner": values["agent_name_owner"], "source": values["agent_name_source"]},
						"route": map[string]any{"presence": values["agent_route_presence"], "scope_key": values["agent_flow_scope_key"], "instance_id": values["agent_flow_instance_id"], "instance_path": values["agent_flow_instance_path"]},
					}
				}
				values["agent_identity"] = identity
				for _, key := range []string{"agent_name_owner", "agent_name_source", "agent_route_presence", "agent_flow_scope_key", "agent_flow_instance_id", "agent_flow_instance_path"} {
					delete(values, key)
				}
				return values
			},
		}
	case FamilyCommittedReplayScopes:
		spec = projectionSpec{
			query:   `SELECT CAST(s.event_id AS TEXT), CAST(s.event_id AS TEXT), CAST(s.run_id AS TEXT), s.scope, s.created_at, s.updated_at FROM committed_replay_scopes s WHERE s.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"created_at": valueTime, "updated_at": valueTime}, "event_id", "run_id", "scope", "created_at", "updated_at"),
		}
	case FamilyEventReceipts:
		spec = projectionSpec{
			query:   `SELECT CAST(r.receipt_id AS TEXT), CAST(r.receipt_id AS TEXT), CAST(r.event_id AS TEXT), r.subscriber_type, r.subscriber_id, r.outcome, r.reason_code, r.processed_at FROM event_receipts r JOIN events e ON e.event_id = r.event_id WHERE e.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"processed_at": valueTime}, "receipt_id", "event_id", "subscriber_type", "subscriber_id", "outcome", "reason_code", "processed_at"),
		}
	case FamilyDeadLetters:
		spec = projectionSpec{
			query:   `SELECT CAST(d.dead_letter_id AS TEXT), CAST(d.dead_letter_id AS TEXT), CAST(d.original_event_id AS TEXT), COALESCE(CAST(d.delivery_id AS TEXT), ''), d.handler_node, d.created_at FROM dead_letters d JOIN events e ON e.event_id = d.original_event_id WHERE e.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"created_at": valueTime}, "dead_letter_id", "original_event_id", "delivery_id", "handler_node", "created_at"),
		}
	case FamilyFanOutObligations:
		names := []string{
			"fact_kind", "triggering_delivery_id", "package_key", "element_id", "bundle_hash", "semantic_digest",
			"source_kind", "source_event_id", "source_run_id", "source_entity_id", "source_field", "source_mutation_id",
			"source_resource_package_key", "source_resource_event_name", "source_resource_version_id",
			"cardinality", "cursor", "status", "capsule", "blocked_reason",
			"created_at", "ordinal", "outcome_kind", "event_id", "outcome_source_event_id", "inherited_disposition", "failure",
			"barrier_target_package_key", "barrier_target_flow_id", "barrier_target_node_id", "barrier_handler_event", "barrier_join_id",
			"barrier_route_scope_key", "barrier_route_instance_id", "barrier_route_instance_path", "barrier_entity_id",
			"barrier_routing_source", "barrier_execution_mode", "barrier_timer_handle", "barrier_status", "barrier_summary", "barrier_schedule_key", "barrier_updated_at",
		}
		spec = projectionSpec{
			query: `
				SELECT 'intent|' || CAST(i.triggering_delivery_id AS TEXT) || '|' || i.package_key || '|' || i.element_id,
					'intent', CAST(i.triggering_delivery_id AS TEXT), i.package_key, i.element_id, i.bundle_hash, i.semantic_digest,
					i.source_kind, CAST(i.source_event_id AS TEXT), CAST(i.source_run_id AS TEXT), CAST(i.source_entity_id AS TEXT), i.source_field, CAST(i.source_mutation_id AS TEXT),
					i.source_resource_package_key, i.source_resource_event_name, i.source_resource_version_id,
					i.cardinality, i.cursor, i.status, CAST(i.capsule AS TEXT), i.blocked_reason,
					CAST(i.created_at AS TEXT), NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM fan_out_intents i WHERE i.run_id=$1
				UNION ALL
				SELECT 'outcome|' || CAST(o.triggering_delivery_id AS TEXT) || '|' || o.package_key || '|' || o.element_id || '|' || CAST(o.ordinal AS TEXT),
					'outcome', CAST(o.triggering_delivery_id AS TEXT), o.package_key, o.element_id, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL,
					CAST(o.created_at AS TEXT), o.ordinal, o.outcome_kind, CAST(o.event_id AS TEXT), CAST(o.source_event_id AS TEXT), o.inherited_disposition, CAST(o.failure AS TEXT),
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
				FROM fan_out_outcomes o WHERE o.run_id=$1
				UNION ALL
				SELECT 'barrier|' || CAST(b.triggering_delivery_id AS TEXT) || '|' || b.package_key || '|' || b.element_id,
					'barrier', CAST(b.triggering_delivery_id AS TEXT), b.package_key, b.element_id, b.bundle_hash, b.semantic_digest,
					NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
					NULL, NULL, NULL, NULL, NULL,
					CAST(b.created_at AS TEXT), NULL, NULL, NULL, NULL, NULL, NULL,
					b.target_package_key, b.target_flow_id, b.target_node_id, b.handler_event, b.join_id,
					b.route_scope_key, CAST(b.route_instance_id AS TEXT), b.route_instance_path, CAST(b.entity_id AS TEXT),
					CAST(b.routing_source AS TEXT), b.execution_mode, CAST(b.timer_handle AS TEXT), b.status, CAST(b.summary AS TEXT), b.schedule_key, CAST(b.updated_at AS TEXT)
				FROM fan_out_obligation_barriers b WHERE b.run_id=$1`,
			columns: typedColumns(map[string]valueKind{
				"capsule": valueJSON, "failure": valueJSON, "created_at": valueTime,
				"barrier_routing_source": valueJSON, "barrier_timer_handle": valueJSON, "barrier_summary": valueJSON, "barrier_updated_at": valueTime,
			}, names...),
		}
	case FamilyTimers:
		names := []string{"timer_id", "timer_name", "schedule_scope", "schedule_key", "immutable_hash", "run_id", "source_timer_id", "forked_from_run_id", "forked_from_event_id", "reconstruction_owner", "entity_id", "flow_scope_key", "flow_instance_id", "flow_instance", "fire_event", "fire_payload", "routing_source", "execution_mode", "fire_at", "initial_fire_at", "recurring", "recurrence_interval", "owner_node", "owner_agent", "owner_kind", "agent_name_owner", "agent_name_source", "agent_route_presence", "agent_flow_scope_key", "agent_flow_instance_id", "reply_context_id", "task_id", "due_basis_kind", "due_basis_absolute", "due_basis_duration", "due_basis_cron", "occurrence_event_id", "occurrence_admitted_at", "accepted_at", "cancel_cause", "cancelled_at", "failure_code", "failure_message", "failed_at", "task_type", "status", "fired_at", "created_at"}
		spec = projectionSpec{
			query: `SELECT CAST(t.timer_id AS TEXT), CAST(t.timer_id AS TEXT), t.timer_name, t.schedule_scope, t.schedule_key, t.immutable_hash, CAST(t.run_id AS TEXT), CAST(t.source_timer_id AS TEXT), CAST(t.forked_from_run_id AS TEXT), CAST(t.forked_from_event_id AS TEXT), t.reconstruction_owner, CAST(t.entity_id AS TEXT), t.flow_scope_key, CAST(t.flow_instance_id AS TEXT), t.flow_instance, t.fire_event, t.fire_payload, t.routing_source, t.execution_mode, t.fire_at, t.initial_fire_at, t.recurring, t.recurrence_interval, t.owner_node, t.owner_agent, t.owner_kind, t.agent_name_owner, t.agent_name_source, t.agent_route_presence, t.agent_flow_scope_key, CAST(t.agent_flow_instance_id AS TEXT), t.reply_context_id, t.task_id, t.due_basis_kind, t.due_basis_absolute, t.due_basis_duration, t.due_basis_cron, CAST(t.occurrence_event_id AS TEXT), t.occurrence_admitted_at, t.accepted_at, t.cancel_cause, t.cancelled_at, t.failure_code, t.failure_message, t.failed_at, t.task_type, t.status, t.fired_at, t.created_at FROM timers t WHERE t.run_id = $1`,
			columns: typedColumns(map[string]valueKind{
				"fire_payload": valueJSON, "routing_source": valueJSON,
				"recurring": valueBool,
				"fire_at":   valueTime, "initial_fire_at": valueTime, "due_basis_absolute": valueTime,
				"occurrence_admitted_at": valueTime, "accepted_at": valueTime, "cancelled_at": valueTime, "failed_at": valueTime, "fired_at": valueTime, "created_at": valueTime,
			}, names...),
		}
	case FamilyAgentSessions:
		spec = projectionSpec{
			query:   `SELECT CAST(s.session_id AS TEXT), CAST(s.session_id AS TEXT), s.status, s.created_at, s.terminated_at FROM agent_sessions s WHERE s.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"created_at": valueTime, "terminated_at": valueTime}, "session_id", "status", "created_at", "terminated_at"),
		}
	case FamilyAgentTurns:
		spec = projectionSpec{
			query:   `SELECT CAST(t.turn_id AS TEXT), CAST(t.turn_id AS TEXT), CAST(t.session_id AS TEXT), t.created_at FROM agent_turns t WHERE t.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"created_at": valueTime}, "turn_id", "session_id", "created_at"),
		}
	case FamilyAgentConversationAudits:
		spec = projectionSpec{
			query:   `SELECT CAST(a.session_id AS TEXT), CAST(a.session_id AS TEXT), a.status, a.created_at, a.updated_at FROM agent_conversation_audits a WHERE a.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"created_at": valueTime, "updated_at": valueTime}, "session_id", "status", "created_at", "updated_at"),
		}
	case FamilyReplyContexts:
		spec = projectionSpec{
			query:   `SELECT r.reply_context_id, r.reply_context_id, CAST(r.request_event_id AS TEXT), r.state, r.created_at, r.updated_at, r.terminal_at FROM reply_contexts r WHERE r.run_id = $1`,
			columns: typedColumns(map[string]valueKind{"created_at": valueTime, "updated_at": valueTime, "terminal_at": valueTime}, "reply_context_id", "request_event_id", "state", "created_at", "updated_at", "terminal_at"),
		}
	default:
		return projectionSpec{}, false
	}
	return spec, true
}
