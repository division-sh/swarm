package eventpersistence

import "context"

// ConstraintEventRow is a named physical-row witness, including deliberately
// invalid or NULL values. Only the real schema decides whether it is valid.
type ConstraintEventRow struct {
	Class                    any
	EventID                  any
	RunID                    any
	EventName                any
	TaskID                   any
	EntityID                 any
	FlowInstance             any
	Scope                    any
	Payload                  any
	PayloadSchemaBundleHash  any
	PayloadSchemaFlowID      any
	PayloadSchemaEventKey    any
	PayloadSchemaDigest      any
	PayloadSchemaClass       any
	ExecutionMode            any
	ChainDepth               any
	ProducedBy               any
	ProducedByType           any
	SourceEventID            any
	CreatedAt                any
	RoutingSourceKind        any
	RoutingSourceAuthority   any
	SourceRoute              any
	TargetRoute              any
	TargetSet                any
	OperatorReferenceEventID any
}

func (s *EventPostgresOwner) InsertConstraintEventRowForTest(ctx context.Context, row ConstraintEventRow) error {
	_, err := s.backend.ExecContext(ctx, `
		INSERT INTO events (
			event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload, payload_bytes,
			payload_schema_bundle_hash, payload_schema_flow_id, payload_schema_event_key,
			payload_schema_digest, payload_schema_class,
			execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
			routing_source_kind, routing_source_authority, source_route, target_route, target_set,
				operator_reference_event_id, route_settlement, run_owner_key
			) VALUES (
				$1, $2::uuid, $3::uuid, $4, $5, $6::uuid, $7, $8, $9::jsonb, $10::bytea,
				$11, $12, $13, $14, $15, $16, $17, $18, $19,
				$20::uuid, $21, $22, $23, $24::jsonb, $25::jsonb, $26::jsonb, $27::uuid, $28::jsonb,
				COALESCE('run:' || CAST($3::uuid AS TEXT), '')
			)
		`, constraintEventArguments(row)...)
	return err
}

func (s *EventSQLiteOwner) InsertConstraintEventRowForTest(ctx context.Context, row ConstraintEventRow) error {
	_, err := s.backend.ExecContext(ctx, `
		INSERT INTO events (
			event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload, payload_bytes,
			payload_schema_bundle_hash, payload_schema_flow_id, payload_schema_event_key,
			payload_schema_digest, payload_schema_class,
			execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
			routing_source_kind, routing_source_authority, source_route, target_route, target_set,
				operator_reference_event_id, route_settlement, run_owner_key
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE('run:' || CAST(?3 AS TEXT), ''))
	`, constraintEventArguments(row)...)
	return err
}

func constraintEventArguments(row ConstraintEventRow) []any {
	payloadBytes := row.Payload
	if value, ok := row.Payload.(string); ok {
		payloadBytes = []byte(value)
	}
	args := []any{
		row.Class, row.EventID, row.RunID, row.EventName, row.TaskID, row.EntityID, row.FlowInstance, row.Scope, row.Payload, payloadBytes,
		row.PayloadSchemaBundleHash, row.PayloadSchemaFlowID, row.PayloadSchemaEventKey,
		row.PayloadSchemaDigest, row.PayloadSchemaClass,
		row.ExecutionMode, row.ChainDepth, row.ProducedBy, row.ProducedByType, row.SourceEventID, row.CreatedAt,
		row.RoutingSourceKind, row.RoutingSourceAuthority, row.SourceRoute, row.TargetRoute, row.TargetSet, row.OperatorReferenceEventID,
		constraintEventRouteSettlement(row),
	}
	return args
}

func constraintEventRouteSettlement(row ConstraintEventRow) string {
	switch row.EventName {
	case "platform.runtime_log":
		return `{"write_class":"runtime_log_direct","arm":"no_delivery","reason":"no_subscriber_by_design"}`
	case "platform.inbound_recorded":
		return `{"write_class":"inbound_evidence_direct","arm":"no_delivery","reason":"no_subscriber_by_design"}`
	case "platform.agent_directive":
		return `{"write_class":"directive_direct","arm":"no_delivery","reason":"no_subscriber_by_design"}`
	}
	writeClass := "normal_publication"
	if row.Class == "selected_fork_replay" {
		writeClass = "selected_fork_publication"
	}
	return `{"write_class":"` + writeClass + `","arm":"no_delivery","reason":"declared_consumer_no_plan","evaluation":{"plans":[]}}`
}
