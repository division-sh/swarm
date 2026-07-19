package store

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	eventtestsql "github.com/division-sh/swarm/internal/store/testsql"
	"github.com/google/uuid"
)

func TestFreshEventDDLRejectsMalformedStructuralFactsParity(t *testing.T) {
	type ddlCase struct {
		name     string
		category string
		baseline func() unsafeEventRow
		mutate   func(*unsafeEventRow)
	}
	cases := []ddlCase{
		{"missing_class", "not_null", validUnsafeEventRow, func(row *unsafeEventRow) { row.class = nil }},
		{"invalid_class", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.class = "projection" }},
		{"noncanonical_event_name", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.eventName = " schema.contract " }},
		{"noncanonical_task_id", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.taskID = " task " }},
		{"missing_producer_id", "not_null", validUnsafeEventRow, func(row *unsafeEventRow) { row.producedBy = nil }},
		{"blank_producer_id", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.producedBy = "  " }},
		{"noncanonical_producer_id", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.producedBy = " schema-proof " }},
		{"missing_producer_type", "not_null", validUnsafeEventRow, func(row *unsafeEventRow) { row.producedByType = nil }},
		{"invalid_producer_type", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.producedByType = "unknown" }},
		{"child_without_parent", "check", validUnsafeChildEventRow, func(row *unsafeEventRow) { row.sourceEventID = nil }},
		{"root_without_run", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.runID = nil }},
		{"operator_without_run", "check", validUnsafeOperatorEventRow, func(row *unsafeEventRow) { row.runID = nil }},
		{"selected_fork_without_run", "check", validUnsafeSelectedForkEventRow, func(row *unsafeEventRow) { row.runID = nil }},
		{"root_with_parent", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.sourceEventID = uuid.NewString() }},
		{"non_operator_with_operator_reference", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.operatorReferenceEventID = uuid.NewString() }},
		{"root_with_platform_producer", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.producedByType = "platform" }},
		{"runtime_with_external_producer", "check", validUnsafeRuntimeControlEventRow, func(row *unsafeEventRow) { row.producedByType = "external" }},
		{"reserved_label_under_root", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.eventName = "platform.runtime_log" }},
		{"diagnostic_direct_unknown_label", "check", validUnsafeRuntimeLogEventRow, func(row *unsafeEventRow) { row.eventName = "schema.contract" }},
		{"runtime_log_wrong_producer_id", "check", validUnsafeRuntimeLogEventRow, func(row *unsafeEventRow) { row.producedBy = "not-runtime" }},
		{"runtime_log_entity_scope", "check", validUnsafeRuntimeLogEventRow, func(row *unsafeEventRow) { row.scope = "entity" }},
		{"runtime_log_flow_scope", "check", validUnsafeRuntimeLogEventRow, func(row *unsafeEventRow) { row.scope = "flow" }},
		{"inbound_recorded_without_run", "check", validUnsafeInboundRecordedEventRow, func(row *unsafeEventRow) { row.runID = nil }},
		{"inbound_recorded_flow_scope", "check", validUnsafeInboundRecordedEventRow, func(row *unsafeEventRow) { row.scope = "flow" }},
		{"agent_directive_without_run", "check", validUnsafeAgentDirectiveEventRow, func(row *unsafeEventRow) { row.runID = nil }},
		{"agent_directive_entity_scope", "check", validUnsafeAgentDirectiveEventRow, func(row *unsafeEventRow) { row.scope = "entity" }},
		{"agent_directive_flow_scope", "check", validUnsafeAgentDirectiveEventRow, func(row *unsafeEventRow) { row.scope = "flow" }},
		{"lineaged_runtime_event_without_run", "check", validUnsafeLineagedRuntimeEventRow, func(row *unsafeEventRow) { row.runID = nil }},
		{"invalid_execution_mode", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.executionMode = "unknown" }},
		{"negative_chain_depth", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.chainDepth = -1 }},
		{"invalid_scope", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.scope = "unknown" }},
		{"entity_scope_without_entity", "check", validUnsafeEntityEventRow, func(row *unsafeEventRow) { row.entityID = nil }},
		{"flow_scope_without_flow", "check", validUnsafeFlowEventRow, func(row *unsafeEventRow) { row.flowInstance = nil }},
		{"flow_scope_with_entity", "check", validUnsafeFlowEventRow, func(row *unsafeEventRow) { row.entityID = uuid.NewString() }},
		{"global_scope_with_entity", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.entityID = uuid.NewString() }},
		{"global_scope_with_flow", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.flowInstance = "flow-a/1" }},
		{"noncanonical_flow_whitespace", "check", validUnsafeFlowEventRow, func(row *unsafeEventRow) { row.flowInstance = " flow-a/1 " }},
		{"missing_payload", "not_null", validUnsafeEventRow, func(row *unsafeEventRow) { row.payload = nil }},
		{"missing_created_at", "not_null", validUnsafeEventRow, func(row *unsafeEventRow) { row.createdAt = nil }},
		{"runtime_source_without_route", "check", validUnsafeRuntimeSourceEventRow, func(row *unsafeEventRow) { row.sourceRoute = `{}` }},
		{"declared_source_without_authority", "check", validUnsafeDeclaredSourceEventRow, func(row *unsafeEventRow) { row.routingSourceAuthority = nil }},
		{"noncanonical_source_authority", "check", validUnsafeDeclaredSourceEventRow, func(row *unsafeEventRow) { row.routingSourceAuthority = " schema-proof " }},
		{"absent_source_with_route", "check", validUnsafeEventRow, func(row *unsafeEventRow) { row.sourceRoute = `{"flow_id":"flow-a"}` }},
	}
	for _, eventType := range []string{"platform.runtime_log", "platform.inbound_recorded", "platform.agent_directive"} {
		baseline := validUnsafeRuntimeLogEventRow
		switch eventType {
		case "platform.inbound_recorded":
			baseline = validUnsafeInboundRecordedEventRow
		case "platform.agent_directive":
			baseline = validUnsafeAgentDirectiveEventRow
		}
		for _, producerType := range []string{"external", "agent", "node"} {
			producerType := producerType
			cases = append(cases, ddlCase{
				name: eventType + "_producer_" + producerType, category: "check", baseline: baseline,
				mutate: func(row *unsafeEventRow) { row.producedByType = producerType },
			})
		}
	}

	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			for _, control := range []struct {
				name string
				row  func() unsafeEventRow
			}{
				{"runtime_log_global_without_run", validUnsafeRuntimeLogEventRow},
				{"runtime_log_global_with_run", validUnsafeRuntimeLogWithRunEventRow},
				{"inbound_recorded_global", validUnsafeInboundRecordedEventRow},
				{"inbound_recorded_entity", validUnsafeInboundRecordedEntityEventRow},
				{"agent_directive_global", validUnsafeAgentDirectiveEventRow},
				{"generic_entity_scope", validUnsafeEntityEventRow},
				{"generic_flow_scope", validUnsafeFlowEventRow},
			} {
				row := control.row()
				if runID, ok := row.runID.(string); ok && runID != "" {
					seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
				}
				insertUnsafeEventRow(t, ctx, fixture, row, control.name)
			}
			for _, malformed := range cases {
				t.Run(malformed.name, func(t *testing.T) {
					control := malformed.baseline()
					if runID, ok := control.runID.(string); ok && runID != "" {
						seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
					}
					insertUnsafeEventRow(t, ctx, fixture, control, malformed.name+" control")
					invalid := control
					invalid.eventID = uuid.NewString()
					malformed.mutate(&invalid)
					rejectUnsafeEventRow(t, ctx, fixture, invalid, malformed.name, malformed.category)
				})
			}
		})
	}
}

type unsafeEventRow struct {
	class                    any
	eventID                  any
	runID                    any
	eventName                any
	taskID                   any
	entityID                 any
	flowInstance             any
	scope                    any
	payload                  any
	executionMode            any
	chainDepth               any
	producedBy               any
	producedByType           any
	sourceEventID            any
	createdAt                any
	routingSourceKind        any
	routingSourceAuthority   any
	sourceRoute              any
	targetRoute              any
	targetSet                any
	operatorReferenceEventID any
}

func validUnsafeEventRow() unsafeEventRow {
	return unsafeEventRow{
		class: "root_ingress", eventID: uuid.NewString(), runID: uuid.NewString(), eventName: "schema.contract",
		taskID: "task", scope: string(events.EventScopeGlobal), payload: `{}`, executionMode: "live", chainDepth: 0,
		producedBy: "schema-proof", producedByType: "external", createdAt: time.Date(2026, 7, 18, 20, 0, 0, 0, time.UTC),
		routingSourceKind: "", sourceRoute: `{}`, targetRoute: `{}`, targetSet: `[]`,
	}
}

func validUnsafeOperatorEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.class = "operator_injected"
	return row
}

func validUnsafeEntityEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.entityID = uuid.NewString()
	row.flowInstance = "flow-a/1"
	row.scope = "entity"
	return row
}

func validUnsafeFlowEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.flowInstance = "flow-a/1"
	row.scope = "flow"
	return row
}

func validUnsafeChildEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.class = "child"
	row.producedBy = "worker"
	row.producedByType = "agent"
	row.sourceEventID = uuid.NewString()
	return row
}

func validUnsafeSelectedForkEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.class = "selected_fork_replay"
	row.producedBy = "fork-owner"
	row.producedByType = "platform"
	return row
}

func validUnsafeRuntimeControlEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.class = "runtime_control"
	row.eventName = "platform.boot"
	row.producedBy = "runtime"
	row.producedByType = "platform"
	return row
}

func validUnsafeLineagedRuntimeEventRow() unsafeEventRow {
	row := validUnsafeRuntimeControlEventRow()
	row.sourceEventID = uuid.NewString()
	return row
}

func validUnsafeRuntimeLogEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.class = "diagnostic_direct"
	row.eventName = "platform.runtime_log"
	row.producedBy = "runtime"
	row.producedByType = "platform"
	row.runID = nil
	return row
}

func validUnsafeRuntimeLogWithRunEventRow() unsafeEventRow {
	row := validUnsafeRuntimeLogEventRow()
	row.runID = uuid.NewString()
	return row
}

func validUnsafeInboundRecordedEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.class = "diagnostic_direct"
	row.eventName = "platform.inbound_recorded"
	row.producedBy = "runtime"
	row.producedByType = "platform"
	return row
}

func validUnsafeInboundRecordedEntityEventRow() unsafeEventRow {
	row := validUnsafeInboundRecordedEventRow()
	row.entityID = uuid.NewString()
	row.scope = "entity"
	return row
}

func validUnsafeAgentDirectiveEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.class = "diagnostic_direct"
	row.eventName = "platform.agent_directive"
	row.producedBy = "runtime"
	row.producedByType = "platform"
	return row
}

func validUnsafeRuntimeSourceEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.routingSourceKind = "runtime_instance"
	row.sourceRoute = `{"flow_id":"flow-a","flow_instance":"flow-a/1"}`
	return row
}

func validUnsafeDeclaredSourceEventRow() unsafeEventRow {
	row := validUnsafeEventRow()
	row.routingSourceKind = "declared_ingress"
	row.routingSourceAuthority = "schema-proof"
	row.sourceRoute = `{"flow_id":"flow-a","flow_instance":"flow-a/1"}`
	return row
}

func insertUnsafeEventRow(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, row unsafeEventRow, reason string) {
	t.Helper()
	sqliteStatement, postgresStatement, args := unsafeEventInsert(row)
	statement := sqliteStatement
	if fixture.dialect == "postgres" {
		statement = postgresStatement
	}
	if _, err := fixture.db.ExecContext(ctx, statement, args...); err != nil {
		t.Fatalf("insert valid event DDL control for %s: %v", reason, err)
	}
}

func rejectUnsafeEventRow(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, row unsafeEventRow, reason, category string) {
	t.Helper()
	sqliteStatement, postgresStatement, args := unsafeEventInsert(row)
	eventtestsql.RejectEventStoreCorruptionCategory(t, ctx, fixture.db, fixture.dialect, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.exact_persistence",
		Reason:    "prove fresh DDL rejects " + reason,
	}, category, sqliteStatement, postgresStatement, args...)
}

func unsafeEventInsert(row unsafeEventRow) (string, string, []any) {
	args := []any{
		row.class, row.eventID, row.runID, row.eventName, row.taskID, row.entityID, row.flowInstance, row.scope, row.payload,
		row.executionMode, row.chainDepth, row.producedBy, row.producedByType, row.sourceEventID, row.createdAt,
		row.routingSourceKind, row.routingSourceAuthority, row.sourceRoute, row.targetRoute, row.targetSet, row.operatorReferenceEventID,
	}
	return `
		INSERT INTO events (
			event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload,
			execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
			routing_source_kind, routing_source_authority, source_route, target_route, target_set,
			operator_reference_event_id
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, `
		INSERT INTO events (
			event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload,
			execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
			routing_source_kind, routing_source_authority, source_route, target_route, target_set,
			operator_reference_event_id
		) VALUES (
			$1, $2::uuid, $3::uuid, $4, $5, $6::uuid, $7, $8, $9::jsonb, $10, $11, $12, $13,
			$14::uuid, $15, $16, $17, $18::jsonb, $19::jsonb, $20::jsonb, $21::uuid
		)
	`, args
}
