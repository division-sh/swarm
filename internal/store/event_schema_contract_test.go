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
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx := testAuthorActivityContext()
			for _, malformed := range []struct {
				name     string
				category string
				mutate   func(*unsafeEventRow)
			}{
				{"missing_class", "not_null", func(row *unsafeEventRow) { row.class = nil }},
				{"invalid_class", "check", func(row *unsafeEventRow) { row.class = "projection" }},
				{"missing_producer_id", "not_null", func(row *unsafeEventRow) { row.producedBy = nil }},
				{"blank_producer_id", "check", func(row *unsafeEventRow) { row.producedBy = "  " }},
				{"missing_producer_type", "not_null", func(row *unsafeEventRow) { row.producedByType = nil }},
				{"invalid_producer_type", "check", func(row *unsafeEventRow) { row.producedByType = "unknown" }},
				{"child_without_parent", "check", func(row *unsafeEventRow) { row.class = "child"; row.producedByType = "agent" }},
				{"root_without_run", "check", func(row *unsafeEventRow) { row.runID = nil }},
				{"operator_without_run", "check", func(row *unsafeEventRow) { row.class = "operator_injected"; row.runID = nil }},
				{"selected_fork_without_run", "check", func(row *unsafeEventRow) { row.class = "selected_fork_replay"; row.runID = nil }},
				{"root_with_parent", "check", func(row *unsafeEventRow) { row.sourceEventID = uuid.NewString() }},
				{"non_operator_with_operator_reference", "check", func(row *unsafeEventRow) { row.operatorReferenceEventID = uuid.NewString() }},
				{"root_with_platform_producer", "check", func(row *unsafeEventRow) { row.producedByType = "platform" }},
				{"runtime_with_external_producer", "check", func(row *unsafeEventRow) { row.class = "runtime_control" }},
				{"reserved_label_under_root", "check", func(row *unsafeEventRow) {
					row.eventName = "platform.runtime_log"
					row.producedBy = "runtime"
					row.producedByType = "platform"
				}},
				{"diagnostic_direct_unknown_label", "check", func(row *unsafeEventRow) {
					row.class = "diagnostic_direct"
					row.producedBy = "runtime"
					row.producedByType = "platform"
				}},
				{"runtime_log_wrong_producer_id", "check", func(row *unsafeEventRow) {
					row.class = "diagnostic_direct"
					row.eventName = "platform.runtime_log"
					row.producedByType = "platform"
				}},
				{"lineaged_runtime_event_without_run", "check", func(row *unsafeEventRow) {
					row.class = "runtime_control"
					row.producedBy = "runtime"
					row.producedByType = "platform"
					row.runID = nil
					row.sourceEventID = uuid.NewString()
				}},
				{"invalid_execution_mode", "check", func(row *unsafeEventRow) { row.executionMode = "unknown" }},
				{"negative_chain_depth", "check", func(row *unsafeEventRow) { row.chainDepth = -1 }},
				{"invalid_scope", "check", func(row *unsafeEventRow) { row.scope = "unknown" }},
				{"missing_payload", "not_null", func(row *unsafeEventRow) { row.payload = nil }},
				{"missing_created_at", "not_null", func(row *unsafeEventRow) { row.createdAt = nil }},
				{"runtime_source_without_route", "check", func(row *unsafeEventRow) { row.routingSourceKind = "runtime_instance" }},
				{"declared_source_without_authority", "check", func(row *unsafeEventRow) {
					row.routingSourceKind = "declared_ingress"
					row.sourceRoute = `{"flow_id":"flow-a","flow_instance":"flow-a/1"}`
				}},
				{"absent_source_with_route", "check", func(row *unsafeEventRow) { row.sourceRoute = `{"flow_id":"flow-a"}` }},
			} {
				t.Run(malformed.name, func(t *testing.T) {
					row := validUnsafeEventRow()
					seedAuthorActivityReceiptRun(t, fixture, ctx, row.runID.(string))
					malformed.mutate(&row)
					rejectUnsafeEventRow(t, ctx, fixture, row, malformed.name, malformed.category)
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

func rejectUnsafeEventRow(t *testing.T, ctx context.Context, fixture authorActivityReceiptFixture, row unsafeEventRow, reason, category string) {
	t.Helper()
	args := []any{
		row.class, row.eventID, row.runID, row.eventName, row.taskID, row.entityID, row.flowInstance, row.scope, row.payload,
		row.executionMode, row.chainDepth, row.producedBy, row.producedByType, row.sourceEventID, row.createdAt,
		row.routingSourceKind, row.routingSourceAuthority, row.sourceRoute, row.targetRoute, row.targetSet, row.operatorReferenceEventID,
	}
	eventtestsql.RejectEventStoreCorruptionCategory(t, ctx, fixture.db, fixture.dialect, eventtestsql.EventCorruptionClaim{
		Invariant: "store.event_record.exact_persistence",
		Reason:    "prove fresh DDL rejects " + reason,
	}, category, `
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
	`, args...)
}
