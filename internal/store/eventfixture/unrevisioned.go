package eventfixture

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

// InsertUnrevisionedChild seeds a child event for a later explicit shared
// revision capture. It is not a production event writer.
func InsertUnrevisionedChild(
	ctx context.Context,
	tx *sql.Tx,
	dialect authoractivityfixture.Dialect,
	eventID, runID, parentEventID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) (events.Event, error) {
	facts, err := eventFacts(eventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		return events.Event{}, err
	}
	event, err := events.NewChildEvent(events.ChildEventInput{
		Facts:   facts,
		Lineage: events.EventLineage{RunID: runID, ParentEventID: parentEventID, ExecutionMode: executionmode.Live},
	})
	if err != nil {
		return events.Event{}, err
	}
	bound, err := BindPayload(event)
	if err != nil {
		return events.Event{}, err
	}
	admitted, err := events.AdmitForPersistence(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return events.Event{}, err
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return events.Event{}, fmt.Errorf("selected-fork replay fixture requires exact lineage persistence")
	}
	settlement, err := fixtureSettlement(admitted.Event())
	if err != nil {
		return events.Event{}, err
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return events.Event{}, err
	}
	if _, err := InsertUnrevisioned(ctx, tx, dialect, record); err != nil {
		return events.Event{}, err
	}
	return event, nil
}

// InsertUnrevisioned seeds a raw test precondition that a later fixture capture
// will place in the same first revision as its remaining domain facts.
func InsertUnrevisioned(ctx context.Context, tx *sql.Tx, dialect authoractivityfixture.Dialect, record eventrecord.Record) (bool, error) {
	if tx == nil {
		return false, fmt.Errorf("unrevisioned event fixture requires a transaction")
	}
	if err := record.Validate(); err != nil {
		return false, err
	}
	query := `
		INSERT INTO events (
			event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload, payload_bytes,
			payload_schema_bundle_hash, payload_schema_flow_id, payload_schema_event_key,
			payload_schema_digest, payload_schema_class,
			execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
			routing_source_kind, routing_source_authority, source_route, target_route, target_set,
			route_settlement, operator_reference_event_id, inherited_fan_out_origin
		) VALUES (
			$1, $2::uuid, NULLIF($3,'')::uuid, $4, NULLIF($5,''), NULLIF($6,'')::uuid, NULLIF($7,''), $8, $9::jsonb, $10::bytea,
			$11, NULLIF($12,''), $13, $14, $15,
			$16, $17, $18, $19, NULLIF($20,'')::uuid, $21,
			$22, NULLIF($23,''), $24::jsonb, $25::jsonb, $26::jsonb,
			$27::jsonb, NULLIF($28,'')::uuid, NULLIF($29,'')::jsonb
		) ON CONFLICT (event_id) DO NOTHING`
	if dialect == authoractivityfixture.DialectSQLite {
		query = `
			INSERT INTO events (
				event_class, event_id, run_id, event_name, task_id, entity_id, flow_instance, scope, payload, payload_bytes,
				payload_schema_bundle_hash, payload_schema_flow_id, payload_schema_event_key,
				payload_schema_digest, payload_schema_class,
				execution_mode, chain_depth, produced_by, produced_by_type, source_event_id, created_at,
				routing_source_kind, routing_source_authority, source_route, target_route, target_set,
				route_settlement, operator_reference_event_id, inherited_fan_out_origin
			) VALUES (?, ?, NULLIF(?, ''), ?, NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''))
			ON CONFLICT(event_id) DO NOTHING`
	} else if dialect != authoractivityfixture.DialectPostgres {
		return false, fmt.Errorf("unrevisioned event fixture dialect %q is unsupported", dialect)
	}
	result, err := tx.ExecContext(ctx, query,
		record.Class, record.EventID, record.RunID, record.EventName, record.TaskID,
		record.EntityID, record.FlowInstance, record.Scope, string(record.Payload), record.Payload,
		record.PayloadSchemaBundleHash, record.PayloadSchemaFlowID,
		record.PayloadSchemaEventKey, record.PayloadSchemaDigest, record.PayloadSchemaClass, record.ExecutionMode,
		record.ChainDepth, record.ProducedBy, record.ProducedByType, record.SourceEventID, record.CreatedAt.UTC(),
		record.RoutingSourceKind, record.RoutingSourceAuthority, string(record.SourceRoute),
		string(record.TargetRoute), string(record.TargetSet), string(record.RouteSettlement), record.OperatorReferencedEventID, string(record.InheritedFanOutOrigin))
	if err != nil {
		return false, fmt.Errorf("insert unrevisioned event fixture: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	var existing eventrecord.Record
	var found bool
	switch dialect {
	case authoractivityfixture.DialectPostgres:
		existing, found, err = eventrecordpostgres.Load(ctx, tx, record.EventID)
	case authoractivityfixture.DialectSQLite:
		existing, found, err = eventrecordsqlite.Load(ctx, tx, record.EventID)
	}
	if err != nil {
		return false, err
	}
	if !found || !record.Equal(existing) {
		return false, fmt.Errorf("unrevisioned event fixture %s conflicts with canonical readback", record.EventID)
	}
	return rows == 1, nil
}
