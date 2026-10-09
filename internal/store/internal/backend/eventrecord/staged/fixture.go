// Package staged owns physical fixture events that precede an explicit history cut.
package staged

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/counterprojection"
)

// Insert stages a validated fixture event and its exact counter in the caller's
// native transaction. The caller retains its later explicit history cut.
func Insert(ctx context.Context, tx *sql.Tx, dialect authoractivity.Dialect, record eventrecord.Record) (bool, error) {
	inserted, err := insertRecord(ctx, tx, dialect, record)
	if err != nil || !inserted || record.RunID == "" {
		return inserted, err
	}
	if err := counterprojection.Apply(ctx, tx, dialect, record.RunID, 1); err != nil {
		return false, err
	}
	return true, nil
}

// InsertWithinAttempt leaves counter finalization with the native attempt,
// without declaring the history cut that the fixture's caller captures later.
func InsertWithinAttempt(ctx context.Context, attempt *mutationprotocol.Attempt, dialect authoractivity.Dialect, record eventrecord.Record) (bool, error) {
	var inserted bool
	err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		inserted, err = insertRecord(ctx, tx, dialect, record)
		return err
	})
	if err != nil || !inserted || record.RunID == "" {
		return inserted, err
	}
	if err := attempt.AddEventCountDelta(record.RunID, 1); err != nil {
		return false, err
	}
	return true, nil
}

func insertRecord(ctx context.Context, tx *sql.Tx, dialect authoractivity.Dialect, record eventrecord.Record) (bool, error) {
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
	if dialect == authoractivity.DialectSQLite {
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
	} else if dialect != authoractivity.DialectPostgres {
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
	case authoractivity.DialectPostgres:
		existing, found, err = eventrecordpostgres.Load(ctx, tx, record.EventID)
	case authoractivity.DialectSQLite:
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
