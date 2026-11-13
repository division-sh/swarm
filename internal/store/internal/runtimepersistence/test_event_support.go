package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

type ActivityForkSourceStorage struct {
	Flow, EntityType, Stage, AttemptStatus string
	Fields, Accumulator, Payload           []byte
}

type ActivityForkResultStorage = eventrecord.ActivityForkResultStorage

type ActivityForkAttemptStorage = activityjournal.ActivityForkAttemptStorage

// Preserve the three independent source observations, including the original
// missing-attempt empty status. This is not a new atomic source snapshot.
func ReadActivityForkSourceStorageForTest(ctx context.Context, selected any, runID, entityID, requestID string) (ActivityForkSourceStorage, error) {
	var out ActivityForkSourceStorage
	for _, id := range []string{runID, entityID, requestID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return ActivityForkSourceStorage{}, err
		}
	}
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		row, err := pipelinepersistence.ReadActivityForkEntityStorageTx(ctx, tx, runID, entityID)
		out.Flow, out.EntityType, out.Stage = row.Flow, row.EntityType, row.Stage
		out.Fields, out.Accumulator = row.Fields, row.Accumulator
		return err
	})
	if err == nil {
		err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			if _, postgres := selected.(*PostgresStore); postgres {
				out.Payload, err = eventrecordpostgres.ReadActivityForkPayloadBytesStorage(ctx, tx, runID, requestID)
			} else {
				out.Payload, err = eventrecordsqlite.ReadActivityForkPayloadBytesStorage(ctx, tx, runID, requestID)
			}
			return err
		})
	}
	if err == nil {
		err = readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			out.AttemptStatus, err = activityjournal.ReadActivityForkAttemptStatusStorageTx(ctx, tx, runID, requestID)
			return err
		})
	}
	if err != nil {
		return ActivityForkSourceStorage{}, err
	}
	return out, nil
}

func ReadActivityForkRequestPayloadStorageForTest(ctx context.Context, selected any, runID, eventID string) ([]byte, error) {
	for _, id := range []string{runID, eventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return nil, err
		}
	}
	var out []byte
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadActivityForkRequestPayloadStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadActivityForkResultStorageForTest(ctx context.Context, selected any, runID, eventID string) (ActivityForkResultStorage, error) {
	for _, id := range []string{runID, eventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return ActivityForkResultStorage{}, err
		}
	}
	var out ActivityForkResultStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadActivityForkResultStorageTx(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return ActivityForkResultStorage{}, err
	}
	return out, nil
}

func ReadActivityForkAttemptStorageForTest(ctx context.Context, selected any, requestID string) (ActivityForkAttemptStorage, error) {
	if err := validateStandaloneStorageEvent(selected, requestID); err != nil {
		return ActivityForkAttemptStorage{}, err
	}
	_, postgres := selected.(*PostgresStore)
	var out ActivityForkAttemptStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = activityjournal.ReadActivityForkAttemptStorageTx(ctx, tx, postgres, requestID)
		return err
	})
	if err != nil {
		return ActivityForkAttemptStorage{}, err
	}
	return out, nil
}

func CountActivityForkAttemptStorageForTest(ctx context.Context, selected any, requestID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, requestID); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = activityjournal.CountActivityForkAttemptStorageTx(ctx, tx, requestID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadSelectedSourceRefusalStorageForTest(ctx context.Context, selected any, sourceRunID, sourceEventID string) (map[string]int, error) {
	for _, id := range []string{sourceRunID, sourceEventID} {
		if err := validateStandaloneStorageEvent(selected, id); err != nil {
			return nil, err
		}
	}
	var out map[string]int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = runforkpersistence.ReadSelectedSourceRefusalStorageTx(ctx, tx, sourceRunID)
		if err != nil {
			return err
		}
		out["fork_events"], err = eventrecord.CountSourceEventForeignRunStorageTx(ctx, tx, sourceEventID, sourceRunID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func CountRunNamedEventStorageForTest(ctx context.Context, selected any, runID, eventName string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, runID); err != nil {
		return 0, err
	}
	var out int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.CountRunNamedEventStorageTx(ctx, tx, runID, eventName)
		return err
	})
	if err != nil {
		return 0, err
	}
	return out, nil
}

// Fixed physical witnesses retain the predecessor/fork/global scopes of the
// refusal proofs. Delivery SQL belongs to the closed delivery projection owner.
func ReadSelectedForkExecutableCardinalityForTest(ctx context.Context, selected any, runID string) (map[string]string, error) {
	if err := validateSelectedForkStorageIdentity(runID); err != nil {
		return nil, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	queries := map[string]string{
		"events":             `SELECT COUNT(*) FROM events WHERE run_id=$1`,
		"entity_state":       `SELECT COUNT(*) FROM entity_state WHERE run_id=$1`,
		"timers":             `SELECT COUNT(*) FROM timers WHERE run_id=$1`,
		"readiness":          `SELECT COUNT(*) FROM flow_instance_runtime_readiness WHERE run_id=$1`,
		"agents":             `SELECT COUNT(*) FROM agents WHERE CAST($1 AS TEXT) IS NOT NULL`,
		"flow_instances":     `SELECT COUNT(*) FROM flow_instances WHERE CAST($1 AS TEXT) IS NOT NULL`,
		"execution_lineage":  `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id=$1`,
		"runtime_executions": `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1`,
	}
	result := make(map[string]string, len(queries)+2)
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1`, runID).Scan(&status); err != nil {
			return err
		}
		result["run_status"] = status
		for name, query := range queries {
			var count int
			if err := tx.QueryRowContext(ctx, query, runID).Scan(&count); err != nil {
				return err
			}
			result[name] = strconv.Itoa(count)
		}
		// platform_tables.retired_instance_routing forbids the old membership
		// mirror. Keep its global physical refusal witness at the canonical
		// persisted delivery facts, independently of this run's deliveries.
		routes, err := deliveryadapter.FixtureDeliveryCardinalityTx(ctx, tx)
		if err != nil {
			return err
		}
		result["persisted_delivery_routes"] = strconv.Itoa(routes)
		count, err := deliveryadapter.CountSelectedForkDeliveryStorageTx(ctx, tx, runID)
		result["deliveries"] = strconv.Itoa(count)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type SelectedCausalDiagnosticStorage struct {
	Events     []eventrecord.LifecycleDiagnosticEventLineage
	Projection []byte
}

func ReadAgentSettledAttemptCountForTest(ctx context.Context, selected any, eventID, agentID string) (int, error) {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return 0, err
	}
	_, postgres := selected.(*PostgresStore)
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = deliveryadapter.ReadAgentSettledAttemptCountTx(ctx, tx, postgres, eventID, agentID)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func SetEventChainDepthForTest(ctx context.Context, selected any, eventID string, depth int) error {
	if err := validateStandaloneStorageEvent(selected, eventID); err != nil {
		return err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil || store.eventPostgresOwner == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return store.eventPostgresOwner.SetEventChainDepthForTest(ctx, eventID, depth)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil || store.eventSQLiteOwner == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return store.eventSQLiteOwner.SetEventChainDepthForTest(ctx, eventID, depth)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

type SelectedCausalDiagnosticConservation struct {
	Events, Pending int
}

func ReadSelectedCausalDiagnosticStorageForTest(ctx context.Context, selected any, outboxID string) (SelectedCausalDiagnosticStorage, error) {
	if err := validateSelectedForkStorageIdentity(outboxID); err != nil {
		return SelectedCausalDiagnosticStorage{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedCausalDiagnosticStorage{}, err
	}
	var out SelectedCausalDiagnosticStorage
	_, postgres := selected.(*PostgresStore)
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Events, err = eventrecord.ReadLifecycleDiagnosticEventLineage(ctx, tx, postgres, outboxID)
		if err != nil {
			return err
		}
		out.Projection, err = eventpersistence.ReadLifecycleDiagnosticProjection(ctx, tx, outboxID)
		return err
	})
	if err != nil {
		return SelectedCausalDiagnosticStorage{}, err
	}
	return out, nil
}

func ReadSelectedCausalDiagnosticConservationForTest(ctx context.Context, selected any, outboxID string) (SelectedCausalDiagnosticConservation, error) {
	if err := validateSelectedForkStorageIdentity(outboxID); err != nil {
		return SelectedCausalDiagnosticConservation{}, err
	}
	if err := validateChannelObservationOwner(selected); err != nil {
		return SelectedCausalDiagnosticConservation{}, err
	}
	var out SelectedCausalDiagnosticConservation
	_, postgres := selected.(*PostgresStore)
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out.Events, err = eventrecord.CountLifecycleDiagnosticEvents(ctx, tx, postgres, outboxID)
		if err != nil {
			return err
		}
		out.Pending, err = eventpersistence.CountPendingLifecycleDiagnostic(ctx, tx, outboxID)
		return err
	})
	if err != nil {
		return SelectedCausalDiagnosticConservation{}, err
	}
	return out, nil
}

type CausalEventStorageRow = eventrecord.CausalObservationRow

type GlobalEventChronologyRow = eventrecord.GlobalEventChronologyRow

func ReadGlobalEventChronologyForTest(ctx context.Context, selected any) ([]GlobalEventChronologyRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	_, postgres := selected.(*PostgresStore)
	var out []GlobalEventChronologyRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadGlobalEventChronologyTx(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

type ScatterGatherPhysicalCounts struct {
	Entities, Timers, FanOutIntents, DomainEvents int
}

func ReadScatterGatherPhysicalCountsForTest(ctx context.Context, selected any, runID string) (ScatterGatherPhysicalCounts, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	var out ScatterGatherPhysicalCounts
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if out.Entities, err = pipelinepersistence.CountWorkflowFieldRowsForRun(ctx, tx, runID); err != nil {
			return err
		}
		if out.Timers, err = genericschedule.CountTimerRowsForRun(ctx, tx, runID); err != nil {
			return err
		}
		if out.FanOutIntents, err = pipelinepersistence.CountFanOutIntentsForRun(ctx, tx, runID); err != nil {
			return err
		}
		out.DomainEvents, err = eventrecord.CountScatterGatherDomainEvents(ctx, tx, runID)
		return err
	})
	if err != nil {
		return ScatterGatherPhysicalCounts{}, err
	}
	return out, nil
}

func CountRunEventNameStorageForTest(ctx context.Context, selected any, run, name string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		count, err = eventrecord.CountRunEventNameStorage(ctx, tx, run, name)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

type SourceRouteSettlementStorage = eventrecord.SourceRouteSettlementStorage

func ReadSourceRouteSettlementStorageForTest(ctx context.Context, selected any, runID, eventID string) (SourceRouteSettlementStorage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return SourceRouteSettlementStorage{}, err
	}
	var out SourceRouteSettlementStorage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		out, err = eventrecord.ReadSourceRouteSettlementStorage(ctx, tx, runID, eventID)
		return err
	})
	if err != nil {
		return SourceRouteSettlementStorage{}, err
	}
	return out, nil
}

func CountChainDepthDiagnosticStorageForTest(ctx context.Context, selected any, runID, entityID, handlerNode string) (int, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return 0, err
	}
	var count int
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		count, err = eventrecord.CountChainDepthDiagnosticStorage(ctx, tx, postgres, runID, entityID, handlerNode)
		return err
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func ReadLatestHandlerErrorLogForTest(ctx context.Context, selected any) (json.RawMessage, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out json.RawMessage
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		out, err = eventrecord.ReadLatestHandlerErrorLog(ctx, tx, postgres)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func ReadCausalEventStorageSinceForTest(ctx context.Context, selected any, since time.Time) ([]CausalEventStorageRow, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return nil, err
	}
	var out []CausalEventStorageRow
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		_, postgres := selected.(*PostgresStore)
		var err error
		out, err = eventrecord.ReadCausalObservationSince(ctx, tx, postgres, since)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Record-bearing fixture evidence uses the publication fixture's closed codec.
// Callers receive detached facts, never another record interpreter.
type SemanticEventFixtureEvidence struct {
	Record                      eventrecord.Record
	RecordFound                 bool
	RevisionCount               int
	PipelineReceiptCount        int
	PipelineReceiptOutcome      string
	PipelineReceiptReason       string
	PipelineReceiptFailure      *runtimefailures.Envelope
	CommittedScope              string
	CommittedScopeFound         bool
	RunStatus                   string
	RunEventCount               int
	SettledDeliveryAttemptCount int
	DeliveryProjections         map[string][18]string
	DeliveryStatuses            map[string]string
	NonPlatformReceiptCount     int
}

func loadCanonicalFixtureRecordTx(ctx context.Context, tx *sql.Tx, postgres bool, eventID string) (eventrecord.Record, bool, error) {
	if postgres {
		return eventrecordpostgres.Load(ctx, tx, eventID)
	}
	return eventrecordsqlite.Load(ctx, tx, eventID)
}

// LoadCanonicalEventRecordForTest requires presence; storage reading and
// complete decoding belong only to ReadCanonicalEventRecordForTest.
func LoadCanonicalEventRecordForTest(ctx context.Context, selected any, eventID string) (events.Event, error) {
	var empty events.Event
	event, found, err := ReadCanonicalEventRecordForTest(ctx, selected, eventID)
	if err != nil {
		return empty, err
	}
	if !found {
		return empty, fmt.Errorf("canonical event record %s is missing", eventID)
	}
	return event, nil
}
func CommitSemanticEventFixtureForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, false, false)
}

func ReadLatestRuntimeLogRecordForTest(ctx context.Context, selected any) (events.Event, error) {
	return readLatestRuntimeLogRecordForTest(ctx, selected, false)
}

func ReadLatestStartupRecoveryDecisionRecordForTest(ctx context.Context, selected any) (events.Event, error) {
	return readLatestRuntimeLogRecordForTest(ctx, selected, true)
}

func readLatestRuntimeLogRecordForTest(ctx context.Context, selected any, startupDecision bool) (events.Event, error) {
	var empty events.Event
	if err := validateChannelObservationOwner(selected); err != nil {
		return empty, err
	}
	_, postgres := selected.(*PostgresStore)
	query := `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' ORDER BY created_at DESC LIMIT 1`
	if startupDecision {
		query = `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' AND json_extract(payload,'$.details.component')='runtime' AND json_extract(payload,'$.details.action')='startup_recovery_decision' ORDER BY created_at DESC LIMIT 1`
		if postgres {
			query = `SELECT CAST(event_id AS TEXT) FROM events WHERE event_name='platform.runtime_log' AND payload->'details'->>'component'='runtime' AND payload->'details'->>'action'='startup_recovery_decision' ORDER BY created_at DESC LIMIT 1`
		}
	}
	var record eventrecord.Record
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var id string
		if err := tx.QueryRowContext(ctx, query).Scan(&id); err != nil {
			return err
		}
		var found bool
		var err error
		record, found, err = loadCanonicalFixtureRecordTx(ctx, tx, postgres, id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("selected runtime log %s is absent from its original read snapshot", id)
		}
		return nil
	})
	if err != nil {
		return empty, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, err
	}
	return admitted.Event(), nil
}

func readFixtureEventLineageStorage(ctx context.Context, selected any, tx *sql.Tx, eventID string) (string, string, error) {
	switch selected.(type) {
	case *PostgresStore:
		return eventrecordpostgres.ReadFixtureLineageStorage(ctx, tx, eventID)
	case *SQLiteRuntimeStore:
		return eventrecordsqlite.ReadFixtureLineageStorage(ctx, tx, eventID)
	default:
		return "", "", fmt.Errorf("event lineage observation requires the original selected read owner")
	}
}

func ReadCanonicalEventRecordForTest(ctx context.Context, selected any, eventID string) (events.Event, bool, error) {
	var empty events.Event
	if err := validateChannelObservationOwner(selected); err != nil {
		return empty, false, err
	}
	var record eventrecord.Record
	var found bool
	err := readServedDeliveryObservation(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		_, postgres := selected.(*PostgresStore)
		record, found, err = loadCanonicalFixtureRecordTx(ctx, tx, postgres, eventID)
		return err
	})
	if err != nil || !found {
		return empty, false, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return empty, false, fmt.Errorf("decode canonical event record %s: %w", eventID, err)
	}
	return admitted.Event(), true, nil
}

func InsertCanonicalEventRecordForTest(ctx context.Context, selected any, record eventrecord.Record) (bool, error) {
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return false, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store.eventPostgresOwner == nil {
			return false, fmt.Errorf("event fixture selected store %T is not initialized", selected)
		}
		return store.eventPostgresOwner.InsertCanonicalEventRecordForTest(ctx, record)
	case *SQLiteRuntimeStore:
		if store.eventSQLiteOwner == nil {
			return false, fmt.Errorf("event fixture selected store %T is not initialized", selected)
		}
		return store.eventSQLiteOwner.InsertCanonicalEventRecordForTest(ctx, record)
	default:
		return false, fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

// CommitRevisionedSemanticEventFixtureForTest exercises the canonical event
// and delivery adapters, including their fork revision projection.
func CommitRevisionedSemanticEventFixtureForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, true, false)
}

// CommitSemanticForkFrontierForTest captures all selected-fork fact families
// in the same PostgreSQL fixture transaction.
func CommitSemanticForkFrontierForTest(ctx context.Context, selected any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition) (bool, error) {
	if store, ok := selected.(*PostgresStore); !ok || store == nil {
		return false, fmt.Errorf("semantic fork frontier fixture requires the original PostgreSQL owner, got %T", selected)
	}
	return commitSelectedSemanticEventFixtureForTest(ctx, selected, admitted, settlement, routes, scope, disposition, true, true)
}

// InsertRevisionedExistingRunRootEventForTest keeps event admission and revision
// capture on the original native store, without handing callers SQL authority.
func InsertRevisionedExistingRunRootEventForTest(ctx context.Context, selected any, eventID, runID string,
	eventType events.EventType, producer events.ProducerIdentity, payload []byte,
	envelope events.EventEnvelope, createdAt time.Time,
) (events.Event, error) {
	var rejected events.Event
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return rejected, err
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store.eventPostgresOwner == nil {
			return rejected, fmt.Errorf("event fixture selected store %T is not initialized", selected)
		}
		return store.eventPostgresOwner.InsertRevisionedExistingRunRootEventForTest(ctx, eventID, runID, eventType, producer, payload, envelope, createdAt)
	case *SQLiteRuntimeStore:
		if store.eventSQLiteOwner == nil {
			return rejected, fmt.Errorf("event fixture selected store %T is not initialized", selected)
		}
		return store.eventSQLiteOwner.InsertRevisionedExistingRunRootEventForTest(ctx, eventID, runID, eventType, producer, payload, envelope, createdAt)
	default:
		return rejected, fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func eventFixtureDialectForTest(selected any) (authoractivityfixture.Dialect, error) {
	switch store := selected.(type) {
	case *PostgresStore:
		if store != nil && store.backend != nil && store.pipelinePostgresOwner != nil {
			if err := store.requireCurrentSchema(); err != nil {
				return "", err
			}
			return authoractivityfixture.DialectPostgres, nil
		}
	case *SQLiteRuntimeStore:
		if store != nil && store.backend != nil && store.pipelineSQLiteOwner != nil {
			if err := store.requireCurrentSchema(); err != nil {
				return "", err
			}
			return authoractivityfixture.DialectSQLite, nil
		}
	}
	return "", fmt.Errorf("event fixture selected store %T is not initialized", selected)
}

func runEventFixtureMutationForTest(ctx context.Context, selected any, write func(context.Context, *mutationprotocol.Attempt) error) error {
	apply := func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(txctx, attempt)
	}
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return mutationprotocol.RunPostgres(ctx, store.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, apply).Err()
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return mutationprotocol.RunSQLite(ctx, store.backend, "canonical event fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, store.runLifecycleCandidates, apply).Err()
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func runUnrevisionedEventFixtureTransactionForTest(ctx context.Context, selected any, write func(context.Context, *sql.Tx) error) error {
	switch store := selected.(type) {
	case *PostgresStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("postgres event fixture store is required")
		}
		return store.backend.RunTransaction(ctx, write)
	case *SQLiteRuntimeStore:
		if store == nil || store.backend == nil {
			return fmt.Errorf("sqlite event fixture store is required")
		}
		return store.backend.RunTransaction(ctx, "unrevisioned event fixture", write)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}

func commitSelectedSemanticEventFixtureForTest(ctx context.Context, selectedStore any, admitted events.AdmittedEvent, settlement events.RouteSettlement, routes []events.DeliveryRoute, scope runtimepipelineobligation.CommittedScope, pipelineDisposition *runtimepipelineobligation.Disposition, revisioned, captureForkFrontier bool) (bool, error) {
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		return false, fmt.Errorf("selected-fork replay events require their closed named persistence operation")
	}
	if err := settlement.Validate(routes); err != nil {
		return false, err
	}
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		return false, err
	}
	if _, err := eventFixtureDialectForTest(selectedStore); err != nil {
		return false, err
	}
	switch store := selectedStore.(type) {
	case *PostgresStore:
		if store.eventPostgresOwner == nil {
			return false, fmt.Errorf("event fixture selected store %T is not initialized", selectedStore)
		}
		return store.eventPostgresOwner.CommitSemanticEventFixtureForTest(ctx, record, routes, scope, pipelineDisposition, revisioned, captureForkFrontier)
	case *SQLiteRuntimeStore:
		if store.eventSQLiteOwner == nil {
			return false, fmt.Errorf("event fixture selected store %T is not initialized", selectedStore)
		}
		return store.eventSQLiteOwner.CommitSemanticEventFixtureForTest(ctx, record, routes, scope, pipelineDisposition, revisioned, captureForkFrontier)
	default:
		return false, fmt.Errorf("event fixture store %T is unsupported", selectedStore)
	}
}

func commitRevisionedEventFixtureFactsForTest(ctx context.Context, selected any, attempt *mutationprotocol.Attempt, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CommitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, at)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CommitRevisionedEventFixtureFactsTx(ctx, attempt, eventID, scope, disposition, at)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
