package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventpersistence"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

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
	_, postgres := selected.(*PostgresStore)
	return runUnrevisionedEventFixtureTransactionForTest(ctx, selected, func(ctx context.Context, tx *sql.Tx) error {
		return eventrecord.SetFixtureEventChainDepthTx(ctx, tx, postgres, eventID, depth)
	})
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
	event, found, err := ReadCanonicalEventRecordForTest(ctx, selected, eventID)
	if err != nil {
		return events.Event{}, err
	}
	if !found {
		return events.Event{}, fmt.Errorf("canonical event record %s is missing", eventID)
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
	if err := validateChannelObservationOwner(selected); err != nil {
		return events.Event{}, err
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
		return events.Event{}, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return events.Event{}, err
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
	if err := validateChannelObservationOwner(selected); err != nil {
		return events.Event{}, false, err
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
		return events.Event{}, false, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return events.Event{}, false, fmt.Errorf("decode canonical event record %s: %w", eventID, err)
	}
	return admitted.Event(), true, nil
}

func insertCanonicalFixtureRecordForTest(ctx context.Context, attempt *mutationprotocol.Attempt, dialect authoractivityfixture.Dialect, record eventrecord.Record) (bool, error) {
	var inserted bool
	var err error
	if dialect == authoractivityfixture.DialectPostgres {
		inserted, err = eventrecordpostgres.Insert(ctx, attempt, record)
	} else {
		inserted, err = eventrecordsqlite.Insert(ctx, attempt, record)
	}
	if err != nil || inserted {
		return inserted, err
	}
	var existing eventrecord.Record
	var found bool
	err = attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if dialect == authoractivityfixture.DialectPostgres {
			existing, found, err = eventrecordpostgres.Load(ctx, tx, record.EventID)
		} else {
			existing, found, err = eventrecordsqlite.Load(ctx, tx, record.EventID)
		}
		return err
	})
	if err != nil {
		return false, err
	}
	if !found || !record.Equal(existing) {
		return false, fmt.Errorf("canonical event fixture %s conflicts with its persisted record", record.EventID)
	}
	return false, nil
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
	dialect, err := eventFixtureDialectForTest(selectedStore)
	if err != nil {
		return false, err
	}
	postgres := dialect == authoractivityfixture.DialectPostgres
	deliveryAdapter, err := deliveryadapter.NewAdapter(deliveryadapter.Dialect(dialect))
	if err != nil {
		return false, err
	}
	if !revisioned {
		return commitUnrevisionedSemanticEventFixtureForTest(ctx, selectedStore, dialect, record, routes, scope, pipelineDisposition, deliveryAdapter)
	}
	inserted := false
	err = runEventFixtureMutationForTest(ctx, selectedStore, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		inserted, err = insertCanonicalFixtureRecordForTest(txctx, attempt, dialect, record)
		if err != nil || !inserted {
			return err
		}
		var authority runtimedelivery.ExecutionAuthority
		if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			authority, err = persistedEventDeliveryFixtureAuthority(txctx, tx, record.RunID, !postgres)
			return err
		}); err != nil {
			return err
		}
		if _, err := deliveryAdapter.CommitInitial(txctx, attempt, record.EventID, record.RunID, events.NormalizeDeliveryRoutes(routes), authority); err != nil {
			return err
		}
		if err := commitRevisionedEventFixtureFactsForTest(txctx, selectedStore, attempt, record.EventID, scope, pipelineDisposition, time.Now().UTC()); err != nil {
			return err
		}
		if captureForkFrontier {
			if !postgres {
				return fmt.Errorf("semantic fork frontier fixture requires PostgreSQL, got %T", selectedStore)
			}
			for _, family := range []runforkrevision.Family{
				runforkrevision.FamilyEvents,
				runforkrevision.FamilyEventDeliveries,
				runforkrevision.FamilyCommittedReplayScopes,
				runforkrevision.FamilyEventReceipts,
			} {
				if err := attempt.AddWholeFamily(record.RunID, family); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return false, err
	}
	return inserted, nil
}

func commitUnrevisionedSemanticEventFixtureForTest(
	ctx context.Context,
	selectedStore any,
	dialect authoractivityfixture.Dialect,
	record eventrecord.Record,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	pipelineDisposition *runtimepipelineobligation.Disposition,
	adapter *deliveryadapter.Adapter,
) (bool, error) {
	inserted := false
	var authority runtimedelivery.ExecutionAuthority
	err := runUnrevisionedEventFixtureTransactionForTest(ctx, selectedStore, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if dialect == authoractivityfixture.DialectPostgres {
			inserted, err = eventrecordpostgres.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		} else {
			inserted, err = eventrecordsqlite.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		}
		if err != nil || !inserted {
			return err
		}
		authority, err = persistedEventDeliveryFixtureAuthority(ctx, tx, record.RunID, dialect == authoractivityfixture.DialectSQLite)
		if err != nil {
			return err
		}
		if err := events.ValidateDeliveryRoutes(routes); err != nil {
			return err
		}
		admitted, err := record.Decode()
		if err != nil {
			return err
		}
		if err := events.ValidateReceiverMaterializations(admitted.Event(), routes); err != nil {
			return err
		}
		for _, route := range events.NormalizeDeliveryRoutes(routes) {
			if err := insertUnrevisionedDeliveryFixtureForTest(ctx, tx, dialect, adapter, record.EventID, record.RunID, route, authority); err != nil {
				return err
			}
		}
		return commitUnrevisionedEventFixtureFactsForTest(ctx, selectedStore, tx, record.EventID, scope, pipelineDisposition, time.Now().UTC())
	})
	if err != nil {
		return false, err
	}
	if !inserted {
		return false, nil
	}
	reader, ok := selectedStore.(interface {
		Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
	})
	if !ok {
		return false, fmt.Errorf("unrevisioned semantic fixture store %T cannot read delivery snapshots", selectedStore)
	}
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		obligation, err := runtimedelivery.NewObligation(record.EventID, record.RunID, route, authority)
		if err != nil {
			return false, err
		}
		snapshot, err := reader.Snapshot(ctx, obligation.DeliveryID())
		if err != nil {
			return false, fmt.Errorf("read unrevisioned delivery fixture %s: %w", obligation.DeliveryID(), err)
		}
		wantRoute, err := json.Marshal(obligation.Route())
		if err != nil {
			return false, err
		}
		gotRoute, err := json.Marshal(snapshot.Route.Normalized())
		if err != nil {
			return false, err
		}
		if snapshot.DeliveryID != obligation.DeliveryID() || snapshot.EventID != record.EventID || snapshot.RunID != record.RunID ||
			events.EncodeDeliveryRouteIdentity(snapshot.RouteIdentity) != events.EncodeDeliveryRouteIdentity(obligation.RouteIdentity()) ||
			snapshot.SubscriberClass != obligation.SubscriberClass() || snapshot.SubscriberID != obligation.SubscriberID() ||
			snapshot.Status != runtimedelivery.StatusPending || snapshot.RetryCount != 0 || snapshot.MaxRetries != obligation.MaxRetries() ||
			snapshot.ClaimVersion != 0 || snapshot.NextEligibleAt.IsZero() || !snapshot.Authority.Equal(authority) ||
			!bytes.Equal(gotRoute, wantRoute) {
			return false, fmt.Errorf("unrevisioned delivery fixture %s differs from canonical route/authority projection", obligation.DeliveryID())
		}
	}
	return true, nil
}

func insertUnrevisionedDeliveryFixtureForTest(ctx context.Context, tx *sql.Tx, dialect authoractivityfixture.Dialect, adapter *deliveryadapter.Adapter, eventID, runID string, route events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) error {
	obligation, err := runtimedelivery.NewObligation(eventID, runID, route, authority)
	if err != nil {
		return err
	}
	_, err = adapter.InsertUnrevisionedFixtureObligationTx(ctx, tx, obligation)
	return err
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

func commitUnrevisionedEventFixtureFactsForTest(ctx context.Context, selected any, tx *sql.Tx, eventID string, scope runtimepipelineobligation.CommittedScope, disposition *runtimepipelineobligation.Disposition, at time.Time) error {
	switch store := selected.(type) {
	case *PostgresStore:
		return store.pipelinePostgresOwner.CommitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, at)
	case *SQLiteRuntimeStore:
		return store.pipelineSQLiteOwner.CommitUnrevisionedEventFixtureFactsTx(ctx, tx, eventID, scope, disposition, at)
	default:
		return fmt.Errorf("event fixture store %T is unsupported", selected)
	}
}
