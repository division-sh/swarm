package storetest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/google/uuid"
)

func AcknowledgedPipelineDisposition() *runtimepipelineobligation.Disposition {
	disposition := runtimepipelineobligation.Acknowledged("pipeline_persisted")
	return &disposition
}

// InsertCanonicalEventRecord seeds an already-persisted event precondition.
// The caller must still choose and construct the exact semantic event class;
// durable encoding and backend SQL remain private to the event record adapters.
func InsertCanonicalEventRecord(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	event events.Event,
) runtimebus.EventAppendOutcome {
	t.Helper()
	if db == nil {
		t.Fatal("canonical event record fixture requires a database")
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("admit canonical event record fixture: %v", err)
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		t.Fatal("selected-fork replay fixture requires exact lineage persistence")
	}
	settlement := canonicalFixtureSettlement(t, admitted.Event(), nil)
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatalf("project canonical event record fixture: %v", err)
	}
	var (
		inserted bool
		existing eventrecord.Record
		found    bool
	)
	switch dialect {
	case authoractivityfixture.DialectPostgres:
		inserted, err = eventrecordpostgres.Insert(ctx, db, record)
		if err == nil && !inserted {
			existing, found, err = eventrecordpostgres.Load(ctx, db, record.EventID)
		}
	case authoractivityfixture.DialectSQLite:
		inserted, err = eventrecordsqlite.Insert(ctx, db, record)
		if err == nil && !inserted {
			existing, found, err = eventrecordsqlite.Load(ctx, db, record.EventID)
		}
	default:
		t.Fatalf("canonical event record fixture dialect %q is unsupported", dialect)
	}
	if err != nil {
		t.Fatalf("insert canonical event record fixture: %v", err)
	}
	if inserted {
		return runtimebus.EventAppendInserted
	}
	if !found || !record.Equal(existing) {
		t.Fatalf("canonical event record fixture %s conflicts with its persisted record", record.EventID)
	}
	return runtimebus.EventAppendExactDuplicate
}

// CommitDeliveryObligationsForPersistedEvent seeds exact executable routes for
// an event that was already inserted as a fixture precondition. Delivery SQL
// stays behind the canonical adapter even in tests.
func CommitDeliveryObligationsForPersistedEvent(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
) {
	t.Helper()
	if err := commitPersistedEventDeliveryFixture(ctx, selectedStore, event.ID(), event.RunID(), routes); err != nil {
		t.Fatalf("commit persisted event delivery fixture: %v", err)
	}
}

type DeliveryLifecycleStore interface {
	ClaimDelivery(context.Context, runtimedelivery.ExecutionAuthority, events.Event, events.DeliveryRoute) (runtimedelivery.ClaimResult, error)
	Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
}

// ClaimDelivery acquires an exact fixture delivery through the same typed
// authority boundary as production. It is intentionally unsuitable for tests
// that need to assert non-acquired dispositions; those should call the store
// method directly and inspect ClaimResult.
func ClaimDelivery(ctx context.Context, selected DeliveryLifecycleStore, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	snapshot, err := selected.Snapshot(ctx, deliveryID)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	result, err := selected.ClaimDelivery(ctx, snapshot.Authority, event, route)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	claimed, ok := result.Acquired()
	if !ok {
		return runtimedelivery.ClaimedObligation{}, fmt.Errorf("delivery %s was not acquired: %s", deliveryID, result.Disposition)
	}
	return claimed, nil
}

// LoadCanonicalEventRecord exercises the same complete-record decoder used by
// runtime recovery and replay readers.
func LoadCanonicalEventRecord(t testing.TB, ctx context.Context, selectedStore any, eventID string) events.Event {
	t.Helper()
	var (
		record eventrecord.Record
		found  bool
		err    error
	)
	switch selected := selectedStore.(type) {
	case *store.PostgresStore:
		record, found, err = eventrecordpostgres.Load(ctx, DatabaseForTest(selected), eventID)
	case *store.SQLiteRuntimeStore:
		record, found, err = eventrecordsqlite.Load(ctx, DatabaseForTest(selected), eventID)
	default:
		t.Fatalf("canonical event readback store %T is unsupported", selectedStore)
	}
	if err != nil || !found {
		t.Fatalf("load canonical event record %s: found=%v err=%v", eventID, found, err)
	}
	admitted, err := record.Decode()
	if err != nil {
		t.Fatalf("decode canonical event record %s: %v", eventID, err)
	}
	return admitted.Event()
}

func InsertExistingRunRootEventRecord(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	eventID string,
	runID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event, err := eventfixture.ExistingRunRoot(ctx, db, dialect, eventID, runID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		t.Fatalf("construct canonical root event record %s: %v", eventID, err)
	}
	return event
}

func InsertChildEventRecord(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	eventID string,
	runID string,
	parentEventID string,
	eventType events.EventType,
	producer events.ProducerIdentity,
	payload []byte,
	envelope events.EventEnvelope,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event, err := eventfixture.Child(ctx, db, dialect, eventID, runID, parentEventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		t.Fatalf("construct canonical child event record %s: %v", eventID, err)
	}
	return event
}

func InsertDiagnosticDirectEventRecord(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	eventID string,
	producerID string,
	payload []byte,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event, err := eventfixture.DiagnosticDirect(ctx, db, dialect, eventID, producerID, payload, createdAt)
	if err != nil {
		t.Fatalf("construct canonical diagnostic-direct event record %s: %v", eventID, err)
	}
	return event
}

func InsertDiagnosticDirectEventRecordForRun(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	eventID string,
	runID string,
	parentEventID string,
	producerID string,
	payload []byte,
	createdAt time.Time,
) events.Event {
	t.Helper()
	event, err := eventfixture.DiagnosticDirectForRun(ctx, db, dialect, eventID, runID, parentEventID, producerID, payload, createdAt)
	if err != nil {
		t.Fatalf("construct canonical diagnostic-direct event record %s: %v", eventID, err)
	}
	return event
}

func CommitSemanticEvent(t testing.TB, ctx context.Context, selectedStore any, event events.Event) runtimebus.EventAppendOutcome {
	t.Helper()
	return CommitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, nil, runtimepipelineobligation.ScopeDirect, nil)
}

func CommitSemanticEventWithRoutes(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
) runtimebus.EventAppendOutcome {
	t.Helper()
	return CommitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, routes, scope, nil)
}

func CommitSemanticEventWithInitialFacts(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	pipelineDisposition *runtimepipelineobligation.Disposition,
) runtimebus.EventAppendOutcome {
	t.Helper()
	return commitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, routes, scope, pipelineDisposition, false)
}

// CommitSemanticForkFrontier seeds the exact PostgreSQL fact families that a
// selected-fork snapshot reads and captures their revision in the same
// transaction.
func CommitSemanticForkFrontier(
	t testing.TB,
	ctx context.Context,
	selectedStore *store.PostgresStore,
	event events.Event,
	routes []events.DeliveryRoute,
	pipelineDisposition *runtimepipelineobligation.Disposition,
) runtimebus.EventAppendOutcome {
	t.Helper()
	return commitSemanticEventWithInitialFacts(
		t, ctx, selectedStore, event, routes,
		runtimepipelineobligation.ScopeSubscribed,
		pipelineDisposition,
		true,
	)
}

func commitSemanticEventWithInitialFacts(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	pipelineDisposition *runtimepipelineobligation.Disposition,
	captureForkFrontier bool,
) runtimebus.EventAppendOutcome {
	t.Helper()
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatalf("admit event fixture: %v", err)
	}
	if admitted.Class() == events.EventAdmissionSelectedForkReplay {
		t.Fatal(fmt.Errorf("selected-fork replay events require their closed named persistence operation"))
	}
	settlement := canonicalFixtureSettlement(t, admitted.Event(), routes)
	record, err := eventrecord.FromAdmitted(admitted, settlement)
	if err != nil {
		t.Fatalf("project admitted event fixture: %v", err)
	}

	var (
		db              *sql.DB
		deliveryAdapter *deliveryadapter.Adapter
		insert          func(context.Context, *sql.Tx, eventrecord.Record) (bool, error)
		load            func(context.Context, *sql.Tx, string) (eventrecord.Record, bool, error)
		postgres        bool
	)
	switch selected := selectedStore.(type) {
	case *store.PostgresStore:
		db = DatabaseForTest(selected)
		postgres = true
		deliveryAdapter, err = deliveryadapter.NewAdapter(deliveryadapter.DialectPostgres)
		insert = func(ctx context.Context, tx *sql.Tx, record eventrecord.Record) (bool, error) {
			return eventrecordpostgres.Insert(ctx, tx, record)
		}
		load = func(ctx context.Context, tx *sql.Tx, eventID string) (eventrecord.Record, bool, error) {
			return eventrecordpostgres.Load(ctx, tx, eventID)
		}
	case *store.SQLiteRuntimeStore:
		db = DatabaseForTest(selected)
		deliveryAdapter, err = deliveryadapter.NewAdapter(deliveryadapter.DialectSQLite)
		insert = func(ctx context.Context, tx *sql.Tx, record eventrecord.Record) (bool, error) {
			return eventrecordsqlite.Insert(ctx, tx, record)
		}
		load = func(ctx context.Context, tx *sql.Tx, eventID string) (eventrecord.Record, bool, error) {
			return eventrecordsqlite.Load(ctx, tx, eventID)
		}
	default:
		t.Fatalf("semantic event fixture store %T is unsupported", selectedStore)
	}
	if err != nil {
		t.Fatalf("construct semantic delivery fixture adapter: %v", err)
	}
	if db == nil || deliveryAdapter == nil {
		t.Fatalf("semantic event fixture store %T is not initialized", selectedStore)
	}
	runner, ok := selectedStore.(runLifecycleOperationRunner)
	if !ok {
		t.Fatalf("semantic event fixture store %T has no run lifecycle mutation owner", selectedStore)
	}
	if admitted.RunDisposition() != events.AdmittedRunless {
		startedAt := record.CreatedAt
		if startedAt.IsZero() {
			startedAt = time.Now().UTC()
		}
		if err := EnsureRunForAdmittedEvent(ctx, runner, admitted, startedAt); err != nil {
			t.Fatalf("ensure semantic event fixture run: %v", err)
		}
	}
	obligationProvider, ok := selectedStore.(interface {
		PipelineObligations() runtimepipelineobligation.Store
	})
	if !ok {
		t.Fatalf("semantic event fixture store %T has no pipeline obligation owner", selectedStore)
	}
	obligationOwner := obligationProvider.PipelineObligations()
	publicationClaim, err := obligationOwner.ClaimPublication(ctx, record.EventID)
	if err != nil {
		t.Fatalf("claim semantic event fixture publication: %v", err)
	}
	defer func() {
		if err := obligationOwner.Release(context.WithoutCancel(ctx), publicationClaim); err != nil {
			t.Fatalf("release semantic event fixture publication: %v", err)
		}
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin semantic event fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	inserted, err := insert(ctx, tx, record)
	if err != nil {
		t.Fatalf("insert semantic event fixture: %v", err)
	}
	if !inserted {
		existing, found, loadErr := load(ctx, tx, record.EventID)
		if loadErr != nil {
			t.Fatalf("load duplicate semantic event fixture: %v", loadErr)
		}
		if !found || !record.Equal(existing) {
			t.Fatalf("semantic event fixture %s conflicts with its persisted record", record.EventID)
		}
		return runtimebus.EventAppendExactDuplicate
	}
	authority := deliveryFixtureAuthority(t, ctx, tx, record.RunID, deliveryAdapter)
	if _, err := deliveryAdapter.CommitInitial(ctx, tx, record.EventID, record.RunID, events.NormalizeDeliveryRoutes(routes), authority); err != nil {
		t.Fatalf("commit semantic event fixture routes: %v", err)
	}
	if err := insertPipelineScopeFixture(ctx, tx, record.EventID, scope, postgres, time.Now().UTC()); err != nil {
		t.Fatalf("commit semantic event fixture pipeline scope: %v", err)
	}
	if pipelineDisposition != nil {
		if err := insertPipelineDispositionFixture(ctx, tx, record.EventID, *pipelineDisposition, postgres, time.Now().UTC()); err != nil {
			t.Fatalf("commit semantic event fixture pipeline disposition: %v", err)
		}
	}
	if captureForkFrontier {
		if _, ok := selectedStore.(*store.PostgresStore); !ok {
			t.Fatalf("semantic fork frontier fixture requires PostgreSQL, got %T", selectedStore)
		}
		effects := runforkrevision.NewEffects()
		if err := effects.Add(
			record.RunID,
			runforkrevision.FamilyEvents,
			runforkrevision.FamilyEventDeliveries,
			runforkrevision.FamilyCommittedReplayScopes,
			runforkrevision.FamilyEventReceipts,
		); err != nil {
			t.Fatalf("declare semantic fork frontier fixture: %v", err)
		}
		if _, err := runforkrevision.FinalizePostgres(ctx, tx, effects); err != nil {
			t.Fatalf("finalize semantic fork frontier fixture: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit semantic event fixture: %v", err)
	}
	return runtimebus.EventAppendInserted
}

func canonicalFixtureSettlement(t testing.TB, event events.Event, routes []events.DeliveryRoute) events.RouteSettlement {
	t.Helper()
	var (
		settlement events.RouteSettlement
		err        error
	)
	switch event.Type() {
	case events.EventTypePlatformRuntimeLog:
		settlement, err = events.NewNoDeliverySettlement(events.EventWriteRuntimeLogDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	case events.EventTypePlatformInboundRecord:
		settlement, err = events.NewNoDeliverySettlement(events.EventWriteInboundEvidenceDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	case events.EventTypePlatformAgentDirective:
		settlement, err = events.NewNoDeliverySettlement(events.EventWriteDirectiveDirect, events.NoDeliveryNoSubscriberByDesign, events.ConnectEvaluationLedger{})
	default:
		var ledger events.ConnectEvaluationLedger
		ledger, err = events.NewConnectEvaluationLedger(nil)
		if err == nil && len(routes) > 0 {
			settlement, err = events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
		} else if err == nil {
			settlement, err = events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
		}
	}
	if err != nil {
		t.Fatalf("construct canonical event fixture settlement: %v", err)
	}
	if err := settlement.Validate(routes); err != nil {
		t.Fatalf("validate canonical event fixture settlement: %v", err)
	}
	return settlement
}

func deliveryFixtureAuthority(t testing.TB, ctx context.Context, tx *sql.Tx, runID string, adapter *deliveryadapter.Adapter) runtimedelivery.ExecutionAuthority {
	t.Helper()
	query := `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`
	if adapter.Dialect() == deliveryadapter.DialectSQLite {
		query = `SELECT bundle_hash FROM runs WHERE run_id=?`
	}
	var bundleHash string
	if err := tx.QueryRowContext(ctx, query, runID).Scan(&bundleHash); err != nil {
		t.Fatalf("load delivery fixture run source artifact: %v", err)
	}
	source, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		t.Fatalf("construct delivery fixture source: %v", err)
	}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "storetest:"+runID, 1)
	if err != nil {
		t.Fatalf("construct delivery fixture authority: %v", err)
	}
	return authority
}

func insertPipelineScopeFixture(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	scope runtimepipelineobligation.CommittedScope,
	postgres bool,
	now time.Time,
) error {
	if _, err := runtimepipelineobligation.ParseCommittedScope(string(scope)); err != nil {
		return err
	}
	query := `
		INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
		SELECT e.event_id, e.run_id, ?, ?, ? FROM events e WHERE e.event_id = ?
		ON CONFLICT(event_id) DO NOTHING`
	args := []any{string(scope), now, now, eventID}
	if postgres {
		query = `
			INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
			SELECT e.event_id, e.run_id, $2, $3, $3 FROM events e WHERE e.event_id = $1::uuid
			ON CONFLICT(event_id) DO NOTHING`
		args = []any{eventID, string(scope), now}
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("pipeline scope fixture affected %d rows, want 1", rows)
	}
	return nil
}

func insertPipelineDispositionFixture(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
	now time.Time,
) error {
	if err := disposition.ValidateFor(runtimepipelineobligation.PurposeRecovery); err != nil {
		return err
	}
	outcome := "success"
	managerStatus := "processed"
	if !disposition.Successful() {
		outcome = "dead_letter"
		managerStatus = "error"
		if disposition.Kind() == runtimepipelineobligation.DispositionDeadLetter {
			managerStatus = "dead_letter"
		}
	}
	reasonCode := disposition.ReasonCode()
	if reasonCode == "" {
		if disposition.Successful() {
			reasonCode = "pipeline_persisted"
		} else {
			reasonCode = "pipeline_error"
		}
	}
	var failureJSON any
	if failure := disposition.Failure(); failure != nil {
		raw, err := runtimefailures.MarshalEnvelope(*failure)
		if err != nil {
			return err
		}
		failureJSON = string(raw)
	}
	sideEffects, err := json.Marshal(map[string]string{
		"manager_status": managerStatus,
		"reason_code":    reasonCode,
	})
	if err != nil {
		return err
	}
	query := `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT ?, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance, ?, ?, ?, ?, ?
		FROM events e WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO NOTHING`
	args := []any{uuid.NewString(), outcome, reasonCode, failureJSON, string(sideEffects), now, eventID}
	if postgres {
		query = `
			INSERT INTO event_receipts (
				receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
				outcome, reason_code, failure, side_effects, processed_at
			)
			SELECT $1::uuid, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance,
				$2, $3, $4::jsonb, $5::jsonb, $6
			FROM events e WHERE e.event_id = $7::uuid
			ON CONFLICT(event_id, subscriber_type, subscriber_id) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("pipeline disposition fixture affected %d rows, want 1", rows)
	}
	if disposition.Successful() {
		dialect := deliveryadapter.DialectSQLite
		if postgres {
			dialect = deliveryadapter.DialectPostgres
		}
		adapter, err := deliveryadapter.NewAdapter(dialect)
		if err != nil {
			return err
		}
		if err := adapter.CommitPipelineHandoff(ctx, tx, eventID); err != nil {
			return err
		}
	}
	return nil
}
