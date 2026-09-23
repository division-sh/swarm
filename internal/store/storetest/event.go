package storetest

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
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
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
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
	var err error
	event, err = eventfixture.BindPayload(event)
	if err != nil {
		t.Fatalf("bind canonical event payload fixture: %v", err)
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
	err = runCanonicalEventMutation(ctx, db, dialect, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		switch dialect {
		case authoractivityfixture.DialectPostgres:
			inserted, err = eventrecordpostgres.Insert(txctx, attempt, record)
		case authoractivityfixture.DialectSQLite:
			inserted, err = eventrecordsqlite.Insert(txctx, attempt, record)
		}
		if err != nil || inserted {
			return err
		}
		return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			switch dialect {
			case authoractivityfixture.DialectPostgres:
				existing, found, err = eventrecordpostgres.Load(txctx, tx, record.EventID)
			case authoractivityfixture.DialectSQLite:
				existing, found, err = eventrecordsqlite.Load(txctx, tx, record.EventID)
			}
			return err
		})
	})
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

func runCanonicalEventMutation(ctx context.Context, db *sql.DB, dialect authoractivityfixture.Dialect, write func(context.Context, *mutationprotocol.Attempt) error) error {
	if db == nil {
		return fmt.Errorf("canonical event record fixture requires a database")
	}
	if write == nil {
		return fmt.Errorf("canonical event record fixture requires a mutation writer")
	}
	apply := func(txctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(txctx, attempt)
	}
	switch dialect {
	case authoractivityfixture.DialectPostgres:
		backend, err := postgresbackend.New(db)
		if err != nil {
			return err
		}
		return mutationprotocol.RunPostgres(ctx, backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, apply).Err()
	case authoractivityfixture.DialectSQLite:
		backend, err := sqlitebackend.New(db)
		if err != nil {
			return err
		}
		return mutationprotocol.RunSQLite(ctx, backend, "canonical event fixture", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, apply).Err()
	default:
		return fmt.Errorf("canonical event record fixture dialect %q is unsupported", dialect)
	}
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
	var event events.Event
	err := runCanonicalEventMutation(ctx, db, dialect, func(txctx context.Context, attempt *mutationprotocol.Attempt) (err error) {
		event, err = eventfixture.ExistingRunRoot(txctx, attempt, dialect, eventID, runID, eventType, producer, payload, envelope, createdAt)
		return err
	})
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
	var event events.Event
	err := runCanonicalEventMutation(ctx, db, dialect, func(txctx context.Context, attempt *mutationprotocol.Attempt) (err error) {
		event, err = eventfixture.Child(txctx, attempt, dialect, eventID, runID, parentEventID, eventType, producer, payload, envelope, createdAt)
		return err
	})
	if err != nil {
		t.Fatalf("construct canonical child event record %s: %v", eventID, err)
	}
	return event
}

// InsertUnrevisionedChildEventRecord leaves the child in the same first
// revision as other test-only facts captured later by the caller.
func InsertUnrevisionedChildEventRecord(
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
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin unrevisioned child event fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	event, err := eventfixture.InsertUnrevisionedChild(ctx, tx, dialect, eventID, runID, parentEventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		t.Fatalf("insert unrevisioned child event fixture %s: %v", eventID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit unrevisioned child event fixture %s: %v", eventID, err)
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
	var event events.Event
	err := runCanonicalEventMutation(ctx, db, dialect, func(txctx context.Context, attempt *mutationprotocol.Attempt) (err error) {
		event, err = eventfixture.DiagnosticDirect(txctx, attempt, dialect, eventID, producerID, payload, createdAt)
		return err
	})
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
	var event events.Event
	err := runCanonicalEventMutation(ctx, db, dialect, func(txctx context.Context, attempt *mutationprotocol.Attempt) (err error) {
		event, err = eventfixture.DiagnosticDirectForRun(txctx, attempt, dialect, eventID, runID, parentEventID, producerID, payload, createdAt)
		return err
	})
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
	var err error
	if _, bound := event.PayloadAdmission(); !bound {
		event, err = eventfixture.BindPayload(event)
		if err != nil {
			t.Fatalf("bind event payload fixture: %v", err)
		}
	}
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
		insert          func(context.Context, *mutationprotocol.Attempt, eventrecord.Record) (bool, error)
		load            func(context.Context, *sql.Tx, string) (eventrecord.Record, bool, error)
		postgres        bool
		dialect         authoractivityfixture.Dialect
	)
	switch selected := selectedStore.(type) {
	case *store.PostgresStore:
		db = DatabaseForTest(selected)
		postgres = true
		dialect = authoractivityfixture.DialectPostgres
		deliveryAdapter, err = deliveryadapter.NewAdapter(deliveryadapter.DialectPostgres)
		insert = eventrecordpostgres.Insert
		load = func(ctx context.Context, tx *sql.Tx, eventID string) (eventrecord.Record, bool, error) {
			return eventrecordpostgres.Load(ctx, tx, eventID)
		}
	case *store.SQLiteRuntimeStore:
		db = DatabaseForTest(selected)
		dialect = authoractivityfixture.DialectSQLite
		deliveryAdapter, err = deliveryadapter.NewAdapter(deliveryadapter.DialectSQLite)
		insert = eventrecordsqlite.Insert
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
	if !captureForkFrontier {
		inserted, err := commitUnrevisionedSemanticEventFixture(ctx, selectedStore, db, dialect, record, routes, scope, pipelineDisposition, deliveryAdapter)
		if err != nil {
			t.Fatalf("commit unrevisioned semantic event fixture: %v", err)
		}
		if !inserted {
			return runtimebus.EventAppendExactDuplicate
		}
		return runtimebus.EventAppendInserted
	}

	inserted := false
	err = runCanonicalEventMutation(ctx, db, dialect, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var err error
		inserted, err = insert(txctx, attempt, record)
		if err != nil {
			return err
		}
		if !inserted {
			var existing eventrecord.Record
			var found bool
			if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
				existing, found, err = load(txctx, tx, record.EventID)
				return err
			}); err != nil {
				return err
			}
			if !found || !record.Equal(existing) {
				return fmt.Errorf("semantic event fixture %s conflicts with its persisted record", record.EventID)
			}
			return nil
		}
		var authority runtimedelivery.ExecutionAuthority
		if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			var err error
			authority, err = deliveryFixtureAuthority(txctx, tx, record.RunID, deliveryAdapter)
			return err
		}); err != nil {
			return err
		}
		if _, err := deliveryAdapter.CommitInitial(txctx, attempt, record.EventID, record.RunID, events.NormalizeDeliveryRoutes(routes), authority); err != nil {
			return err
		}
		if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
			return insertPipelineScopeFixture(txctx, tx, record.EventID, scope, postgres, time.Now().UTC())
		}); err != nil {
			return err
		}
		if record.RunID != "" {
			if err := attempt.AddFact(record.RunID, runforkrevision.FamilyCommittedReplayScopes, record.EventID); err != nil {
				return err
			}
		}
		if pipelineDisposition != nil {
			if err := insertPipelineDispositionFixture(txctx, attempt, record.RunID, record.EventID, *pipelineDisposition, postgres, time.Now().UTC()); err != nil {
				return err
			}
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
		t.Fatalf("commit semantic event fixture: %v", err)
	}
	if !inserted {
		return runtimebus.EventAppendExactDuplicate
	}
	return runtimebus.EventAppendInserted
}

func commitUnrevisionedSemanticEventFixture(
	ctx context.Context,
	selectedStore any,
	db *sql.DB,
	dialect authoractivityfixture.Dialect,
	record eventrecord.Record,
	routes []events.DeliveryRoute,
	scope runtimepipelineobligation.CommittedScope,
	pipelineDisposition *runtimepipelineobligation.Disposition,
	adapter *deliveryadapter.Adapter,
) (bool, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	inserted, err := eventfixture.InsertUnrevisioned(ctx, tx, dialect, record)
	if err != nil || !inserted {
		return inserted, err
	}
	authority, err := deliveryFixtureAuthority(ctx, tx, record.RunID, adapter)
	if err != nil {
		return false, err
	}
	if err := events.ValidateDeliveryRoutes(routes); err != nil {
		return false, err
	}
	admitted, err := record.Decode()
	if err != nil {
		return false, err
	}
	if err := events.ValidateReceiverMaterializations(admitted.Event(), routes); err != nil {
		return false, err
	}
	for _, route := range events.NormalizeDeliveryRoutes(routes) {
		if err := insertUnrevisionedDeliveryFixture(ctx, tx, dialect, adapter, record.EventID, record.RunID, route, authority); err != nil {
			return false, err
		}
	}
	postgres := dialect == authoractivityfixture.DialectPostgres
	if err := insertPipelineScopeFixture(ctx, tx, record.EventID, scope, postgres, time.Now().UTC()); err != nil {
		return false, err
	}
	if pipelineDisposition != nil {
		if _, err := insertPipelineDispositionFixtureTx(ctx, tx, record.EventID, *pipelineDisposition, postgres, time.Now().UTC()); err != nil {
			return false, err
		}
		if pipelineDisposition.Successful() {
			if _, err := tx.ExecContext(ctx, `UPDATE event_deliveries SET continuation_handoff_at = COALESCE(continuation_handoff_at, CURRENT_TIMESTAMP) WHERE event_id = $1`, record.EventID); err != nil {
				return false, err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
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

func insertUnrevisionedDeliveryFixture(ctx context.Context, tx *sql.Tx, dialect authoractivityfixture.Dialect, adapter *deliveryadapter.Adapter, eventID, runID string, route events.DeliveryRoute, authority runtimedelivery.ExecutionAuthority) error {
	obligation, err := runtimedelivery.NewObligation(eventID, runID, route, authority)
	if err != nil {
		return err
	}
	route = obligation.Route()
	fields := agentidentity.StorageFields{}
	if route.Recipient.IsAgent() {
		fields, err = route.AgentIdentity.StorageFields()
		if err != nil {
			return err
		}
		if fields.AgentID != route.Recipient.ID() {
			return fmt.Errorf("unrevisioned delivery fixture agent identity conflicts with subscriber")
		}
	} else if !route.AgentIdentity.IsZero() {
		return fmt.Errorf("unrevisioned node delivery fixture carries agent identity")
	}
	target, err := json.Marshal(route.Target)
	if err != nil {
		return err
	}
	deliveryContext, err := json.Marshal(route.Context)
	if err != nil {
		return err
	}
	projection, err := json.Marshal(route.PayloadProjection)
	if err != nil {
		return err
	}
	connectClaim, err := json.Marshal(route.ConnectClaim)
	if err != nil {
		return err
	}
	materialization, err := events.EncodeReceiverMaterializationRecord(route)
	if err != nil {
		return err
	}
	now, err := adapter.CaptureSnapshotTime(ctx, tx)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, route_identity, subscriber_type, subscriber_id,
			agent_name_owner, agent_name_source, agent_route_presence,
			agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
			delivery_target_route, delivery_context, delivery_payload_projection, connect_execution_claim,
			receiver_materialization_plan, execution_authority_kind, authority_bundle_hash,
			execution_authority_id, execution_authority_generation,
			status, retry_count, max_retries, next_eligible_at, claim_version, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, $3::uuid, $4, $5, $6,
			$7, $8, $9, $10, $11, $12,
			$13::jsonb, $14::jsonb, $15::jsonb, $16::jsonb,
			$17::jsonb, $18, $19, $20, $21,
			'pending', 0, $22, $23, 0, $23, $23
		) ON CONFLICT (event_id, route_identity) DO NOTHING`
	if dialect == authoractivityfixture.DialectSQLite {
		query = `
			INSERT INTO event_deliveries (
				delivery_id, run_id, event_id, route_identity, subscriber_type, subscriber_id,
				agent_name_owner, agent_name_source, agent_route_presence,
				agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
				delivery_target_route, delivery_context, delivery_payload_projection, connect_execution_claim,
				receiver_materialization_plan, execution_authority_kind, authority_bundle_hash,
				execution_authority_id, execution_authority_generation,
				status, retry_count, max_retries, next_eligible_at, claim_version, created_at, updated_at
			) VALUES (
				?1, ?2, ?3, ?4, ?5, ?6,
				?7, ?8, ?9, ?10, ?11, ?12,
				?13, ?14, ?15, ?16,
				?17, ?18, ?19, ?20, ?21,
				'pending', 0, ?22, ?23, 0, ?23, ?23
			) ON CONFLICT(event_id, route_identity) DO NOTHING`
	}
	result, err := tx.ExecContext(ctx, query,
		obligation.DeliveryID(), runID, eventID, events.EncodeDeliveryRouteIdentity(obligation.RouteIdentity()),
		string(obligation.SubscriberClass()), obligation.SubscriberID(),
		fields.NameOwner, fields.NameSource, fields.RoutePresence,
		fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		string(target), string(deliveryContext), string(projection), string(connectClaim),
		string(materialization), string(authority.Kind()), authority.SourceArtifact().BundleHash(),
		authority.ExecutionID(), authority.Generation(), obligation.MaxRetries(), now)
	if err != nil {
		return fmt.Errorf("insert unrevisioned delivery fixture: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("unrevisioned delivery fixture affected %d rows, want 1", rows)
	}
	return nil
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

func deliveryFixtureAuthority(ctx context.Context, tx *sql.Tx, runID string, adapter *deliveryadapter.Adapter) (runtimedelivery.ExecutionAuthority, error) {
	query := `SELECT bundle_hash FROM runs WHERE run_id=$1::uuid`
	if adapter.Dialect() == deliveryadapter.DialectSQLite {
		query = `SELECT bundle_hash FROM runs WHERE run_id=?`
	}
	var bundleHash string
	if err := tx.QueryRowContext(ctx, query, runID).Scan(&bundleHash); err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("load delivery fixture run source artifact: %w", err)
	}
	source, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("construct delivery fixture source: %w", err)
	}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "storetest:"+runID, 1)
	if err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("construct delivery fixture authority: %w", err)
	}
	return authority, nil
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
	attempt *mutationprotocol.Attempt,
	runID string,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
	now time.Time,
) error {
	var receiptID string
	if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) (err error) {
		receiptID, err = insertPipelineDispositionFixtureTx(ctx, tx, eventID, disposition, postgres, now)
		return err
	}); err != nil {
		return err
	}
	if runID != "" {
		if err := attempt.AddFact(runID, runforkrevision.FamilyEventReceipts, receiptID); err != nil {
			return err
		}
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
		return adapter.CommitPipelineHandoff(ctx, attempt, eventID)
	}
	return nil
}

func insertPipelineDispositionFixtureTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID string,
	disposition runtimepipelineobligation.Disposition,
	postgres bool,
	now time.Time,
) (string, error) {
	if err := disposition.ValidateFor(runtimepipelineobligation.PurposeRecovery); err != nil {
		return "", err
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
			return "", err
		}
		failureJSON = string(raw)
	}
	sideEffects, err := json.Marshal(map[string]string{
		"manager_status": managerStatus,
		"reason_code":    reasonCode,
	})
	if err != nil {
		return "", err
	}
	query := `
		INSERT INTO event_receipts (
			receipt_id, event_id, subscriber_type, subscriber_id, entity_id, flow_instance,
			outcome, reason_code, failure, side_effects, processed_at
		)
		SELECT ?, e.event_id, 'platform', 'pipeline', e.entity_id, e.flow_instance, ?, ?, ?, ?, ?
		FROM events e WHERE e.event_id = ?
		ON CONFLICT(event_id, subscriber_type, subscriber_id) DO NOTHING`
	receiptID := uuid.NewString()
	args := []any{receiptID, outcome, reasonCode, failureJSON, string(sideEffects), now, eventID}
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
		return "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows != 1 {
		return "", fmt.Errorf("pipeline disposition fixture affected %d rows, want 1", rows)
	}
	return receiptID, nil
}
