package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
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
	event, err := private.LoadCanonicalEventRecordForTest(ctx, selectedStore, eventID)
	if err != nil {
		t.Fatalf("load canonical event record %s: %v", eventID, err)
	}
	return event
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
	selected any,
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
	event, err := private.InsertStagedChildEventForTest(ctx, selected, eventID, runID, parentEventID, eventType, producer, payload, envelope, createdAt)
	if err != nil {
		t.Fatalf("insert unrevisioned child event fixture %s: %v", eventID, err)
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
	return commitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, routes, scope, pipelineDisposition, false, false)
}

// CommitSemanticForkFrontier seeds the exact PostgreSQL fact families that a
// selected-fork snapshot reads and captures their revision in the same
// transaction.
func CommitSemanticForkFrontier(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	pipelineDisposition *runtimepipelineobligation.Disposition,
) runtimebus.EventAppendOutcome {
	t.Helper()
	if selected, ok := selectedStore.(*private.PostgresStore); !ok || selected == nil {
		t.Fatalf("semantic fork frontier fixture requires the original PostgreSQL owner, got %T", selectedStore)
	}
	return commitSemanticEventWithInitialFacts(
		t, ctx, selectedStore, event, routes,
		runtimepipelineobligation.ScopeSubscribed,
		pipelineDisposition,
		true, true,
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
	revisioned, captureForkFrontier bool,
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

	runner, ok := selectedStore.(RunFixtureStore)
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
	var inserted bool
	if captureForkFrontier {
		inserted, err = private.CommitSemanticForkFrontierForTest(ctx, selectedStore, admitted, settlement, routes, scope, pipelineDisposition)
	} else if revisioned {
		inserted, err = private.CommitRevisionedSemanticEventFixtureForTest(ctx, selectedStore, admitted, settlement, routes, scope, pipelineDisposition)
	} else {
		inserted, err = private.CommitSemanticEventFixtureForTest(ctx, selectedStore, admitted, settlement, routes, scope, pipelineDisposition)
	}
	if err != nil {
		t.Fatalf("commit semantic event fixture: %v", err)
	}
	if !inserted {
		return runtimebus.EventAppendExactDuplicate
	}
	return runtimebus.EventAppendInserted
}

func canonicalFixtureSettlement(t testing.TB, event events.Event, routes []events.DeliveryRoute) events.RouteSettlement {
	t.Helper()
	settlement, err := fixtureSettlement(event, routes)
	if err != nil {
		t.Fatalf("construct canonical event fixture settlement: %v", err)
	}
	return settlement
}

func fixtureSettlement(event events.Event, routes []events.DeliveryRoute) (events.RouteSettlement, error) {
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
		return events.RouteSettlement{}, err
	}
	if err := settlement.Validate(routes); err != nil {
		return events.RouteSettlement{}, err
	}
	return settlement, nil
}
