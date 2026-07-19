package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/internal/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/eventrecord/sqlite"
)

type semanticFixtureTarget interface {
	InsertEventDeliveryRoutesTx(context.Context, *sql.Tx, string, []events.DeliveryRoute) error
	UpsertCommittedReplayScopeTx(context.Context, *sql.Tx, string, runtimereplayclaim.CommittedReplayScope) error
	UpsertPipelineReceiptTx(context.Context, *sql.Tx, string, string, *runtimefailures.Envelope) error
}

// InsertCanonicalEventRecord seeds an already-persisted event precondition.
// The caller must still choose and construct the exact semantic event class;
// durable encoding and backend SQL remain private to the event record adapters.
func InsertCanonicalEventRecord(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	dialect runtimeauthoractivity.Dialect,
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
	record, err := eventrecord.FromAdmitted(admitted)
	if err != nil {
		t.Fatalf("project canonical event record fixture: %v", err)
	}
	var (
		inserted bool
		existing eventrecord.Record
		found    bool
	)
	switch dialect {
	case runtimeauthoractivity.DialectPostgres:
		inserted, err = eventrecordpostgres.Insert(ctx, db, record)
		if err == nil && !inserted {
			existing, found, err = eventrecordpostgres.Load(ctx, db, record.EventID)
		}
	case runtimeauthoractivity.DialectSQLite:
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
		record, found, err = eventrecordpostgres.Load(ctx, selected.DB, eventID)
	case *store.SQLiteRuntimeStore:
		record, found, err = eventrecordsqlite.Load(ctx, selected.DB, eventID)
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
	dialect runtimeauthoractivity.Dialect,
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
	dialect runtimeauthoractivity.Dialect,
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
	dialect runtimeauthoractivity.Dialect,
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
	dialect runtimeauthoractivity.Dialect,
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
	return CommitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, nil, runtimereplayclaim.CommittedReplayScopeDirect, nil)
}

func CommitSemanticEventWithRoutes(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
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
	scope runtimereplayclaim.CommittedReplayScope,
	pipelineReceipt *runtimebus.InitialPipelineReceipt,
) runtimebus.EventAppendOutcome {
	t.Helper()
	return commitSemanticEventWithInitialFacts(t, ctx, selectedStore, event, routes, scope, pipelineReceipt, false)
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
	pipelineReceipt *runtimebus.InitialPipelineReceipt,
) runtimebus.EventAppendOutcome {
	t.Helper()
	return commitSemanticEventWithInitialFacts(
		t, ctx, selectedStore, event, routes,
		runtimereplayclaim.CommittedReplayScopeSubscribed,
		pipelineReceipt,
		true,
	)
}

func commitSemanticEventWithInitialFacts(
	t testing.TB,
	ctx context.Context,
	selectedStore any,
	event events.Event,
	routes []events.DeliveryRoute,
	scope runtimereplayclaim.CommittedReplayScope,
	pipelineReceipt *runtimebus.InitialPipelineReceipt,
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
	record, err := eventrecord.FromAdmitted(admitted)
	if err != nil {
		t.Fatalf("project admitted event fixture: %v", err)
	}

	var (
		db     *sql.DB
		target semanticFixtureTarget
		insert func(context.Context, *sql.Tx, eventrecord.Record) (bool, error)
		load   func(context.Context, *sql.Tx, string) (eventrecord.Record, bool, error)
	)
	switch selected := selectedStore.(type) {
	case *store.PostgresStore:
		db, target = selected.DB, selected
		insert = func(ctx context.Context, tx *sql.Tx, record eventrecord.Record) (bool, error) {
			return eventrecordpostgres.Insert(ctx, tx, record)
		}
		load = func(ctx context.Context, tx *sql.Tx, eventID string) (eventrecord.Record, bool, error) {
			return eventrecordpostgres.Load(ctx, tx, eventID)
		}
	case *store.SQLiteRuntimeStore:
		db, target = selected.DB, selected
		insert = func(ctx context.Context, tx *sql.Tx, record eventrecord.Record) (bool, error) {
			return eventrecordsqlite.Insert(ctx, tx, record)
		}
		load = func(ctx context.Context, tx *sql.Tx, eventID string) (eventrecord.Record, bool, error) {
			return eventrecordsqlite.Load(ctx, tx, eventID)
		}
	default:
		t.Fatalf("semantic event fixture store %T is unsupported", selectedStore)
	}
	if db == nil || target == nil {
		t.Fatalf("semantic event fixture store %T is not initialized", selectedStore)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin semantic event fixture: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureSemanticFixtureRun(ctx, tx, record, selectedStore); err != nil {
		t.Fatalf("insert semantic event fixture run: %v", err)
	}
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
	if err := target.InsertEventDeliveryRoutesTx(ctx, tx, record.EventID, events.NormalizeDeliveryRoutes(routes)); err != nil {
		t.Fatalf("commit semantic event fixture routes: %v", err)
	}
	if err := target.UpsertCommittedReplayScopeTx(ctx, tx, record.EventID, scope); err != nil {
		t.Fatalf("commit semantic event fixture replay scope: %v", err)
	}
	if pipelineReceipt != nil {
		if err := target.UpsertPipelineReceiptTx(ctx, tx, record.EventID, pipelineReceipt.Status, pipelineReceipt.Failure); err != nil {
			t.Fatalf("commit semantic event fixture pipeline receipt: %v", err)
		}
	}
	if captureForkFrontier {
		if _, ok := selectedStore.(*store.PostgresStore); !ok {
			t.Fatalf("semantic fork frontier fixture requires PostgreSQL, got %T", selectedStore)
		}
		if _, err := runforkrevision.Capture(
			ctx,
			tx,
			record.RunID,
			runforkrevision.FamilyEvents,
			runforkrevision.FamilyEventDeliveries,
			runforkrevision.FamilyEventReceipts,
		); err != nil {
			t.Fatalf("capture semantic fork frontier fixture: %v", err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit semantic event fixture: %v", err)
	}
	return runtimebus.EventAppendInserted
}

func ensureSemanticFixtureRun(ctx context.Context, tx *sql.Tx, record eventrecord.Record, selectedStore any) error {
	if record.RunID == "" {
		return nil
	}
	startedAt := record.CreatedAt
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	switch selectedStore.(type) {
	case *store.PostgresStore:
		_, err := tx.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES ($1::uuid, 'running', $2)
			ON CONFLICT (run_id) DO NOTHING
		`, record.RunID, startedAt)
		return err
	case *store.SQLiteRuntimeStore:
		_, err := tx.ExecContext(ctx, `
			INSERT INTO runs (run_id, status, started_at)
			VALUES (?, 'running', ?)
			ON CONFLICT (run_id) DO NOTHING
		`, record.RunID, startedAt)
		return err
	default:
		return fmt.Errorf("semantic event fixture store %T is unsupported", selectedStore)
	}
}
