package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	replycontextstore "github.com/division-sh/swarm/internal/store/internal/backend/replycontext"
	"github.com/google/uuid"
)

func TestIssue642ReplyCorrelationScopesBothStores(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (replyContextStoreTestSurface, func(context.Context, string, ...string) error)
	}{{"sqlite", setupSQLiteReplyContextStoreTest}, {"postgres", setupPostgresReplyContextStoreTest}} {
		t.Run(backend.name, func(t *testing.T) {
			store, seed := backend.setup(t)
			ctx := context.Background()
			origin := events.RouteIdentity{FlowID: "requester", FlowInstance: "requester/shared-key", EntityID: uuid.NewString()}
			create := func(t *testing.T, run string) runtimereplycontext.Record {
				t.Helper()
				request := uuid.NewString()
				if run == "" {
					seedReplyRunlessPhysicalEvent(t, ctx, store, request)
				} else if err := seed(ctx, run, request); err != nil {
					t.Fatal(err)
				}
				record := replyContextFreezeTestRecord(request, run, "shared", time.Now().UTC().Truncate(time.Microsecond))
				record.RequesterFlowID, record.Origin = "requester", origin
				record.RequestCorrelationID, record.CorrelationKey = "business-key", "key"
				record.ID = runtimereplycontext.DeterministicID(request, record.RequesterFlowID, record.RequestOutputPin, record.ReplyInputPin, record.ProviderFlowID, origin)
				return record
			}
			requireCreated := func(t *testing.T, record runtimereplycontext.Record) {
				t.Helper()
				var err error
				if record.RunID == "" {
					err = runUnrevisionedEventFixtureTransactionForTest(ctx, store, func(ctx context.Context, tx *sql.Tx) error {
						_, err := replycontextstore.InsertConstraintFixtureForTest(ctx, tx, backend.name == "postgres", record)
						return err
					})
				} else {
					err = store.CreateReplyContext(ctx, record)
				}
				if err != nil {
					t.Fatal(err)
				}
				got, err := store.LoadReplyContext(ctx, record.ID)
				if err != nil || !reflect.DeepEqual(got, record.Normalized()) {
					t.Fatalf("read exact context: got=%+v err=%v want=%+v", got, err, record)
				}
			}
			first := create(t, uuid.NewString())
			requireCreated(t, first)
			t.Run("no_join_keyed_source_and_child", func(t *testing.T) { requireCreated(t, create(t, uuid.NewString())) })
			t.Run("no_join_keyed_sibling_forks", func(t *testing.T) { requireCreated(t, create(t, uuid.NewString())) })
			t.Run("same_run_exact_idempotence", func(t *testing.T) { requireCreated(t, first) })
			t.Run("same_run_distinct_request_collision", func(t *testing.T) {
				collision := create(t, first.RunID)
				if err := store.CreateReplyContext(ctx, collision); err == nil {
					t.Fatal("same-run distinct request reused correlation authority")
				}
				if _, err := store.LoadReplyContext(ctx, collision.ID); !errors.Is(err, runtimereplycontext.ErrNotFound) {
					t.Fatalf("collision leaked context: %v", err)
				}
				requireCreated(t, first)
			})
			runless := create(t, "")
			t.Run("runful_vs_runless_independent_scope", func(t *testing.T) { requireCreated(t, runless) })
			t.Run("runless_exact_idempotence", func(t *testing.T) { requireCreated(t, runless) })
			t.Run("runless_distinct_request_collision", func(t *testing.T) {
				collision := create(t, "")
				var inserted bool
				if err := runUnrevisionedEventFixtureTransactionForTest(ctx, store, func(ctx context.Context, tx *sql.Tx) (err error) {
					inserted, err = replycontextstore.InsertConstraintFixtureForTest(ctx, tx, backend.name == "postgres", collision)
					return err
				}); err != nil || inserted {
					t.Fatal("runless distinct request reused correlation authority")
				}
				if _, err := store.LoadReplyContext(ctx, collision.ID); !errors.Is(err, runtimereplycontext.ErrNotFound) {
					t.Fatalf("runless collision leaked context: %v", err)
				}
				requireCreated(t, runless)
			})
		})
	}
}

func seedReplyRunlessPhysicalEvent(t *testing.T, ctx context.Context, store replyContextStoreTestSurface, id string) {
	t.Helper()
	// The ordinary publication path gives standalone runtime work its own run.
	// A direct diagnostic is the existing genuinely runless physical control.
	event := eventtest.DiagnosticDirect(id, events.EventTypePlatformRuntimeLog, "runtime", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())
	event, err := bindSemanticEventFixturePayload(event)
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := events.AdmitForPersistence(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil || admitted.Event().RunID() != "" {
		t.Fatalf("direct diagnostic acquired invented run: %q %v", admitted.Event().RunID(), err)
	}
	record, err := eventrecord.FromAdmitted(admitted, testRouteSettlement(admitted.Event(), nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := runUnrevisionedEventFixtureTransactionForTest(ctx, store, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		if _, postgres := store.(*PostgresStore); postgres {
			_, err = eventrecordpostgres.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		} else {
			_, err = eventrecordsqlite.InsertUnrevisionedFixtureRecord(ctx, tx, record)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	persisted, found, err := ReadCanonicalEventRecordForTest(ctx, store, id)
	if err != nil || !found || persisted.RunID() != "" {
		t.Fatalf("runless physical control must exist with absent run: found=%v run=%q err=%v", found, persisted.RunID(), err)
	}
}
