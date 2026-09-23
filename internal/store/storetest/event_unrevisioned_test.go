package storetest

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestUnrevisionedSemanticEventFixtureMatchesCanonicalMutationProjection(t *testing.T) {
	for _, backend := range []struct {
		name    string
		dialect authoractivityfixture.Dialect
		open    func(*testing.T) (runLifecycleOperationRunner, *sql.DB)
	}{
		{"sqlite", authoractivityfixture.DialectSQLite, func(t *testing.T) (runLifecycleOperationRunner, *sql.DB) {
			selected := StartSQLiteRuntimeStore(t)
			return selected, DatabaseForTest(selected)
		}},
		{"postgres", authoractivityfixture.DialectPostgres, func(t *testing.T) (runLifecycleOperationRunner, *sql.DB) {
			_, db, _ := testutil.StartPostgres(t)
			return AdmitPostgresRuntimeStore(t, db), db
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			rawStore, rawDB := backend.open(t)
			canonicalStore, canonicalDB := backend.open(t)
			runID, eventID := uuid.NewString(), uuid.NewString()
			at := time.Now().UTC().Truncate(time.Microsecond)
			for _, selected := range []runLifecycleOperationRunner{rawStore, canonicalStore} {
				RequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin(), StartedAt: at.Add(-time.Minute)})
			}
			node, err := runtimeidentity.ParseExecutableNode("flow_a", "fixture-node")
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "flow_a", FlowInstance: "flow_a"}),
			}
			name, err := agentidentity.DeclaredName("fixture-agent", "fixture-owner")
			if err != nil {
				t.Fatal(err)
			}
			identity, err := agentidentity.New(runID, name, agentidentity.RootRoute())
			if err != nil {
				t.Fatal(err)
			}
			routes := []events.DeliveryRoute{route, {
				Recipient:     events.MustAgentDeliveryRecipient("fixture-agent"),
				AgentIdentity: identity,
			}}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID, "fixture.ready", "fixture", "", []byte(`{"value":1}`), 0, runID,
				events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at,
			)
			beforeRevision := fixtureRevisionCount(t, rawDB, runID)
			if got := CommitSemanticEventWithRoutes(t, ctx, rawStore, event, routes, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendInserted {
				t.Fatalf("raw fixture outcome = %v, want inserted", got)
			}
			if got := fixtureRevisionCount(t, rawDB, runID); got != beforeRevision {
				t.Fatalf("raw fixture revisions = %d, want unchanged %d", got, beforeRevision)
			}

			bound, err := eventfixture.BindPayload(event)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := events.AdmitForPublish(bound, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil {
				t.Fatal(err)
			}
			record, err := eventrecord.FromAdmitted(admitted, canonicalFixtureSettlement(t, admitted.Event(), routes))
			if err != nil {
				t.Fatal(err)
			}
			adapter, err := deliveryadapter.NewAdapter(deliveryadapter.Dialect(backend.dialect))
			if err != nil {
				t.Fatal(err)
			}
			canonicalRevision := fixtureRevisionCount(t, canonicalDB, runID)
			err = runCanonicalEventMutation(ctx, canonicalDB, backend.dialect, func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				var inserted bool
				var err error
				if backend.dialect == authoractivityfixture.DialectPostgres {
					inserted, err = eventrecordpostgres.Insert(txctx, attempt, record)
				} else {
					inserted, err = eventrecordsqlite.Insert(txctx, attempt, record)
				}
				if err != nil || !inserted {
					return err
				}
				var authority runtimedelivery.ExecutionAuthority
				if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					var err error
					authority, err = deliveryFixtureAuthority(txctx, tx, runID, adapter)
					return err
				}); err != nil {
					return err
				}
				if _, err := adapter.CommitInitial(txctx, attempt, eventID, runID, routes, authority); err != nil {
					return err
				}
				return attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
					return insertPipelineScopeFixture(txctx, tx, eventID, runtimepipelineobligation.ScopeSubscribed, backend.dialect == authoractivityfixture.DialectPostgres, at)
				})
			})
			if err != nil {
				t.Fatalf("canonical mutation fixture: %v", err)
			}
			if got := fixtureRevisionCount(t, canonicalDB, runID); got <= canonicalRevision {
				t.Fatalf("canonical mutation revisions = %d, want after %d", got, canonicalRevision)
			}

			rawRecord := fixtureLoadedEventRecord(t, ctx, rawDB, backend.dialect, eventID)
			canonicalRecord := fixtureLoadedEventRecord(t, ctx, canonicalDB, backend.dialect, eventID)
			if !rawRecord.Equal(canonicalRecord) {
				t.Fatalf("unrevisioned event record differs from canonical mutation projection: raw=%#v canonical=%#v", rawRecord, canonicalRecord)
			}
			for _, selectedRoute := range routes {
				deliveryID, err := runtimedelivery.DeliveryID(eventID, selectedRoute)
				if err != nil {
					t.Fatal(err)
				}
				if raw, canonical := fixtureDeliveryProjection(t, rawDB, deliveryID), fixtureDeliveryProjection(t, canonicalDB, deliveryID); !reflect.DeepEqual(raw, canonical) {
					t.Fatalf("unrevisioned delivery projection differs from canonical mutation: raw=%#v canonical=%#v", raw, canonical)
				}
			}
			if got := CommitSemanticEventWithRoutes(t, ctx, rawStore, event, routes, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendExactDuplicate {
				t.Fatalf("exact duplicate outcome = %v, want exact duplicate", got)
			}
			if got := fixtureRevisionCount(t, rawDB, runID); got != beforeRevision {
				t.Fatalf("duplicate fixture revisions = %d, want unchanged %d", got, beforeRevision)
			}
			if got := fixtureDeliveryCount(t, rawDB, eventID); got != len(routes) {
				t.Fatalf("duplicate fixture delivery count = %d, want %d", got, len(routes))
			}
		})
	}
}

func TestUnrevisionedSemanticDeliveryFixtureImmediateTerminalization(t *testing.T) {
	type terminalizingStore interface {
		runLifecycleOperationRunner
		TerminalizeRun(context.Context, string, string) ([]runtimedelivery.Terminalization, error)
		Snapshot(context.Context, string) (runtimedelivery.Snapshot, error)
	}
	for _, backend := range []struct {
		name string
		open func(*testing.T) terminalizingStore
	}{
		{"sqlite", func(t *testing.T) terminalizingStore { return StartSQLiteRuntimeStore(t) }},
		{"postgres", func(t *testing.T) terminalizingStore {
			_, db, _ := testutil.StartPostgres(t)
			return AdmitPostgresRuntimeStore(t, db)
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx := semanticFixtureContext(context.Background(), sourceartifactfixture.Fact())
			selected := backend.open(t)
			runID, eventID := uuid.NewString(), uuid.NewString()
			at := time.Now().UTC()
			RequireRun(t, ctx, selected, RunFixture{RunID: runID, Origin: ScenarioSetupOrigin(), StartedAt: at.Add(-time.Minute)})
			node, err := runtimeidentity.ParseExecutableNode("flow_a", "fixture-node")
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "flow_a", FlowInstance: "flow_a"}),
			}
			event := eventtest.ExistingRunRootIngressWithRoutingSource(
				eventID, "fixture.ready", "fixture", "", []byte(`{"value":1}`), 0, runID,
				events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at,
			)
			if got := CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed); got != runtimebus.EventAppendInserted {
				t.Fatalf("fixture outcome = %v, want inserted", got)
			}
			deliveryID, err := runtimedelivery.DeliveryID(eventID, route)
			if err != nil {
				t.Fatal(err)
			}
			before, err := selected.Snapshot(ctx, deliveryID)
			if err != nil {
				t.Fatal(err)
			}
			if backend.name == "sqlite" && before.CreatedAt.Nanosecond()%int(time.Millisecond) != 0 {
				t.Fatalf("SQLite fixture created_at = %s, want database-clock millisecond precision", before.CreatedAt)
			}
			transitions, err := selected.TerminalizeRun(ctx, runID, "run_terminal")
			if err != nil {
				t.Fatalf("immediate terminalization: %v", err)
			}
			if len(transitions) != 1 || transitions[0].Current.DeliveryID != deliveryID || transitions[0].Current.Status != runtimedelivery.StatusDeadLetter {
				t.Fatalf("terminalization transitions = %#v, want one dead-lettered delivery", transitions)
			}
			if transitions[0].Current.UpdatedAt.Before(before.CreatedAt) {
				t.Fatalf("terminalized updated_at %s precedes created_at %s", transitions[0].Current.UpdatedAt, before.CreatedAt)
			}
		})
	}
}

func fixtureRevisionCount(t *testing.T, db *sql.DB, runID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM run_fork_revisions WHERE run_id = $1`, runID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func fixtureDeliveryCount(t *testing.T, db *sql.DB, eventID string) int {
	t.Helper()
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1`, eventID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func fixtureLoadedEventRecord(t *testing.T, ctx context.Context, db *sql.DB, dialect authoractivityfixture.Dialect, eventID string) eventrecord.Record {
	t.Helper()
	var record eventrecord.Record
	var found bool
	var err error
	if dialect == authoractivityfixture.DialectPostgres {
		record, found, err = eventrecordpostgres.Load(ctx, db, eventID)
	} else {
		record, found, err = eventrecordsqlite.Load(ctx, db, eventID)
	}
	if err != nil || !found {
		t.Fatalf("load event record: found=%v err=%v", found, err)
	}
	return record
}

func fixtureDeliveryProjection(t *testing.T, db *sql.DB, deliveryID string) []string {
	t.Helper()
	var values [18]string
	args := make([]any, len(values))
	for i := range values {
		args[i] = &values[i]
	}
	if err := db.QueryRow(`
		SELECT route_identity, subscriber_type, subscriber_id,
			agent_name_owner, agent_name_source, agent_route_presence,
			agent_flow_scope_key, agent_flow_instance_id, agent_flow_instance_path,
			CAST(delivery_target_route AS TEXT), CAST(delivery_context AS TEXT),
			CAST(delivery_payload_projection AS TEXT), CAST(connect_execution_claim AS TEXT),
			CAST(receiver_materialization_plan AS TEXT), execution_authority_kind,
			authority_bundle_hash, execution_authority_id, CAST(execution_authority_generation AS TEXT)
		FROM event_deliveries WHERE delivery_id = $1`, deliveryID).Scan(args...); err != nil {
		t.Fatal(err)
	}
	return values[:]
}
