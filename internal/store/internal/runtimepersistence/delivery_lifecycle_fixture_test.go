package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	deliveryadapter "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

func postgresDeliveryFixtureStore(db *sql.DB) *PostgresStore {
	store := newPostgresStoreWithBackend(mustPostgresBackend(db))
	store.acceptCurrentSchemaForTest()
	return store
}

func loadPostgresDeliveryFixtureEvent(t testing.TB, ctx context.Context, db *sql.DB, eventID string) events.Event {
	t.Helper()
	record, found, err := eventrecordpostgres.Load(ctx, db, eventID)
	if err != nil {
		t.Fatalf("load delivery fixture event %s: %v", eventID, err)
	}
	if !found {
		t.Fatalf("load delivery fixture event %s: not found", eventID)
	}
	admitted, err := record.Decode()
	if err != nil {
		t.Fatalf("decode delivery fixture event %s: %v", eventID, err)
	}
	return admitted.Event()
}

func loadSQLiteDeliveryFixtureEvent(t testing.TB, ctx context.Context, db *sql.DB, eventID string) events.Event {
	t.Helper()
	record, found, err := eventrecordsqlite.Load(ctx, db, eventID)
	if err != nil {
		t.Fatalf("load delivery fixture event %s: %v", eventID, err)
	}
	if !found {
		t.Fatalf("load delivery fixture event %s: not found", eventID)
	}
	admitted, err := record.Decode()
	if err != nil {
		t.Fatalf("decode delivery fixture event %s: %v", eventID, err)
	}
	return admitted.Event()
}

func commitPostgresDeliveryFixture(t testing.TB, ctx context.Context, db *sql.DB, eventID string, route events.DeliveryRoute) events.Event {
	t.Helper()
	route = canonicalDeliveryFixtureRoute(t, route)
	event := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
	store := postgresDeliveryFixtureStore(db)
	if err := store.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		authority, err := deliveryFixtureAuthorityForRun(txctx, tx, deliveryadapter.DialectPostgres, event.RunID())
		if err != nil {
			return err
		}
		if _, err = postgresDeliveryAdapter.CommitInitial(txctx, tx, event.ID(), event.RunID(), []events.DeliveryRoute{route}, authority); err != nil {
			return err
		}
		_, err = finalizePostgresRunForkTestRevision(txctx, tx, event.RunID(), runforkrevision.FamilyEventDeliveries)
		return err
	}); err != nil {
		t.Fatalf("commit delivery fixture %s/%s: %v", eventID, route.Recipient.ID(), err)
	}
	return event
}

func claimPostgresDeliveryFixture(t testing.TB, ctx context.Context, db *sql.DB, event events.Event, route events.DeliveryRoute) runtimedelivery.ClaimedObligation {
	t.Helper()
	route = canonicalDeliveryFixtureRoute(t, route)
	store := postgresDeliveryFixtureStore(db)
	claimed, err := claimDeliveryFixture(ctx, store, event, route)
	if err != nil {
		t.Fatalf("claim delivery fixture %s/%s: %v", event.ID(), route.Recipient.ID(), err)
	}
	return claimed
}

type deliveryFixtureStore interface{ runtimedelivery.Store }

func seedAgentDeliveryStateFixture(
	t testing.TB,
	ctx context.Context,
	store deliveryFixtureStore,
	event events.Event,
	route events.DeliveryRoute,
	state runtimedelivery.State,
	failure *runtimefailures.Envelope,
) runtimedelivery.Snapshot {
	return seedDeliveryStateFixture(t, ctx, store, event, route, state, failure)
}

func seedDeliveryStateFixture(
	t testing.TB,
	ctx context.Context,
	store deliveryFixtureStore,
	event events.Event,
	route events.DeliveryRoute,
	state runtimedelivery.State,
	failure *runtimefailures.Envelope,
) runtimedelivery.Snapshot {
	t.Helper()
	route = canonicalDeliveryFixtureRoute(t, route)
	if err := commitDeliveryObligationFixture(ctx, store, event, route); err != nil {
		t.Fatalf("commit delivery fixture %s/%s: %v", event.ID(), route.Recipient.ID(), err)
	}
	if state == runtimedelivery.StateQueued {
		return loadDeliverySnapshotFixture(t, ctx, store, event.ID(), route)
	}
	claimed, err := claimDeliveryFixture(ctx, store, event, route)
	if err != nil {
		t.Fatalf("claim delivery fixture %s/%s: %v", event.ID(), route.Recipient.ID(), err)
	}
	switch state {
	case runtimedelivery.StateLaunching:
		return claimed.Snapshot
	case runtimedelivery.StateRetrying:
		if failure == nil {
			t.Fatal("retrying delivery fixture requires a failure")
		}
		snapshot, err := store.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureRetry,
			Failure:     failure,
			RetryBase:   time.Hour, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
		})
		if err != nil {
			t.Fatalf("settle retrying delivery fixture %s/%s: %v", event.ID(), route.Recipient.ID(), err)
		}
		return snapshot
	case runtimedelivery.StateDelivered:
		snapshot, err := store.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond, runtimedelivery.NotApplicableHandlerRuleSelection())
		if err != nil {
			t.Fatalf("settle delivered fixture %s/%s: %v", event.ID(), route.Recipient.ID(), err)
		}
		return snapshot
	case runtimedelivery.StateExhausted:
		if failure == nil {
			t.Fatal("exhausted delivery fixture requires a failure")
		}
		snapshot, err := store.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
			Disposition: runtimedelivery.FailureDeadLetter,
			ReasonCode:  failure.Detail.Code,
			Failure:     failure, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
		})
		if err != nil {
			t.Fatalf("settle exhausted delivery fixture %s/%s: %v", event.ID(), route.Recipient.ID(), err)
		}
		return snapshot
	default:
		t.Fatalf("agent delivery fixture state %q is unsupported", state)
		return runtimedelivery.Snapshot{}
	}
}

func canonicalDeliveryFixtureRoute(t testing.TB, route events.DeliveryRoute) events.DeliveryRoute {
	t.Helper()
	return canonicalDeliveryFixtureRouteValue(route)
}

func canonicalDeliveryFixtureRouteValue(route events.DeliveryRoute) events.DeliveryRoute {
	route = route.Normalized()
	if route.Recipient.IsAgent() && route.AgentIdentity.IsZero() {
		route.AgentIdentity = mustTestAgentIdentity(route.Recipient.ID(), "fixture/"+route.Recipient.ID())
	}
	return route
}

func testEntitylessNodeDeliveryRoute(nodeID string) events.DeliveryRoute {
	return events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode(nodeID)),
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{
			FlowID: "fixture", FlowInstance: "fixture/" + strings.TrimSpace(nodeID),
		}),
	}
}

func commitAgentDeliveryObligationFixture(ctx context.Context, store deliveryFixtureStore, event events.Event, route events.DeliveryRoute) error {
	return commitDeliveryObligationFixture(ctx, store, event, route)
}

func commitDeliveryObligationFixture(ctx context.Context, store deliveryFixtureStore, event events.Event, route events.DeliveryRoute) error {
	route = canonicalDeliveryFixtureRouteValue(route)
	switch selected := store.(type) {
	case *PostgresStore:
		return selected.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			authority, err := deliveryFixtureAuthorityForRun(txctx, tx, deliveryadapter.DialectPostgres, event.RunID())
			if err != nil {
				return err
			}
			if _, err = postgresDeliveryAdapter.CommitInitial(txctx, tx, event.ID(), event.RunID(), []events.DeliveryRoute{route}, authority); err != nil {
				return err
			}
			_, err = finalizePostgresRunForkTestRevision(txctx, tx, event.RunID(), runforkrevision.FamilyEventDeliveries)
			return err
		})
	case *SQLiteRuntimeStore:
		return selected.runEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
			authority, err := deliveryFixtureAuthorityForRun(txctx, tx, deliveryadapter.DialectSQLite, event.RunID())
			if err != nil {
				return err
			}
			if _, err = sqliteDeliveryAdapter.CommitInitial(txctx, tx, event.ID(), event.RunID(), []events.DeliveryRoute{route}, authority); err != nil {
				return err
			}
			_, err = finalizeSQLiteRunForkTestRevision(txctx, tx, event.RunID(), runforkrevision.FamilyEventDeliveries)
			return err
		})
	default:
		return fmt.Errorf("delivery fixture store %T is unsupported", store)
	}
}

func deliveryFixtureAuthorityForRun(ctx context.Context, queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, dialect deliveryadapter.Dialect, runID string) (runtimedelivery.ExecutionAuthority, error) {
	query := `SELECT bundle_hash, bundle_source FROM runs WHERE run_id=$1::uuid`
	if dialect == deliveryadapter.DialectSQLite {
		query = `SELECT bundle_hash, bundle_source FROM runs WHERE run_id=?`
	}
	var bundleHash, bundleSource string
	if err := queryer.QueryRowContext(ctx, query, runID).Scan(&bundleHash, &bundleSource); err != nil {
		return runtimedelivery.ExecutionAuthority{}, fmt.Errorf("load delivery fixture run authority: %w", err)
	}
	source, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return runtimedelivery.ExecutionAuthority{}, err
	}
	return runtimedelivery.NewNormalExecutionAuthority(source, "delivery-fixture:"+runID, 1)
}

func claimDeliveryFixture(ctx context.Context, store deliveryFixtureStore, event events.Event, route events.DeliveryRoute) (runtimedelivery.ClaimedObligation, error) {
	route = canonicalDeliveryFixtureRouteValue(route)
	deliveryID, err := runtimedelivery.DeliveryID(event.ID(), route)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	snapshot, err := store.Snapshot(ctx, deliveryID)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	result, err := store.ClaimDelivery(ctx, snapshot.Authority, event, route)
	if err != nil {
		return runtimedelivery.ClaimedObligation{}, err
	}
	claimed, ok := result.Acquired()
	if !ok {
		return runtimedelivery.ClaimedObligation{}, fmt.Errorf("delivery %s was not acquired: %s", deliveryID, result.Disposition)
	}
	return claimed, nil
}

func loadAgentDeliverySnapshotFixture(t testing.TB, ctx context.Context, store deliveryFixtureStore, eventID string, route events.DeliveryRoute) runtimedelivery.Snapshot {
	return loadDeliverySnapshotFixture(t, ctx, store, eventID, route)
}

func loadDeliverySnapshotFixture(t testing.TB, ctx context.Context, store deliveryFixtureStore, eventID string, route events.DeliveryRoute) runtimedelivery.Snapshot {
	t.Helper()
	route = canonicalDeliveryFixtureRoute(t, route)
	var (
		snapshots []runtimedelivery.Snapshot
		err       error
	)
	switch selected := store.(type) {
	case *PostgresStore:
		snapshots, err = selected.DeliverySnapshotsForEvent(ctx, eventID)
	case *SQLiteRuntimeStore:
		snapshots, err = selected.DeliverySnapshotsForEvent(ctx, eventID)
	default:
		t.Fatalf("delivery fixture store %T is unsupported", store)
	}
	if err != nil {
		t.Fatalf("load delivery fixture snapshot %s: %v", eventID, err)
	}
	for _, snapshot := range snapshots {
		if events.SameDeliveryRouteIdentity(snapshot.Route, route) {
			return snapshot
		}
	}
	t.Fatalf("delivery fixture snapshot %s/%s was not found", eventID, route.Recipient.ID())
	return runtimedelivery.Snapshot{}
}

func deletePostgresDeliveryFixturesForRun(t testing.TB, ctx context.Context, db *sql.DB, runID string) {
	t.Helper()
	cleanupFailure := mustMarshalTestFailure(t, testFailureEnvelope(runtimefailures.ClassLifecycleConflict, "fixture_cleanup", nil))
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin delete delivery fixtures for run %s: %v", runID, err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, query := range []string{
		`UPDATE event_deliveries SET status = 'dead_letter', reason_code = 'fixture_cleanup', failure = $2::jsonb, next_eligible_at = NULL, current_attempt_version = NULL, current_attempt_open = NULL, settled_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress', 'failed')`,
		`DELETE FROM dead_letters WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = $1::uuid)`,
		`DELETE FROM event_delivery_outcomes WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = $1::uuid)`,
		`DELETE FROM event_delivery_attempts WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id = $1::uuid)`,
		`DELETE FROM event_deliveries WHERE run_id = $1::uuid`,
	} {
		args := []any{runID}
		if strings.Contains(query, "$2") {
			args = append(args, cleanupFailure)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("delete delivery fixtures for run %s: %v", runID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("delete delivery fixtures for run %s: %v", runID, err)
	}
}

func setPostgresDeliveryFixtureTimes(t testing.TB, ctx context.Context, db *sql.DB, snapshot runtimedelivery.Snapshot, createdAt, transitionAt time.Time) {
	t.Helper()
	createdAt = createdAt.UTC()
	transitionAt = transitionAt.UTC()
	if transitionAt.IsZero() {
		transitionAt = createdAt
	}
	var query string
	switch snapshot.Status {
	case runtimedelivery.StatusPending:
		query = `UPDATE event_deliveries SET created_at=$2::timestamptz, updated_at=$3::timestamptz, next_eligible_at=$3::timestamptz WHERE delivery_id=$1::uuid`
	case runtimedelivery.StatusInProgress:
		if _, err := db.ExecContext(ctx, `UPDATE event_delivery_attempts SET started_at=$2::timestamptz, lease_expires_at=$3::timestamptz + interval '5 minutes' WHERE delivery_id=$1::uuid AND open_marker=TRUE`, snapshot.DeliveryID, createdAt, transitionAt); err != nil {
			t.Fatalf("set delivery fixture %s attempt times: %v", snapshot.DeliveryID, err)
		}
		query = `UPDATE event_deliveries SET created_at=$2::timestamptz, updated_at=$3::timestamptz, started_at=$3::timestamptz WHERE delivery_id=$1::uuid`
	case runtimedelivery.StatusFailed:
		query = `UPDATE event_deliveries SET created_at=$2::timestamptz, updated_at=$3::timestamptz, started_at=$2::timestamptz, next_eligible_at=$3::timestamptz + interval '1 hour' WHERE delivery_id=$1::uuid`
	case runtimedelivery.StatusDelivered, runtimedelivery.StatusDeadLetter:
		query = `UPDATE event_deliveries SET created_at=$2::timestamptz, updated_at=$3::timestamptz, started_at=$2::timestamptz, settled_at=$3::timestamptz WHERE delivery_id=$1::uuid`
	default:
		t.Fatalf("delivery fixture %s has unsupported status %q", snapshot.DeliveryID, snapshot.Status)
	}
	if _, err := db.ExecContext(ctx, query, snapshot.DeliveryID, createdAt, transitionAt); err != nil {
		t.Fatalf("set delivery fixture %s times: %v", snapshot.DeliveryID, err)
	}
}

func setSQLiteDeliveryFixtureTimes(t testing.TB, ctx context.Context, db *sql.DB, snapshot runtimedelivery.Snapshot, createdAt, transitionAt time.Time) {
	t.Helper()
	createdAt = createdAt.UTC()
	transitionAt = transitionAt.UTC()
	if transitionAt.IsZero() {
		transitionAt = createdAt
	}
	var (
		query string
		args  []any
	)
	switch snapshot.Status {
	case runtimedelivery.StatusPending:
		query = `UPDATE event_deliveries SET created_at=?, updated_at=?, next_eligible_at=? WHERE delivery_id=?`
		args = []any{createdAt, transitionAt, transitionAt, snapshot.DeliveryID}
	case runtimedelivery.StatusInProgress:
		if _, err := db.ExecContext(ctx, `UPDATE event_delivery_attempts SET started_at=?, lease_expires_at=? WHERE delivery_id=? AND open_marker=TRUE`, createdAt, transitionAt.Add(5*time.Minute), snapshot.DeliveryID); err != nil {
			t.Fatalf("set delivery fixture %s attempt times: %v", snapshot.DeliveryID, err)
		}
		query = `UPDATE event_deliveries SET created_at=?, updated_at=?, started_at=? WHERE delivery_id=?`
		args = []any{createdAt, transitionAt, transitionAt, snapshot.DeliveryID}
	case runtimedelivery.StatusFailed:
		query = `UPDATE event_deliveries SET created_at=?, updated_at=?, started_at=?, next_eligible_at=? WHERE delivery_id=?`
		args = []any{createdAt, transitionAt, createdAt, transitionAt.Add(time.Hour), snapshot.DeliveryID}
	case runtimedelivery.StatusDelivered, runtimedelivery.StatusDeadLetter:
		query = `UPDATE event_deliveries SET created_at=?, updated_at=?, started_at=?, settled_at=? WHERE delivery_id=?`
		args = []any{createdAt, transitionAt, createdAt, transitionAt, snapshot.DeliveryID}
	default:
		t.Fatalf("delivery fixture %s has unsupported status %q", snapshot.DeliveryID, snapshot.Status)
	}
	if _, err := db.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("set delivery fixture %s times: %v", snapshot.DeliveryID, err)
	}
}
