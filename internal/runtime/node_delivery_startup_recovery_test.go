package runtime_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type nodeDeliveryRecoveryStore interface {
	runtimebus.EventStore
	runtimedelivery.Store
	runtimepipeline.RuntimeMutationRunner
}

type renewalTrackingDeliveryStore struct {
	runtimedelivery.Store
	renewals atomic.Int64
}

func (s *renewalTrackingDeliveryStore) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.Snapshot, error) {
	s.renewals.Add(1)
	return s.Store.RenewClaim(ctx, claim)
}

func TestPipelineCoordinatorRecoverNodeDeliveriesUsesCanonicalSelectedStoreOwner(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				ctx := seedRuntimeTestRun(t, db)
				return ctx, db, storetest.AdmitPostgresRuntimeStore(t, db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				if _, err := selected.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, templateInstanceDeliveryRunID); err != nil {
					t.Fatalf("seed SQLite recovery run: %v", err)
				}
				return ctx, selected.DB, selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, db, selected := backend.setup(t)
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
			if backend.name == "sqlite" {
				workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, selected)
			}
			if err := workflowStore.Upsert(ctx, artifactActionResultWorkflowInstance()); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			deliveryOwner := &renewalTrackingDeliveryStore{Store: selected}
			pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
				Module:        newRuntimeTestWorkflowModule(t, source),
				WorkflowStore: workflowStore,
				DeliveryStore: deliveryOwner,
			})

			eventID := "99999999-9999-4999-8999-999999999981"
			target := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID}
			event := eventtest.RunCreatingRootIngress(
				eventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				"",
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				time.Now().UTC(),
			)
			route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "repo-scaffold-node", Target: target}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)

			if err := pc.RecoverNodeDeliveries(ctx); err != nil {
				t.Fatalf("RecoverNodeDeliveries: %v", err)
			}
			assertRecoveredNodeDelivery(t, ctx, selected, eventID, route, 1)
			if got := deliveryOwner.renewals.Load(); got < 2 {
				t.Fatalf("claim renewals = %d, want immediate and final handler renewal", got)
			}
			if err := pc.RecoverNodeDeliveries(ctx); err != nil {
				t.Fatalf("repeat RecoverNodeDeliveries: %v", err)
			}
			assertRecoveredNodeDelivery(t, ctx, selected, eventID, route, 1)
		})
	}
}

func TestPipelineCoordinatorStandingRecoveryClaimsNewlyEligibleNodeDeliveries(t *testing.T) {
	for _, backend := range []struct {
		name  string
		setup func(*testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore)
	}{
		{
			name: "postgres",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				ctx := seedRuntimeTestRun(t, db)
				return ctx, db, storetest.AdmitPostgresRuntimeStore(t, db)
			},
		},
		{
			name: "sqlite",
			setup: func(t *testing.T) (context.Context, *sql.DB, nodeDeliveryRecoveryStore) {
				selected := storetest.StartSQLiteRuntimeStore(t)
				ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), templateInstanceDeliveryRunID)
				if _, err := selected.DB.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES (?, 'running')`, templateInstanceDeliveryRunID); err != nil {
					t.Fatalf("seed SQLite standing recovery run: %v", err)
				}
				return ctx, selected.DB, selected
			},
		},
	} {
		t.Run(backend.name, func(t *testing.T) {
			ctx, db, selected := backend.setup(t)
			bundle := loadRuntimeTempBundle(t, artifactActionResultDeliveryFixtureFiles())
			source := semanticview.Wrap(bundle)
			bus, err := newScopedTestEventBus(t, selected, runtimebus.EventBusOptions{ContractBundle: source})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
			if backend.name == "sqlite" {
				workflowStore = runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(db, selected)
			}
			if err := workflowStore.Upsert(ctx, artifactActionResultWorkflowInstance()); err != nil {
				t.Fatalf("seed workflow instance: %v", err)
			}
			handlerStarted := make(chan struct{}, 4)
			deliveryOwner := &renewalTrackingDeliveryStore{Store: selected}
			pc := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, runtimepipeline.PipelineCoordinatorOptions{
				WorkOwner:     runtimeTestEventBusWorkOwner(t, bus),
				Module:        newRuntimeTestWorkflowModule(t, source),
				WorkflowStore: workflowStore,
				DeliveryStore: deliveryOwner,
				TestWorkflowNodeHandlerStartHook: func(context.Context, string, events.Event) error {
					select {
					case handlerStarted <- struct{}{}:
					default:
					}
					return nil
				},
			})
			pc.SetTestMaintenanceInterval(5 * time.Millisecond)

			eventID := "99999999-9999-4999-8999-999999999982"
			target := events.RouteIdentity{FlowID: "repo-scaffold", FlowInstance: "repo-scaffold/inst-1", EntityID: artifactActionResultEntityID}
			event := eventtest.RunCreatingRootIngress(
				eventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				"",
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				time.Now().UTC(),
			)
			route := events.DeliveryRoute{SubscriberType: "node", SubscriberID: "repo-scaffold-node", Target: target}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
			claimed, err := selected.ClaimNodeDelivery(ctx, event, route)
			if err != nil {
				t.Fatalf("claim node delivery before retry: %v", err)
			}
			failure := runtimefailures.FromError(errors.New("retry later"), "node-recovery-test", "schedule_retry")
			retrying, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
				Disposition: runtimedelivery.FailureRetry,
				Failure:     &failure.Failure,
				RetryBase:   time.Hour,
			})
			if err != nil || retrying.Status != runtimedelivery.StatusFailed {
				t.Fatalf("schedule node retry = %#v, err=%v", retrying, err)
			}

			expiringEventID := "99999999-9999-4999-8999-999999999983"
			expiringEvent := eventtest.RunCreatingRootIngress(
				expiringEventID,
				"repo-scaffold/inst-1/repo_scaffold.repo_commit_succeeded",
				"test",
				"",
				[]byte(`{}`),
				0,
				templateInstanceDeliveryRunID,
				"",
				events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, artifactActionResultEntityID), target),
				time.Now().UTC(),
			)
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, expiringEvent, []events.DeliveryRoute{route}, runtimereplayclaim.CommittedReplayScopeSubscribed)
			expiringClaim, err := selected.ClaimNodeDelivery(ctx, expiringEvent, route)
			if err != nil {
				t.Fatalf("claim node delivery before lease expiry: %v", err)
			}
			if err := pc.RecoverNodeDeliveries(ctx); err != nil {
				t.Fatalf("startup recovery before eligibility: %v", err)
			}

			maintenanceCtx, cancelMaintenance := context.WithCancel(ctx)
			maintenanceDone := make(chan struct{})
			go func() {
				defer close(maintenanceDone)
				pc.RunMaintenance(maintenanceCtx)
			}()
			defer func() {
				cancelMaintenance()
				select {
				case <-maintenanceDone:
				case <-time.After(time.Second):
					t.Errorf("standing recovery did not stop after cancellation")
				}
			}()
			makeNodeDeliveryImmediatelyEligible(t, maintenanceCtx, db, backend.name == "postgres", retrying.DeliveryID)
			expireNodeDeliveryClaim(t, maintenanceCtx, db, backend.name == "postgres", expiringClaim.Snapshot.DeliveryID)
			for recovered := 0; recovered < 2; recovered++ {
				select {
				case <-handlerStarted:
				case <-time.After(2 * time.Second):
					t.Fatalf("standing recovery started %d handlers, want retry-eligible and expired-claim handlers", recovered)
				}
			}
			waitForRecoveredNodeDelivery(t, ctx, selected, eventID, route, 2)
			waitForRecoveredNodeDelivery(t, ctx, selected, expiringEventID, route, 1)
			assertExpiredNodeDeliveryAttemptHistory(t, maintenanceCtx, db, backend.name == "postgres", expiringClaim.Snapshot.DeliveryID)
			if got := deliveryOwner.renewals.Load(); got < 4 {
				t.Fatalf("standing recovery claim renewals = %d, want immediate and final renewal for two handlers", got)
			}
		})
	}
}

func expireNodeDeliveryClaim(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, deliveryID string) {
	t.Helper()
	transaction, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin node claim expiry: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()
	startedAt := time.Now().Add(-2 * time.Hour).UTC()
	expiresAt := time.Now().Add(-time.Hour).UTC()
	deliveryQuery := `UPDATE event_deliveries SET created_at = $1, started_at = $1, updated_at = $2, claim_expires_at = $2 WHERE delivery_id = $3::uuid AND status = 'in_progress'`
	attemptQuery := `UPDATE event_delivery_attempts SET started_at = $1, lease_expires_at = $2 WHERE delivery_id = $3::uuid AND outcome IS NULL`
	if !postgres {
		deliveryQuery = `UPDATE event_deliveries SET created_at = ?, started_at = ?, updated_at = ?, claim_expires_at = ? WHERE delivery_id = ? AND status = 'in_progress'`
		attemptQuery = `UPDATE event_delivery_attempts SET started_at = ?, lease_expires_at = ? WHERE delivery_id = ? AND outcome IS NULL`
	}
	deliveryArgs := []any{startedAt, expiresAt, deliveryID}
	if !postgres {
		deliveryArgs = []any{startedAt, startedAt, expiresAt, expiresAt, deliveryID}
	}
	if result, execErr := transaction.ExecContext(ctx, deliveryQuery, deliveryArgs...); execErr != nil {
		t.Fatalf("expire node delivery claim: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("expire node delivery claim affected %d rows, err=%v", rows, rowsErr)
	}
	if result, execErr := transaction.ExecContext(ctx, attemptQuery, startedAt, expiresAt, deliveryID); execErr != nil {
		t.Fatalf("expire node delivery attempt: %v", execErr)
	} else if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("expire node delivery attempt affected %d rows, err=%v", rows, rowsErr)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit node claim expiry: %v", err)
	}
}

func assertExpiredNodeDeliveryAttemptHistory(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, deliveryID string) {
	t.Helper()
	query := `SELECT claim_version, outcome FROM event_delivery_attempts WHERE delivery_id = $1::uuid ORDER BY claim_version`
	if !postgres {
		query = `SELECT claim_version, outcome FROM event_delivery_attempts WHERE delivery_id = ? ORDER BY claim_version`
	}
	rows, err := db.QueryContext(ctx, query, deliveryID)
	if err != nil {
		t.Fatalf("load recovered node attempt history: %v", err)
	}
	defer rows.Close()
	var attempts []struct {
		version int64
		outcome string
	}
	for rows.Next() {
		var attempt struct {
			version int64
			outcome string
		}
		if err := rows.Scan(&attempt.version, &attempt.outcome); err != nil {
			t.Fatalf("scan recovered node attempt: %v", err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read recovered node attempts: %v", err)
	}
	if len(attempts) != 2 || attempts[0].version != 1 || attempts[0].outcome != "lease_expired" || attempts[1].version != 2 || attempts[1].outcome != "delivered" {
		t.Fatalf("recovered node attempts = %#v, want lease_expired then delivered", attempts)
	}
}

func makeNodeDeliveryImmediatelyEligible(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, deliveryID string) {
	t.Helper()
	query := `UPDATE event_deliveries SET next_eligible_at = $1 WHERE delivery_id = $2::uuid AND status = 'failed'`
	if !postgres {
		query = `UPDATE event_deliveries SET next_eligible_at = ? WHERE delivery_id = ? AND status = 'failed'`
	}
	result, err := db.ExecContext(ctx, query, time.Now().Add(-time.Hour).UTC(), deliveryID)
	if err != nil {
		t.Fatalf("make node delivery immediately eligible: %v", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		t.Fatalf("make node delivery eligible affected %d rows, err=%v", rows, rowsErr)
	}
}

func waitForRecoveredNodeDelivery(t *testing.T, ctx context.Context, selected runtimedelivery.Store, eventID string, route events.DeliveryRoute, wantOutcomes int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		proof, err := selected.ProveHandoff(ctx, eventID, route)
		if err != nil {
			t.Fatalf("ProveHandoff: %v", err)
		}
		snapshot, err := selected.Snapshot(ctx, proof.DeliveryID())
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		if snapshot.Status == runtimedelivery.StatusDelivered {
			assertRecoveredNodeDelivery(t, ctx, selected, eventID, route, wantOutcomes)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("standing recovery status = %q, want delivered", snapshot.Status)
		}
		<-ticker.C
	}
}

func assertRecoveredNodeDelivery(t *testing.T, ctx context.Context, selected runtimedelivery.Store, eventID string, route events.DeliveryRoute, wantOutcomes int) {
	t.Helper()
	proof, err := selected.ProveHandoff(ctx, eventID, route)
	if err != nil {
		t.Fatalf("ProveHandoff: %v", err)
	}
	snapshot, err := selected.Snapshot(ctx, proof.DeliveryID())
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snapshot.Status != runtimedelivery.StatusDelivered {
		t.Fatalf("recovered delivery status = %q, want delivered", snapshot.Status)
	}
	outcomes, err := selected.Outcomes(ctx, snapshot.DeliveryID)
	if err != nil {
		t.Fatalf("Outcomes: %v", err)
	}
	if len(outcomes) != wantOutcomes {
		t.Fatalf("recovered delivery outcomes = %d, want %d: %#v", len(outcomes), wantOutcomes, outcomes)
	}
}
