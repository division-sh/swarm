package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/tools"
	deliveryowner "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/google/uuid"
)

// Only scheduling is replaced: the real owner must authorize and lock the
// receiver before this barrier leaves the outer claim transaction open.
type receiverGrantClaimBarrier struct {
	owner   deliveryowner.ReceiverExecutionAdmission
	ready   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *receiverGrantClaimBarrier) ReceiverExecutionReadyTx(ctx context.Context, tx *sql.Tx, route events.DeliveryRoute, authority deliverylifecycle.ExecutionAuthority, claim bool) (bool, error) {
	ready, err := b.owner.ReceiverExecutionReadyTx(ctx, tx, route, authority, claim)
	if err != nil || !ready || !claim {
		return ready, err
	}
	b.once.Do(func() { close(b.ready) })
	select {
	case <-b.release:
		return ready, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func TestReceiverOrdinaryGrantRetirementFencesClaimBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base, selected := newAgentFixtureAuthorityStore(t, backend)
			ctx, cancel := context.WithTimeout(base, 20*time.Second)
			defer cancel()
			runID, entityID := uuid.NewString(), uuid.NewString()
			ctx = correlation.WithRunID(ctx, runID)
			identity := mustTestAgentIdentityForRun(runID, "grant-receiver", "global")
			if err := agentfixture.UpsertStatic(t, ctx, selected, agentFixtureStaticRecord(t, identity)); err != nil {
				t.Fatal(err)
			}
			plan := currentAgentFixtureSourceSet(t, ctx, selected)
			if len(plan.Sources) != 1 {
				t.Fatalf("fixture source census: %+v", plan.Sources)
			}
			grant, err := agentfixture.AdmitGeneration(t, ctx, selected, plan, plan.Sources[0])
			if err != nil {
				t.Fatal(err)
			}
			if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatal(err)
			}
			evidence, err := grant.AdmitExecution(ctx)
			if err != nil {
				t.Fatal(err)
			}
			state, found, err := selected.LoadAgentLifecycleState(ctx, identity)
			if err != nil || !found || state.ProcessBinding.GenerationGrantID != evidence.GrantID || evidence.SelectedFork != nil {
				t.Fatalf("exact ordinary lifecycle/grant: state=%+v grant=%+v found=%v err=%v", state, evidence, found, err)
			}
			writer := selected.(interface {
				CreateEntity(context.Context, tools.EntityCreateRecord) error
			})
			if err := writer.CreateEntity(ctx, tools.EntityCreateRecord{
				RunID: runID, EntityID: entityID, FlowInstance: identity.FlowInstance(),
				EntityType: "receiver", CurrentState: "active", FieldsJSON: []byte(`{}`), CreatedAt: time.Now().UTC(),
				Writer: tools.EntityMutationWriter{Type: "agent", ID: identity.AgentID(), HandlerStep: "grant_probe_setup"},
			}); err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{
				Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity,
				Target: events.MustMaterializingEntityTarget(events.RouteIdentity{FlowID: "global", FlowInstance: identity.FlowInstance(), EntityID: entityID}),
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.grant_receiver", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatal(err)
			}
			authority, err := deliverylifecycle.NewNormalExecutionAuthority(mustStoreTestSourceArtifactFact(evidence.BundleHash), evidence.GrantID, evidence.RuntimeGeneration)
			if err != nil {
				t.Fatal(err)
			}
			if err := selected.ActivateDeliveryAuthority(ctx, authority); err != nil {
				t.Fatal(err)
			}
			before := loadDeliverySnapshotFixture(t, ctx, selected, event.ID(), route)
			if before.Status != deliverylifecycle.StatusPending || before.ClaimVersion != 0 || !before.Authority.Equal(authority) {
				t.Fatalf("initial exact pending obligation: %+v", before)
			}
			barrier := &receiverGrantClaimBarrier{ready: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(barrier.release) }) }
			defer release()
			var claimStore deliverylifecycle.Store
			var db *sql.DB
			switch store := selected.(type) {
			case *PostgresStore:
				barrier.owner = store.agentPostgresOwner
				owner, err := deliveryowner.NewDeliveryPostgresOwner(store.deliveryPostgresOwner.DeadLetterPostgresOwner, store.runLifecyclePostgresOwner, barrier)
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.BindReceiverTargetPersistence(store.pipelinePostgresOwner); err != nil {
					t.Fatal(err)
				}
				claimStore, db = owner, store.backend.ConstructionHandle()
			case *SQLiteRuntimeStore:
				barrier.owner = store.agentSQLiteOwner
				owner, err := deliveryowner.NewDeliverySQLiteOwner(store.deliverySQLiteOwner.DeadLetterSQLiteOwner, store.runLifecycleSQLiteOwner, barrier, nil)
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.BindReceiverTargetPersistence(store.pipelineSQLiteOwner); err != nil {
					t.Fatal(err)
				}
				claimStore, db = owner, store.backend.ConstructionHandle()
			default:
				t.Fatalf("unsupported fixture %T", selected)
			}
			type claimOutcome struct {
				result deliverylifecycle.ClaimResult
				err    error
			}
			claimed := make(chan claimOutcome, 1)
			go func() {
				result, err := claimStore.ClaimDelivery(ctx, authority, event, route)
				claimed <- claimOutcome{result, err}
			}()
			select {
			case <-barrier.ready:
			case result := <-claimed:
				t.Fatalf("claim did not reach real authorized receiver barrier: %+v err=%v", result.result, result.err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// A bounded retirement call is deterministic with respect to the barrier:
			// no claim write or commit can occur until this call has returned.
			retireCtx, retireCancel := context.WithTimeout(ctx, time.Second)
			retireErr := grant.Retire(retireCtx)
			retireCancel()
			var durableGrantState string
			if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_generation_grants WHERE grant_id=$1 ORDER BY state_version DESC LIMIT 1`, evidence.GrantID).Scan(&durableGrantState); err != nil {
				t.Fatal(err)
			}
			release()
			var outcome claimOutcome
			select {
			case outcome = <-claimed:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if outcome.err != nil {
				t.Fatalf("claim: %v", outcome.err)
			}
			after := loadDeliverySnapshotFixture(t, ctx, selected, event.ID(), route)
			var attempts int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_delivery_attempts WHERE delivery_id=$1`, before.DeliveryID).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			t.Logf("backend=%s grant=%s run=%s delivery=%s retireWhileClaimPaused=%v durableGrantBeforeRelease=%s claim=%s status=%s claimVersion=%d attempts=%d", backend, evidence.GrantID, runID, before.DeliveryID, retireErr, durableGrantState, outcome.result.Disposition, after.Status, after.ClaimVersion, attempts)
			if retireErr == nil {
				if durableGrantState != string(startupownership.GrantRetired) {
					t.Fatalf("retirement returned before durable fact: %s", durableGrantState)
				}
				if _, acquired := outcome.result.Acquired(); acquired || after.ClaimVersion != before.ClaimVersion || attempts != 0 {
					t.Errorf("retired ordinary grant admitted a new durable claim after retirement committed: disposition=%s claimVersion=%d attempts=%d", outcome.result.Disposition, after.ClaimVersion, attempts)
				}
			} else {
				if !errors.Is(retireErr, context.DeadlineExceeded) {
					t.Fatalf("unexpected retirement error: %v", retireErr)
				}
				if durableGrantState != string(startupownership.GrantAdmitted) {
					t.Fatalf("blocked retirement changed grant: %s", durableGrantState)
				}
				if _, acquired := outcome.result.Acquired(); !acquired || after.ClaimVersion != 1 || attempts != 1 {
					t.Fatalf("live fenced claim failed: %+v attempts=%d", outcome.result, attempts)
				}
				if err := grant.Retire(ctx); err != nil {
					t.Fatalf("retire after claim commit: %v", err)
				}
			}
		})
	}
}
