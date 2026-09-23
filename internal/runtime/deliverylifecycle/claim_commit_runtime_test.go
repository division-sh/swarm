package deliverylifecycle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type renewalCleanupWrapper struct {
	runtimedelivery.Store
	cleanupErr   error
	missingAck   bool
	realRenewals int
}

func (w *renewalCleanupWrapper) RenewClaim(ctx context.Context, claim runtimedelivery.Claim) (runtimedelivery.ClaimCommit, error) {
	if w.missingAck {
		return runtimedelivery.ClaimCommit{}, nil
	}
	w.realRenewals++
	commit, err := w.Store.RenewClaim(ctx, claim)
	if err != nil || !commit.Acknowledged {
		return commit, err
	}
	return commit, w.cleanupErr
}

func TestClaimHeartbeatRealStoreAcknowledgedCleanupParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected runtimedelivery.Store
			if backend == "postgres" {
				_, db, _ := testutil.StartPostgres(t)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			} else {
				selected = storetest.StartSQLiteRuntimeStore(t)
			}
			source := sourceartifactfixture.Fact()
			ctx := runtimecorrelation.WithSourceArtifactFact(context.Background(), source)
			ctx = runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(uuid.NewString(), source.BundleHash()))
			runID := uuid.NewString()
			event := eventtest.RunCreatingRootIngress(
				uuid.NewString(), "delivery.claim.commit", "fixture", "", []byte(`{"ok":true}`), 0,
				runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			node, err := runtimeidentity.ParseExecutableNode("claim_commit", "fixture-node")
			if err != nil {
				t.Fatal(err)
			}
			route := events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "claim_commit", FlowInstance: "claim_commit"}),
			}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, runtimepipelineobligation.ScopeSubscribed)
			claimed, err := storetest.ClaimDelivery(ctx, selected, event, route)
			if err != nil {
				t.Fatal(err)
			}
			beforeMissingAck, err := selected.Snapshot(ctx, claimed.Snapshot.DeliveryID)
			if err != nil {
				t.Fatal(err)
			}
			process := worklifetime.NewProcess()
			owner, err := process.NewRuntime(ctx, worklifetime.RuntimeIdentity{RuntimeInstanceID: uuid.NewString(), BundleHash: source.BundleHash()})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if _, err := owner.RetireAndWait(context.Background()); err != nil {
					t.Errorf("retire runtime: %v", err)
				}
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Errorf("join process: %v", err)
				}
			})
			missing := &renewalCleanupWrapper{Store: selected, missingAck: true}
			if next, err := runtimedelivery.StartClaimHeartbeat(ctx, owner, missing, claimed.Claim); next != nil || err == nil {
				t.Fatalf("missing acknowledgement started handler: heartbeat=%v err=%v", next, err)
			}
			if missing.realRenewals != 0 {
				t.Fatalf("missing acknowledgement issued %d selected-store renewals", missing.realRenewals)
			}
			afterMissingAck, err := selected.Snapshot(ctx, claimed.Snapshot.DeliveryID)
			if err != nil {
				t.Fatal(err)
			}
			if afterMissingAck.ClaimVersion != beforeMissingAck.ClaimVersion ||
				!afterMissingAck.ClaimExpiresAt.Equal(beforeMissingAck.ClaimExpiresAt) ||
				afterMissingAck.Status != beforeMissingAck.Status {
				t.Fatalf("missing acknowledgement mutated exact claim: before=%#v after=%#v", beforeMissingAck, afterMissingAck)
			}
			cleanup := errors.New("injected postcommit renewal cleanup")
			wrapper := &renewalCleanupWrapper{Store: selected, cleanupErr: cleanup}
			heartbeat, err := runtimedelivery.StartClaimHeartbeat(ctx, owner, wrapper, claimed.Claim)
			if err != nil {
				t.Fatalf("acknowledged renewal refused handler: %v", err)
			}
			if heartbeat.Context().Err() != nil {
				t.Fatalf("acknowledged renewal retired exact claim: %v", heartbeat.Context().Err())
			}
			guard, err := heartbeat.BeginSettlement()
			if err != nil {
				t.Fatalf("acknowledged renewal refused settlement: %v", err)
			}
			settled, settleErr := selected.SettleSuccess(guard.Context(), claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection())
			if settleErr != nil || !settled.MatchesSettlementClaim(claimed.Claim) {
				guard.Abort()
				t.Fatalf("exact terminal settlement = %#v err=%v", settled, settleErr)
			}
			if err := guard.Finish(true); err != nil {
				t.Fatalf("acknowledged cleanup classified as authority failure: %v", err)
			}
			if err := heartbeat.Stop(); err != nil {
				t.Fatalf("idempotent heartbeat stop: %v", err)
			}
			if err := heartbeat.CleanupDiagnostic(); !errors.Is(err, cleanup) {
				t.Fatalf("heartbeat diagnostic = %v, want injected cleanup", err)
			}
			if wrapper.realRenewals != 2 {
				t.Fatalf("real selected-store renewals = %d, want initial and pre-settlement only", wrapper.realRenewals)
			}
			after, err := selected.Snapshot(ctx, claimed.Snapshot.DeliveryID)
			if err != nil {
				t.Fatal(err)
			}
			if after.Status != runtimedelivery.StatusDelivered || !after.MatchesSettlementClaim(claimed.Claim) {
				t.Fatalf("acknowledged cleanup lost terminal settlement: %#v", after)
			}
		})
	}
}
