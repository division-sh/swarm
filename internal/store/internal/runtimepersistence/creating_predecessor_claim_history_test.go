package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

func TestCreatingPredecessorClaimHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"busy", "future_retry", "reclaimable", "eligible_retry"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				proveCreatingPredecessorClaimHistory(t, backend, state)
			})
		}
	}
}

func proveCreatingPredecessorClaimHistory(t *testing.T, backend, state string) {
	t.Helper()
	f := newReceiverConfigActivationFixtureWithAgents(t, backend, false)
	f.constructKeylessRoot(t)
	req := f.request("claim-history", "claim-history", "initial")
	plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
		t.Fatal(err)
	}
	store := f.store.(deliverylifecycle.Store)
	creator := req.TriggerEvent
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "review", "ordered-node")),
		Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: plan.Identity.TemplateID, FlowInstance: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID})}
	if err := insertCanonicalEventRecordFixture(f.ctx, f.store, creator); err != nil {
		t.Fatal(err)
	}
	if err := commitDeliveryObligationFixture(f.ctx, store, creator, route); err != nil {
		t.Fatal(err)
	}
	creating, err := claimDeliveryFixture(f.ctx, store, creator, route)
	if err != nil {
		t.Fatal(err)
	}
	creatorBefore, err := store.SettleSuccess(f.ctx, creating.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
	if err != nil {
		t.Fatal(err)
	}
	regression, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, "terminal_creator_reopen", "predecessor_test", "settle", nil))
	if !ok {
		t.Fatal("missing terminal regression failure")
	}
	if _, err := store.SettleFailure(f.ctx, creating.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: &regression, RetryBase: time.Hour, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); !errors.Is(err, deliverylifecycle.ErrConflict) {
		t.Fatalf("claimed-follower history allowed its terminal creator to reopen: %v", err)
	}
	follower := eventtest.ExistingRunRootIngress(uuid.NewString(), creator.Type(), creator.SourceAgent(), creator.TaskID(), creator.Payload(), creator.ChainDepth(), creator.RunID(), creator.Envelope(), creator.CreatedAt())
	if err := insertCanonicalEventRecordFixture(f.ctx, f.store, follower); err != nil {
		t.Fatal(err)
	}
	if err := commitDeliveryObligationFixture(f.ctx, store, follower, route); err != nil {
		t.Fatal(err)
	}
	if err := runDeliveryFixtureAttempt(f.ctx, store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
		adapter := sqliteDeliveryAdapter
		if backend == "postgres" {
			adapter = postgresDeliveryAdapter
		}
		return adapter.CommitPipelineHandoff(ctx, attempt, follower.ID())
	}); err != nil {
		t.Fatal(err)
	}
	first, err := claimDeliveryFixture(f.ctx, store, follower, route)
	if err != nil {
		t.Fatal(err)
	}
	want, wake := deliverylifecycle.ClaimBusy, true
	switch state {
	case "future_retry", "eligible_retry":
		failure, ok := failures.EnvelopeFromError(failures.New(failures.ClassLifecycleConflict, "claim_history_retry", "predecessor_test", "settle", nil))
		if !ok {
			t.Fatal("missing typed retry failure")
		}
		if _, err := store.SettleFailure(f.ctx, first.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: &failure, RetryBase: time.Hour, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); err != nil {
			t.Fatal(err)
		}
		want = deliverylifecycle.ClaimDeferred
		if state == "eligible_retry" {
			if err := MakeManagerRetryEligibleForTest(f.ctx, f.store, follower, route); err != nil {
				t.Fatal(err)
			}
			want, wake = deliverylifecycle.ClaimAcquired, false
		}
	case "reclaimable":
		if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, first.Claim); err != nil {
			t.Fatal(err)
		}
		want, wake = deliverylifecycle.ClaimReclaimable, false
	}
	before := loadDeliverySnapshotFixture(t, f.ctx, store, follower.ID(), route)
	assertClaimHistoryContinuations(t, f.ctx, store, before, want, wake)
	direct, err := store.ClaimDelivery(f.ctx, before.Authority, follower, route)
	if err != nil {
		t.Fatal(err)
	}
	if wake {
		_, acquired := direct.Acquired()
		if direct.Disposition != want || acquired || direct.Renewal.Acknowledged {
			t.Fatalf("clock-blocked direct claim differs from observation: %+v want=%s", direct, want)
		}
		after, err := store.Snapshot(f.ctx, before.DeliveryID)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("clock-blocked direct claim mutated history: before=%+v after=%+v err=%v", before, after, err)
		}
	} else {
		acquired, ok := direct.Acquired()
		if !ok || acquired.Claim.Version() != first.Claim.Version()+1 || (state == "reclaimable" && direct.Previous != deliverylifecycle.ClaimReclaimable) {
			t.Fatalf("eligible history did not mint its exact successor: %+v", direct)
		}
		if _, err := store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err == nil {
			t.Fatal("stale claim settled its successor")
		}
	}
	creatorAfter, err := store.Snapshot(f.ctx, creatorBefore.DeliveryID)
	if err != nil || !reflect.DeepEqual(creatorBefore, creatorAfter) || creatorAfter.Status != deliverylifecycle.StatusDelivered {
		t.Fatalf("claim history regressed the terminal creator: before=%+v after=%+v err=%v", creatorBefore, creatorAfter, err)
	}
}

func assertClaimHistoryContinuations(t *testing.T, ctx context.Context, store deliverylifecycle.Store, before deliverylifecycle.Snapshot, want deliverylifecycle.ClaimDisposition, timedWake bool) {
	t.Helper()
	single, err := store.ObserveDeliveryContinuation(ctx, before.Authority, before.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := store.ObserveDeliveryContinuations(ctx, before.Authority, []string{before.DeliveryID})
	if err != nil || len(batch) != 1 {
		t.Fatalf("claim-history batch: %+v err=%v", batch, err)
	}
	page, err := store.ScanDeliveryContinuations(ctx, before.Authority, deliverylifecycle.ContinuationCursor{}, 64)
	if err != nil {
		t.Fatal(err)
	}
	observations := []deliverylifecycle.ContinuationObservation{single, batch[0]}
	for _, item := range page.Items {
		if item.DeliveryID == before.DeliveryID {
			observations = append(observations, deliverylifecycle.ContinuationObservation{DeliveryID: item.DeliveryID, Disposition: item.Disposition, Wake: item.Wake, Invariant: item.Invariant})
		}
	}
	if len(observations) != 3 {
		t.Fatalf("exact history absent from scan: %+v", page)
	}
	for _, observation := range observations {
		after, present := observation.Wake.After()
		if observation.Disposition != want || observation.Invariant != nil || present != timedWake || (timedWake && after <= 0) {
			t.Fatalf("first-claim gate changed the store-owned disposition/wake: %+v want=%s timedWake=%t", observation, want, timedWake)
		}
	}
	after, err := store.Snapshot(ctx, before.DeliveryID)
	if err != nil || !reflect.DeepEqual(before, after) || after.ClaimVersion != 1 {
		t.Fatalf("claim-history observation mutated exact persisted state: before=%+v after=%+v err=%v", before, after, err)
	}
}
