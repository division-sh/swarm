package manager_test

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestS6DifferentStoryAgentOrderJoinDiagnosticBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openManagerNativeDelivery(t, backend)
			ctx := fixture.Context
			owner := fixture.Store.(managerNativeDeliveryOwner)
			runID := uuid.NewString()
			fixture.RequireRun(t, runID)
			identity := agentidentitytest.RootRuntimeForRun(t, runID, "s6-order", "s6.audit")
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
			at := time.Now().UTC().Truncate(time.Microsecond)
			eventsByID := map[string]events.Event{}
			var ids []string
			for _, eventID := range []string{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"} {
				event := eventtest.ExistingRunRootIngress(eventID, "s6.generated.result", "source", "", nil, 0, runID, events.EventEnvelope{}, at)
				id, err := deliverylifecycle.DeliveryID(event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				eventsByID[id] = event
				ids = append(ids, id)
			}
			sort.Strings(ids)
			// Equal event timestamps are not an author-activity sequence.
			for _, id := range []string{ids[1], ids[0]} {
				event := eventsByID[id]
				fixture.Publish(t, ctx, event, []events.DeliveryRoute{route}, fixture.Authority)
				work, err := owner.PipelineObligations().ClaimEvent(ctx, event.ID(), pipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatal(err)
				}
				outcome, err := owner.PipelineObligations().Settle(ctx, work.Claim, pipelineobligation.Acknowledged("processed"))
				if err != nil || !outcome.Committed() || !outcome.DeliveryHandoffCommitted() {
					t.Fatalf("native handoff: outcome=%+v err=%v", outcome, err)
				}
				snapshot, err := owner.Snapshot(ctx, id)
				if err != nil || snapshot.Status != deliverylifecycle.StatusPending || snapshot.ClaimVersion != 0 {
					t.Fatalf("unclaimed agent delivery: %+v err=%v", snapshot, err)
				}
			}
			activity := s6OrderDiagnosticActivity(t, ctx, owner, runID, eventsByID[ids[0]].ID(), eventsByID[ids[1]].ID())
			for _, occurrence := range activity {
				if occurrence.SourceOwner == "events" || occurrence.Kind == authoractivity.KindDeliveryLifecycle {
					t.Fatalf("different-story pending deliveries unexpectedly acquired an ordering occurrence: %+v", occurrence)
				}
			}
			first, err := owner.ScanDeliveryContinuations(ctx, fixture.Authority, deliverylifecycle.ContinuationCursor{}, 1)
			if err != nil || len(first.Items) != 1 || first.Items[0].Disposition != deliverylifecycle.ClaimAcquired {
				t.Fatalf("first native scan: %+v err=%v", first, err)
			}
			second, err := owner.ScanDeliveryContinuations(ctx, fixture.Authority, first.Next, 1)
			if err != nil || len(second.Items) != 1 || first.Items[0].DeliveryID == second.Items[0].DeliveryID || !second.Exhausted {
				t.Fatalf("tie cursor: %+v err=%v", second, err)
			}
			claim, err := owner.ClaimDelivery(ctx, fixture.Authority, eventsByID[ids[0]], route)
			acquired, ok := claim.Acquired()
			if err != nil || !ok || !claim.Acknowledged {
				t.Fatalf("different-story native agent claim: %+v err=%v", claim, err)
			}
			after := s6OrderDiagnosticActivity(t, ctx, owner, runID, eventsByID[ids[0]].ID(), eventsByID[ids[1]].ID())
			found := false
			for _, occurrence := range after {
				if occurrence.Kind == authoractivity.KindDeliveryLifecycle && occurrence.SourceIdentity == acquired.Claim.DeliveryID() {
					found = true
				}
			}
			if !found || reflect.DeepEqual(activity, after) {
				t.Fatal("claim did not add its later delivery-lifecycle occurrence")
			}
			if _, err := owner.SettleSuccess(ctx, acquired.Claim, nil, 0, handlerselection.NotApplicable()); err != nil {
				t.Fatal(err)
			}
			t.Logf("native commit order=%v; scan order=%v; delivery timestamps=%v,%v; equal event timestamp=%v; no pre-claim event/delivery activity sequence for StoryDifferent", []string{ids[1], ids[0]}, []string{first.Items[0].DeliveryID, second.Items[0].DeliveryID}, first.Items[0].Snapshot.CreatedAt, second.Items[0].Snapshot.CreatedAt, at)
		})
	}
}

func s6OrderDiagnosticActivity(t *testing.T, ctx context.Context, owner managerNativeDeliveryOwner, runID string, publicationScopes ...string) []authoractivity.Occurrence {
	t.Helper()
	scope, found := authoractivity.ScopeFromContext(ctx)
	if !found {
		t.Fatal("native delivery has no author activity scope")
	}
	var occurrences []authoractivity.Occurrence
	for _, runtimeID := range append(publicationScopes, scope.RuntimeInstanceID) {
		page, err := owner.ListAuthorActivity(ctx, authoractivity.ListOptions{RunID: runID, RuntimeInstanceID: runtimeID, BundleHashes: []string{sourceartifactfixture.BundleHash}, Limit: 500})
		if err != nil {
			t.Fatal(err)
		}
		occurrences = append(occurrences, page.Occurrences...)
	}
	return occurrences
}
