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
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
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

func TestS6AuthoredAgentReadyCursorJoinDiagnosticBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openManagerNativeDelivery(t, backend)
			ctx := fixture.Context
			owner := fixture.Store.(managerNativeDeliveryOwner)
			runID := uuid.NewString()
			fixture.RequireRun(t, runID)
			identity := agentidentitytest.RootRuntimeForRun(t, runID, "s6-ready-order", "s6.audit")
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
			eventByDelivery := map[string]events.Event{}
			var ids []string
			at := time.Now().UTC().Truncate(time.Microsecond)
			for _, eventID := range []string{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"} {
				event := eventtest.ExistingRunRootIngress(eventID, "s6.authored.requested", "source", "", nil, 0, runID, events.EventEnvelope{}, at)
				id, err := deliverylifecycle.DeliveryID(event.ID(), route)
				if err != nil {
					t.Fatal(err)
				}
				ids = append(ids, id)
				eventByDelivery[id] = event
			}
			sort.Strings(ids)
			for _, id := range ids {
				event := eventByDelivery[id]
				scope := authoractivity.BundleScope(event.ID(), fixture.Authority.SourceArtifact().BundleHash())
				descriptor := authoractivity.EventDescriptor{EventType: string(event.Type()), Disposition: authoractivity.StoryAuthored}
				catalog, err := owner.RegisterAuthorActivityEventCatalog(scope, []authoractivity.EventDescriptor{descriptor})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(catalog.Release)
				publication, err := owner.PipelineObligations().ClaimPublication(ctx, event.ID())
				if err != nil {
					t.Fatal(err)
				}
				ledger, err := events.NewConnectEvaluationLedger(nil)
				if err != nil {
					t.Fatal(err)
				}
				settlement, err := events.NewDeliverySettlement(events.EventWriteNormalPublication, ledger)
				if err != nil {
					t.Fatal(err)
				}
				committed, err := owner.CommitPublication(ctx, runtimebus.PublicationCommand{
					Commit: runtimebus.CommitPublishRequest{
						Event: storetest.AdmitNativeDeliveryEvent(t, event), RouteSettlement: settlement,
						DeliveryRoutes: []events.DeliveryRoute{route}, DeliveryAuthority: fixture.Authority,
						ReplayScope: pipelineobligation.ScopeSubscribed, PipelineClaim: publication,
					},
					AuthorScope: scope, HasAuthorScope: true, AuthorDescriptor: descriptor, HasAuthorDescriptor: true,
				})
				releaseErr := owner.PipelineObligations().Release(context.WithoutCancel(ctx), publication)
				if err != nil || releaseErr != nil || !committed.Acknowledged {
					t.Fatalf("authored native publication: acknowledged=%t err=%v release=%v", committed.Acknowledged, err, releaseErr)
				}
			}
			activity := s6OrderDiagnosticActivity(t, ctx, owner, runID, eventByDelivery[ids[0]].ID(), eventByDelivery[ids[1]].ID())
			sequences := map[string]int64{}
			for _, occurrence := range activity {
				if occurrence.Kind == authoractivity.KindEventEmitted && occurrence.SourceOwner == "events" {
					sequences[occurrence.SourceIdentity] = occurrence.Sequence
				}
			}
			if len(sequences) != 2 || sequences[eventByDelivery[ids[0]].ID()] <= 0 || sequences[eventByDelivery[ids[0]].ID()] >= sequences[eventByDelivery[ids[1]].ID()] {
				t.Fatalf("exact authored event sequence join: %+v", sequences)
			}
			var cursor deliverylifecycle.ContinuationCursor
			for _, id := range []string{ids[1], ids[0]} {
				work, err := owner.PipelineObligations().ClaimEvent(ctx, eventByDelivery[id].ID(), pipelineobligation.PurposeRecovery)
				if err != nil {
					t.Fatal(err)
				}
				outcome, err := owner.PipelineObligations().Settle(ctx, work.Claim, pipelineobligation.Acknowledged("processed"))
				if err != nil || !outcome.Committed() || !outcome.DeliveryHandoffCommitted() {
					t.Fatalf("held readiness handoff: outcome=%+v err=%v", outcome, err)
				}
				if id == ids[1] {
					page, err := owner.ScanDeliveryContinuations(ctx, fixture.Authority, deliverylifecycle.ContinuationCursor{}, 1)
					if err != nil || len(page.Items) != 1 || page.Items[0].DeliveryID != ids[1] || page.Items[0].Disposition != deliverylifecycle.ClaimAcquired {
						t.Fatalf("later authored event should be the sole ready delivery: page=%+v err=%v", page, err)
					}
					_, _, _, started := page.Next.Position()
					if !started {
						t.Fatal("ready scan did not retain its typed cursor")
					}
					cursor = page.Next
				}
			}
			past, err := owner.ScanDeliveryContinuations(ctx, fixture.Authority, cursor, 2)
			if err != nil || len(past.Items) != 0 || !past.Exhausted {
				t.Fatalf("earlier newly-ready delivery crossed an exclusive later cursor: page=%+v err=%v", past, err)
			}
			fresh, err := owner.ScanDeliveryContinuations(ctx, fixture.Authority, deliverylifecycle.ContinuationCursor{}, 2)
			if err != nil || len(fresh.Items) != 2 || fresh.Items[0].DeliveryID != ids[0] || fresh.Items[1].DeliveryID != ids[1] {
				t.Fatalf("fresh pass did not recover exact oldest-ready order: page=%+v err=%v", fresh, err)
			}
			for _, item := range fresh.Items {
				if item.Disposition != deliverylifecycle.ClaimAcquired || item.Snapshot.Status != deliverylifecycle.StatusPending || item.Snapshot.ClaimVersion != 0 {
					t.Fatalf("read-only selection changed an unclaimed pending row: %+v", item)
				}
			}
			t.Logf("authored native sequence join=%v; later event alone READY while earlier handoff held; advanced cursor misses earlier newly-ready row, fresh pass sees both pending/v0; not a dispatcher repair", sequences)
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
