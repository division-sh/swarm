package runtimepersistence

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

type deliveryReadProjectionStore interface {
	authorActivityReceiptStore
	runtimedelivery.Store
	ListPendingAgentDeliveryFacts(context.Context, []agentidentity.Identity, time.Time) (map[agentidentity.Identity]operatorread.PendingAgentDeliveryFacts, error)
	ListPendingAgentDeliveryDetails(context.Context, operatorread.PendingAgentDeliveryListOptions) (operatorread.PendingAgentDeliveryPage, error)
	ListAgentDeliveryLifecycleFacts(context.Context, []agentidentity.Identity) (map[agentidentity.Identity]operatorread.AgentDeliveryLifecycleFacts, error)
}

func TestPendingAgentDeliveryRetryEligibilityPreservesSubsecondStoreParity(t *testing.T) {
	const retryBase = 30 * time.Second
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(deliveryReadProjectionStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			identity := testAgentIdentity(t, "retry-agent", "delivery-projection/retry")
			event := eventtest.PersistedProjection(
				uuid.NewString(), "projection.retry", "gateway", "", json.RawMessage(`{}`), 0,
				runID, "", events.EventEnvelope{}, time.Now().UTC(),
			)
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(identity.AgentID()), AgentIdentity: identity}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, event, []events.DeliveryRoute{route}); err != nil {
				t.Fatalf("commit retry delivery event: %v", err)
			}
			claimed, err := claimDeliveryFixture(ctx, selected, event, route)
			if err != nil {
				t.Fatalf("claim retry delivery: %v", err)
			}
			snapshot, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{
				Disposition: runtimedelivery.FailureRetry,
				Failure:     testRetryableFailure(),
				RetryBase:   retryBase,
			})
			if err != nil {
				t.Fatalf("settle retry delivery: %v", err)
			}
			if got := snapshot.NextEligibleAt.Sub(snapshot.UpdatedAt); got != retryBase {
				t.Fatalf("retry cadence = %s, want %s", got, retryBase)
			}

			facts, err := selected.ListPendingAgentDeliveryFacts(ctx, []agentidentity.Identity{identity}, event.CreatedAt().Add(-time.Minute))
			if err != nil {
				t.Fatalf("list deferred retry facts: %v", err)
			}
			if got := facts[identity].PendingCount; got != 0 {
				t.Fatalf("deferred retry pending count = %d, want 0", got)
			}

			eligibleAt := time.Now().UTC().Add(-time.Second)
			query := `UPDATE event_deliveries SET next_eligible_at=? WHERE delivery_id=?`
			args := []any{eligibleAt, snapshot.DeliveryID}
			if fixture.dialect == "postgres" {
				query = `UPDATE event_deliveries SET next_eligible_at=$2::timestamptz WHERE delivery_id=$1::uuid`
				args = []any{snapshot.DeliveryID, eligibleAt}
			}
			if _, err := fixture.db.ExecContext(ctx, query, args...); err != nil {
				t.Fatalf("make retry eligible: %v", err)
			}
			page, err := selected.ListPendingAgentDeliveryDetails(ctx, operatorread.PendingAgentDeliveryListOptions{
				AgentIdentity: identity,
				Since:         event.CreatedAt().Add(-time.Minute),
				Limit:         10,
			})
			if err != nil {
				t.Fatalf("list eligible retry details: %v", err)
			}
			if len(page.PendingDeliveries) != 1 || page.PendingDeliveries[0].DeliveryID != snapshot.DeliveryID {
				t.Fatalf("eligible retry page = %#v, want exact delivery %s", page, snapshot.DeliveryID)
			}
		})
	}
}

func TestDeliveryReadProjectionBoundsAndExactIdentityParity(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			selected := fixture.store.(deliveryReadProjectionStore)
			ctx := testAuthorActivityContext()
			runID := uuid.NewString()
			base := time.Date(2026, 7, 14, 11, 0, 0, 0, time.UTC)
			seedAuthorActivityReceiptRun(t, fixture, ctx, runID)

			siblingEvent := eventtest.PersistedProjection(
				uuid.NewString(), "projection.siblings", "gateway", "", json.RawMessage(`{"kind":"siblings"}`), 0,
				runID, "", events.EventEnvelope{}, base,
			)
			pageAgent := "page-agent"
			pageIdentity := testAgentIdentity(t, pageAgent, "delivery-projection/page")
			siblingRoutes := []events.DeliveryRoute{
				{Recipient: events.MustAgentDeliveryRecipient(pageAgent), AgentIdentity: pageIdentity,
					Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "projection-page-one"}},
					Target:  events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "delivery-projection", FlowInstance: "delivery-projection/one", EntityID: uuid.NewString()}),
				},
				{Recipient: events.MustAgentDeliveryRecipient(pageAgent), AgentIdentity: pageIdentity,
					Context: events.DeliveryContext{Reply: &events.ReplyContextRef{ID: "projection-page-two"}},
					Target:  events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "delivery-projection", FlowInstance: "delivery-projection/two", EntityID: uuid.NewString()}),
				},
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, siblingEvent, siblingRoutes); err != nil {
				t.Fatalf("commit sibling delivery event: %v", err)
			}
			siblingIDs := make([]string, 0, len(siblingRoutes))
			for _, route := range siblingRoutes {
				snapshot := loadDeliverySnapshotFixture(t, ctx, selected, siblingEvent.ID(), route)
				setDeliveryReadProjectionFixtureTimes(t, ctx, fixture, snapshot, base)
				siblingIDs = append(siblingIDs, snapshot.DeliveryID)
			}
			sort.Strings(siblingIDs)

			tailEvent := eventtest.PersistedProjection(
				uuid.NewString(), "projection.malformed_tail", "gateway", "", json.RawMessage(`{"kind":"tail"}`), 0,
				runID, "", events.EventEnvelope{}, base.Add(time.Minute),
			)
			tailRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(pageAgent), AgentIdentity: pageIdentity}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, tailEvent, []events.DeliveryRoute{tailRoute}); err != nil {
				t.Fatalf("commit malformed-tail event: %v", err)
			}
			tailSnapshot := loadDeliverySnapshotFixture(t, ctx, selected, tailEvent.ID(), tailRoute)
			setDeliveryReadProjectionFixtureTimes(t, ctx, fixture, tailSnapshot, base.Add(time.Minute))
			corruptOperatorAgentDeliveryTail(t, ctx, fixture, tailSnapshot.DeliveryID)

			facts, err := selected.ListPendingAgentDeliveryFacts(ctx, []agentidentity.Identity{pageIdentity}, base.Add(-time.Minute))
			if err != nil {
				t.Fatalf("load pending aggregate: %v", err)
			}
			if facts[pageIdentity].PendingCount != 3 || facts[pageIdentity].OldestPendingAgeSec <= 0 {
				t.Fatalf("pending aggregate = %#v, want three obligations with positive age", facts[pageIdentity])
			}

			first, err := selected.ListPendingAgentDeliveryDetails(ctx, operatorread.PendingAgentDeliveryListOptions{
				AgentIdentity: pageIdentity, Since: base.Add(-time.Minute), Limit: 1,
			})
			if err != nil {
				t.Fatalf("load first pending page: %v", err)
			}
			if len(first.PendingDeliveries) != 1 || first.PendingDeliveries[0].DeliveryID != siblingIDs[0] || first.NextCursor == "" {
				t.Fatalf("first pending page = %#v, want first exact sibling plus cursor", first)
			}
			second, err := selected.ListPendingAgentDeliveryDetails(ctx, operatorread.PendingAgentDeliveryListOptions{
				AgentIdentity: pageIdentity, Since: base.Add(-time.Minute), Limit: 1, Cursor: first.NextCursor,
			})
			if err != nil {
				t.Fatalf("load second pending page with malformed row beyond lookahead: %v", err)
			}
			if len(second.PendingDeliveries) != 1 || second.PendingDeliveries[0].DeliveryID != siblingIDs[1] || second.NextCursor == "" {
				t.Fatalf("second pending page = %#v, want second exact sibling plus cursor", second)
			}

			currentAgent := "current-agent"
			currentIdentity := testAgentIdentity(t, currentAgent, "delivery-projection/current")
			currentEvent := eventtest.PersistedProjection(
				uuid.NewString(), "projection.current", "gateway", "", json.RawMessage(`{"kind":"current"}`), 0,
				runID, "", events.EventEnvelope{}, base.Add(2*time.Minute),
			)
			currentRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(currentAgent), AgentIdentity: currentIdentity}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, currentEvent, []events.DeliveryRoute{currentRoute}); err != nil {
				t.Fatalf("commit current lifecycle event: %v", err)
			}
			currentSnapshot := loadDeliverySnapshotFixture(t, ctx, selected, currentEvent.ID(), currentRoute)
			setDeliveryReadProjectionFixtureTimes(t, ctx, fixture, currentSnapshot, base.Add(2*time.Minute))

			historyEvent := eventtest.PersistedProjection(
				uuid.NewString(), "projection.delivered_history", "gateway", "", json.RawMessage(`{"kind":"history"}`), 0,
				runID, "", events.EventEnvelope{}, base.Add(3*time.Minute),
			)
			historyRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(currentAgent), AgentIdentity: currentIdentity}
			if err := commitSemanticEventFixtureWithRoutes(ctx, selected, historyEvent, []events.DeliveryRoute{historyRoute}); err != nil {
				t.Fatalf("commit delivered history event: %v", err)
			}
			historySnapshot := seedDeliveryStateFixture(t, ctx, selected, historyEvent, historyRoute, runtimedelivery.StateDelivered, nil)
			setDeliveryReadProjectionFixtureTimes(t, ctx, fixture, historySnapshot, base.Add(3*time.Minute))
			corruptOperatorAgentDeliveryTail(t, ctx, fixture, historySnapshot.DeliveryID)

			lifecycle, err := selected.ListAgentDeliveryLifecycleFacts(ctx, []agentidentity.Identity{currentIdentity})
			if err != nil {
				t.Fatalf("load batched current lifecycle with malformed delivered history: %v", err)
			}
			if got := lifecycle[currentIdentity]; got.CurrentState != string(runtimedelivery.StateQueued) || got.BlockingLayer != "delivery_queue" {
				t.Fatalf("current lifecycle = %#v, want queued delivery_queue", got)
			}
		})
	}
}

func setDeliveryReadProjectionFixtureTimes(
	t *testing.T,
	ctx context.Context,
	fixture authorActivityReceiptFixture,
	snapshot runtimedelivery.Snapshot,
	at time.Time,
) {
	t.Helper()
	if fixture.dialect == "postgres" {
		setPostgresDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, at, at)
		return
	}
	setSQLiteDeliveryFixtureTimes(t, ctx, fixture.db, snapshot, at, at)
}
