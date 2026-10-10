package runtimepersistence

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestAgentSettledAttemptStoragePreservesEventRoleSubscriberAndOriginalReadBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture, ctx := backend.open(t), testAuthorActivityContext()
			run := uuid.NewString()
			seedAuthorActivityReceiptRun(t, fixture, ctx, run)
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "attempt.storage", "gateway", "", nil, 0, run, events.EventEnvelope{}, time.Now().UTC())
			routes := []events.DeliveryRoute{
				{Recipient: events.MustAgentDeliveryRecipient("agent-a"), AgentIdentity: mustTestAgentIdentityForRun(run, "agent-a", "storage/agent-a")},
				{Recipient: events.MustAgentDeliveryRecipient("agent-b"), AgentIdentity: mustTestAgentIdentityForRun(run, "agent-b", "storage/agent-b")},
				testEntitylessNodeDeliveryRoute("storage-node"),
			}
			if err := commitSemanticEventFixtureWithRoutes(ctx, fixture.store, event, routes); err != nil {
				t.Fatal(err)
			}
			if n, err := ReadAgentSettledAttemptCountForTest(ctx, fixture.store, event.ID(), "agent-a"); err != nil || n != 0 {
				t.Fatalf("pending attempt counted as settled: %d,%v", n, err)
			}
			for _, route := range routes {
				claimed, err := claimDeliveryFixture(ctx, fixture.store.(deliveryFixtureStore), event, route)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.SettleSuccess(ctx, claimed.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatal(err)
				}
			}
			probe, restore, err := InstallTransactionProbeForTest(fixture.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			for _, row := range []struct {
				event, agent string
				want         int
			}{
				{event.ID(), "agent-a", 1}, {event.ID(), "agent-b", 1},
				{event.ID(), routes[2].Recipient.ID(), 0}, {event.ID(), "foreign", 0}, {uuid.NewString(), "agent-a", 0},
			} {
				if n, err := ReadAgentSettledAttemptCountForTest(ctx, fixture.store, row.event, row.agent); err != nil || n != row.want {
					t.Fatalf("exact attempt %s/%s=%d,%v;want%d", row.event, row.agent, n, err, row.want)
				}
			}
			if counts := probe.Snapshot(); counts.Total.ReadCommits != 5 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("attempt evidence escaped original read owner: %+v", counts)
			}
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			if n, err := ReadAgentSettledAttemptCountForTest(cancelled, fixture.store, event.ID(), "agent-a"); !errors.Is(err, context.Canceled) || n != 0 {
				t.Fatalf("cancelled attempt returned evidence: %d,%v", n, err)
			}
			switch owner := fixture.store.(type) {
			case *PostgresStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			case *SQLiteRuntimeStore:
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if n, err := ReadAgentSettledAttemptCountForTest(ctx, fixture.store, event.ID(), "agent-a"); err == nil || n != 0 {
				t.Fatalf("closed attempt returned evidence: %d,%v", n, err)
			}
		})
	}
	for _, invalid := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), struct{}{}} {
		if n, err := ReadAgentSettledAttemptCountForTest(context.Background(), invalid, uuid.NewString(), "agent-a"); err == nil || n != 0 {
			t.Fatalf("missing owner returned evidence: %d,%v", n, err)
		}
	}
}
