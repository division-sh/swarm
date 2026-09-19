package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/manager"
)

func TestB07GroupMissingAgentRouteRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, seen, _, am := newMixedExecutionFixtureWithManager(t, backend, nil)
			f.prepare(t)
			f.seal(t)
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.bus.FinalizeFanOutPublications(f.ctx, f.group, committed.Publications); err != nil {
				t.Fatal(err)
			}
			routes := f.plans[1].(bus.EnginePublicationPlan).PublicationCommand().Commit.DeliveryRoutes
			if len(routes) != 1 || !routes[0].Recipient.IsAgent() {
				t.Fatalf("missing-recipient proof needs the exact real agent route: %+v", routes)
			}
			identity := routes[0].AgentIdentity
			state, found, err := f.raw.(manager.AgentLifecycleStateReader).LoadAgentLifecycleState(f.ctx, identity)
			if err != nil || !found {
				t.Fatalf("admitted recipient lifecycle found=%v err=%v", found, err)
			}
			// Remove transport availability, not the compiled or durable recipient.
			// The current lifecycle token is loaded from its canonical owner.
			f.bus.RemoveAgentRoute(effects.LifecycleToken{Identity: identity, AgentID: routes[0].Recipient.ID(), RuntimeEpoch: state.RuntimeEpoch, Generation: state.Generation})
			err = f.bus.DispatchFanOutPublications(f.ctx, f.group, committed.Publications)
			failure, ok := failures.As(err)
			if !ok || failure.Failure.Detail.Code != "authoritative_delivery_incomplete" {
				t.Fatalf("missing exact agent route did not retain typed incomplete outcome: %v", err)
			}
			mixedAssertReceipt(t, f, f.events[0].ID(), "success", "pipeline_persisted")
			for i := 1; i < len(f.events); i++ {
				var count int
				if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id=$1 AND subscriber_type='platform' AND subscriber_id='pipeline'`, f.events[i].ID()).Scan(&count); err != nil || count != 0 {
					t.Fatalf("incomplete/unvisited member%d fabricated receipt: count=%d err=%v", i, count, err)
				}
			}
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			result, err := am.Restart(f.ctx, agentcontrol.RestartRequest{AgentID: routes[0].Recipient.ID(), RunID: identity.RunID, FlowInstance: identity.FlowInstance()})
			if err != nil || result.Generation <= state.Generation {
				t.Fatalf("canonical recipient restart=%+v err=%v", result, err)
			}
			if _, err := f.bus.ReleaseRunQueue(f.ctx, f.seed.runID, 8); err != nil {
				t.Fatal(err)
			}
			mixedAwaitDeliveries(t, f, 6)
			for _, event := range f.events {
				mixedAssertReceipt(t, f, event.ID(), "success", "pipeline_persisted")
			}
			observed := map[string]int{}
			for len(observed) < 2 {
				select {
				case event := <-seen:
					if event.ID() != f.events[1].ID() && event.ID() != f.events[2].ID() {
						t.Fatalf("recovery executed an unselected agent event: %s", event.ID())
					}
					observed[event.ID()]++
				case <-time.After(5 * time.Second):
					t.Fatalf("missing actual recovered agent execution: %v", observed)
				}
			}
			select {
			case event := <-seen:
				t.Fatalf("duplicate agent execution: %s", event.ID())
			default:
			}
			for id, count := range observed {
				if count != 1 {
					t.Fatalf("agent event %s executed %d times", id, count)
				}
			}
			// Repeated durable recovery must not rerun the already settled prefix.
			if recovered, err := f.bus.ReleaseRunQueue(f.ctx, f.seed.runID, 8); err != nil || recovered.Settled != 0 {
				t.Fatalf("terminal recovery repeated member work: %+v err=%v", recovered, err)
			}
		})
	}
}
