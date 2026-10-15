package runtimepersistence

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// Exercise the native history reader and the unchanged selected admission
// policies together. A stored binding must affect only cuts that retain it.
func proveDeliveryHistoricalEmitterSessionPredicatesBothStores(t *testing.T) {
	t.Helper()
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			store := fixture.store.(completionSettlementTestStore)
			completion := newCompletionSettlementFixture(t, store, fixture.db, backend.name == "sqlite")
			ctx := testAuthorActivityContext()
			source := managedCompletionTestEvent(completion.authority)
			publishCompleteRunForkRevisionBaseline(t, ctx, fixture.db, backend.name == "postgres", source.RunID())
			owner := fixture.store.(runForkSelectedLifecycleStore)
			cut := func(producer string) runfork.RunForkPlan {
				t.Helper()
				event := eventtest.Child(uuid.NewString(), "delivery.history.emitted", producer, "", []byte(`{}`), 1, source, events.EventEnvelope{}, time.Now().UTC())
				if err := commitSemanticPipelineProcessedEventFixture(ctx, fixture.store, event); err != nil {
					t.Fatal(err)
				}
				plan, err := owner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: source.RunID(), At: event.ID()})
				if err != nil {
					t.Fatal(err)
				}
				return plan
			}
			check := func(plan runfork.RunForkPlan, bound, sameEmitter bool) {
				t.Helper()
				var input *runfork.RunForkPendingWork
				for i := range plan.PendingWork {
					if plan.PendingWork[i].DeliveryID == completion.origin.DeliveryID() {
						input = &plan.PendingWork[i]
					}
				}
				want := runtimedelivery.SourceExecutionAtCut{InProgress: true, Started: true, SessionBound: bound}
				if input == nil || input.SourceExecution != want {
					t.Fatalf("native cut lost exact stored execution predicates: input=%+v want=%+v", input, want)
				}
				if got := runfork.ActiveSourceDeliveryConversationCouplingAdmitted(plan, *input); got != sameEmitter {
					t.Fatalf("same-source emitter exception=%t, want=%t, input=%+v point=%+v", got, sameEmitter, input, plan.ForkPoint)
				}
				if !runForkTestHasPlanBlocker(plan, runfork.RunForkBlockerSessionHistoryUnproven) || runForkTestHasPlanBlocker(plan, runfork.RunForkBlockerActiveTurnHistoryUnproven) != bound {
					t.Fatalf("native session/turn summary changed: bound=%t admission=%+v", bound, plan.ReplayResumeAdmission)
				}
				// The policy transforms its admission slice in place. Keep the
				// captured native plan untouched for the later old-cut comparison.
				plan.ReplayResumeAdmission.Dispositions = slices.Clone(plan.ReplayResumeAdmission.Dispositions)
				admission := runfork.RunForkSelectedContractSessionTurnAuditLineageAdmission(plan)
				admission = runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingAdmission(plan, admission)
				blocked := false
				for _, blocker := range admission.UnsupportedBlockers {
					blocked = blocked || blocker.Code == runfork.RunForkBlockerSessionHistoryUnproven
					if sameEmitter && (blocker.Code == runfork.RunForkBlockerSessionHistoryUnproven || blocker.Code == runfork.RunForkBlockerActiveTurnHistoryUnproven) {
						t.Fatalf("exact unbound emitter lost lineage-only admission: %+v", admission)
					}
				}
				if !sameEmitter && !blocked {
					t.Fatalf("bound or unrelated delivery lost its exact refusal: %+v", admission)
				}
			}
			unbound := cut(completion.agentID)
			check(unbound, false, true)
			bound, err := store.BindAgentSession(ctx, completion.origin, completion.sessionID)
			if err != nil || !bound.Acknowledged || bound.Snapshot.ActiveSessionID != completion.sessionID {
				t.Fatalf("bind exact original session: %+v err=%v", bound, err)
			}
			boundCut := cut(completion.agentID)
			check(boundCut, true, false)
			check(cut("different-agent"), true, false)
			var due time.Time
			if err := runDeliveryFixtureAttempt(ctx, store, func(ctx context.Context, attempt *mutationprotocol.Attempt) error {
				adapter := sqliteDeliveryAdapter
				if backend.name == "postgres" {
					adapter = postgresDeliveryAdapter
				}
				renewed, err := adapter.RenewClaim(ctx, attempt, completion.origin, time.Millisecond)
				due = renewed.ClaimExpiresAt
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if wait := time.Until(due); wait > 0 {
				time.Sleep(wait)
			}
			route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(completion.agentID), AgentIdentity: completion.authority.Normal.Identity}
			fresh, err := claimDeliveryFixture(ctx, store, source, route)
			if err != nil || fresh.Claim.Version() != completion.origin.Version()+1 || fresh.Snapshot.ActiveSessionID != "" {
				t.Fatalf("reclaim did not clear original session: %+v err=%v", fresh, err)
			}
			check(cut(completion.agentID), false, true)
			for _, previous := range []runfork.RunForkPlan{unbound, boundCut} {
				again, err := owner.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: source.RunID(), At: previous.ForkPoint.EventID})
				if err != nil || !reflect.DeepEqual(previous.PendingWork, again.PendingWork) || !reflect.DeepEqual(previous.ReplayResumeAdmission, again.ReplayResumeAdmission) {
					t.Fatalf("later bind/clear/reclaim changed old cut: before=%+v after=%+v err=%v", previous, again, err)
				}
			}
		})
	}
}
