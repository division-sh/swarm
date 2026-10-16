package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestRunOperationalSessionWorkIsNotStalledBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newCompletionSettlementFixture(t, selected.selected.(completionSettlementTestStore), selected.db, !selected.postgres)
		ctx := correlation.WithRunID(fixture.context, fixture.origin.RunID())
		ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
		ctx = effects.WithLifecycleToken(ctx, fixture.authority.Normal)
		bundle, err := contracts.LoadWorkflowContractBundleFromArtifact("", sourceartifactfixture.Artifact(), platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		constructStaticPublicationSourceFixture(t, ctx, fixture.store.(agentFixtureFlowStore), bundle, "global")
		origin, err := fixture.store.Snapshot(ctx, fixture.origin.DeliveryID())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.SettleSuccess(ctx, fixture.origin, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
			t.Fatal(err)
		}
		if err := acknowledgePipelineEventFixture(ctx, fixture.store, origin.EventID); err != nil {
			t.Fatal(err)
		}
		registry := fixture.store.(sessions.Registry)
		lease, err := registry.Acquire(ctx, testAgentMemoryIdentity(t, fixture.origin.RunID(), fixture.agentID, "global"), fixture.leaseHolder)
		if err != nil {
			t.Fatal(err)
		}
		for _, released := range []bool{false, true} {
			if released {
				if result, err := registry.ReleaseOutcome(ctx, lease); err != nil || !result.Acknowledged {
					t.Fatalf("release exact native session: %+v %v", result, err)
				}
			}
			for read := 0; read < 2; read++ {
				report, err := fixture.store.(operatorread.RunReader).LoadRunDebugReport(ctx, fixture.origin.RunID(), operatorread.RunDebugQueryOptions{})
				if err != nil {
					t.Fatal(err)
				}
				projected := operatorread.ProjectRunOperationalStatus(report)
				wantState, wantDisposition, wantSessions := "running", "active", 1
				if released {
					wantState, wantDisposition, wantSessions = "stalled", "stalled", 0
				}
				if projected.State != wantState || report.TestQuiescence.ActiveSessionLeases != wantSessions || report.EventCount != 1 {
					t.Fatalf("session work changed classification: released=%t status=%+v work=%+v", released, projected, report.TestQuiescence)
				}
				found := false
				for _, instance := range projected.Instances {
					if instance.FlowInstance == "global" {
						found = true
						if instance.Disposition != wantDisposition || instance.Work.Sessions != wantSessions || instance.Work.Deliveries != 0 {
							t.Fatalf("session lost its exact instance owner: %+v", instance)
						}
					}
				}
				if !found {
					t.Fatal("session work omitted the constructed instance")
				}
			}
		}
		requireProviderAttemptCount(t, fixture, 0)
	})
}

func TestRunOperationalFutureTimerIsWaitingBothStores(t *testing.T) {
	eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
		fixture := newCompletionSettlementFixtureForFlow(t, selected.selected.(completionSettlementTestStore), selected.db, !selected.postgres, agentmemory.Plan{}, "global")
		ctx := correlation.WithRunID(fixture.context, fixture.origin.RunID())
		ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
		bundle, err := contracts.LoadWorkflowContractBundleFromArtifact("", sourceartifactfixture.Artifact(), platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		source := constructStaticPublicationSourceFixture(t, ctx, fixture.store.(agentFixtureFlowStore), bundle, "global")
		if _, err := fixture.store.SettleSuccess(ctx, fixture.origin, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
			t.Fatal(err)
		}
		command := testAgentGenericScheduleCommand(t, fixture.origin.RunID(), fixture.agentID, "global", source.Route().EntityID, "stall-wait-proof", genericschedule.AbsoluteDue(time.Now().UTC().Add(time.Hour)))
		admitGenericScheduleFixture(t, ctx, selected.selected.(genericschedule.Store), command)
		for read := 0; read < 2; read++ {
			report, err := fixture.store.(operatorread.RunReader).LoadRunDebugReport(ctx, fixture.origin.RunID(), operatorread.RunDebugQueryOptions{})
			if err != nil {
				t.Fatal(err)
			}
			projected := operatorread.ProjectRunOperationalStatus(report)
			if projected.State != "running" || report.TestQuiescence.DueTimers != 0 || report.EventCount != 1 {
				t.Fatalf("future timer became a stall or executed work: status=%+v quiescence=%+v events=%d", projected, report.TestQuiescence, report.EventCount)
			}
			found := false
			for _, instance := range projected.Instances {
				if instance.FlowInstance == "global" {
					found = true
					if instance.Disposition != "waiting" || instance.Work.Timers != 1 || instance.Work.Deliveries != 0 || instance.Work.Sessions != 0 {
						t.Fatalf("timer ownership = %+v", instance)
					}
				}
			}
			if !found {
				t.Fatal("future timer lost its constructed instance")
			}
			for _, attribution := range projected.Attributions {
				if attribution.FlowInstance == "global" {
					t.Fatalf("waiting instance acquired stall blame: %+v", attribution)
				}
			}
		}
		requireProviderAttemptCount(t, fixture, 0)
	})
}

func TestRunOperationalHumanWaitIsWaitingBothStores(t *testing.T) {
	for _, phase := range []string{decisioncard.HumanTaskContinuationPending, decisioncard.HumanTaskContinuationDecisionCommitted} {
		t.Run(phase, func(t *testing.T) {
			eachExactFactStore(t, func(t *testing.T, selected exactFactStore) {
				fixture := newCompletionSettlementFixtureForFlow(t, selected.selected.(completionSettlementTestStore), selected.db, !selected.postgres, agentmemory.Plan{}, "global")
				ctx := correlation.WithRunID(fixture.context, fixture.origin.RunID())
				ctx = correlation.WithSourceArtifactFact(ctx, sourceartifactfixture.Fact())
				bundle, err := contracts.LoadWorkflowContractBundleFromArtifact("", sourceartifactfixture.Artifact(), platform.PlatformSpecDisplayPath, contracts.WorkflowContractLoadOptions{})
				if err != nil {
					t.Fatal(err)
				}
				source := constructStaticPublicationSourceFixture(t, ctx, fixture.store.(agentFixtureFlowStore), bundle, "global")
				origin, err := fixture.store.Snapshot(ctx, fixture.origin.DeliveryID())
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.SettleSuccess(ctx, fixture.origin, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
					t.Fatal(err)
				}
				now := time.Now().UTC().Truncate(time.Microsecond)
				card, continuation := newHumanTaskDecisionCardTestFixture(t, fixture.origin.RunID(), "stall-wait-"+phase, now, 0, now.Add(time.Hour))
				anchor, err := card.Anchor.HumanTask()
				if err != nil {
					t.Fatal(err)
				}
				anchor.RequesterAgentID, anchor.Source = fixture.agentID, source
				anchor.Scope = decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: source.Route().FlowInstance}
				card.Anchor, err = decisioncard.NewHumanTaskAnchor(anchor)
				if err != nil {
					t.Fatal(err)
				}
				card.BundleHash = sourceartifactfixture.Fact().BundleHash()
				card, err = decisioncard.New(card)
				if err != nil {
					t.Fatal(err)
				}
				continuation.RequesterRoute = source.Route()
				continuation.SourceEventID, continuation.BudgetBundleHash = origin.EventID, card.BundleHash
				human := fixture.store.(decisioncard.HumanTaskStore)
				cards := fixture.store.(decisioncard.Store)
				if err := human.CreateHumanTaskCard(ctx, card, continuation); err != nil {
					t.Fatal(err)
				}
				if phase == decisioncard.HumanTaskContinuationDecisionCommitted {
					outcome, err := DecisionCardDomainForTest(fixture.store).ApplyDecisionForTest(ctx, decisioncard.DecideRequest{
						CardID: card.CardID, Verdict: "approve", PrincipalID: "operator",
						ObservedContentHash: card.CardContentHash, DecisionEventID: uuid.NewString(), Now: now.Add(time.Microsecond),
					})
					if err != nil || outcome.ForcedDeferred || outcome.Card.Status != decisioncard.StatusDecided {
						t.Fatalf("commit decision: %+v %v", outcome, err)
					}
				}
				beforeCard, err := cards.GetDecisionCard(ctx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				beforeContinuation, err := human.LoadHumanTaskContinuation(ctx, card.CardID)
				if err != nil || beforeContinuation.State != phase {
					t.Fatalf("wait fixture: %+v %v", beforeContinuation, err)
				}
				for read := 0; read < 2; read++ {
					report, err := fixture.store.(operatorread.RunReader).LoadRunDebugReport(ctx, fixture.origin.RunID(), operatorread.RunDebugQueryOptions{})
					if err != nil {
						t.Fatal(err)
					}
					projected := operatorread.ProjectRunOperationalStatus(report)
					if projected.State != "running" || report.EventCount != 1 {
						t.Fatalf("human wait became a stall or dispatched work: %+v events=%d", projected, report.EventCount)
					}
					found := false
					for _, instance := range projected.Instances {
						if instance.FlowInstance != source.Route().FlowInstance {
							continue
						}
						found = true
						if instance.Disposition != "waiting" || instance.Work.Await != 1 || instance.Work.Deliveries != 0 || instance.Work.Sessions != 0 || instance.Work.Timers != 0 {
							t.Fatalf("human wait ownership: %+v", instance)
						}
					}
					if !found {
						t.Fatal("human wait lost its constructed instance")
					}
					for _, attribution := range projected.Attributions {
						if attribution.FlowInstance == source.Route().FlowInstance {
							t.Fatalf("waiting instance acquired stall blame: %+v", attribution)
						}
					}
				}
				afterCard, err := cards.GetDecisionCard(ctx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				afterContinuation, err := human.LoadHumanTaskContinuation(ctx, card.CardID)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(beforeCard, afterCard) || !reflect.DeepEqual(beforeContinuation, afterContinuation) {
					t.Fatalf("diagnostics mutated human task: before=%+v/%+v after=%+v/%+v", beforeCard, beforeContinuation, afterCard, afterContinuation)
				}
				requireProviderAttemptCount(t, fixture, 0)
			})
		})
	}
}
