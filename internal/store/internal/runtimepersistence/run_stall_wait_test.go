package runtimepersistence

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

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
