package pipeline_test

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func workflowMutationNativeFixture(t *testing.T, backend, runID string, writes, mutations uint64) pipeline.WorkflowMutationNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	return workflowMutationNativeFixtureFromSelected(t, selected, reopen, runID, writes, mutations)
}

func workflowMutationNativeFixtureFromSelected(t *testing.T, selected timerReplaySelectedStore, reopen func() timerReplaySelectedStore, runID string, writes, mutations uint64) pipeline.WorkflowMutationNativeFixtureForTest {
	t.Helper()
	native := workflowActivityNativeFixtureFromSelected(t, selected, reopen)
	ctx := withLiveGateExecution(correlation.WithRunID(native.Context, runID))
	if err := native.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.Total.WriteCommits != writes || counts.ByOperation[storetest.TransactionWorkflowMutation].WriteCommits != mutations || counts.Active != 0 {
			t.Errorf("mutation cohort escaped original native owners: %+v", counts)
		}
	})
	return pipeline.WorkflowMutationNativeFixtureForTest{
		WorkflowProjectionNativeFixtureForTest: pipeline.WorkflowProjectionNativeFixtureForTest{Persistence: native.Persistence, Context: ctx, Construct: native.Construct},
		NewCoordinator:                         native.NewCoordinator,
		PublishAndClaim: func(ctx context.Context, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimedObligation, error) {
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
			return storetest.ClaimDelivery(ctx, selected, event, route)
		},
	}
}

func TestWorkflowInstanceStoreMutateE_RollsBackCallbackFailure(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreMutateE_RollsBackCallbackFailureForTest(t, func(t *testing.T, runID string) pipeline.WorkflowMutationNativeFixtureForTest {
				return workflowMutationNativeFixture(t, backend, runID, 1, 0)
			})
		})
	}
}

func TestWorkflowInstanceStoreMutate_RejectsOverlappingStaleSnapshots(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreMutate_RejectsOverlappingStaleSnapshotsForTest(t, func(t *testing.T, runID string) pipeline.WorkflowMutationNativeFixtureForTest {
				return workflowMutationNativeFixture(t, backend, runID, 2, 1)
			})
		})
	}
}

func TestUpdateEntityState_RejectsCompetingStaleCallbackSnapshot(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyUpdateEntityState_RejectsCompetingStaleCallbackSnapshotForTest(t, func(t *testing.T, runID string) pipeline.WorkflowMutationNativeFixtureForTest {
				return workflowMutationNativeFixture(t, backend, runID, 4, 1)
			})
		})
	}
}

func TestWorkflowInstanceStoreMutate_PersistsSingleWriterUpdates(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreMutate_PersistsSingleWriterUpdatesForTest(t, func(t *testing.T, runID string) pipeline.WorkflowMutationNativeFixtureForTest {
				return workflowMutationNativeFixture(t, backend, runID, 2, 1)
			})
		})
	}
}

func TestNativeWorkflowMutationTransitionRefusesMissingOrForeignClaimBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyNativeWorkflowMutationTransitionRefusesMissingOrForeignClaimForTest(t, func(t *testing.T, runID string) pipeline.WorkflowMutationNativeFixtureForTest {
				return workflowMutationNativeFixture(t, backend, runID, 4, 1)
			})
		})
	}
}
