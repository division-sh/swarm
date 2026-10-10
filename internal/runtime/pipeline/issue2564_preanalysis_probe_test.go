package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func VerifyIssue2564EvaluatedSnapshotMustFenceCommitBothStoresForTest(t *testing.T, open func(*testing.T, string, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			source := loadWorkflowTempSource(t, map[string]string{
				"schema.yaml":   "name: stale-evaluated-snapshot\nstages:\n  pending: {}\n",
				"entities.yaml": "test_entity:\n  count: integer\n  marker: integer\n",
			})
			fixture := open(t, tc.name, source)
			pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: source}})
			store := pc.workflowStore
			ctx := correlation.WithRunID(fixture.Context, testPipelineRunID)
			if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
				t.Fatal(err)
			}
			repo := pipelineEngineStateRepo{coordinator: pc}
			entityID := runtimeRunID(ctx)
			if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, EntityID: entityID,
				WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(), CurrentState: "pending",
				Fields: map[string]any{"count": 0, "marker": 0}, EntityType: "test_entity",
			})); err != nil {
				t.Fatal(err)
			}
			address := runtimeengine.StateAddress{FlowID: identity.NormalizeFlowID("."),
				FlowInstance: testRunScopedWorkflowInstanceFromContext(ctx, entityID), EntityID: identity.NormalizeEntityID(entityID)}
			r1, found, err := repo.LoadState(ctx, address)
			if err != nil || !found {
				t.Fatalf("R1: found=%v err=%v", found, err)
			}
			stale := runtimeengine.StateMutation{StateCarrier: runtimeengine.NewStateCarrierWithOwners(
				r1.Fields, r1.Bookkeeping, r1.Control, r1.Gates, r1.StateBuckets)}
			stale.SetField("count", 1)
			owner := pipelineEngineMutationOwner{store: store, state: repo}
			fresh := runtimeengine.StateMutation{StateCarrier: runtimeengine.NewStateCarrierWithOwners(
				r1.Fields, r1.Bookkeeping, r1.Control, r1.Gates, r1.StateBuckets)}
			fresh.SetField("marker", 1)
			if committed, err := owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{Address: address, EvaluatedState: r1, State: fresh}); err != nil || !committed.Committed {
				t.Fatalf("competing commit: acknowledged=%v err=%v", committed.Committed, err)
			}
			r2, found, err := repo.LoadState(ctx, address)
			if err != nil || !found || r2.Revision <= r1.Revision || r2.Fields["marker"] != int64(1) {
				t.Fatalf("R2: revision=%d fields=%v found=%v err=%v", r2.Revision, r2.Fields, found, err)
			}
			committed, commitErr := owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{Address: address, EvaluatedState: r1, State: stale})
			after, found, err := repo.LoadState(ctx, address)
			if err != nil || !found {
				t.Fatalf("read after stale commit: %v %v", found, err)
			}
			if committed.Committed || !failures.IsStateContention(commitErr) || after.Revision != r2.Revision || after.Fields["marker"] != int64(1) {
				t.Fatalf("stale evaluated snapshot admitted: R1=%d R2=%d final=%d acknowledged=%v err=%v fields=%v; want typed refusal and preserved marker=1",
					r1.Revision, r2.Revision, after.Revision, committed.Committed, commitErr, after.Fields)
			}
		})
	}
}
