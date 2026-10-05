package pipeline

import (
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Pre-analysis counterexample only. No production behavior is changed.
func TestIssue2564EvaluatedSnapshotMustFenceCommitBothStores(t *testing.T) {
	for _, tc := range workflowJoinStoreCases() {
		t.Run(tc.name, func(t *testing.T) {
			store, ctx := tc.open(t)
			source := testRootEntityContractSource("root", "test_entity")
			bundle, _ := semanticview.Bundle(source)
			bundle.RootEntities["test_entity"].Fields["count"] = runtimecontracts.EntityFieldDecl{Type: "integer"}
			bundle.RootEntities["test_entity"].Fields["marker"] = runtimecontracts.EntityFieldDecl{Type: "integer"}
			pc := &PipelineCoordinator{workflowStore: store, module: &pipelineFixtureWorkflowModule{source: source}}
			repo := pipelineEngineStateRepo{coordinator: pc}
			entityID := runtimeRunID(ctx)
			if err := store.upsert(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
				InstanceID: entityID, StorageRef: entityID, EntityID: entityID,
				WorkflowName: ".", WorkflowVersion: "1.0.0", CurrentState: "pending",
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
			if committed, err := owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{Address: address, State: fresh}); err != nil || !committed.Committed {
				t.Fatalf("competing commit: acknowledged=%v err=%v", committed.Committed, err)
			}
			r2, found, err := repo.LoadState(ctx, address)
			if err != nil || !found || r2.Revision <= r1.Revision || r2.Fields["marker"] != int64(1) {
				t.Fatalf("R2: revision=%d fields=%v found=%v err=%v", r2.Revision, r2.Fields, found, err)
			}
			committed, commitErr := owner.CommitEngineMutation(ctx, runtimeengine.EngineMutation{Address: address, State: stale})
			after, found, err := repo.LoadState(ctx, address)
			if err != nil || !found {
				t.Fatalf("read after stale commit: %v %v", found, err)
			}
			if committed.Committed || commitErr == nil || after.Fields["marker"] != int64(1) {
				t.Fatalf("stale evaluated snapshot admitted: R1=%d R2=%d final=%d acknowledged=%v err=%v fields=%v; want typed refusal and preserved marker=1",
					r1.Revision, r2.Revision, after.Revision, committed.Committed, commitErr, after.Fields)
			}
		})
	}
}
