package runtimepersistence

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestR7ConstructionAddressImmutableBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":        "name: immutable-construction\ninstance: item_id\npins:\n  inputs:\n    - item.created\n",
				"events.yaml":        "item.created:\n  item_id: text\n",
				"entities.yaml":      "item:\n  item_id: text\n",
				"detail/schema.yaml": "name: detail\n",
			}, nil)
			runID := correlation.RunIDFromContext(f.ctx)
			request := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
			request.Instance = flowidentity.Stored(request.ContractBundle, ".", runID, runID, runID, "")
			request.ConstructorInput, request.ResolvedKey = "item.created", "original"
			request.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "item.created", "constructor-fixture", "", []byte(`{"item_id":"original"}`), 0, runID, events.EventEnvelope{}, request.OccurredAt)
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
				t.Fatal(err)
			}
			for _, construction := range plan.ConstructionPlans() {
				owner, err := flowidentity.NewRunScopedFlowInstance(runID, construction.Identity.Route())
				if err != nil {
					t.Fatal(err)
				}
				stored, found, err := f.workflows.Load(f.ctx, owner)
				if err != nil || !found || stored.ParentFlowInstance != construction.Identity.ParentRoute.FlowInstance || stored.InstanceKey != construction.Instance.InstanceKey {
					t.Fatalf("constructed address readback: %+v found=%t err=%v", stored, found, err)
				}
				evidence, err := f.store.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(f.ctx, owner, stored.EntityID)
				if err != nil || evidence.InstanceKey != stored.InstanceKey || evidence.Identity != construction.Identity {
					t.Fatalf("immutable address evidence: %+v err=%v", evidence, err)
				}
			}
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, plan.Identity.Route())
			if err != nil {
				t.Fatal(err)
			}
			record, err := plan.PersistenceRecord()
			if err != nil {
				t.Fatal(err)
			}
			mutation := record.State
			mutation.Transition = pipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
			mutation.ExpectedState, mutation.ExpectedRevision = mutation.CurrentState, 1
			mutation.UpdatedAt = mutation.CreatedAt.Add(time.Second)
			mutation.Fields = json.RawMessage(`{"item_id":"changed-business-field"}`)
			writer := f.store.(pipeline.WorkflowEngineMutationOwner)
			if result, err := writer.CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: mutation}); err != nil || !result.Committed {
				t.Fatalf("ordinary field update: committed=%t err=%v", result.Committed, err)
			}
			stored, found, err := f.workflows.Load(f.ctx, owner)
			if err != nil || !found || stored.InstanceKey != "original" || stored.Fields["item_id"] != "changed-business-field" || stored.Revision != 2 {
				t.Fatalf("mutable fields changed construction identity: %+v found=%t err=%v", stored, found, err)
			}
			evidence, err := f.store.(pipeline.FlowConstructionPublicationReader).LoadFlowConstructionPublication(f.ctx, owner, stored.EntityID)
			if err != nil || evidence.InstanceKey != "original" || evidence.Fields["item_id"] != "original" {
				t.Fatalf("field update rewrote the creating receipt: %+v err=%v", evidence, err)
			}
			mutation.ExpectedRevision = 2
			mutation.UpdatedAt = mutation.UpdatedAt.Add(time.Second)
			mutation.InstanceKey = "changed-business-field"
			if result, err := writer.CommitWorkflowEngineMutation(f.ctx, pipeline.WorkflowEngineMutationCommand{State: mutation}); err == nil || result.Committed {
				t.Fatalf("ordinary update rewrote immutable key: committed=%t err=%v", result.Committed, err)
			}
			stored, found, err = f.workflows.Load(f.ctx, owner)
			if err != nil || !found || stored.InstanceKey != "original" || stored.Revision != 2 {
				t.Fatalf("rejected key update mutated state: %+v found=%t err=%v", stored, found, err)
			}
		})
	}
}
