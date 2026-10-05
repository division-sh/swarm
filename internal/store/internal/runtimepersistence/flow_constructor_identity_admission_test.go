package runtimepersistence

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

func TestFlowConstructorRejectsNoncanonicalRequestIdentityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, map[string]string{
				"schema.yaml":        "name: constructor-identity\n",
				"review/schema.yaml": "name: review\n",
			}, nil)
			runID := correlation.RunIDFromContext(f.ctx)
			root := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
			root.Instance = flowidentity.Stored(root.ContractBundle, ".", runID, runID, runID, "")
			rootPlan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, root)
			if err != nil || len(rootPlan.Children) != 1 {
				t.Fatalf("prepare exact root tree: plan=%+v err=%v", rootPlan, err)
			}
			child := sqliteFlowActivationRequest(f.bundle, "review", "review", "", "review")
			child.Instance = rootPlan.Children[0].Identity
			if _, err := f.manager.PrepareFlowInstanceActivation(f.ctx, child); err != nil {
				t.Fatalf("prepare exact constructed child identity: %v", err)
			}
			for _, tc := range []struct {
				name string
				req  pipeline.FlowInstanceActivationRequest
				edit func(*flowidentity.Instance)
			}{
				{"foreign_root_run", root, func(i *flowidentity.Instance) {
					i.InstanceID, i.InstancePath, i.EntityID = uuid.NewString(), uuid.NewString(), uuid.NewString()
				}},
				{"foreign_root_entity", root, func(i *flowidentity.Instance) { i.EntityID = uuid.NewString() }},
				{"missing_child_parent", child, func(i *flowidentity.Instance) { i.ParentRoute, i.ParentEntityID = flowidentity.ParentRoute{}, "" }},
				{"foreign_parent_run", child, func(i *flowidentity.Instance) {
					foreign := uuid.NewString()
					i.ParentRoute.FlowInstance, i.ParentRoute.EntityID, i.ParentEntityID = foreign, foreign, foreign
				}},
				{"crossed_parent_entity", child, func(i *flowidentity.Instance) { i.ParentEntityID = uuid.NewString() }},
				{"foreign_child_entity", child, func(i *flowidentity.Instance) { i.EntityID = uuid.NewString() }},
				{"authored_path_as_instance", child, func(i *flowidentity.Instance) { i.InstancePath, i.InstanceID = "review/extra", "extra" }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					req := tc.req
					tc.edit(&req.Instance)
					if _, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req); err == nil || !strings.Contains(err.Error(), "construction identity") {
						t.Errorf("invalid constructor identity reached lifecycle planning: %+v err=%v", req.Instance, err)
					}
					assertConstructorRows(t, f, backend, 0)
				})
			}
		})
	}
}
