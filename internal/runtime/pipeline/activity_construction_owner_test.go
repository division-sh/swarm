package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

type activityConstructionReader struct {
	WorkflowInstancePersistenceReader
	load func(flowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error)
}

func (r activityConstructionReader) LoadWorkflowInstance(_ context.Context, owner flowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error) {
	return r.load(owner)
}

func TestReadOnlyActivityConsumesConstructedParentBeforeGeneration(t *testing.T) {
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopyLifecycleNestedTemplates(t), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	parent := flowidentity.Derive(source, "outer/left/sink", "same-revision")
	child, err := flowidentity.KeylessChild(source, parent, parent.TemplateID+"/final")
	if err != nil {
		t.Fatal(err)
	}
	activation, err := loopruntime.New(testPipelineRunID, child.EntityID, child.TemplateID, "revision", "revision_id", uuid.NewString(), "review", 3, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	buckets := map[string]map[string]any{}
	if err := loopruntime.Store(buckets, activation); err != nil {
		t.Fatal(err)
	}
	valid := WorkflowInstance{
		WorkflowName: child.TemplateID, InstanceID: child.InstanceID, StorageRef: child.InstancePath, EntityID: child.EntityID,
		ParentFlowID: child.ParentRoute.FlowID, ParentFlowInstance: child.ParentRoute.FlowInstance, ParentEntityID: child.ParentEntityID,
		CurrentState: "review", StateBuckets: runtimeengine.NewStateCarrier(nil, nil, buckets).PersistedStateBuckets(),
	}
	childFlow, err := identity.AdmitFlowIdentity(child.TemplateID)
	if err != nil {
		t.Fatal(err)
	}
	parentFlow, err := identity.AdmitFlowIdentity(parent.TemplateID)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"exact", "missing", "foreign_run", "foreign_flow", "foreign_entity", "missing_parent", "crossed_parent", "stale_generation"} {
		t.Run(variant, func(t *testing.T) {
			instance := valid
			intent := runtimeengine.ActivityIntent{
				SourceRunID: testPipelineRunID, ExecutionFlowID: identity.FlowID(childFlow.String()),
				FlowInstance: child.InstancePath, EntityID: identity.EntityID(child.EntityID), Generation: activation.Generation(), LoopStage: "review",
			}
			switch variant {
			case "foreign_run":
				intent.SourceRunID = uuid.NewString()
			case "foreign_flow":
				intent.ExecutionFlowID = identity.FlowID(parentFlow.String())
			case "foreign_entity":
				intent.EntityID = identity.EntityID(parent.EntityID)
			case "missing_parent":
				instance.ParentFlowInstance = ""
			case "crossed_parent":
				other := flowidentity.Derive(source, parent.TemplateID, "other-revision")
				instance.ParentFlowInstance, instance.ParentEntityID = other.InstancePath, other.EntityID
			case "stale_generation":
				intent.Generation.RevisionID = uuid.NewString()
			}
			reader := activityConstructionReader{load: func(owner flowidentity.RunScopedFlowInstance) (WorkflowInstance, bool, error) {
				if owner.RunID != testPipelineRunID || variant == "missing" {
					return WorkflowInstance{}, false, nil
				}
				if owner.Route.InstancePath != child.InstancePath || owner.Route.ScopeKey != flowidentity.ScopeKey(source, intent.ExecutionFlowID.String()) {
					t.Fatalf("activity lookup changed its admitted coordinate: %+v", owner)
				}
				return instance, true, nil
			}}
			pc := &PipelineCoordinator{
				module: staticSemanticWorkflowModule{source: source}, workflowStore: &workflowInstanceStore{instanceReader: reader},
				entityLocks: map[string]*sync.Mutex{},
			}
			err := (pipelineActivityDispatcher{coordinator: pc}).admitReadOnlyActivityGeneration(context.Background(), intent)
			if (err == nil) != (variant == "exact") {
				t.Fatalf("activity construction/generation admission %s: %v", variant, err)
			}
		})
	}
}
