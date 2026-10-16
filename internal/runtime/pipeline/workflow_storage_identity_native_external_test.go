package pipeline_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
)

func workflowStorageIdentityNativeFixture(t *testing.T, backend string, wantCommits uint64) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	fixture := workflowActivityNativeFixtureFromSelected(t, selected, reopen)
	fixture.Context = withLiveGateExecution(fixture.Context)
	// The first run also persists the canonical source artifact once.
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.Total.WriteCommits != wantCommits || counts.ByOperation[storetest.TransactionWorkflowMutation].WriteCommits != 0 || counts.Active != 0 {
			t.Errorf("storage identity escaped original selected construction: %+v, want commits=%d", counts, wantCommits)
		}
	})
	return fixture
}

func TestWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleed(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStore_RunScopedCurrentStateRowsDoNotBleedForTest(t, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowStorageIdentityNativeFixture(t, backend, 5)
			})
		})
	}
}

func TestWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifyWorkflowInstanceStoreAddressesRowsOnlyByExactRouteOnBothStoresForTest(t, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowStorageIdentityNativeFixture(t, backend, 4)
			})
		})
	}
}

func TestSQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadata(t *testing.T) {
	bundle := loadPipelineLifecycleFixtureBundle(t, map[string]string{
		"schema.yaml":                  "name: storage-identity\nstages:\n  created: {}\n",
		"operating/schema.yaml":        "name: operating\ninstance: instance_key\nstages:\n  created: {}\n",
		"operating/state.yaml":         "test_entity:\n  instance_key: text\n",
		"operating/review/schema.yaml": "name: review\ninstance: instance_key\nstages:\n  created: {}\n",
		"operating/review/state.yaml":  "test_entity:\n  instance_key: text\n",
		"review/schema.yaml":           "name: review\ninstance: instance_key\nstages:\n  created: {}\n",
		"review/state.yaml":            "test_entity:\n  instance_key: text\n",
	})
	source := semanticview.Wrap(bundle)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			pipeline.VerifySQLiteWorkflowInstanceStore_PreservesParentRouteControlMetadataForTest(t, source, func(t *testing.T) pipeline.WorkflowActivityNativeFixtureForTest {
				return workflowParentRouteControlNativeFixture(t, backend, source)
			})
		})
	}
}

func workflowParentRouteControlNativeFixture(t *testing.T, backend string, source semanticview.Source) pipeline.WorkflowActivityNativeFixtureForTest {
	t.Helper()
	bundle, found := semanticview.Bundle(source)
	if !found || bundle.SourceArtifact == nil {
		t.Fatal("parent-route proof requires its exact declared source")
	}
	selected, _, reopen := openTimerReplayNativeStore(t, backend)
	fixture := workflowActivityNativeFixtureFromSelectedSource(t, selected, reopen, bundle.SourceArtifact)
	fixture.Context = withLiveGateExecution(fixture.Context)
	probe := storetest.CollectTransactions(t, selected, storetest.TransactionProbeOptions{})
	t.Cleanup(func() {
		counts := probe.Snapshot()
		if counts.Total.WriteCommits != 3 || counts.ByOperation[storetest.TransactionWorkflowMutation].WriteCommits != 0 || counts.Active != 0 {
			t.Errorf("storage identity escaped original selected construction: %+v, want commits=3", counts)
		}
	})
	fixture.Construct = func(ctx context.Context, instance pipeline.WorkflowInstance) error {
		runID := correlation.RunIDFromContext(ctx)
		root := flowidentity.Stored(source, ".", runID, runID, flowidentity.EntityID(runID), "")
		parent, err := flowidentity.KeyedChild(source, root, "operating", "root")
		if err != nil {
			return err
		}
		parent.EntityID = eventtest.UUID("parent-ent")
		parentKey, err := pipeline.AdmitFlowInstanceKey(source, parent.TemplateID, "root")
		if err != nil {
			return err
		}
		rootCommand, err := flowactivationfixture.Command(ctx, pipeline.WorkflowInstance{
			InstanceID: root.InstanceID, StorageRef: root.InstancePath, EntityID: root.EntityID,
			WorkflowName: root.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: "static",
			CurrentState: "created", StageDefined: true, Fields: map[string]any{},
			CreatedAt: instance.CreatedAt, EnteredStageAt: instance.EnteredStageAt,
		}, pipeline.WorkflowLifecycleMutationPlan{}, instance.CreatedAt)
		if err != nil {
			return err
		}
		parentCommand, err := flowactivationfixture.Command(ctx, pipeline.WorkflowInstance{
			InstanceID: parent.InstanceID, StorageRef: parent.InstancePath, EntityID: parent.EntityID,
			ParentFlowID: parent.ParentRoute.FlowID, ParentFlowInstance: parent.ParentRoute.FlowInstance, ParentEntityID: parent.ParentEntityID,
			WorkflowName: parent.TemplateID, WorkflowVersion: source.WorkflowVersion(), Mode: "template", InstanceKey: parentKey,
			CurrentState: "created", StageDefined: true, EntityType: "test_entity", Fields: map[string]any{"instance_key": "root"},
			CreatedAt: instance.CreatedAt, EnteredStageAt: instance.EnteredStageAt,
		}, pipeline.WorkflowLifecycleMutationPlan{}, instance.CreatedAt)
		if err != nil {
			return err
		}
		childCommand, err := flowactivationfixture.Command(ctx, instance, pipeline.WorkflowLifecycleMutationPlan{}, instance.CreatedAt)
		if err != nil {
			return err
		}
		parentCommand.Plan.Children = []pipeline.FlowInstanceActivationPlan{childCommand.Plan}
		if err := parentCommand.Validate(); err != nil {
			return err
		}
		rootCommand.Plan.Children = []pipeline.FlowInstanceActivationPlan{parentCommand.Plan}
		if err := rootCommand.Validate(); err != nil {
			return fmt.Errorf("lawful run-root/keyed-parent tree rejected before native construction: %w", err)
		}
		committed, err := selected.(runtimebus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, rootCommand)
		if err != nil {
			return err
		}
		if !committed.Acknowledged || !committed.Created || len(committed.Children) != 1 || !committed.Children[0].Acknowledged || !committed.Children[0].Created || len(committed.Children[0].Children) != 1 || !committed.Children[0].Children[0].Acknowledged || !committed.Children[0].Children[0].Created {
			return fmt.Errorf("parent-route construction did not acknowledge the exact complete native tree")
		}
		if err := committed.Validate(); err != nil {
			return fmt.Errorf("acknowledged native tree evidence: %w", err)
		}
		for _, plan := range rootCommand.Plan.ConstructionPlans() {
			owner, err := flowidentity.NewRunScopedFlowInstance(runID, plan.Identity.Route())
			if err != nil {
				return err
			}
			publication, err := storetest.ReadReceiverConstructionPublication(ctx, selected, owner, plan.Identity.EntityID)
			if err != nil || publication.Identity != plan.Identity || publication.InstanceKey != plan.Instance.InstanceKey ||
				publication.WorkflowVersion != source.WorkflowVersion() || publication.BundleHash != plan.Readiness.BundleHash ||
				publication.CreatingInput != plan.CreatingInput || !reflect.DeepEqual(publication.Fields, plan.Instance.Fields) {
				return fmt.Errorf("immutable native receipt changed %s: evidence=%+v err=%v", plan.Identity.InstancePath, publication, err)
			}
			readiness, found, err := selected.(pipeline.DynamicFlowRuntimeReadinessPersistence).LoadDynamicFlowRuntimeReadiness(ctx, runID, owner.Route)
			want, normalizeErr := plan.Readiness.Normalized()
			wantHash, hashErr := plan.Readiness.Hash()
			if err != nil || normalizeErr != nil || hashErr != nil || !found || readiness.AttemptOrdinal != 1 ||
				readiness.Phase != pipeline.FlowAttachmentPlanned || readiness.PlanHash != wantHash || !reflect.DeepEqual(readiness.Plan, want) {
				return fmt.Errorf("immutable native readiness changed %s: readiness=%+v err=%v normalize=%v hash=%v", plan.Identity.InstancePath, readiness, err, normalizeErr, hashErr)
			}
		}
		return nil
	}
	return fixture
}
