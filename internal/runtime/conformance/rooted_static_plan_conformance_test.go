package conformance

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestS1ZeroAgentStartupRefusesOtherRootBothStores(t *testing.T) {
	first, _ := sourceartifactfixture.Rooted(t, "first", sourceartifactfixture.New("schema.yaml", []byte("stages: []\n")))
	second, _ := sourceartifactfixture.Rooted(t, "second", first.Artifact())
	options := runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory}
	compiled, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), first, "", options)
	if err != nil {
		t.Fatal(err)
	}
	other, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), second, "", options)
	if err != nil {
		t.Fatal(err)
	}
	spare, _ := sourceartifactfixture.Rooted(t, "spare", sourceartifactfixture.New("schema.yaml", []byte("required_agents: []\n")))
	spareCompiled, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), spare, "", options)
	if err != nil {
		t.Fatal(err)
	}
	spareCoordinate, err := agenttopology.NewSourceCoordinate(spare.Admission())
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected any
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				selected = storetest.StartPostgresRuntimeStore(t)
			}
			ctx := testAuthorActivityContextForBundle(context.Background(), sourceartifactfixture.FactFor(first.Artifact()))
			storetest.RequireBundleDataCatalog(t, ctx, selected.(storetest.RootedDataCatalogStore), other)
			storetest.RequireBundleDataCatalog(t, ctx, selected.(storetest.RootedDataCatalogStore), spareCompiled)
			module := conformanceLoadedWorkflowModule{source: semanticview.Wrap(compiled)}
			rt, capability, _ := newStageLifecycleIdentityRuntime(t, selected, module)
			before, present, err := capability.CurrentSourceSet(ctx)
			if err != nil || !present || len(before.Sources) != 1 || before.Sources[0].RootReference() != first.Admission().Reference() || len(before.Agents) != 0 {
				t.Fatalf("exact zero-agent native source set: %+v, %v", before, err)
			}
			if err := rt.Manager.PrepareStaticTopologyForStartup(ctx, module.source); err != nil {
				t.Fatalf("exact-root zero-agent startup refused: %v", err)
			}
			if err := rt.Manager.PrepareStaticTopologyForStartup(ctx, semanticview.Wrap(other)); err == nil {
				t.Fatal("native zero-agent startup accepted another admitted root under the first plan")
			}
			after, present, err := capability.CurrentSourceSet(ctx)
			if err != nil || !present || !reflect.DeepEqual(before, after) {
				t.Fatalf("foreign-root startup changed native authority: %+v, %v", after, err)
			}
			startStageLifecycleIdentityRuntime(t, rt)
			projection, err := packadmission.FromBundle(compiled)
			if err != nil {
				t.Fatal(err)
			}
			subjects, err := projection.ProviderTriggers.InstalledCapabilitySubjects()
			if err != nil {
				t.Fatal(err)
			}
			contexts, err := runtimepkg.NewRuntimeContextManager(nil, runtimepkg.BundleContext{
				SourceArtifactFact: sourceartifactfixture.FactFor(first.Artifact()), RootAdmission: first.Admission(),
				Source: module.source, Runtime: rt, WorkOwner: rt.WorkOccurrence(),
				PackInventoryDigest: compiled.PackInventory.Digest(), ProviderTriggerGeneration: projection.ProviderTriggers.Generation(), InstalledTriggerSubjects: subjects,
			})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := agenttopology.NewSourceSetPlan(append(before.Sources, spareCoordinate), before.Agents)
			if err != nil || plan.Revision == before.Revision {
				t.Fatalf("distinct successor source set: %+v, %v", plan, err)
			}
			transition, err := contexts.PrepareSourceSetTransition(ctx, plan)
			if err != nil || transition == nil {
				t.Fatalf("prepare exact-root zero-agent source-set rebind: %v", err)
			}
			defer func() {
				if err := transition.Abort(); err != nil {
					t.Error(err)
				}
			}()
			if _, err := capability.RestoreSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), ExpectedRevision: before.Revision, Plan: plan}); err != nil {
				t.Fatal(err)
			}
			if err := transition.Commit(ctx, capability); err != nil {
				t.Fatalf("commit exact-root zero-agent source-set rebind: %v", err)
			}
			after, present, err = capability.CurrentSourceSet(ctx)
			if err != nil || !present || !reflect.DeepEqual(plan, after) {
				t.Fatalf("native successor lost exact source membership: %+v, %v", after, err)
			}
			loaded, present := contexts.LookupBundleHash(first.Admission().BundleHash())
			if !present || !loaded.RootAdmission.Equal(first.Admission()) {
				t.Fatal("source-set rebind changed or hid the accepted runtime root")
			}
		})
	}
}
