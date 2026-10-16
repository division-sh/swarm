package conformance

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
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
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected any
			if backend == "sqlite" {
				selected = storetest.StartSQLiteRuntimeStore(t)
			} else {
				_, db, cleanup := testutil.StartPostgres(t)
				t.Cleanup(cleanup)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
			}
			ctx := testAuthorActivityContextForBundle(context.Background(), sourceartifactfixture.FactFor(first.Artifact()))
			storetest.RequireBundleDataCatalog(t, ctx, selected.(storetest.RootedDataCatalogStore), other)
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
		})
	}
}
