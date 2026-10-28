package runtimepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestS1RuntimeContextUsesNativeRunRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunAvailabilityStore)
			artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
				"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
				"schema.yaml":   []byte("stages: []\n"),
			})
			if err != nil {
				t.Fatal(err)
			}
			first, _ := sourceartifactfixture.Rooted(t, "first", artifact)
			second, _ := sourceartifactfixture.Rooted(t, "second", first.Artifact())
			ctx := originalStoreFixtureContext(t, testAuthorActivityContextForBundle(first.Artifact().BundleHash()), selected)
			for _, source := range []sourceartifact.RootedSource{first, second} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			request := rootedRunCreationRequest(first)
			if _, err := selected.CreateRun(ctx, request); err != nil {
				t.Fatal(err)
			}
			retained, err := selected.LoadRunRootedSource(ctx, request.RunID)
			if err != nil {
				t.Fatal(err)
			}
			coordinate, err := agenttopology.NewSourceCoordinate(retained.Admission())
			if err != nil {
				t.Fatal(err)
			}
			sourceSet, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
			if err != nil {
				t.Fatal(err)
			}
			grant, err := agentfixture.AdmitGeneration(t, ctx, selected.(agentfixture.Store), sourceSet, coordinate)
			if err != nil {
				t.Fatal(err)
			}
			admitAgentFixtureFlowGrant(t, ctx, grant)
			ctx, err = startupownership.WithRuntimeMutationAuthority(ctx, grant)
			if err != nil {
				t.Fatal(err)
			}
			evidence, err := grant.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			definition := nativeRootedContextFixture(t, retained, evidence.RuntimeInstanceID)
			manager, err := runtimepkg.NewRuntimeContextManager(selected, definition)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				for _, result := range manager.DeactivateAll(runtimepkg.RuntimeContextCauseUnloaded) {
					if result.ShutdownErr != nil {
						t.Error(result.ShutdownErr)
					}
				}
			})
			use, lookup, availability, err := manager.AcquireRun(ctx, request.RunID)
			if err != nil || use == nil || !lookup.Loaded() || availability.RootReference != first.Admission().Reference() {
				t.Fatalf("native exact-root dispatch: %+v, %v", availability, err)
			}
			root, present, rootErr := correlation.RootAdmissionFromContext(use.WorkContext())
			doneErr := use.Done()
			if rootErr != nil || doneErr != nil || !present || !root.Equal(first.Admission()) {
				t.Fatalf("native dispatch lost its exact work root: root=%+v rootErr=%v doneErr=%v", root.Reference(), rootErr, doneErr)
			}
			if _, err := selected.ReviseRunSource(ctx, runlifecycle.SourceRevisionRequest{
				RunID: request.RunID, Source: request.Source, RootAdmission: second.Admission(),
			}); err != nil {
				t.Fatal(err)
			}
			use, _, _, err = manager.AcquireRun(ctx, request.RunID)
			if use != nil {
				_ = use.Done()
				t.Fatal("revised native run borrowed the same-hash predecessor context")
			}
			if err == nil {
				t.Fatal("native root mismatch was not refused")
			}
			if lookup, _, err := manager.LookupRunStatus(ctx, request.RunID); err == nil || lookup.Loaded() {
				t.Fatalf("native readback borrowed the previous root: %+v, %v", lookup, err)
			}
			if err := manager.Register(nativeRootedContextFixture(t, second, uuid.NewString())); err == nil || manager.Len() != 1 {
				t.Fatal("second native root created a second public same-hash context")
			}
			use, _, err = manager.AcquireBundleHash(ctx, definition.BundleHash())
			if err != nil || use == nil || !use.Context.RootAdmission.Equal(first.Admission()) {
				t.Fatalf("failed native dispatch damaged the first context: %v", err)
			}
			if err := use.Done(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// This component fixture installs real compiled and occurrence owners with the
// native availability reader; it is not a RunServe or provider execution proof.
func nativeRootedContextFixture(t *testing.T, source sourceartifact.RootedSource, runtimeID string) runtimepkg.BundleContext {
	t.Helper()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), source, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	providers, err := packadmission.AdmitProviderSource(source, bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	scope, err := providers.Scope(".")
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := scope.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := catalog.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	t.Cleanup(func() {
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Error(err)
		}
	})
	fact := sourceartifactfixture.FactFor(source.Artifact())
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{RuntimeInstanceID: runtimeID, BundleHash: fact.BundleHash()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := owner.RetireAndWait(context.Background()); err != nil {
			t.Error(err)
		}
	})
	semantic := semanticview.Wrap(bundle)
	bus, err := runtimebus.NewEphemeralEventBusWithOptions(nil, runtimebus.EventBusOptions{
		ContractBundle: semantic, SourceArtifactFact: fact, RuntimeInstanceID: runtimeID, WorkOwner: owner,
		ReceiverExecution: eventreceiver.NormalExecution(), ExecutionPosture: executionposture.Live,
	})
	if err != nil {
		t.Fatal(err)
	}
	return runtimepkg.BundleContext{
		Source: semantic, SourceArtifactFact: fact, RootAdmission: source.Admission(), WorkOwner: owner,
		Runtime: &runtimepkg.Runtime{Bus: bus, Options: runtimepkg.RuntimeOptions{RuntimeInstanceID: runtimeID, SourceArtifactFact: fact,
			WorkflowModule: runForkGateWorkflowModule{source: semantic}}},
		PackInventoryDigest: bundle.PackInventory.Digest(), ProviderTriggerGeneration: catalog.Generation(),
		InstalledTriggerSubjects: subjects, ProviderTriggerCatalog: catalog,
	}
}
