package runtimepersistence

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

func TestS1RuntimeContextUsesNativeRunRootBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunAvailabilityStore)
			first, _ := sourceartifactfixture.Rooted(t, "first", sourceartifactfixture.New("schema.yaml", []byte("stages: []\n")))
			second, _ := sourceartifactfixture.Rooted(t, "second", first.Artifact())
			ctx := testAuthorActivityContextForBundle(first.Artifact().BundleHash())
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
			definition := nativeRootedContextFixture(t, retained)
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
			if err := manager.Register(nativeRootedContextFixture(t, second)); err == nil || manager.Len() != 1 {
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
func nativeRootedContextFixture(t *testing.T, source sourceartifact.RootedSource) runtimepkg.BundleContext {
	t.Helper()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(canonicalrouting.RepoRoot(t), source, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	projection, err := packadmission.Admit(bundle.PackInventory, bundle.Platform)
	if err != nil {
		t.Fatal(err)
	}
	bundle.PackAdmission = projection
	subjects, err := projection.ProviderTriggers.InstalledCapabilitySubjects()
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
	runtimeID := uuid.NewString()
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
		Runtime:             &runtimepkg.Runtime{Bus: bus, Options: runtimepkg.RuntimeOptions{RuntimeInstanceID: runtimeID}},
		PackInventoryDigest: bundle.PackInventory.Digest(), ProviderTriggerGeneration: projection.ProviderTriggers.Generation(),
		InstalledTriggerSubjects: subjects,
	}
}
