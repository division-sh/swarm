package runtime

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func rootedContextFixture(t *testing.T, directory string) (BundleContext, sourceartifact.RootedSource) {
	t.Helper()
	root, source := loadRootedBudgetSource(t, directory, "stages: []\n")
	return rootedContextForSource(t, source, root), root
}

func rootedContextForSource(t *testing.T, source semanticview.Source, root sourceartifact.RootedSource) BundleContext {
	t.Helper()
	fact := sourceartifactfixture.FactFor(root.Artifact())
	owner := runtimeTestOccurrence(t, fact.BundleHash())
	bus, err := newRuntimeTestEventBusWithOptions(t, nil, runtimebus.EventBusOptions{
		WorkOwner: owner, ContractBundle: source, SourceArtifactFact: fact,
	})
	if err != nil {
		t.Fatal(err)
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("rooted fixture has no compiled bundle")
	}
	admitRuntimeTestBundle(t, bundle)
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := projection.ProviderTriggers.InstalledCapabilitySubjects()
	if err != nil {
		t.Fatal(err)
	}
	return BundleContext{
		Source: source, SourceArtifactFact: fact, WorkOwner: owner,
		Runtime:             &Runtime{Bus: bus, workOccurrence: owner},
		PackInventoryDigest: bundle.PackInventory.Digest(), ProviderTriggerGeneration: projection.ProviderTriggers.Generation(),
		InstalledTriggerSubjects: subjects,
	}
}

func TestS1RuntimeContextRefusesCrossRootHashAcquisition(t *testing.T) {
	first, firstRoot := rootedContextFixture(t, "first")
	_, otherRoot := rootedContextFixture(t, "second")
	if firstRoot.Artifact().BundleHash() != otherRoot.Artifact().BundleHash() {
		t.Fatal("fixture must differ only in its admitted root")
	}
	manager, err := newTestRuntimeContextManager(t, nil, first)
	if err != nil {
		t.Fatal(err)
	}
	ctx := correlation.WithSourceArtifactFact(context.Background(), first.SourceArtifactFact)
	ctx = correlation.WithRootAdmission(ctx, otherRoot.Admission())
	use, _, err := manager.AcquireBundleHash(ctx, first.BundleHash())
	if use != nil {
		if doneErr := use.Done(); doneErr != nil {
			t.Error(doneErr)
		}
		t.Fatalf("same-content foreign root acquired the loaded context: %v", err)
	}
	if err == nil {
		t.Fatal("foreign rooted acquisition was not refused")
	}
	lookup := manager.LookupBundleHashStatus(first.BundleHash())
	if !lookup.Loaded() {
		t.Fatal("refused acquisition damaged the original context")
	}
}

func TestS1RuntimeContextRefusesRunWithoutRootReference(t *testing.T) {
	first, _ := rootedContextFixture(t, "first")
	reader := fakeRunBundleAvailability{rows: map[string]runbundle.Availability{
		"run": {RunID: "run", BundleHash: first.BundleHash(), SourceArtifactPresent: true},
	}}
	manager, err := newTestRuntimeContextManager(t, reader, first)
	if err != nil {
		t.Fatal(err)
	}
	use, _, _, err := manager.AcquireRun(context.Background(), "run")
	if use != nil {
		if doneErr := use.Done(); doneErr != nil {
			t.Error(doneErr)
		}
		t.Fatalf("content-only run evidence acquired rooted execution: %v", err)
	}
	if err == nil {
		t.Fatal("missing run root reference was not refused")
	}
}

func TestS1RuntimeContextResetRefusesSameContentOtherRoot(t *testing.T) {
	first, firstRoot := rootedContextFixture(t, "first")
	second, secondRoot := rootedContextFixture(t, "second")
	if firstRoot.Artifact().BundleHash() != secondRoot.Artifact().BundleHash() {
		t.Fatal("fixture must differ only in its admitted root")
	}
	manager, err := newTestRuntimeContextManager(t, nil, first)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range manager.DeactivateAll(RuntimeContextCauseReset) {
		if result.ShutdownErr != nil {
			t.Fatal(result.ShutdownErr)
		}
	}
	if err := manager.StageResetRuntimeContexts(second); err == nil {
		t.Fatal("reset replaced its frozen root using identical content")
	}
	if got := manager.contexts[first.BundleHash()].context.Source; got != first.Source {
		t.Fatal("refused reset replaced its predecessor source")
	}
}
