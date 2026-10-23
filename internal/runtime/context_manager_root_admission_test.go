package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/agenttopology"
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
		Source: source, SourceArtifactFact: fact, RootAdmission: root.Admission(), WorkOwner: owner,
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
	use, lookup, _, err := manager.AcquireRun(context.Background(), "run")
	if use != nil {
		if doneErr := use.Done(); doneErr != nil {
			t.Error(doneErr)
		}
		t.Fatalf("content-only run evidence acquired rooted execution: %v", err)
	}
	if lookup.Loaded() {
		t.Fatal("missing run root reference reported a loaded context")
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

func TestS1RuntimeContextAdmitsOnlyExactRunRoot(t *testing.T) {
	first, root := rootedContextFixture(t, "first")
	_, other := rootedContextFixture(t, "second")
	reader := fakeRunBundleAvailability{rows: map[string]runbundle.Availability{
		"run": {RunID: "run", BundleHash: first.BundleHash(), RootReference: root.Admission().Reference(), SourceArtifactPresent: true, RootAdmissionPresent: true},
	}}
	manager, err := newTestRuntimeContextManager(t, reader, first)
	if err != nil {
		t.Fatal(err)
	}
	use, lookup, _, err := manager.AcquireRun(context.Background(), "run")
	if err != nil || use == nil || !lookup.Loaded() {
		t.Fatalf("exact rooted run refused: %v", err)
	}
	rooted, present, err := correlation.RootAdmissionFromContext(use.WorkContext())
	if err != nil || !present || !rooted.Equal(root.Admission()) {
		t.Fatalf("admitted work lost its frozen root: %v", err)
	}
	if err := use.Done(); err != nil {
		t.Fatal(err)
	}
	row := reader.rows["run"]
	row.RootReference = other.Admission().Reference()
	reader.rows["run"] = row
	use, _, _, err = manager.AcquireRun(context.Background(), "run")
	if use != nil {
		_ = use.Done()
		t.Fatal("same-content revised run borrowed the previous context")
	}
	if err == nil || !strings.Contains(err.Error(), "first") || !strings.Contains(err.Error(), "second") {
		t.Fatalf("foreign root refusal lost exact facts: %v", err)
	}
	if lookup, _, err := manager.LookupRunStatus(context.Background(), "run"); err == nil || lookup.Loaded() {
		t.Fatalf("diagnostic run lookup borrowed a different root: %+v, %v", lookup, err)
	}
}

func TestS1RuntimeContextReadbackCannotRetargetFrozenRoot(t *testing.T) {
	first, root := rootedContextFixture(t, "first")
	second, other := rootedContextFixture(t, "second")
	manager, err := newTestRuntimeContextManager(t, nil, first)
	if err != nil {
		t.Fatal(err)
	}
	for _, read := range []func() *BundleContext{
		func() *BundleContext { value, _ := manager.Primary(); return value },
		func() *BundleContext { return manager.LookupBundleHashStatus(first.BundleHash()).Context },
	} {
		copy := read()
		if copy == nil {
			t.Fatal("missing context metadata")
		}
		copy.RootAdmission, copy.Source = other.Admission(), second.Source
		retained := manager.contexts[first.BundleHash()].context.RootAdmission
		if !retained.Equal(root.Admission()) {
			t.Fatal("metadata pointer changed the registered root")
		}
	}
}

func TestS1RuntimeContextRejectsForgedDefinitionAndDuplicateRoot(t *testing.T) {
	first, root := rootedContextFixture(t, "first")
	second, other := rootedContextFixture(t, "second")
	for _, corrupt := range []func(*BundleContext){
		func(c *BundleContext) { c.RootAdmission = sourceartifact.RootAdmission{} },
		func(c *BundleContext) { c.RootAdmission = other.Admission() },
		func(c *BundleContext) { c.Source = second.Source },
		func(c *BundleContext) { c.Runtime = second.Runtime },
	} {
		candidate := first
		corrupt(&candidate)
		if err := ValidateRuntimeContextSet(candidate); err == nil {
			t.Fatal("forged context acquired registration authority")
		}
	}
	manager, err := newTestRuntimeContextManager(t, nil, first)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Register(second); err == nil || manager.Len() != 1 {
		t.Fatal("a second root introduced another public context for one hash")
	}
	if !manager.contexts[first.BundleHash()].context.RootAdmission.Equal(root.Admission()) {
		t.Fatal("rejected duplicate changed the first context")
	}
}

func TestS1RuntimeContextCancellationAndClearAcquireNoLease(t *testing.T) {
	first, root := rootedContextFixture(t, "first")
	manager, err := newTestRuntimeContextManager(t, nil, first)
	if err != nil {
		t.Fatal(err)
	}
	base := correlation.WithSourceArtifactFact(context.Background(), first.SourceArtifactFact)
	canceled, cancel := context.WithCancel(base)
	cancel()
	before := first.WorkOwner.ActiveCount()
	foreignFact := sourceartifactfixture.FactFor(sourceartifactfixture.New("schema.yaml", []byte("stages: [waiting]\n")))
	for _, ctx := range []context.Context{nil, canceled, correlation.WithRootAdmission(base, sourceartifact.RootAdmission{}), correlation.WithRootAdmission(context.Background(), root.Admission()), correlation.WithSourceArtifactFact(context.Background(), foreignFact)} {
		use, _, err := manager.AcquireBundleHash(ctx, first.BundleHash())
		if use != nil {
			_ = use.Done()
			t.Fatal("invalid request acquired a context lease")
		}
		if err == nil {
			t.Fatal("invalid request was not refused")
		}
		if ctx == canceled && !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	}
	if after := first.WorkOwner.ActiveCount(); after != before {
		t.Fatalf("refusal leaked work: before=%d after=%d", before, after)
	}
}

func TestS1RuntimeContextSourceSetRefusesOtherRootBeforeFence(t *testing.T) {
	first, _ := rootedContextFixture(t, "first")
	_, other := rootedContextFixture(t, "second")
	manager, err := newTestRuntimeContextManager(t, nil, first)
	if err != nil {
		t.Fatal(err)
	}
	coordinate, err := agenttopology.NewSourceCoordinate(other.Admission())
	if err != nil {
		t.Fatal(err)
	}
	plan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{coordinate}, nil)
	if err != nil {
		t.Fatal(err)
	}
	transition, err := manager.PrepareSourceSetTransition(context.Background(), plan)
	if transition != nil || err == nil || !strings.Contains(err.Error(), "rooted source") {
		t.Fatalf("foreign-root source set reached execution refresh: %+v, %v", transition, err)
	}
	if !manager.LookupBundleHashStatus(first.BundleHash()).Loaded() || manager.pendingSourceSetTransition != nil {
		t.Fatal("foreign-root refusal fenced the active predecessor")
	}
}

func TestS1RuntimeContextResetKeepsExactRootThroughPublication(t *testing.T) {
	first, root := rootedContextFixture(t, "first")
	manager, err := newTestRuntimeContextManager(t, nil, first)
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range manager.DeactivateAll(RuntimeContextCauseReset) {
		if result.ShutdownErr != nil {
			t.Fatal(result.ShutdownErr)
		}
	}
	next := rootedContextForSource(t, first.Source, root)
	if err := manager.StageResetRuntimeContexts(next); err != nil {
		t.Fatal(err)
	}
	if err := manager.PublishResetRuntimeContexts(next); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleaseResetExecution(next); err != nil {
		t.Fatal(err)
	}
	use, _, err := manager.AcquireBundleHash(context.Background(), first.BundleHash())
	if err != nil || use == nil || use.Runtime() != next.Runtime || !use.Context.RootAdmission.Equal(root.Admission()) {
		t.Fatalf("reset lost its exact successor: %v", err)
	}
	if err := use.Done(); err != nil {
		t.Fatal(err)
	}
}
