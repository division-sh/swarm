package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

func TestRecoveredRuntimeContextsStayFencedUntilExactPublicationRelease(t *testing.T) {
	manager, err := newTestRuntimeContextManager(t, fakeRunBundleAvailability{})
	if err != nil {
		t.Fatal(err)
	}
	a := capturedOrdinaryRuntimeContext(t, "alpha.requested")
	b := capturedOrdinaryRuntimeContext(t, "beta.requested")
	if a.BundleHash() == b.BundleHash() {
		t.Fatal("recovered publication requires distinct captured sources")
	}
	if err := manager.StageRecoveredRuntimeContexts(a, b); err != nil {
		t.Fatal(err)
	}
	if err := manager.StageRecoveredRuntimeContexts(a, b); err == nil {
		t.Fatal("startup recovery overwrote an existing context set")
	}
	if err := manager.ReleaseResetExecution(a, b); err == nil {
		t.Fatal("unpublished recovered contexts became executable")
	}
	if err := manager.PublishResetRuntimeContexts(a, b); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []BundleContext{a, b} {
		use, _, err := manager.AcquireBundleHash(context.Background(), candidate.BundleHash())
		if use != nil {
			_ = use.Done()
		}
		if use != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
			t.Fatalf("recovery publication escaped convergence fence: %v", err)
		}
	}
	if err := manager.ReleaseResetExecution(a); err == nil {
		t.Fatal("partial recovery released the complete-set fence")
	}
	if err := manager.ReleaseResetExecution(a, b); err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []BundleContext{a, b} {
		use, _, err := manager.AcquireBundleHash(context.Background(), candidate.BundleHash())
		if err != nil || use == nil || use.Runtime() != candidate.Runtime {
			t.Fatalf("converged recovery selected a different runtime: %v", err)
		}
		if err := use.Done(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestResetRuntimeContextsRequireRetirementAndPublishWholeIdenticalSet(t *testing.T) {
	catalog, source := runtimeAdmissionTestCatalog(t, "a", "first")
	a := runtimeAdmissionTestContext(t, source, catalog, uuid.NewString())
	bundleB, rootedB, _ := loadS1EffectiveProviderScope(t, map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"schema.yaml":   []byte("stages: []\npins: {inputs: [beta.requested]}\n"),
		"events.yaml":   []byte("beta.requested:\n"),
	})
	providersB, err := packadmission.AdmitProviderSource(rootedB, bundleB.Platform)
	if err != nil {
		t.Fatal(err)
	}
	scopeB, err := providersB.Scope(".")
	if err != nil {
		t.Fatal(err)
	}
	catalogB, err := scopeB.TriggerCatalog()
	if err != nil {
		t.Fatal(err)
	}
	sourceB, err := SourceWithProviderTriggerEvents(semanticview.Wrap(bundleB), catalogB)
	if err != nil {
		t.Fatal(err)
	}
	b := runtimeAdmissionTestContext(t, sourceB, catalogB, uuid.NewString())
	if a.BundleHash() == b.BundleHash() || len(b.StandingTargets) != 0 {
		t.Fatal("reset requires distinct captured sources with an ordinary second context")
	}
	availability := fakeRunBundleAvailability{rows: map[string]runbundle.Availability{
		a.StandingTargets[0].RunID: {RunID: a.StandingTargets[0].RunID, BundleHash: a.BundleHash(), SourceArtifactPresent: true,
			RootReference: a.RootAdmission.Reference(), RootAdmissionPresent: true},
	}}
	manager, err := newTestRuntimeContextManager(t, availability, a, b)
	if err != nil {
		t.Fatal(err)
	}
	nextA := runtimeAdmissionTestContext(t, source, catalog, a.StandingTargets[0].RunID)
	nextB := runtimeAdmissionTestContext(t, sourceB, catalogB, uuid.NewString())
	staged := false
	t.Cleanup(func() {
		if !staged {
			for _, candidate := range []BundleContext{nextA, nextB} {
				if err := candidate.Runtime.Shutdown(); err != nil {
					t.Error(err)
				}
			}
		}
	})
	if err := manager.StageResetRuntimeContexts(nextA, nextB); err == nil {
		t.Fatal("accepted live predecessor")
	}
	for _, result := range manager.DeactivateAll(RuntimeContextCauseUnloaded) {
		if result.ShutdownErr != nil {
			t.Fatal(result.ShutdownErr)
		}
	}
	if err := manager.StageResetRuntimeContexts(nextA); err == nil {
		t.Fatal("accepted incomplete source set")
	}
	if err := manager.StageResetRuntimeContexts(a, b); err == nil {
		t.Fatal("accepted retired runtime reuse")
	}
	if err := manager.StageResetRuntimeContexts(nextA, nextB); err != nil {
		t.Fatal(err)
	}
	staged = true
	for _, hash := range []string{a.BundleHash(), b.BundleHash()} {
		use, lookup, err := manager.AcquireBundleHash(context.Background(), hash)
		if use != nil {
			_ = use.Done()
		}
		if err != nil || use != nil || lookup.Loaded() {
			t.Fatalf("staged source was selectable: %+v %v", lookup, err)
		}
	}
	if err := manager.PublishResetRuntimeContexts(a, b); err == nil {
		t.Fatal("published a different occurrence")
	}
	if err := manager.PublishResetRuntimeContexts(nextA, nextB); err != nil {
		t.Fatal(err)
	}
	for _, hash := range []string{a.BundleHash(), b.BundleHash()} {
		use, _, err := manager.AcquireBundleHash(context.Background(), hash)
		if use != nil {
			_ = use.Done()
		}
		if use != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
			t.Fatalf("publication escaped convergence fence: %v", err)
		}
	}
	for _, invalid := range [][]BundleContext{{nextA}, {a, b}, {nextA, nextA}} {
		if err := manager.ReleaseResetExecution(invalid...); err == nil {
			t.Fatal("released a partial, stale, or duplicate publication")
		}
	}
	checks := map[string]func() (*RuntimeContextUse, error){
		"run": func() (*RuntimeContextUse, error) {
			use, _, _, err := manager.AcquireRun(context.Background(), a.StandingTargets[0].RunID)
			return use, err
		},
		"channel": func() (*RuntimeContextUse, error) {
			use, _, err := manager.AcquireIngress(context.Background(), a.StandingTargets[0].Alias, "acme")
			return use, err
		},
	}
	for name, acquire := range checks {
		use, err := acquire()
		if use != nil {
			_ = use.Done()
		}
		if use != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
			t.Fatalf("%s escaped reset convergence: %v", name, err)
		}
	}
	if err := manager.ReleaseResetExecution(nextA, nextB); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReleaseResetExecution(nextA, nextB); err == nil {
		t.Fatal("released the same publication twice")
	}
	for name, acquire := range checks {
		use, err := acquire()
		if err != nil || use == nil {
			t.Fatalf("%s did not reopen after convergence: %v", name, err)
		}
		if err := use.Done(); err != nil {
			t.Fatal(err)
		}
	}
	for _, expected := range []BundleContext{nextA, nextB} {
		use, lookup, err := manager.AcquireBundleHash(context.Background(), expected.BundleHash())
		var acquired *Runtime
		if use != nil {
			acquired = use.Runtime()
			if err := use.Done(); err != nil {
				t.Fatal(err)
			}
		}
		if err != nil || use == nil || !lookup.Loaded() {
			t.Fatalf("published context unavailable: %+v %v", lookup, err)
		}
		if acquired != expected.Runtime {
			t.Fatal("published predecessor runtime")
		}
	}
}
