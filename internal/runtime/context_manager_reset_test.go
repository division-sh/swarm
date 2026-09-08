package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

func TestResetStandingPreparationFailureDoesNotRetainPrecedingOccurrence(t *testing.T) {
	owner := runtimeTestOccurrence(t, runtimeContextTestHashA)
	manager := newRuntimeContextManagerState(nil)
	before := owner.ActiveCount()
	_, err := manager.newStandingOccurrencesLocked(owner, []StandingTarget{
		{ServiceID: "first", RunID: "run-first", Generation: 1},
		{ServiceID: "invalid", RunID: "run-invalid", Generation: 0},
	})
	if err == nil {
		t.Fatal("accepted invalid standing generation")
	}
	if after := owner.ActiveCount(); after != before {
		t.Fatalf("failed preparation retained a standing occurrence: before=%d after=%d", before, after)
	}
}

func TestRecoveredRuntimeContextsStayFencedUntilExactPublicationRelease(t *testing.T) {
	manager, err := newTestRuntimeContextManager(t, fakeRunBundleAvailability{})
	if err != nil {
		t.Fatal(err)
	}
	a := testBundleContext(t, runtimeContextTestHashA, "alpha.requested")
	b := testBundleContext(t, runtimeContextTestHashB, "beta.requested")
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
	catalog := runtimeAdmissionTestCatalog(t, "a")
	a := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "first", catalog)
	b := testBundleContext(t, runtimeContextTestHashB, "beta.requested")
	availability := fakeRunBundleAvailability{rows: map[string]runbundle.Availability{
		"run-first": {RunID: "run-first", BundleHash: runtimeContextTestHashA, SourceArtifactPresent: true},
	}}
	manager, err := newTestRuntimeContextManager(t, availability, a, b)
	if err != nil {
		t.Fatal(err)
	}
	nextA := runtimeAdmissionTestContext(t, runtimeContextTestHashA, "first", catalog)
	nextB := testBundleContext(t, runtimeContextTestHashB, "beta.requested")
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
	for _, hash := range []string{runtimeContextTestHashA, runtimeContextTestHashB} {
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
	for _, hash := range []string{runtimeContextTestHashA, runtimeContextTestHashB} {
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
			use, _, _, err := manager.AcquireRun(context.Background(), "run-first")
			return use, err
		},
		"standing": func() (*RuntimeContextUse, error) {
			use, _, err := manager.AcquireStandingService(context.Background(), "service-first")
			return use, err
		},
		"channel": func() (*RuntimeContextUse, error) {
			use, _, err := manager.AcquireIngress(context.Background(), "first", "acme")
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
	origin, err := runlifecycle.StandingGenerationRunOrigin("service-first", 1)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := manager.BeginStandingRunRecovery(context.Background(), "run-first", origin)
	if lease != nil {
		_ = lease.Done()
	}
	if lease != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
		t.Fatalf("standing recovery escaped reset convergence: %v", err)
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
		// Structural ownership guard: the bus must never retain the temporary
		// preparation manager, which cannot observe later process transitions.
		boundOwner := reflect.ValueOf(expected.Runtime.Bus).Elem().FieldByName("standingRunWorkOwner")
		if boundOwner.IsNil() || boundOwner.Elem().Pointer() != reflect.ValueOf(manager).Pointer() {
			t.Fatal("reset bound standing recovery to a detached preparation manager")
		}
	}
}
