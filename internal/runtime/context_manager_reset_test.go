package runtime

import (
	"context"
	"reflect"
	"testing"
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

func TestResetRuntimeContextsRequireRetirementAndPublishWholeIdenticalSet(t *testing.T) {
	a := testBundleContext(t, runtimeContextTestHashA, "alpha.requested")
	b := testBundleContext(t, runtimeContextTestHashB, "beta.requested")
	manager, err := newTestRuntimeContextManager(t, nil, a, b)
	if err != nil {
		t.Fatal(err)
	}
	nextA := testBundleContext(t, runtimeContextTestHashA, "alpha.requested")
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
