package runtime

import (
	"context"
	"errors"
	"reflect"
	"testing"

	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecorrelation "swarm/internal/runtime/correlation"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/store/runbundle"
	storerunlifecycle "swarm/internal/store/runlifecycle"
)

const (
	runtimeContextTestHashA = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	runtimeContextTestHashB = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func TestRuntimeContextManagerRegistersAndLooksUpPinnedContexts(t *testing.T) {
	availability := fakeRunBundleAvailability{
		rows: map[string]runbundle.Availability{
			"run-b": {
				RunID:            "run-b",
				BundleHash:       runtimeContextTestHashB,
				BundleSource:     storerunlifecycle.BundleSourcePersisted,
				BundleRowPresent: true,
			},
		},
	}
	manager, err := NewRuntimeContextManager(availability,
		testBundleContext(t, runtimeContextTestHashB, "beta.requested"),
		testBundleContext(t, runtimeContextTestHashA, "alpha.requested"),
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	if manager.Len() != 2 || !manager.MultiContext() {
		t.Fatalf("manager Len/MultiContext = %d/%v, want 2/true", manager.Len(), manager.MultiContext())
	}
	if got, want := manager.BundleHashes(), []string{runtimeContextTestHashA, runtimeContextTestHashB}; !reflect.DeepEqual(got, want) {
		t.Fatalf("BundleHashes = %#v, want %#v", got, want)
	}
	primary, ok := manager.Primary()
	if !ok || primary.BundleHash != runtimeContextTestHashA {
		t.Fatalf("Primary = %#v/%v, want %s", primary, ok, runtimeContextTestHashA)
	}
	contextDef, availabilityResult, loaded, err := manager.LookupRun(context.Background(), "run-b")
	if err != nil {
		t.Fatalf("LookupRun: %v", err)
	}
	if !loaded || contextDef == nil || contextDef.BundleHash != runtimeContextTestHashB {
		t.Fatalf("LookupRun context = %#v loaded=%v, want %s", contextDef, loaded, runtimeContextTestHashB)
	}
	if availabilityResult.BundleHash != runtimeContextTestHashB || !availabilityResult.Available() {
		t.Fatalf("LookupRun availability = %#v, want available %s", availabilityResult, runtimeContextTestHashB)
	}
}

func TestRuntimeContextManagerRejectsDuplicateBundleHashes(t *testing.T) {
	if _, err := NewRuntimeContextManager(nil,
		testBundleContext(t, runtimeContextTestHashA, "alpha.requested"),
		testBundleContext(t, runtimeContextTestHashA, "alpha.requested"),
	); err == nil {
		t.Fatal("NewRuntimeContextManager duplicate error = nil")
	}
}

func testBundleContext(t *testing.T, bundleHash, eventName string) BundleContext {
	t.Helper()
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
	}
	source := semanticview.Wrap(bundle)
	bus, err := runtimebus.NewEventBusWithOptions(nil, runtimebus.EventBusOptions{
		ContractBundle: source,
		BundleSourceFact: runtimecorrelation.BundleSourceFact{
			BundleHash:   bundleHash,
			BundleSource: storerunlifecycle.BundleSourcePersisted,
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	return BundleContext{
		BundleHash: bundleHash,
		BundleSourceFact: runtimecorrelation.BundleSourceFact{
			BundleHash:   bundleHash,
			BundleSource: storerunlifecycle.BundleSourcePersisted,
		},
		BundleIdentity: runtimecontracts.BundleIdentity{WorkflowName: "review", WorkflowVersion: "1.0.0"},
		Source:         source,
		Runtime:        &Runtime{Bus: bus},
	}
}

type fakeRunBundleAvailability struct {
	rows map[string]runbundle.Availability
}

func (f fakeRunBundleAvailability) LoadRunBundleAvailability(_ context.Context, runID string) (runbundle.Availability, error) {
	row, ok := f.rows[runID]
	if !ok {
		return runbundle.Availability{}, runbundle.ErrRunNotFound
	}
	if row.ErrorCode == "" && row.BundleSource == storerunlifecycle.BundleSourcePersisted && !row.BundleRowPresent {
		return row, errors.New("invalid fake persisted row without bundle")
	}
	return row, nil
}
