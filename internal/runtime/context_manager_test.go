package runtime

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/runbundle"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
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

func TestRuntimeContextManagerRejectsDuplicateAgentSlugs(t *testing.T) {
	_, err := NewRuntimeContextManager(nil,
		testBundleContextWithAgents(t, runtimeContextTestHashA, "alpha.requested", "shared-worker"),
		testBundleContextWithAgents(t, runtimeContextTestHashB, "beta.requested", "shared-worker"),
	)
	if err == nil {
		t.Fatal("NewRuntimeContextManager duplicate agent slug error = nil")
	}
	for _, want := range []string{
		`duplicate runtime context agent_id "shared-worker"`,
		runtimeContextTestHashA,
		runtimeContextTestHashB,
		"bundle_source=persisted",
		"workflow=review@1.0.0",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("duplicate agent slug error missing %q:\n%s", want, err.Error())
		}
	}
}

func TestRuntimeContextManagerRejectsDuplicateEffectiveAgentIDs(t *testing.T) {
	_, err := NewRuntimeContextManager(nil,
		testBundleContextWithAgentEntries(t, runtimeContextTestHashA, "alpha.requested", map[string]runtimecontracts.AgentRegistryEntry{
			"alpha": {ID: "shared-worker", Role: "alpha"},
		}),
		testBundleContextWithAgentEntries(t, runtimeContextTestHashB, "beta.requested", map[string]runtimecontracts.AgentRegistryEntry{
			"beta": {ID: "shared-worker", Role: "beta"},
		}),
	)
	if err == nil {
		t.Fatal("NewRuntimeContextManager duplicate effective agent_id error = nil")
	}
	for _, want := range []string{
		`duplicate runtime context agent_id "shared-worker"`,
		runtimeContextTestHashA,
		runtimeContextTestHashB,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("duplicate effective agent_id error missing %q:\n%s", want, err.Error())
		}
	}
}

func TestRuntimeContextManagerRegisterRejectsDuplicateAgentSlugWithoutMutatingManager(t *testing.T) {
	manager, err := NewRuntimeContextManager(nil,
		testBundleContextWithAgents(t, runtimeContextTestHashA, "alpha.requested", "shared-worker"),
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}
	err = manager.Register(testBundleContextWithAgents(t, runtimeContextTestHashB, "beta.requested", "shared-worker"))
	if err == nil {
		t.Fatal("Register duplicate agent slug error = nil")
	}
	for _, want := range []string{
		`duplicate runtime context agent_id "shared-worker"`,
		runtimeContextTestHashA,
		runtimeContextTestHashB,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Register duplicate agent slug error missing %q:\n%s", want, err.Error())
		}
	}
	if manager.Len() != 1 || manager.MultiContext() {
		t.Fatalf("manager Len/MultiContext after rejected register = %d/%v, want 1/false", manager.Len(), manager.MultiContext())
	}
	if got, want := manager.BundleHashes(), []string{runtimeContextTestHashA}; !reflect.DeepEqual(got, want) {
		t.Fatalf("BundleHashes after rejected register = %#v, want %#v", got, want)
	}
	if _, ok := manager.LookupBundleHash(runtimeContextTestHashB); ok {
		t.Fatal("LookupBundleHash found rejected duplicate context")
	}
}

func TestRuntimeContextManagerAllowsDistinctAgentSlugs(t *testing.T) {
	manager, err := NewRuntimeContextManager(nil,
		testBundleContextWithAgents(t, runtimeContextTestHashA, "alpha.requested", "alpha-worker"),
		testBundleContextWithAgents(t, runtimeContextTestHashB, "beta.requested", "beta-worker"),
	)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager distinct agent slugs: %v", err)
	}
	if manager.Len() != 2 || !manager.MultiContext() {
		t.Fatalf("manager Len/MultiContext = %d/%v, want 2/true", manager.Len(), manager.MultiContext())
	}
}

func TestRuntimeContextManagerDeactivatesPinnedContextFailClosed(t *testing.T) {
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
	contextA := testBundleContext(t, runtimeContextTestHashA, "alpha.requested")
	contextB := testBundleContext(t, runtimeContextTestHashB, "beta.requested")
	manager, err := NewRuntimeContextManager(availability, contextA, contextB)
	if err != nil {
		t.Fatalf("NewRuntimeContextManager: %v", err)
	}

	result := manager.DeactivateBundleHash(runtimeContextTestHashB, RuntimeContextCauseUnloaded)
	if !result.Found || !result.Changed || result.State != RuntimeContextStateUnloaded || result.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("DeactivateBundleHash result = %#v, want changed unloaded", result)
	}
	if contextB.Runtime == nil || !contextB.Runtime.shutdownAdmissionClosed() {
		t.Fatal("deactivated runtime shutdown admission is still open")
	}
	if manager.Len() != 2 || !manager.MultiContext() {
		t.Fatalf("manager Len/MultiContext after deactivation = %d/%v, want 2/true", manager.Len(), manager.MultiContext())
	}
	if _, ok := manager.LookupBundleHash(runtimeContextTestHashB); ok {
		t.Fatal("LookupBundleHash loaded deactivated bundle = true")
	}
	lookup := manager.LookupBundleHashStatus(runtimeContextTestHashB)
	if lookup.Loaded() || !lookup.Found || lookup.State != RuntimeContextStateUnloaded || lookup.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("LookupBundleHashStatus = %#v, want found unloaded", lookup)
	}
	primary, ok := manager.Primary()
	if !ok || primary.BundleHash != runtimeContextTestHashA {
		t.Fatalf("Primary after deactivation = %#v/%v, want %s", primary, ok, runtimeContextTestHashA)
	}
	runLookup, availabilityResult, err := manager.LookupRunStatus(context.Background(), "run-b")
	if err != nil {
		t.Fatalf("LookupRunStatus: %v", err)
	}
	if runLookup.Loaded() || runLookup.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("LookupRunStatus lookup = %#v, want unloaded", runLookup)
	}
	if availabilityResult.BundleHash != runtimeContextTestHashB || !availabilityResult.Available() {
		t.Fatalf("LookupRunStatus availability = %#v, want available %s", availabilityResult, runtimeContextTestHashB)
	}

	second := manager.DeactivateBundleHash(runtimeContextTestHashB, RuntimeContextCauseUnavailable)
	if !second.Found || second.Changed || second.Cause != RuntimeContextCauseUnloaded {
		t.Fatalf("second DeactivateBundleHash result = %#v, want idempotent original cause", second)
	}
}

func testBundleContext(t *testing.T, bundleHash, eventName string) BundleContext {
	t.Helper()
	return testBundleContextWithAgents(t, bundleHash, eventName)
}

func testBundleContextWithAgents(t *testing.T, bundleHash, eventName string, agentIDs ...string) BundleContext {
	t.Helper()
	agents := map[string]runtimecontracts.AgentRegistryEntry{}
	for _, agentID := range agentIDs {
		agentID = strings.TrimSpace(agentID)
		if agentID == "" {
			continue
		}
		agents[agentID] = runtimecontracts.AgentRegistryEntry{ID: agentID, Role: agentID}
	}
	return testBundleContextWithAgentEntries(t, bundleHash, eventName, agents)
}

func testBundleContextWithAgentEntries(t *testing.T, bundleHash, eventName string, agents map[string]runtimecontracts.AgentRegistryEntry) BundleContext {
	t.Helper()
	bundle := &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{Name: "review", Version: "1.0.0"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventName: {},
		},
		Agents: agents,
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
