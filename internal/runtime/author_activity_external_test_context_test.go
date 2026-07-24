package runtime_test

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = runtimecorrelation.BundleSourceFact{
	BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
	BundleSource:      "ephemeral",
	BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
}

var externalRuntimeTestEventBusOwners sync.Map

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash,
	))
}

func newScopedTestEventBus(t *testing.T, store runtimebus.EventStore, opts runtimebus.EventBusOptions, differentEvents ...string) (*runtimebus.EventBus, error) {
	t.Helper()
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}

	if registrar, ok := store.(testAuthorActivityCatalogRegistrar); ok {
		descriptors := testAuthorActivityEventDescriptors(t, opts)
		for _, eventType := range differentEvents {
			descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
				EventType: strings.TrimSpace(eventType), Disposition: runtimeauthoractivity.StoryDifferent,
			})
		}
		lease, err := registrar.RegisterAuthorActivityEventCatalog(
			runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.BundleSourceFact.BundleHash),
			descriptors,
		)
		if err != nil {
			return nil, err
		}
		t.Cleanup(lease.Release)
	}
	return newRuntimeTestEventBusWithOptions(t, store, opts)
}

func newRuntimeTestEventBus(t testing.TB, store runtimebus.EventStore) (*runtimebus.EventBus, error) {
	t.Helper()
	return newRuntimeTestEventBusWithOptions(t, store, runtimebus.EventBusOptions{})
}

func newRuntimeTestEventBusWithOptions(t testing.TB, store runtimebus.EventStore, opts runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.WorkOwner == nil {
		process := worklifetime.NewProcess()
		owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
			RuntimeInstanceID: opts.RuntimeInstanceID,
			BundleHash:        opts.BundleSourceFact.BundleHash,
		})
		if err != nil {
			return nil, err
		}
		opts.WorkOwner = owner
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := owner.RetireAndWait(ctx); err != nil {
				t.Errorf("retire external runtime test occurrence: %v", err)
			}
			if _, err := process.Join(ctx); err != nil {
				t.Errorf("join external runtime test process: %v", err)
			}
		})
	}
	if opts.PipelineObligations == nil {
		if provider, ok := store.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	var bus *runtimebus.EventBus
	var err error
	if opts.PipelineObligations == nil {
		bus, err = runtimebus.NewEphemeralEventBusWithOptions(store, opts)
	} else {
		bus, err = runtimebus.NewEventBusWithOptions(store, opts)
	}
	if err != nil {
		return nil, err
	}
	externalRuntimeTestEventBusOwners.Store(bus, opts.WorkOwner)
	t.Cleanup(func() { externalRuntimeTestEventBusOwners.Delete(bus) })
	return bus, nil
}

func runtimeTestEventBusWorkOwner(t testing.TB, bus *runtimebus.EventBus) worklifetime.Occurrence {
	t.Helper()
	owner, ok := externalRuntimeTestEventBusOwners.Load(bus)
	if !ok {
		t.Fatal("external runtime test event bus has no registered work owner")
	}
	return owner.(worklifetime.Occurrence)
}

func ownRuntimeTestAgentManager(t testing.TB, manager *runtimemanager.AgentManager) *runtimemanager.AgentManager {
	t.Helper()
	t.Cleanup(func() {
		if err := manager.Shutdown(); err != nil {
			t.Errorf("shutdown external runtime test manager: %v", err)
		}
	})
	return manager
}

func testAuthorActivityEventDescriptors(t *testing.T, opts runtimebus.EventBusOptions) []runtimeauthoractivity.EventDescriptor {
	t.Helper()
	if opts.ContractBundle == nil {
		return nil
	}
	resolved := opts.ContractBundle.ResolvedEventCatalog()
	authored := opts.ContractBundle.AuthoredResolvedEventCatalog()
	byName := make(map[string]runtimeauthoractivity.EventDescriptor, len(resolved)+len(authored))
	add := func(name string, descriptor runtimeauthoractivity.EventDescriptor) {
		name = strings.TrimSpace(name)
		if name == "" {
			return
		}
		descriptor.EventType = name
		if previous, ok := byName[name]; ok && previous != descriptor {
			t.Fatalf("author activity test descriptor %q conflicts: %#v != %#v", name, previous, descriptor)
		}
		byName[name] = descriptor
	}
	for name, entry := range resolved {
		disposition := runtimeauthoractivity.StoryDifferent
		if _, ok := authored[name]; ok {
			disposition = runtimeauthoractivity.StoryAuthored
		}
		add(name, runtimeauthoractivity.EventDescriptor{Disposition: disposition, AuthorSummaryField: strings.TrimSpace(entry.AuthorSummaryField)})
	}
	census := semanticview.BuildAuthoredEventEndpointCensus(opts.ContractBundle)
	endpoints := append(census.Producers(), census.Consumers()...)
	endpoints = append(endpoints, census.InputPins()...)
	endpoints = append(endpoints, census.OutputPins()...)
	for _, endpoint := range endpoints {
		proof := endpoint.Event
		if !proof.HasSchema {
			continue
		}
		disposition := runtimeauthoractivity.StoryDifferent
		if _, ok := authored[strings.TrimSpace(proof.CatalogKey)]; ok {
			disposition = runtimeauthoractivity.StoryAuthored
		}
		add(proof.EventKey(), runtimeauthoractivity.EventDescriptor{Disposition: disposition, AuthorSummaryField: strings.TrimSpace(proof.Entry.AuthorSummaryField)})
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(names))
	for _, name := range names {
		descriptors = append(descriptors, byName[name])
	}
	return descriptors
}
