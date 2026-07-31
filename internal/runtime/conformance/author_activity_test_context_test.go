package conformance

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = mustAuthorActivityTestBundleSourceFact()

func mustAuthorActivityTestBundleSourceFact() runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("e", 64))
	if err != nil {
		panic(err)
	}
	return fact
}

var conformanceTestProcessOwners sync.Map

func conformanceTestProcessOwner(t testing.TB) *worklifetime.Process {
	t.Helper()
	if existing, ok := conformanceTestProcessOwners.Load(t); ok {
		return existing.(*worklifetime.Process)
	}
	process := worklifetime.NewProcess()
	actual, loaded := conformanceTestProcessOwners.LoadOrStore(t, process)
	if loaded {
		return actual.(*worklifetime.Process)
	}
	t.Cleanup(func() {
		defer conformanceTestProcessOwners.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join conformance test process owner: %v", err)
		}
	})
	return process
}

func conformanceTestRuntimeOccurrence(t testing.TB, bundleHash string) *worklifetime.RuntimeOccurrence {
	t.Helper()
	owner, err := conformanceTestProcessOwner(t).NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        strings.TrimSpace(bundleHash),
	})
	if err != nil {
		t.Fatalf("create conformance test runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire conformance test runtime occurrence: %v", err)
		}
	})
	return owner
}

func ownConformanceTestAgentManager(t testing.TB, manager *runtimemanager.AgentManager) *runtimemanager.AgentManager {
	t.Helper()
	t.Cleanup(func() {
		if err := manager.Shutdown(); err != nil {
			t.Errorf("shutdown conformance test manager: %v", err)
		}
	})
	return manager
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, authorActivityTestBundleSourceFact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash(),
	))
}

func testAuthorActivityRuntimeContext(ctx context.Context) context.Context {
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.RuntimeScope(authorActivityTestRuntimeInstanceID))
}

func testAuthorActivityRuntimeOptions(t testing.TB, opts runtimepkg.RuntimeOptions) runtimepkg.RuntimeOptions {
	t.Helper()
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash()) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.ProcessWorkOwner == nil {
		opts.ProcessWorkOwner = conformanceTestProcessOwner(t)
	}
	return opts
}

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func registerTestAuthorActivityCatalog(t *testing.T, target testAuthorActivityCatalogRegistrar, descriptors []runtimeauthoractivity.EventDescriptor) {
	t.Helper()
	lease, err := target.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleSourceFact.BundleHash()),
		descriptors,
	)
	if err != nil {
		t.Fatalf("register test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}

func registerDifferentTestAuthorActivityCatalog(t *testing.T, target testAuthorActivityCatalogRegistrar, eventTypes ...string) {
	t.Helper()
	sort.Strings(eventTypes)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
			EventType: strings.TrimSpace(eventType), Disposition: runtimeauthoractivity.StoryDifferent,
		})
	}
	registerTestAuthorActivityCatalog(t, target, descriptors)
}

func newScopedTestEventBus(t *testing.T, eventStore runtimebus.EventStore, opts runtimebus.EventBusOptions, differentEvents ...string) (*runtimebus.EventBus, error) {
	t.Helper()
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash()) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = conformanceTestRuntimeOccurrence(t, opts.BundleSourceFact.BundleHash())
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, authorityErr := runtimedelivery.NewNormalExecutionAuthority(
			opts.BundleSourceFact,
			opts.RuntimeInstanceID,
			1,
		)
		if authorityErr != nil {
			return nil, authorityErr
		}
		opts.DeliveryAuthority = authority
	}
	if opts.PipelineObligations == nil {
		if provider, ok := eventStore.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	if registrar, ok := eventStore.(testAuthorActivityCatalogRegistrar); ok {
		descriptors := testAuthorActivityEventDescriptors(t, opts)
		for _, eventType := range differentEvents {
			descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
				EventType: strings.TrimSpace(eventType), Disposition: runtimeauthoractivity.StoryDifferent,
			})
		}
		registerTestAuthorActivityCatalog(t, registrar, descriptors)
	}
	var bus *runtimebus.EventBus
	var err error
	if opts.PipelineObligations == nil {
		bus, err = runtimebus.NewEphemeralEventBusWithOptions(eventStore, opts)
	} else {
		bus, err = runtimebus.NewEventBusWithOptions(eventStore, opts)
	}
	if err != nil {
		return nil, err
	}
	if err := bus.SetDeliveryContinuationOwner(
		runtimebustest.NewDeliveryContinuationOwner(opts.PipelineObligations == nil),
	); err != nil {
		return nil, err
	}
	return bus, nil
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
