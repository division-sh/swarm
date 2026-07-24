package pipeline_test

import (
	"context"
	"database/sql"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = runtimecorrelation.BundleSourceFact{
	BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
	BundleSource:      "ephemeral",
	BundleFingerprint: "sha256:" + strings.Repeat("a", 64),
}

type pipelineExternalTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var pipelineExternalTestWorkFixtures sync.Map

func pipelineExternalTestWorkOwner(t *testing.T) *worklifetime.RuntimeOccurrence {
	t.Helper()
	if existing, ok := pipelineExternalTestWorkFixtures.Load(t); ok {
		return existing.(*pipelineExternalTestWorkFixture).runtime
	}
	fixture := &pipelineExternalTestWorkFixture{process: worklifetime.NewProcess()}
	owner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        authorActivityTestBundleSourceFact.BundleHash,
	})
	if err != nil {
		t.Fatalf("create pipeline test work owner: %v", err)
	}
	fixture.runtime = owner
	actual, loaded := pipelineExternalTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*pipelineExternalTestWorkFixture).runtime
	}
	t.Cleanup(func() {
		defer pipelineExternalTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.runtime.RetireAndWait(ctx); err != nil {
			t.Errorf("retire pipeline test work owner: %v", err)
			return
		}
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join pipeline test process owner: %v", err)
		}
	})
	return owner
}

func testAuthorActivityContext(t *testing.T, ctx context.Context) context.Context {
	t.Helper()
	ctx = worklifetime.WithOccurrence(ctx, pipelineExternalTestWorkOwner(t))
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash,
	))
}

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func registerDifferentTestAuthorActivityEvents(t *testing.T, eventStore any, eventTypes ...string) {
	t.Helper()
	registrar, ok := eventStore.(testAuthorActivityCatalogRegistrar)
	if !ok {
		t.Fatalf("event store %T lacks author activity catalog registration", eventStore)
	}
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(eventTypes))
	for _, eventType := range eventTypes {
		descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
			EventType: strings.TrimSpace(eventType), Disposition: runtimeauthoractivity.StoryDifferent,
		})
	}
	lease, err := registrar.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleSourceFact.BundleHash), descriptors,
	)
	if err != nil {
		t.Fatalf("register test author activity event catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}

func newScopedTestEventBus(t *testing.T, eventStore runtimebus.EventStore, opts runtimebus.EventBusOptions, differentEvents ...string) (*runtimebus.EventBus, error) {
	t.Helper()
	if opts.WorkOwner == nil {
		opts.WorkOwner = pipelineExternalTestWorkOwner(t)
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
		lease, err := registrar.RegisterAuthorActivityEventCatalog(
			runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleSourceFact.BundleHash), descriptors,
		)
		if err != nil {
			return nil, err
		}
		t.Cleanup(lease.Release)
	}
	if opts.PipelineObligations == nil {
		return runtimebus.NewEphemeralEventBusWithOptions(eventStore, opts)
	}
	return runtimebus.NewEventBusWithOptions(eventStore, opts)
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

func newRecoveryTestPostgresStore(t *testing.T, db *sql.DB) *store.PostgresStore {
	t.Helper()
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	descriptors := make([]runtimeauthoractivity.EventDescriptor, 0, len(recoveryTestEventTypes))
	for _, eventType := range recoveryTestEventTypes {
		descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
			EventType: eventType, Disposition: runtimeauthoractivity.StoryDifferent,
		})
	}
	lease, err := pg.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, authorActivityTestBundleSourceFact.BundleHash),
		descriptors,
	)
	if err != nil {
		t.Fatalf("register recovery test author activity catalog: %v", err)
	}
	t.Cleanup(lease.Release)
	return pg
}

var recoveryTestEventTypes = []string{
	"fork.ready",
	"system.parent",
	"system.recover",
	"system.recover.direct_empty",
	"system.recover.explicit",
	"system.recover.good",
	"system.recover.internal",
	"system.recover.no-run-id",
	"system.recover.no_recipients",
}
