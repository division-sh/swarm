package conformance

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
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

func conformanceManagerPersistenceRoles(selected any, eventBus *runtimebus.EventBus, pipeline *runtimepipeline.PipelineCoordinator) runtimemanager.PersistenceRoles {
	roles := runtimemanager.PersistenceRoles{
		AgentRoutes: eventBus, RouteInstaller: eventBus, RouteVerifier: eventBus,
		RouteRestorer: eventBus, RouteRetirer: eventBus, RouteRemover: eventBus,
		FlowTermination: pipeline, CreationPublisher: eventBus, DeliveryRuntime: eventBus,
	}
	roles.LifecycleCensus, _ = selected.(runtimemanager.AgentLifecycleCellCensus)
	roles.LifecycleState, _ = selected.(runtimemanager.AgentLifecycleStateReader)
	roles.LifecycleEffects, _ = selected.(runtimeeffects.Store)
	roles.LifecycleDiagnostics, _ = selected.(runtimemanager.AgentLifecycleDiagnosticPersistence)
	roles.EffectsRecovery, _ = selected.(runtimeeffects.RecoveryStore)
	roles.DeliveryQuiescence, _ = selected.(runtimemanager.ActiveRunDeliveryQuiescenceReader)
	roles.EventExistence, _ = selected.(runtimemanager.EventExistenceReader)
	roles.DirectiveOperations, _ = selected.(runtimeagentcontrol.DirectiveOperationStore)
	roles.DirectiveTargets, _ = selected.(runtimemanager.AgentDirectiveRunTargetResolver)
	roles.FlowRoutes, _ = selected.(runtimebus.FlowInstanceRoutePersistence)
	roles.StandingRestarts, _ = selected.(runtimepipeline.StandingRestartDispositionReader)
	return roles
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	return testAuthorActivityContextForBundle(ctx, authorActivityTestBundleSourceFact)
}

func testAuthorActivityContextForBundle(ctx context.Context, fact runtimecorrelation.BundleSourceFact) context.Context {
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		fact.BundleHash(),
	))
}

func conformanceBundleSourceFact(t testing.TB, source semanticview.Source) runtimecorrelation.BundleSourceFact {
	t.Helper()
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil {
		t.Fatal("conformance source must retain its admitted bundle")
	}
	hash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("compute conformance bundle identity: %v", err)
	}
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(hash)
	if err != nil {
		t.Fatalf("admit conformance bundle source fact: %v", err)
	}
	return fact
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
		opts.ProcessWorkOwner = worklifetime.NewProcess()
	}
	return opts
}

func closeConformanceRuntimeGeneration(rt *runtimepkg.Runtime, capability runtimestartupownership.ProcessCapability) error {
	if rt != nil {
		if err := rt.Shutdown(); err != nil {
			return fmt.Errorf("shutdown conformance runtime: %w", err)
		}
		if rt.Options.ProcessWorkOwner != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			rt.Options.ProcessWorkOwner.Retire()
			if _, err := rt.Options.ProcessWorkOwner.Join(ctx); err != nil {
				return fmt.Errorf("join conformance runtime process owner: %w", err)
			}
		}
	}
	if capability != nil {
		select {
		case <-capability.Done():
			return nil
		default:
		}
		if err := capability.Release(context.Background()); err != nil {
			return fmt.Errorf("release conformance process capability: %w", err)
		}
	}
	return nil
}

func installConformanceRuntimeStartupGrant(t testing.TB, ctx context.Context, selected any, rt *runtimepkg.Runtime) runtimestartupownership.ProcessCapability {
	t.Helper()
	capability, _ := installConformanceRuntimeStartupGeneration(t, ctx, selected, rt)
	return capability
}

func installConformanceRuntimeStartupGeneration(
	t testing.TB,
	ctx context.Context,
	selected any,
	rt *runtimepkg.Runtime,
) (runtimestartupownership.ProcessCapability, runtimestartupownership.GenerationGrant) {
	t.Helper()
	store, ok := selected.(runtimestartupownership.Store)
	if !ok {
		t.Fatalf("conformance selected store %T lacks process capability acquisition", selected)
	}
	if rt == nil || rt.Manager == nil || rt.Options.WorkflowModule == nil {
		t.Fatal("conformance runtime grant requires a constructed runtime and semantic source")
	}
	bundleHash, bundleSource := rt.Options.BundleSourceFact.StorageValues()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash, BundleSource: bundleSource}
	desired, err := rt.Manager.CompileStaticTopologyDesiredAgents(rt.Options.WorkflowModule.SemanticSource(), coordinate)
	if err != nil {
		t.Fatalf("compile conformance runtime source set: %v", err)
	}
	capability, err := store.AcquireProcessCapability(ctx, runtimestartupownership.AcquireRequest{
		OwnerID: "conformance-runtime-test", BootID: uuid.NewString(), RuntimeInstanceID: rt.Options.RuntimeInstanceID,
	})
	if err != nil {
		t.Fatalf("acquire conformance process capability: %v", err)
	}
	release := true
	defer func() {
		if release {
			_ = capability.Release(context.Background())
		}
	}()
	current, exists, err := capability.CurrentSourceSet(ctx)
	if err != nil {
		t.Fatalf("load conformance source set: %v", err)
	}
	sources := make([]runtimeagenttopology.SourceCoordinate, 0, len(current.Sources)+1)
	for _, source := range current.Sources {
		if source.Normalize().Key() != coordinate.Normalize().Key() {
			sources = append(sources, source)
		}
	}
	sources = append(sources, coordinate)
	agents := make([]runtimeagenttopology.DesiredAgent, 0, len(current.Agents)+len(desired))
	for _, agent := range current.Agents {
		if agent.Source.Normalize().Key() != coordinate.Normalize().Key() {
			agents = append(agents, agent)
		}
	}
	agents = append(agents, desired...)
	plan, err := runtimeagenttopology.NewSourceSetPlan(sources, agents)
	if err != nil {
		t.Fatalf("construct conformance source set: %v", err)
	}
	commit := runtimeagenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: plan}
	if exists {
		commit.ExpectedRevision = current.Revision
		_, err = capability.RestoreSourceSet(ctx, commit)
	} else {
		_, err = capability.InstallCompleteSourceSet(ctx, commit)
	}
	if err != nil {
		t.Fatalf("commit conformance source set: %v", err)
	}
	grant, err := capability.IssueGenerationGrant(ctx, runtimestartupownership.GrantRequest{
		BundleHash: bundleHash, BundleSource: bundleSource, RuntimeInstanceID: rt.Options.RuntimeInstanceID,
		RuntimeGeneration: 1, SourceSetRevision: plan.Revision,
	})
	if err != nil {
		t.Fatalf("issue conformance runtime generation grant: %v", err)
	}
	if err := rt.InstallStartupGrant(grant); err != nil {
		t.Fatalf("install conformance runtime generation grant: %v", err)
	}
	t.Cleanup(func() {
		if err := closeConformanceRuntimeGeneration(rt, capability); err != nil {
			t.Errorf("close conformance runtime generation: %v", err)
		}
	})
	release = false
	return capability, grant
}

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type conformanceDurableEventBusStore interface {
	runtimebus.EventStore
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry
	runtimebus.FlowInstanceRoutePersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteTopologyPersistence
	runtimebus.FlowInstanceRouteRollbackPersistence
	runtimebus.ActiveAgentDescriptorLister
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.SelectedRunTargetOwnerLister
	runtimepipeline.WorkflowInstancePersistenceReader
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	runtimepipeline.StandingRestartDispositionReader
	PipelineObligations() runtimepipelineobligation.Store
}

func durableConformanceEventBusOptions(store conformanceDurableEventBusStore, opts runtimebus.EventBusOptions) runtimebus.EventBusOptions {
	opts.PipelineObligations = store.PipelineObligations()
	opts.Durable = conformanceDurableEventBusDependencies(store)
	return opts
}

func conformanceDurableEventBusDependencies(store conformanceDurableEventBusStore) runtimebus.DurableDependencies {
	return runtimebus.DurableDependencies{
		ReplyContext: store, RunLifecycle: store, DeliveryLifecycle: store,
		FlowRoutes: store, FlowRouteRecords: store, FlowRouteSets: store, FlowRouteTopology: store, FlowRouteRollback: store,
		ActiveAgents: store, ActiveFlows: store, TargetOwners: store, WorkflowInstances: store, PreparedEvents: store,
		TargetFailureRecorder: store, RunOrigins: store, StandingRestarts: store,
	}
}

func completeConformanceWorkflowDeps(store conformanceDurableEventBusStore, deps runtimepkg.RuntimeDeps) runtimepkg.RuntimeDeps {
	if deps.DeliveryStore == nil {
		deps.DeliveryStore = store
	}
	if deps.DecisionCards == nil {
		deps.DecisionCards = store
	}
	if deps.ProposedEffects == nil {
		deps.ProposedEffects = store
	}
	if deps.DecisionCardHumanTasks == nil {
		deps.DecisionCardHumanTasks = store
	}
	if deps.DecisionCardDraftExpiry == nil {
		deps.DecisionCardDraftExpiry = store
	}
	if deps.HumanTaskExpiry == nil {
		deps.HumanTaskExpiry = store
	}
	return deps
}

func registerTestAuthorActivityCatalog(t *testing.T, target testAuthorActivityCatalogRegistrar, descriptors []runtimeauthoractivity.EventDescriptor) {
	registerTestAuthorActivityCatalogForBundle(t, target, authorActivityTestBundleSourceFact, descriptors)
}

func registerTestAuthorActivityCatalogForBundle(t *testing.T, target testAuthorActivityCatalogRegistrar, fact runtimecorrelation.BundleSourceFact, descriptors []runtimeauthoractivity.EventDescriptor) {
	t.Helper()
	lease, err := target.RegisterAuthorActivityEventCatalog(
		runtimeauthoractivity.BundleScope(authorActivityTestRuntimeInstanceID, fact.BundleHash()),
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
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if strings.TrimSpace(opts.BundleSourceFact.BundleHash()) == "" {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = conformanceTestRuntimeOccurrence(t, opts.BundleSourceFact.BundleHash())
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
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
	if registrar, ok := eventStore.(testAuthorActivityCatalogRegistrar); ok {
		descriptors := testAuthorActivityEventDescriptors(t, opts)
		for _, eventType := range differentEvents {
			descriptors = append(descriptors, runtimeauthoractivity.EventDescriptor{
				EventType: strings.TrimSpace(eventType), Disposition: runtimeauthoractivity.StoryDifferent,
			})
		}
		registerTestAuthorActivityCatalogForBundle(t, registrar, opts.BundleSourceFact, descriptors)
	}
	if opts.PipelineObligations != nil {
		durable, ok := eventStore.(conformanceDurableEventBusStore)
		if !ok {
			return nil, fmt.Errorf("conformance durable event-store fixture %T lacks exact durable roles", eventStore)
		}
		opts = durableConformanceEventBusOptions(durable, opts)
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
		if proof.IsAuthored(opts.ContractBundle) {
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
