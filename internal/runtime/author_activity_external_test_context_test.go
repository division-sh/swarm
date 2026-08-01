package runtime_test

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

var authorActivityTestBundleSourceFact = mustExternalTestBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("e", 64))

var externalRuntimeTestEventBusOwners sync.Map

type testAuthorActivityCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

type externalRuntimeTestMutationOwner interface {
	RunRuntimeMutationContext(context.Context, func(context.Context) error) error
}

type externalRuntimeTestWorkflowOwner interface {
	decisioncard.Store
	decisioncard.ProposedEffectStore
	decisioncard.HumanTaskStore
	runtimepipeline.DecisionCardDraftExpiry
	runtimepipeline.HumanTaskExpiry
}

func newExternalRuntimeTestPipelineCoordinator(
	t testing.TB,
	bus *runtimebus.EventBus,
	db *sql.DB,
	selected any,
	opts runtimepipeline.PipelineCoordinatorOptions,
) *runtimepipeline.PipelineCoordinator {
	t.Helper()
	owner, ok := selected.(externalRuntimeTestWorkflowOwner)
	if !ok {
		t.Fatalf("selected workflow test owner %T lacks exact decision and expiry roles", selected)
	}
	if opts.DecisionCards == nil {
		opts.DecisionCards = owner
	}
	if opts.ProposedEffects == nil {
		opts.ProposedEffects = owner
	}
	if opts.HumanTasks == nil {
		opts.HumanTasks = owner
	}
	if opts.DecisionCardDraftExpiry == nil {
		opts.DecisionCardDraftExpiry = owner
	}
	if opts.HumanTaskExpiry == nil {
		opts.HumanTaskExpiry = owner
	}
	if opts.GatePublisher == nil {
		opts.GatePublisher = bus
	}
	if opts.DirectDecisionPublisher == nil {
		opts.DirectDecisionPublisher = bus
	}
	if opts.DeliveryRuntime == nil {
		opts.DeliveryRuntime = bus
	}
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, db, opts)
	if coordinator == nil {
		t.Fatal("construct durable pipeline coordinator with complete workflow owners")
	}
	return coordinator
}

func completeExternalRuntimeTestWorkflowDeps(t testing.TB, selected any, deps runtimepkg.RuntimeDeps) runtimepkg.RuntimeDeps {
	t.Helper()
	owner, ok := selected.(externalRuntimeTestWorkflowOwner)
	if !ok {
		t.Fatalf("selected workflow test owner %T lacks exact decision and expiry roles", selected)
	}
	if deps.DecisionCards == nil {
		deps.DecisionCards = owner
	}
	if deps.ProposedEffects == nil {
		deps.ProposedEffects = owner
	}
	if deps.DecisionCardHumanTasks == nil {
		deps.DecisionCardHumanTasks = owner
	}
	if deps.DecisionCardDraftExpiry == nil {
		deps.DecisionCardDraftExpiry = owner
	}
	if deps.HumanTaskExpiry == nil {
		deps.HumanTaskExpiry = owner
	}
	return deps
}

type externalRuntimeTestDurableEventStore interface {
	runtimebus.EventStore
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	runtimebus.FlowInstanceRoutePersistence
	runtimebus.FlowInstanceRouteRecordReader
	runtimebus.FlowInstanceRouteSetPersistence
	runtimebus.FlowInstanceRouteTopologyPersistence
	runtimebus.FlowInstanceRouteRollbackPersistence
	runtimebus.ActiveAgentDescriptorLister
	runtimebus.ActiveFlowInstanceDescriptorLister
	runtimebus.EventDeliveryTargetReader
	runtimebus.EventDeliveryRouteSetReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
}

func externalRuntimeTestDurableDependencies(durable externalRuntimeTestDurableEventStore) runtimebus.DurableDependencies {
	return runtimebus.DurableDependencies{
		ReplyContext: durable, RunLifecycle: durable,
		DeliveryLifecycle: durable, FlowRoutes: durable, FlowRouteRecords: durable,
		FlowRouteSets: durable, FlowRouteTopology: durable, FlowRouteRollback: durable, ActiveAgents: durable,
		ActiveFlows: durable, DeliveryTargets: durable, DeliveryRouteSets: durable,
		TargetFailureRecorder: durable, RunOrigins: durable,
	}
}

func externalRuntimeTestManagerBusRoles(bus *runtimebus.EventBus) runtimemanager.PersistenceRoles {
	return runtimemanager.PersistenceRoles{
		AgentRoutes: bus, RouteInstaller: bus, RouteVerifier: bus,
		RouteRestorer: bus, RouteRetirer: bus, RouteRemover: bus,
		CreationPublisher: bus, DeliveryRuntime: bus,
	}
}

func externalRuntimeTestSelectedManagerRoles(selected any) runtimemanager.PersistenceRoles {
	var roles runtimemanager.PersistenceRoles
	roles.LifecycleState, _ = selected.(runtimemanager.AgentLifecycleStateReader)
	roles.LifecycleEffects, _ = selected.(runtimeeffects.Store)
	roles.LifecycleDiagnostics, _ = selected.(runtimemanager.AgentLifecycleDiagnosticPersistence)
	roles.EffectsRecovery, _ = selected.(runtimeeffects.RecoveryStore)
	roles.DeliveryQuiescence, _ = selected.(runtimemanager.ActiveRunDeliveryQuiescenceReader)
	roles.EventExistence, _ = selected.(runtimemanager.EventExistenceReader)
	roles.DirectiveOperations, _ = selected.(runtimeagentcontrol.DirectiveOperationStore)
	roles.DirectiveTargets, _ = selected.(runtimemanager.AgentDirectiveRunTargetResolver)
	roles.FlowRoutes, _ = selected.(runtimebus.FlowInstanceRoutePersistence)
	return roles
}

func testAuthorActivityContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, authorActivityTestBundleSourceFact)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		authorActivityTestRuntimeInstanceID,
		authorActivityTestBundleSourceFact.BundleHash(),
	))
}

func newScopedTestEventBus(t *testing.T, store runtimebus.EventStore, opts runtimebus.EventBusOptions, differentEvents ...string) (*runtimebus.EventBus, error) {
	t.Helper()
	if strings.TrimSpace(opts.RuntimeInstanceID) == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.BundleSourceFact.Validate() != nil {
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
			runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, opts.BundleSourceFact.BundleHash()),
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
	if opts.BundleSourceFact.Validate() != nil {
		opts.BundleSourceFact = authorActivityTestBundleSourceFact
	}
	if opts.WorkOwner == nil {
		process := worklifetime.NewProcess()
		owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
			RuntimeInstanceID: opts.RuntimeInstanceID,
			BundleHash:        opts.BundleSourceFact.BundleHash(),
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
		if provider, ok := store.(interface {
			PipelineObligations() runtimepipelineobligation.Store
		}); ok {
			opts.PipelineObligations = provider.PipelineObligations()
		}
	}
	if opts.PipelineObligations != nil {
		if opts.Durable.FlowRouteTopology == nil {
			durable, ok := store.(externalRuntimeTestDurableEventStore)
			if !ok {
				return nil, fmt.Errorf("external runtime durable event-store fixture %T lacks exact durable roles", store)
			}
			opts.Durable = externalRuntimeTestDurableDependencies(durable)
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
	if err := bus.SetDeliveryContinuationOwner(
		runtimebustest.NewDeliveryContinuationOwner(opts.PipelineObligations == nil),
	); err != nil {
		return nil, err
	}
	externalRuntimeTestEventBusOwners.Store(bus, opts.WorkOwner)
	t.Cleanup(func() { externalRuntimeTestEventBusOwners.Delete(bus) })
	return bus, nil
}

func testBundleSourceFact(t testing.TB, bundleHash string) runtimecorrelation.BundleSourceFact {
	t.Helper()
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(strings.TrimSpace(bundleHash))
	if err != nil {
		t.Fatalf("construct external test bundle source fact: %v", err)
	}
	return fact
}

func mustExternalTestBundleSourceFact(bundleHash string) runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact(strings.TrimSpace(bundleHash))
	if err != nil {
		panic(err)
	}
	return fact
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
