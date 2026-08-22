package manager

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/sessions"
)

type managerTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var managerTestWorkFixtures sync.Map

type managerTestDeliveryContinuationSink interface {
	RetainDeliveryContinuation(runtimedelivery.Snapshot) error
	ReleaseDeliveryContinuation(string) error
}

type managerTestDeliveryRuntime struct {
	authority runtimedelivery.ExecutionAuthority
	sink      managerTestDeliveryContinuationSink
}

type managerTestDirectiveOperationProvider interface {
	managerTestDirectiveOperations() runtimeagentcontrol.DirectiveOperationStore
}

type managerTestBusContinuationSink struct {
	owner runtimebus.DeliveryContinuationOwner
}

func (s managerTestBusContinuationSink) RetainDeliveryContinuation(snapshot runtimedelivery.Snapshot) error {
	return s.owner.Retain(snapshot)
}

func (s managerTestBusContinuationSink) ReleaseDeliveryContinuation(deliveryID string) error {
	return s.owner.Release(deliveryID)
}

func (r managerTestDeliveryRuntime) DeliveryAuthority() (runtimedelivery.ExecutionAuthority, error) {
	if err := r.authority.Validate(); err != nil {
		return runtimedelivery.ExecutionAuthority{}, err
	}
	return r.authority, nil
}

func (r managerTestDeliveryRuntime) RetainDeliveryContinuation(snapshot runtimedelivery.Snapshot) error {
	return r.sink.RetainDeliveryContinuation(snapshot)
}

func (r managerTestDeliveryRuntime) ReleaseDeliveryContinuation(deliveryID string) error {
	return r.sink.ReleaseDeliveryContinuation(deliveryID)
}

func projectManagerTestPersistenceRoles(roles *PersistenceRoles, candidate any) {
	if candidate == nil {
		return
	}
	if roles.AgentRoutes == nil {
		roles.AgentRoutes, _ = candidate.(AgentRouteBus)
	}
	if roles.FlowActivation == nil {
		roles.FlowActivation, _ = candidate.(FlowInstanceActivationCommitter)
	}
	if roles.RouteInstaller == nil {
		roles.RouteInstaller, _ = candidate.(FlowInstanceRouteContextInstaller)
	}
	if roles.RouteVerifier == nil {
		roles.RouteVerifier, _ = candidate.(FlowInstanceRouteContextVerifier)
	}
	if roles.RouteRestorer == nil {
		roles.RouteRestorer, _ = candidate.(PersistedFlowInstanceRouteRestorer)
	}
	if roles.RouteRetirer == nil {
		roles.RouteRetirer, _ = candidate.(PublishedFlowInstanceRouteRetirer)
	}
	if roles.RouteRemover == nil {
		roles.RouteRemover, _ = candidate.(FlowInstanceRouteContextRemover)
	}
	if roles.FlowTermination == nil {
		roles.FlowTermination, _ = candidate.(FlowInstanceTerminalMutationOwner)
	}
	if roles.CreationPublisher == nil {
		roles.CreationPublisher, _ = candidate.(runtimepipeline.DynamicFlowRuntimeCreationOccurrencePublisher)
	}
	if roles.LifecycleCensus == nil {
		roles.LifecycleCensus, _ = candidate.(AgentLifecycleCellCensus)
	}
	if roles.LifecycleState == nil {
		roles.LifecycleState, _ = candidate.(AgentLifecycleStateReader)
	}
	if roles.LifecycleEffects == nil {
		roles.LifecycleEffects, _ = candidate.(runtimeeffects.Store)
	}
	if roles.LifecycleDiagnostics == nil {
		roles.LifecycleDiagnostics, _ = candidate.(AgentLifecycleDiagnosticPersistence)
	}
	if roles.EffectsRecovery == nil {
		roles.EffectsRecovery, _ = candidate.(runtimeeffects.RecoveryStore)
	}
	if roles.DeliveryQuiescence == nil {
		roles.DeliveryQuiescence, _ = candidate.(ActiveRunDeliveryQuiescenceReader)
	}
	if roles.DeliveryRuntime == nil {
		roles.DeliveryRuntime, _ = candidate.(DeliveryRuntimeOwner)
	}
	if roles.EventExistence == nil {
		roles.EventExistence, _ = candidate.(EventExistenceReader)
	}
	if roles.DirectiveOperations == nil {
		roles.DirectiveOperations, _ = candidate.(runtimeagentcontrol.DirectiveOperationStore)
	}
	if roles.DirectiveTargets == nil {
		roles.DirectiveTargets, _ = candidate.(AgentDirectiveRunTargetResolver)
	}
	if roles.FlowRoutes == nil {
		roles.FlowRoutes, _ = candidate.(runtimebus.FlowInstanceRoutePersistence)
	}
}

func admitManagerTestBusContext(ctx context.Context) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return testAuthorActivityContext(ctx), nil
}

func newTestManagerWorkOwner(t *testing.T) worklifetime.Occurrence {
	t.Helper()
	if existing, ok := managerTestWorkFixtures.Load(t); ok {
		return existing.(*managerTestWorkFixture).runtime
	}
	fixture := &managerTestWorkFixture{process: worklifetime.NewProcess()}
	runtimeOwner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: "manager-test-runtime",
		BundleHash:        "manager-test-bundle",
	})
	if err != nil {
		t.Fatalf("create manager test work owner: %v", err)
	}
	fixture.runtime = runtimeOwner
	actual, loaded := managerTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		return actual.(*managerTestWorkFixture).runtime
	}
	t.Cleanup(func() {
		defer managerTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.runtime.RetireAndWait(ctx); err != nil {
			t.Errorf("retire manager test work owner: %v", err)
			return
		}
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join manager test process owner: %v", err)
		}
	})
	return runtimeOwner
}

func newTestAgentManager(t *testing.T, bus Bus, factory AgentFactory, stores ...ManagerPersistence) *AgentManager {
	t.Helper()
	return newTestAgentManagerWithOptions(t, bus, factory, AgentManagerOptions{}, stores...)
}

func newTestManagerEventBus(t *testing.T) (*runtimebus.EventBus, error) {
	t.Helper()
	return runtimebus.NewEphemeralEventBusWithOptions(nil, runtimebus.EventBusOptions{
		ExecutionPosture:  executionposture.Live,
		WorkOwner:         newTestManagerWorkOwner(t),
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
}

func newTestAgentManagerWithOptions(t *testing.T, bus Bus, factory AgentFactory, opts AgentManagerOptions, stores ...ManagerPersistence) *AgentManager {
	t.Helper()
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = newTestManagerWorkOwner(t)
	}
	if opts.DeliveryStore == nil && len(stores) > 0 {
		if deliveryStore, ok := any(stores[0]).(runtimedelivery.Store); ok {
			opts.DeliveryStore = deliveryStore
		}
	}
	if opts.DeliveryStore == nil {
		opts.DeliveryStore = newManagerDeliveryTestStore(t)
	}
	if opts.SessionLifecycle == nil && opts.Sessions != nil {
		opts.SessionLifecycle, _ = opts.Sessions.(sessions.LifecycleProjection)
	}
	if opts.SessionResetter == nil && opts.Sessions != nil {
		opts.SessionResetter, _ = opts.Sessions.(sessions.Resetter)
	}
	if opts.SessionResetter == nil {
		opts.SessionResetter = sessions.NewInMemoryRegistry(0)
	}
	if opts.LifecycleStore == nil && len(stores) > 0 {
		opts.LifecycleStore, _ = any(stores[0]).(AgentLifecyclePersistence)
	}
	projectManagerTestPersistenceRoles(&opts.PersistenceRoles, bus)
	if opts.PersistenceRoles.DirectiveOperations == nil {
		if provider, ok := bus.(managerTestDirectiveOperationProvider); ok {
			opts.PersistenceRoles.DirectiveOperations = provider.managerTestDirectiveOperations()
		}
	}
	projectManagerTestPersistenceRoles(&opts.PersistenceRoles, opts.WorkflowInstances)
	projectManagerTestPersistenceRoles(&opts.PersistenceRoles, opts.DeliveryStore)
	projectManagerTestPersistenceRoles(&opts.PersistenceRoles, opts.LifecycleStore)
	for _, store := range stores {
		projectManagerTestPersistenceRoles(&opts.PersistenceRoles, store)
	}
	if authorityStore, ok := opts.DeliveryStore.(interface {
		managerTestDeliveryAuthority() runtimedelivery.ExecutionAuthority
	}); ok {
		authority := authorityStore.managerTestDeliveryAuthority()
		continuationOwner := runtimebustest.NewDeliveryContinuationOwner(true)
		if setter, ok := bus.(interface {
			SetDeliveryAuthority(runtimedelivery.ExecutionAuthority) error
		}); ok {
			if err := setter.SetDeliveryAuthority(authority); err != nil {
				t.Fatalf("set manager test delivery authority: %v", err)
			}
		}
		if setter, ok := bus.(interface {
			SetDeliveryContinuationOwner(runtimebus.DeliveryContinuationOwner) error
		}); ok {
			if err := setter.SetDeliveryContinuationOwner(continuationOwner); err != nil {
				t.Fatalf("set manager test delivery continuation owner: %v", err)
			}
		}
		if opts.PersistenceRoles.DeliveryRuntime == nil {
			var sink managerTestDeliveryContinuationSink = managerTestBusContinuationSink{owner: continuationOwner}
			if busSink, ok := bus.(managerTestDeliveryContinuationSink); ok {
				sink = busSink
			}
			opts.PersistenceRoles.DeliveryRuntime = managerTestDeliveryRuntime{authority: authority, sink: sink}
		}
	}
	manager := NewAgentManagerWithOptions(bus, factory, opts, stores...)
	manager.mu.Lock()
	manager.startupAgentsHydrated = true
	manager.mu.Unlock()
	t.Cleanup(func() {
		if err := manager.ShutdownWithOptions(ShutdownOptions{Grace: 5 * time.Second}); err != nil {
			t.Errorf("shutdown manager test work owner: %v", err)
		}
	})
	return manager
}
