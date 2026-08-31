package runtimepersistence_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	runtimereplycontext "github.com/division-sh/swarm/internal/runtime/replycontext"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

type externalStoreTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var externalStoreTestWorkFixtures sync.Map

func storeTestWorkOwner(t *testing.T) *worklifetime.RuntimeOccurrence {
	t.Helper()
	if existing, ok := externalStoreTestWorkFixtures.Load(t); ok {
		return existing.(*externalStoreTestWorkFixture).runtime
	}
	fixture := &externalStoreTestWorkFixture{process: worklifetime.NewProcess()}
	owner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        "bundle-v1:sha256:" + strings.Repeat("a", 64),
	})
	if err != nil {
		t.Fatalf("create external store test work owner: %v", err)
	}
	fixture.runtime = owner
	actual, loaded := externalStoreTestWorkFixtures.LoadOrStore(t, fixture)
	if loaded {
		_, _ = owner.RetireAndWait(context.Background())
		return actual.(*externalStoreTestWorkFixture).runtime
	}
	t.Cleanup(func() {
		defer externalStoreTestWorkFixtures.Delete(t)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := fixture.runtime.RetireAndWait(ctx); err != nil {
			t.Errorf("retire external store test work owner: %v", err)
			return
		}
		fixture.process.Retire()
		if _, err := fixture.process.Join(ctx); err != nil {
			t.Errorf("join external store test process owner: %v", err)
		}
	})
	return owner
}

func storeTestWorkContext(t *testing.T, ctx context.Context) context.Context {
	t.Helper()
	return worklifetime.WithOccurrence(ctx, storeTestWorkOwner(t))
}

func ownStoreTestAgentManager(t *testing.T, manager *runtimemanager.AgentManager) *runtimemanager.AgentManager {
	t.Helper()
	t.Cleanup(func() {
		if err := manager.Shutdown(); err != nil {
			t.Errorf("shutdown external store test manager: %v", err)
		}
	})
	return manager
}

type externalStoreTestDurableEventBusStore interface {
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
	runtimebus.SelectedRunTargetOwnerLister
	runtimepipeline.WorkflowInstancePersistenceReader
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	runtimepipeline.StandingRestartDispositionReader
	PipelineObligations() runtimepipelineobligation.Store
}

func newStoreTestEventBus(t *testing.T, selected externalStoreTestDurableEventBusStore, options ...runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	var opts runtimebus.EventBusOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if opts.BundleSourceFact.Validate() != nil {
		opts.BundleSourceFact = mustExternalStoreTestBundleSourceFact()
	}
	if opts.RuntimeInstanceID == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = storeTestWorkOwner(t)
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
	if opts.PipelineObligations == nil {
		opts.PipelineObligations = selected.PipelineObligations()
	}
	opts.Durable = runtimebus.DurableDependencies{
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected, FlowRouteTopology: selected, FlowRouteRollback: selected,
		ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, WorkflowInstances: selected, PreparedEvents: selected,
		TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
	}
	bus, err := runtimebus.NewEventBusWithOptions(selected, opts)
	if err != nil {
		return nil, err
	}
	if err := bus.SetDeliveryContinuationOwner(
		runtimebustest.NewDeliveryContinuationOwner(false),
	); err != nil {
		return nil, err
	}
	return bus, nil
}

func mustExternalStoreTestBundleSourceFact() runtimecorrelation.BundleSourceFact {
	fact, err := runtimecorrelation.NewEphemeralBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		panic(err)
	}
	return fact
}

func testAuthorActivityContext() context.Context {
	bundleHash := "bundle-v1:sha256:" + strings.Repeat("a", 64)
	fact := mustExternalStoreTestBundleSourceFact()
	ctx := runtimecorrelation.WithBundleSourceFact(context.Background(), fact)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		"11111111-1111-1111-1111-111111111111",
		bundleHash,
	))
}
