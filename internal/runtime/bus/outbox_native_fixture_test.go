package bus_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type nativeOutboxFixture struct {
	store       componentFlowConstructionStore
	bus         *runtimebus.EventBus
	coordinator *pipeline.PipelineCoordinator
	ctx         context.Context
	identity    flowidentity.RunScopedFlowInstance
}

func newNativeOutboxFixture(t *testing.T, backend string, descriptors ...runtimebus.ActiveAgentDescriptor) *nativeOutboxFixture {
	t.Helper()
	var selected componentFlowConstructionStore
	switch backend {
	case "sqlite":
		selected = storetest.StartSQLiteRuntimeStore(t)
	case "postgres":
		selected = storetest.StartPostgresRuntimeStore(t)
	default:
		t.Fatalf("unknown native outbox backend %q", backend)
	}
	bundle := loadEventBusTempBundle(t, map[string]string{
		"schema.yaml":   "name: native-outbox\nstages:\n  active: {}\n",
		"entities.yaml": "outbox_state: {}\n",
	})
	source := semanticview.Wrap(bundle)
	process := worklifetime.NewProcess()
	occurrence, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleHash: testSourceArtifactFact(source).BundleHash(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var eb *runtimebus.EventBus
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var cleanupErr error
		if eb != nil {
			cleanupErr = eb.ResetInMemoryState()
		}
		_, retireErr := occurrence.RetireAndWait(ctx)
		process.Retire()
		_, joinErr := process.Join(ctx)
		if err := errors.Join(cleanupErr, retireErr, joinErr); err != nil {
			t.Errorf("join native outbox fixture: %v", err)
		}
	})
	eb, err = newScopedTestEventBus(selected, runtimebus.EventBusOptions{ContractBundle: source, WorkOwner: occurrence})
	if err != nil {
		t.Fatal(err)
	}
	ctx := correlation.WithRunID(testAuthorActivityContextForSource(context.Background(), source), runtimebustest.DefaultRunID)
	ctx = effects.WithExecutionMode(ctx, executionmode.Live)
	at := time.Now().UTC()
	storetest.RequireRun(t, ctx, selected, storetest.RunFixture{
		RunID: runtimebustest.DefaultRunID, State: runlifecycle.StateRunning,
		Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact, StartedAt: at,
	})
	constructed := runtimebus.ConstructedFlowInstanceIdentityFixture(source, ".", "", runtimebustest.DefaultRunID)
	seedComponentFlowConstruction(t, ctx, selected, source, pipeline.WorkflowInstance{
		WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
		InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
		ParentFlowID: constructed.ParentRoute.FlowID, ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
		CreatedAt: at, EnteredStageAt: at,
	})
	for _, descriptor := range descriptors {
		eb.RegisterRuntimeActiveAgentDescriptor(descriptor)
	}
	return &nativeOutboxFixture{
		store: selected, bus: eb, ctx: ctx,
		coordinator: newEventBusWorkflowCoordinator(eb, selected, newFixtureWorkflowModule(t, bundle)),
		identity:    flowidentity.RunScopedFlowInstance{RunID: runtimebustest.DefaultRunID, Route: constructed.Route()},
	}
}

func (f *nativeOutboxFixture) commit(intents []engine.EmitIntent) (engine.CommittedEngineMutation, error) {
	return f.coordinator.CommitConstructedOutboxFixture(f.ctx, f.identity, intents)
}

func (f *nativeOutboxFixture) commitError(intents []engine.EmitIntent) error {
	_, err := f.commit(intents)
	return err
}

func (f *nativeOutboxFixture) eventCount(t *testing.T) int {
	t.Helper()
	count, err := storetest.CountPhysicalRunEvents(f.ctx, f.store, f.identity.RunID)
	if err != nil {
		t.Fatal(err)
	}
	return count
}

func TestNativeOutboxCommitsThroughConstructedWorkflowOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeOutboxFixture(t, backend)
			intent := engine.EmitIntent{Event: eventtest.ExistingRunRootIngress(
				eventtest.UUID("native-outbox-owner"), "custom.emitted", "", "", nil, 0,
				runtimebustest.DefaultRunID, events.EventEnvelope{}, time.Now().UTC(),
			)}
			result, err := f.commit([]engine.EmitIntent{intent})
			if err != nil || !result.Committed || len(result.EmitIntents) != 1 {
				t.Fatalf("native outbox result=%+v err=%v", result, err)
			}
			prepared, found, err := f.store.(runtimebus.PreparedPublishEventReader).LoadPreparedPublishEvent(f.ctx, intent.Event.ID())
			if err != nil || !found || prepared.Event.ID() != intent.Event.ID() {
				t.Fatalf("native outbox readback found=%v event=%+v err=%v", found, prepared, err)
			}
			if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, result.EmitIntents); err != nil {
				t.Fatal(err)
			}
		})
	}
}
