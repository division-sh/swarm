package bus_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

type nativeOutboxFixture struct {
	store       componentFlowConstructionStore
	bus         *runtimebus.EventBus
	coordinator *pipeline.PipelineCoordinator
	ctx         context.Context
	identity    flowidentity.RunScopedFlowInstance
	witness     *nativeOutboxMutationWitness
	source      semanticview.Source
}

type nativeOutboxMutationWitness struct {
	componentFlowConstructionStore
	mu         sync.Mutex
	results    []pipeline.CommittedWorkflowEngineMutation
	plans      []engine.DurablePublicationPlan
	proofFault string
}

func (w *nativeOutboxMutationWitness) CommitWorkflowEngineMutation(ctx context.Context, command pipeline.WorkflowEngineMutationCommand) (pipeline.CommittedWorkflowEngineMutation, error) {
	result, err := w.componentFlowConstructionStore.CommitWorkflowEngineMutation(ctx, command)
	w.mu.Lock()
	defer w.mu.Unlock()
	if result.Committed {
		w.plans = append([]engine.DurablePublicationPlan(nil), command.Publications...)
		w.results = append(w.results, result)
		result.Publications = append([]engine.CommittedDurablePublication(nil), result.Publications...)
		switch w.proofFault {
		case "omit":
			result.Publications = result.Publications[1:]
		case "reverse":
			for left, right := 0, len(result.Publications)-1; left < right; left, right = left+1, right-1 {
				result.Publications[left], result.Publications[right] = result.Publications[right], result.Publications[left]
			}
		}
	}
	return result, err
}

func (f *nativeOutboxFixture) intent(label string) engine.EmitIntent {
	return engine.EmitIntent{Event: eventtest.ExistingRunRootIngress(
		eventtest.UUID(label), "custom.emitted", "", "", []byte(`{"value":"original"}`), 0,
		f.identity.RunID, events.EventEnvelope{}, time.Now().UTC(),
	), Recipients: []string{"reviewer"}}
}

func (f *nativeOutboxFixture) refusedChildIntent(t *testing.T, id string) engine.EmitIntent {
	t.Helper()
	owner := runtimebus.ConstructedFlowInstanceIdentityFixture(f.source, "consumer", "", f.identity.RunID)
	if err := f.coordinator.MarkTerminated(f.ctx, testRunScopedFlowRouteForRun(f.identity.RunID, owner.Route()), identity.NormalizeEntityID(owner.EntityID), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	return engine.EmitIntent{Event: eventtest.ExistingRunRootIngressWithRoutingSource(
		id, "root.ready", "runtime", "", []byte(`{}`), 0, f.identity.RunID,
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: owner.TemplateID, EntityID: owner.EntityID, FlowInstance: owner.InstancePath}),
		eventtest.RootRoutingSource(f.identity.RunID), time.Now().UTC(),
	)}
}

func newNativeRefusedOutboxFixture(t *testing.T, backend string) *nativeOutboxFixture {
	t.Helper()
	source := loadNativeOutboxSource(t, canonicalrouting.CopyRootOutputSingletonConnect(t))
	f := newNativeOutboxFixtureWithOptions(t, backend, runtimebus.EventBusOptions{ContractBundle: source})
	owner := runtimebus.ConstructedFlowInstanceIdentityFixture(source, "consumer", "", f.identity.RunID)
	at := time.Now().UTC()
	seedComponentFlowConstruction(t, f.ctx, f.store, source, pipeline.WorkflowInstance{
		InstanceID: owner.InstanceID, StorageRef: owner.InstancePath, EntityID: owner.EntityID,
		ParentFlowID: owner.ParentRoute.FlowID, ParentFlowInstance: owner.ParentRoute.FlowInstance, ParentEntityID: owner.ParentEntityID,
		WorkflowName: "consumer", WorkflowVersion: source.WorkflowVersion(), CreatedAt: at, EnteredStageAt: at,
	})
	return f
}

func (f *nativeOutboxFixture) eventImage(t *testing.T, id string) []byte {
	t.Helper()
	event, err := f.store.(operatorread.ObservabilityReader).LoadOperatorEvent(f.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	image, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return image
}

func TestNativeOutboxBatchClaimOwnershipAndOrderedReceiptsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"AAA", "ABA"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				proveNativeOutboxOrderedClaimLifecycle(t, backend, order)
			})
		}
	}
}

func proveNativeOutboxOrderedClaimLifecycle(t *testing.T, backend, order string) {
	t.Helper()
	f := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "reviewer", "", ""))
	receiver := runtimebustest.Subscribe(t, f.bus, "reviewer")
	a, b := f.intent("native-batch-A"), f.intent("native-batch-B")
	intents := []engine.EmitIntent{a, a, a}
	if order == "ABA" {
		intents[1] = b
	}
	result, err := f.commit(intents)
	if err != nil || !result.Committed || len(result.EmitIntents) != 3 {
		t.Fatalf("ordinary prospective engine batch result=%+v err=%v", result, err)
	}
	f.assertAppendOutcomes(t, true, order == "ABA", false)
	// Cleanup of a later occurrence cannot release the designated
	// first occurrence's live native claim before it dispatches.
	if err := f.bus.ReleaseEnginePublications(f.ctx, f.witness.plans[2:]); err != nil {
		t.Fatal(err)
	}
	for _, purpose := range []pipelineobligation.Purpose{pipelineobligation.PurposePublication, pipelineobligation.PurposeRecovery} {
		var claimErr error
		if purpose == pipelineobligation.PurposePublication {
			_, claimErr = f.store.PipelineObligations().ClaimPublication(f.ctx, a.Event.ID())
		} else {
			_, claimErr = f.store.PipelineObligations().ClaimEvent(f.ctx, a.Event.ID(), purpose)
		}
		if !errors.Is(claimErr, pipelineobligation.ErrBusy) {
			t.Fatalf("competing %s claim before designated dispatch=%v", purpose, claimErr)
		}
	}
	if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, result.EmitIntents[:1]); err != nil {
		t.Fatal(err)
	}
	if got := requireBusEvent(t, receiver, "designated A"); got.ID() != a.Event.ID() {
		t.Fatalf("first dispatch=%s, want A", got.ID())
	}
	before := f.eventImage(t, a.Event.ID())
	independent, err := f.store.PipelineObligations().ClaimPublication(f.ctx, a.Event.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.store.PipelineObligations().Release(f.ctx, independent); err != nil {
			t.Error(err)
		}
	}()
	if err := f.bus.EngineDispatcher().DispatchPostCommit(f.ctx, result.EmitIntents[1:]); err != nil {
		t.Fatal(err)
	}
	if order == "ABA" {
		if got := requireBusEvent(t, receiver, "designated B"); got.ID() != b.Event.ID() {
			t.Fatalf("second dispatch=%s, want B", got.ID())
		}
	}
	if _, err := f.store.PipelineObligations().ClaimPublication(f.ctx, a.Event.ID()); !errors.Is(err, pipelineobligation.ErrBusy) {
		t.Fatalf("borrowed duplicate cleanup changed the independent claim: %v", err)
	}
	if after := f.eventImage(t, a.Event.ID()); !reflect.DeepEqual(after, before) {
		t.Fatalf("duplicate changed durable event side effects\nbefore=%s\nafter=%s", before, after)
	}
	requireNoBusEvent(t, receiver, "duplicate operation dispatch")
}

func TestNativeOutboxRefusesOmittedOrReorderedCommittedOccurrencesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []string{"omit", "reverse"} {
			t.Run(backend+"/"+fault, func(t *testing.T) {
				f := newNativeOutboxFixture(t, backend, testActiveAgentDescriptor(t, "reviewer", "", ""))
				receiver := runtimebustest.Subscribe(t, f.bus, "reviewer")
				f.witness.proofFault = fault
				intent := f.intent("native-outcome-correspondence")
				result, err := f.commit([]engine.EmitIntent{intent, intent, intent})
				if err == nil || !result.Committed || len(result.EmitIntents) != 0 {
					t.Fatalf("altered committed receipt escaped: result=%+v err=%v", result, err)
				}
				if f.eventCount(t) != 1 {
					t.Fatal("lost the genuine committed event")
				}
				claim, err := f.store.PipelineObligations().ClaimPublication(f.ctx, intent.Event.ID())
				if err != nil {
					t.Fatalf("invalid receipt leaked native claim: %v", err)
				}
				if err := f.store.PipelineObligations().Release(f.ctx, claim); err != nil {
					t.Fatal(err)
				}
				requireNoBusEvent(t, receiver, "invalid receipt dispatch")
			})
		}
	}
}

func newNativeOutboxFixture(t *testing.T, backend string, descriptors ...runtimebus.ActiveAgentDescriptor) *nativeOutboxFixture {
	t.Helper()
	return newNativeOutboxFixtureWithOptions(t, backend, runtimebus.EventBusOptions{}, descriptors...)
}

func newNativeOutboxFixtureWithOptions(t *testing.T, backend string, options runtimebus.EventBusOptions, descriptors ...runtimebus.ActiveAgentDescriptor) *nativeOutboxFixture {
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
	bundle, sourcePresent := semanticview.Bundle(options.ContractBundle)
	if !sourcePresent {
		bundle = loadEventBusTempBundle(t, map[string]string{
			"schema.yaml": "name: native-outbox\nstages:\n  active: {}\n", "entities.yaml": "outbox_state: {}\n",
			"events.yaml": "output.done:\n",
		})
	}
	if bundle == nil || bundle.SourceArtifact == nil {
		t.Fatal("native outbox requires a compiled admitted source")
	}
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
	options.ContractBundle, options.WorkOwner = source, occurrence
	eb, err = newScopedTestEventBus(selected, options)
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
	instances := exactEventBusWorkflowFixtures(t, source, []pipeline.WorkflowInstance{{
		WorkflowName: ".", WorkflowVersion: source.WorkflowVersion(),
		InstanceID: constructed.InstanceID, StorageRef: constructed.InstancePath, EntityID: constructed.EntityID,
		ParentFlowID: constructed.ParentRoute.FlowID, ParentFlowInstance: constructed.ParentRoute.FlowInstance, ParentEntityID: constructed.ParentEntityID,
		CreatedAt: at, EnteredStageAt: at,
	}})
	seedComponentFlowConstruction(t, ctx, selected, source, instances[0])
	for _, descriptor := range descriptors {
		eb.RegisterRuntimeActiveAgentDescriptor(descriptor)
	}
	witness := &nativeOutboxMutationWitness{componentFlowConstructionStore: selected}
	return &nativeOutboxFixture{
		store: selected, bus: eb, ctx: ctx,
		coordinator: newEventBusWorkflowCoordinator(eb, witness, newFixtureWorkflowModule(t, bundle)),
		identity:    flowidentity.RunScopedFlowInstance{RunID: runtimebustest.DefaultRunID, Route: constructed.Route()},
		witness:     witness,
		source:      source,
	}
}

func loadNativeOutboxSource(t *testing.T, path string) semanticview.Source {
	t.Helper()
	repo := filepath.Clean(filepath.Join("..", "..", ".."))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, path, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func (f *nativeOutboxFixture) assertAppendOutcomes(t *testing.T, inserted ...bool) {
	t.Helper()
	f.witness.mu.Lock()
	defer f.witness.mu.Unlock()
	if len(f.witness.results) == 0 {
		t.Fatal("no native committed result")
	}
	publications := f.witness.results[len(f.witness.results)-1].Publications
	if len(publications) != len(inserted) {
		t.Fatalf("native ordered outcomes = %d, want %d", len(publications), len(inserted))
	}
	for index, want := range inserted {
		publication, ok := publications[index].(runtimebus.CommittedEnginePublication)
		if !ok || publication.NewlyInserted() != want {
			t.Fatalf("native outcome %d: %T inserted=%v, want %v", index, publications[index], publication.NewlyInserted(), want)
		}
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
