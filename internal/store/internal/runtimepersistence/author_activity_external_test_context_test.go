package runtimepersistence_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const authorActivityTestRuntimeInstanceID = "11111111-1111-1111-1111-111111111111"

func originalStoreFixtureContext(t testing.TB, ctx context.Context, selected any) context.Context {
	t.Helper()
	if !startupownership.HasMutationAuthority(ctx) {
		if _, found := runtimecorrelation.RuntimeInstanceIDFromContext(ctx); !found {
			runtimeID := authorActivityTestRuntimeInstanceID
			if scope, present := runtimeauthoractivity.ScopeFromContext(ctx); present {
				runtimeID = scope.RuntimeInstanceID
			}
			ctx = runtimecorrelation.WithRuntimeInstanceID(ctx, runtimeID)
		}
	}
	if _, found := worklifetime.ProcessFromContext(ctx); !found {
		ctx = storeTestWorkContext(t, ctx)
	}
	if startupownership.HasMutationAuthority(ctx) {
		original, err := startupownership.CaptureMutationAuthority(ctx)
		if err != nil {
			t.Fatal(err)
		}
		process, _ := worklifetime.ProcessFromContext(ctx)
		if err := process.BindOriginalProcess(original.ProcessAuthority().LeaseStamp()); err != nil {
			t.Fatal(err)
		}
		return ctx
	}
	store, ok := selected.(agentfixture.Store)
	if !ok {
		t.Fatalf("original fixture context requires its selected lifecycle owner: %T", selected)
	}
	ctx, err := agentfixture.ProcessContext(t, ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

type externalStoreTestWorkFixture struct {
	process *worklifetime.Process
	runtime *worklifetime.RuntimeOccurrence
}

var externalStoreTestWorkFixtures sync.Map

func storeTestWorkOwner(t testing.TB) *worklifetime.RuntimeOccurrence {
	t.Helper()
	if existing, ok := externalStoreTestWorkFixtures.Load(t); ok {
		return existing.(*externalStoreTestWorkFixture).runtime
	}
	fixture := &externalStoreTestWorkFixture{process: worklifetime.NewProcess()}
	owner, err := fixture.process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: authorActivityTestRuntimeInstanceID,
		BundleHash:        sourceartifactfixture.BundleHash,
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

func storeTestWorkContext(t testing.TB, ctx context.Context) context.Context {
	t.Helper()
	owner := storeTestWorkOwner(t)
	value, _ := externalStoreTestWorkFixtures.Load(t)
	return worklifetime.WithProcess(worklifetime.WithOccurrence(ctx, owner), value.(*externalStoreTestWorkFixture).process)
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
	runtimepipeline.FlowInstanceIndexReader
	runtimepipeline.FlowConstructionPublicationReader
	sourceartifactfixture.Writer
	SetEventPayloadAdmitter(runtimebus.PayloadAdmitter)
	runtimebus.EventStore
	runtimereplycontext.Store
	runtimerunlifecycle.OperationOwner
	runtimedelivery.Store
	runtimebus.ActiveAgentDescriptorLister
	runtimepipeline.WorkflowInstancePersistenceReader
	runtimebus.PreparedPublishEventReader
	runtimebus.TargetFailureDeadLetterRecorder
	runtimebus.RunOriginReader
	PipelineObligations() runtimepipelineobligation.Store
}

func newStoreTestEventBus(t *testing.T, selected externalStoreTestDurableEventBusStore, options ...runtimebus.EventBusOptions) (*runtimebus.EventBus, error) {
	t.Helper()
	var opts runtimebus.EventBusOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.CarryMutationContext == nil {
		opts.CarryMutationContext = startupownership.CarryMutationAuthority
	}
	if opts.RuntimeInstanceID == "" {
		opts.RuntimeInstanceID = authorActivityTestRuntimeInstanceID
	}
	setupCtx := runtimecorrelation.WithRuntimeInstanceID(testAuthorActivityContext(), opts.RuntimeInstanceID)
	setupBundle := sourceartifactfixture.BundleHash
	if opts.SourceArtifactFact.Validate() == nil {
		setupBundle = opts.SourceArtifactFact.BundleHash()
		setupCtx = runtimecorrelation.WithSourceArtifactFact(setupCtx, opts.SourceArtifactFact)
	}
	setupCtx = runtimeauthoractivity.WithScope(setupCtx, runtimeauthoractivity.BundleScope(opts.RuntimeInstanceID, setupBundle))
	if opts.MutationContext != nil {
		var err error
		setupCtx, err = opts.MutationContext(setupCtx)
		if err != nil {
			return nil, err
		}
	}
	setupCtx = originalStoreFixtureContext(t, setupCtx, selected)
	if opts.MutationContext == nil {
		original := setupCtx
		opts.MutationContext = func(ctx context.Context) (context.Context, error) {
			if !startupownership.HasMutationAuthority(ctx) {
				ctx = startupownership.CarryMutationAuthority(ctx, original)
			}
			_, err := startupownership.CaptureMutationAuthority(ctx)
			return ctx, err
		}
	}
	if !opts.ExecutionPosture.Valid() {
		opts.ExecutionPosture = executionposture.Live
	}
	if opts.SourceArtifactFact.Validate() != nil {
		opts.SourceArtifactFact = sourceartifactfixture.Require(t, setupCtx, selected)
	} else if opts.SourceArtifactFact.BundleHash() == sourceartifactfixture.BundleHash {
		sourceartifactfixture.Require(t, setupCtx, selected)
	}
	if opts.ContractBundle == nil && opts.SourceArtifactFact.BundleHash() == sourceartifactfixture.BundleHash {
		repo := runtimepipeline.WorkflowRepoRoot()
		rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
		bundle, err := runtimecontracts.LoadWorkflowContractBundleFromRootedSource(repo, rooted, runtimecontracts.DefaultPlatformSpecFile(repo), runtimecontracts.WorkflowContractLoadOptions{})
		if err != nil {
			return nil, err
		}
		opts.ContractBundle = semanticview.Wrap(bundle)
	}
	if opts.WorkOwner == nil {
		opts.WorkOwner = storeTestWorkOwner(t)
	}
	if !opts.ReceiverExecution.Configured() {
		opts.ReceiverExecution = eventreceiver.NormalExecution()
	}
	if opts.DeliveryAuthority.Kind() == "" {
		authority, authorityErr := runtimedelivery.NewNormalExecutionAuthority(
			opts.SourceArtifactFact,
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
	if opts.PayloadAdmitter == nil {
		opts.PayloadAdmitter = func(_ context.Context, event events.Event, payloadProducer events.PayloadProducer) (events.PayloadAdmission, error) {
			flowID := payloadProducer.FlowID()
			return eventtest.PayloadAdmission(event, flowID, string(event.Type()))
		}
	}
	selected.SetEventPayloadAdmitter(opts.PayloadAdmitter)
	opts.Durable = runtimebus.DurableDependencies{
		Instances:                selected,
		ConstructionPublications: selected,
		ReplyContext:             selected, RunLifecycle: selected, DeliveryLifecycle: selected,

		ActiveAgents: selected, PreparedEvents: selected,
		TargetFailureRecorder: selected,
	}
	if feedback, ok := selected.(runtimepipeline.WorkflowEmitFeedbackOwner); ok {
		opts.Durable.EmitFeedback = feedback
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

func mustExternalStoreTestSourceArtifactFact() runtimecorrelation.SourceArtifactFact {
	return sourceartifactfixture.Fact()
}

func testAuthorActivityContext() context.Context {
	bundleHash := sourceartifactfixture.BundleHash
	fact := mustExternalStoreTestSourceArtifactFact()
	ctx := runtimecorrelation.WithSourceArtifactFact(context.Background(), fact)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(
		"11111111-1111-1111-1111-111111111111",
		bundleHash,
	))
}
