package bus

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/durabledata"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type publicationAcknowledgementProbeStore struct {
	InMemoryEventStore
	constructionIndexTestReader
	acknowledged   bool
	replay         bool
	err            error
	cancel         context.CancelFunc
	commits        int
	construction   runtimepipeline.FlowConstructionPublicationEvidence
	afterAPICommit func()
}

func (s *publicationAcknowledgementProbeStore) LoadFlowConstructionPublication(ctx context.Context, owner runtimeflowidentity.RunScopedFlowInstance, entity string) (runtimepipeline.FlowConstructionPublicationEvidence, error) {
	if err := ctx.Err(); err != nil {
		return runtimepipeline.FlowConstructionPublicationEvidence{}, err
	}
	if s.construction.Identity.Route() != owner.Route || owner.RunID != s.construction.Identity.InstancePath || entity != s.construction.Identity.EntityID {
		return runtimepipeline.FlowConstructionPublicationEvidence{}, errors.New("absent acknowledgment-probe root receipt")
	}
	return s.construction, nil
}

type deploymentRunStartProbeStore struct {
	InMemoryEventStore
	calls        int
	got          durabledata.RunCreationCommand
	root         runtimepipeline.FlowInstanceActivationPlan
	acknowledged bool
	replay       bool
	rejected     bool
	fault        error
	cancel       context.CancelFunc
}

func (s *deploymentRunStartProbeStore) CommitDeploymentRunCreation(_ context.Context, command DeploymentRunCreationCommand) (CommittedDeploymentRunCreation, error) {
	s.calls++
	s.got, s.root = command.RunCreation, command.Root.Plan
	items := make([]durabledata.RunCreationDataItem, 0, len(command.RunCreation.Data.Pins))
	for _, selected := range command.RunCreation.Data.Pins {
		items = append(items, durabledata.RunCreationDataItem{Kind: "pin", Pin: &durabledata.Pin{
			RunID: command.RunCreation.RunID, RunState: "running", Declaration: selected.Declaration,
			VersionID: selected.VersionID, Selection: "explicit",
			SchemaDigest: "resource-schema-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}})
	}
	page := durabledata.FirstEvidencePage(items)
	record := durabledata.RunCreationOperationRecord{
		Summary: durabledata.RunCreationOperationSummary{
			Kind: "run_creation", RunID: command.RunCreation.RunID, BundleHash: command.RunCreation.BundleHash,
			Outcome: "created", Status: "running", PinCount: len(items),
			CompletedAt: time.Now().UTC().Truncate(time.Microsecond), Rejection: durabledata.NoRunCreationRejection(),
		},
		Binding:  durabledata.DataBinding{State: "bound", RunID: command.RunCreation.RunID, PinCount: len(items), Evidence: &page},
		Evidence: durabledata.RunCreationEvidence{RunBinding: items},
	}
	if s.rejected {
		selected := command.RunCreation.Data.Pins[0]
		record.Summary.Outcome, record.Summary.Status, record.Summary.PinCount = "data_rejected", "", 0
		record.Summary.Rejection = durabledata.RunCreationRejection{State: "rejected", Code: durabledata.RunCreationRejectionVersionMissing, Declaration: &selected.Declaration, VersionID: selected.VersionID}
		record.Binding, record.Evidence = durabledata.DataBinding{State: "none"}, durabledata.RunCreationEvidence{}
	}
	result := CommittedDeploymentRunCreation{Record: record, Acknowledged: s.acknowledged, Replay: s.replay}
	if !s.replay && !s.rejected {
		result.Activations = []runtimepipeline.CommittedFlowInstanceActivation{{Plan: command.Root.Plan, Created: true, ReadinessAttemptOrdinal: 1, Acknowledged: s.acknowledged}}
	}
	if s.cancel != nil {
		s.cancel()
	}
	return result, s.fault
}

func TestDeploymentRunStartDispatchesOnlyAcknowledgedConstruction(t *testing.T) {
	for _, test := range []struct {
		name         string
		acknowledged bool
		replay       bool
		rejected     bool
		cancel       bool
		fault        bool
		wantCalls    int
	}{
		{name: "success", acknowledged: true, wantCalls: 1},
		{name: "unknown-commit", fault: true},
		{name: "unknown-commit-without-error"},
		{name: "acknowledged-cleanup-error", acknowledged: true, fault: true, wantCalls: 1},
		{name: "acknowledged-cancellation", acknowledged: true, cancel: true, fault: true, wantCalls: 1},
		{name: "permanent-receipt-replay", acknowledged: true, replay: true},
		{name: "permanent-receipt-replay-cleanup-error", acknowledged: true, replay: true, fault: true},
		{name: "valid-rejected-receipt", acknowledged: true, rejected: true},
		{name: "valid-rejected-replay", acknowledged: true, replay: true, rejected: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var fault error
			if test.fault {
				fault = errors.New("deployment post-commit diagnostic")
			}
			store := &deploymentRunStartProbeStore{acknowledged: test.acknowledged, replay: test.replay, rejected: test.rejected, fault: fault}
			source, _ := acknowledgedRootInputEndpoint(t)
			bundle, _ := semanticview.Bundle(source)
			bundle.Semantics.Version = "1"
			var calls int
			finalized := make(chan struct{}, 1)
			finalizer := runtimepipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, activation runtimepipeline.CommittedFlowInstanceActivation) error {
				defer func() { finalized <- struct{}{} }()
				calls++
				if !activation.Acknowledged || correlation.RunIDFromContext(ctx) != activation.Plan.Readiness.RunID {
					t.Fatalf("finalizer lost exact acknowledged run authority: %+v", activation)
				}
				if test.cancel && ctx.Err() != nil {
					t.Fatal("accepted finalizer inherited caller cancellation")
				}
				return ctx.Err()
			})
			bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TemplateInstancePlanner: newTestFlowInstanceActivationOwner(nil), FlowActivationFinalizer: finalizer})
			if err != nil {
				t.Fatal(err)
			}
			bus.routeTable, err = DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			ref, err := durabledata.ParseDeclarationRef(".", "records.loaded")
			if err != nil {
				t.Fatal(err)
			}
			command := durabledata.RunCreationCommand{RunID: uuid.NewString(), Actor: "operator", BundleHash: authorActivityTestSourceArtifactFact.BundleHash(), Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{Declaration: ref, VersionID: "resource-version-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}}}}
			ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
			defer cancel()
			if test.cancel {
				store.cancel = cancel
			}
			got, err := bus.StartDeploymentRunAcknowledged(ctx, command, apiidempotency.Request{Method: "run.start"})
			if test.wantCalls != 0 {
				select {
				case <-finalized:
				case <-time.After(2 * time.Second):
					t.Fatal("accepted deployment consequence did not finalize")
				}
			}
			wantErr := fault
			if !test.acknowledged {
				wantErr = errors.Join(fault, errors.New("deployment run creation commit was not acknowledged"))
			}
			if validationErr := durabledata.ValidateRunCreationReceiptForCommand(got.Record, store.got); validationErr != nil {
				t.Fatalf("mock returned an invalid canonical receipt: %v", validationErr)
			}
			if (err == nil) != (wantErr == nil) || (err != nil && err.Error() != wantErr.Error()) || got.Record.Summary.RunID != command.RunID || calls != test.wantCalls || store.calls != 1 {
				t.Fatalf("commit evidence: result=%+v err=%v finalizations=%d commits=%d", got, err, calls, store.calls)
			}
		})
	}
}

func TestDeploymentRunStartForwardsOnlyExactEventlessFeed(t *testing.T) {
	store := &deploymentRunStartProbeStore{acknowledged: true}
	source, _ := acknowledgedRootInputEndpoint(t)
	bundle, _ := semanticview.Bundle(source)
	bundle.Semantics.Version = "1"
	planner := newTestFlowInstanceActivationOwner(nil)
	bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TemplateInstancePlanner: planner})
	if err != nil {
		t.Fatal(err)
	}
	bus.routeTable, err = DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	ref, err := durabledata.ParseDeclarationRef(".", "records.loaded")
	if err != nil {
		t.Fatal(err)
	}
	command := durabledata.RunCreationCommand{
		RunID: uuid.NewString(), Actor: "operator", BundleHash: authorActivityTestSourceArtifactFact.BundleHash(),
		Data: durabledata.RunCreationDataEnvelope{Pins: []durabledata.ExplicitPin{{
			Declaration: ref, VersionID: durabledata.VersionID("resource-version-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		}}},
	}
	request := apiidempotency.Request{Method: "run.start"}
	ctx := testAuthorActivityContext(context.Background())
	got, err := bus.StartDeploymentRunAcknowledged(ctx, command, request)
	if err != nil || got.Record.Summary.RunID != command.RunID || store.calls != 1 || store.got.EventID != "" || len(store.got.InitialEvent) != 0 {
		t.Fatalf("deployment forward = %#v, %v; calls=%d command=%#v", got, err, store.calls, store.got)
	}
	if store.root.Readiness.RunID != command.RunID || store.root.Identity.InstancePath != command.RunID || store.root.CreatingInput != (runtimepipeline.FlowConstructionInput{}) {
		t.Fatalf("feed-only root was not prepared under its exact no-argument owner: %+v", store.root)
	}
	hostile := command
	hostile.BundleHash = "bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
	if _, err := bus.StartDeploymentRunAcknowledged(ctx, hostile, request); err == nil || store.calls != 1 {
		t.Fatalf("mismatched bundle reached selected store: calls=%d error=%v", store.calls, err)
	}
	hostile = command
	hostile.EventID = uuid.NewString()
	hostile.InitialEvent = json.RawMessage(`{"type":"start"}`)
	if _, err := bus.StartDeploymentRunAcknowledged(ctx, hostile, request); err == nil || store.calls != 1 {
		t.Fatalf("event-bearing run reached deployment owner: calls=%d error=%v", store.calls, err)
	}
}

func (s *publicationAcknowledgementProbeStore) CommitPublication(ctx context.Context, command PublicationCommand) (CommittedPublication, error) {
	s.commits++
	if !s.acknowledged {
		return CommittedPublication{}, s.err
	}
	committed, err := s.InMemoryEventStore.CommitPublication(ctx, command)
	if err != nil {
		return committed, err
	}
	committed.Acknowledged = true
	if s.cancel != nil {
		s.cancel()
	}
	return committed, s.err
}

func (s *publicationAcknowledgementProbeStore) CommitAPIEventPublication(ctx context.Context, command APIEventPublicationCommand) (CommittedAPIEventPublication, error) {
	s.commits++
	if s.replay {
		return CommittedAPIEventPublication{Completion: command.Completion, Replay: true}, s.err
	}
	if !s.acknowledged {
		return CommittedAPIEventPublication{}, s.err
	}
	committed, err := s.InMemoryEventStore.CommitPublication(ctx, command.Publication)
	if err != nil {
		return CommittedAPIEventPublication{}, err
	}
	if s.cancel != nil {
		s.cancel()
	}
	if s.afterAPICommit != nil {
		s.afterAPICommit()
	}
	return CommittedAPIEventPublication{Publication: committed, Completion: command.Completion, Acknowledged: true}, s.err
}

func TestAcknowledgedPublishPostCommitErrorStillStartsAsyncDispatch(t *testing.T) {
	fault := errors.New("post-commit asynchronous publication fault")
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "unacknowledged", true: "acknowledged"}[acknowledged], func(t *testing.T) {
			store := &publicationAcknowledgementProbeStore{acknowledged: acknowledged, err: fault}
			probe := &publicationAcknowledgementProbe{started: make(chan struct{}, 1)}
			bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType("task.requested"), "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
			if err := bus.PublishAcknowledged(testAuthorActivityContext(context.Background()), event); !errors.Is(err, fault) {
				t.Fatalf("PublishAcknowledged error = %v, want post-commit fault", err)
			}
			if acknowledged {
				select {
				case <-probe.started:
				case <-time.After(2 * time.Second):
					t.Fatal("acknowledged ordinary publication did not dispatch")
				}
			}
			want := int32(0)
			if acknowledged {
				want = 1
			}
			if probe.persisted.Load() != want || probe.dispatched.Load() != want {
				t.Fatalf("persisted/dispatched = %d/%d, want %d/%d", probe.persisted.Load(), probe.dispatched.Load(), want, want)
			}
		})
	}
}

func TestPublicationNilErrorCannotInventCommitAcknowledgment(t *testing.T) {
	store := &publicationAcknowledgementProbeStore{acknowledged: false}
	probe := &publicationAcknowledgementProbe{}
	bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "task.requested", "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
	if err := bus.Publish(testAuthorActivityContext(context.Background()), event); err == nil || store.commits != 1 || probe.persisted.Load() != 0 || probe.dispatched.Load() != 0 {
		t.Fatalf("nil error was promoted into publication proof: err=%v commits=%d persisted=%d dispatched=%d", err, store.commits, probe.persisted.Load(), probe.dispatched.Load())
	}
}

func TestAPIEventReplayReleaseErrorUsesReplayProofWithoutNewAcknowledgement(t *testing.T) {
	fault := errors.New("API replay authority release fault")
	store := &publicationAcknowledgementProbeStore{replay: true, err: fault}
	probe := &publicationAcknowledgementProbe{}
	source, endpoint := acknowledgedRootInputEndpoint(t)
	bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TestLifecycleProbe: probe, Durable: DurableTestDependencyProjection(store)})
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	event := eventtest.ExistingRunRootIngress(eventID, events.EventType("task.requested"), "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
	store.construction = runtimepipeline.FlowConstructionPublicationEvidence{Identity: ConstructedFlowInstanceIdentityFixture(source, ".", event.RunID(), event.RunID())}
	store.observations = []runtimepipeline.FlowInstanceObservation{constructionIndexObservation(t, source, event.RunID(), store.construction.Identity, "")}
	completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
	actual, replayed, err := bus.PublishAPIEventAcknowledged(testAuthorActivityContext(context.Background()), event, &endpoint, apiidempotency.Request{Method: "event.publish"}, completion)
	if !errors.Is(err, fault) || !replayed || actual.ResourceID != eventID || probe.dispatched.Load() != 0 {
		t.Fatalf("replay completion=%#v replayed=%t error=%v dispatches=%d", actual, replayed, err, probe.dispatched.Load())
	}
}

type flowActivationAcknowledgementProbeStore struct {
	InMemoryEventStore
	acknowledged bool
	fault        error
	commits      int
}

func (s *flowActivationAcknowledgementProbeStore) CommitFlowInstanceActivation(_ context.Context, command FlowInstanceActivationCommand) (runtimepipeline.CommittedFlowInstanceActivation, error) {
	s.commits++
	if !s.acknowledged {
		return runtimepipeline.CommittedFlowInstanceActivation{}, s.fault
	}
	return runtimepipeline.CommittedFlowInstanceActivation{Plan: command.Plan, ReadinessAttemptOrdinal: 1, Acknowledged: true}, s.fault
}

func TestFlowActivationBusHelperPreservesAcknowledgedError(t *testing.T) {
	fault := errors.New("flow activation post-commit fault")
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "unacknowledged", true: "acknowledged"}[acknowledged], func(t *testing.T) {
			source, bus := topologyOperationFixture(t)
			table, err := DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			bus.routeTable = table
			store := &flowActivationAcknowledgementProbeStore{acknowledged: acknowledged, fault: fault}
			bus.store = store
			identity := ConstructedFlowInstanceIdentityFixture(source, "workers", "alpha", busInternalTestRunID)
			readiness := runtimepipeline.DynamicFlowRuntimeReadinessPlan{
				Identity: identity, RunID: busInternalTestRunID, BundleHash: bus.sourceArtifactFact.BundleHash(),
				WorkflowVersion: source.WorkflowVersion(), ExecutionMode: "live",
			}
			plan := runtimepipeline.FlowInstanceActivationPlan{
				Identity: identity, Readiness: readiness, OccurredAt: time.Now().UTC(),
				Instance: runtimepipeline.WorkflowInstance{
					InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityType: "test_entity",
				},
			}
			if err := plan.Validate(); err != nil {
				t.Fatalf("activation fixture: %v", err)
			}
			committed, err := bus.CommitFlowInstanceActivation(context.Background(), plan)
			if !errors.Is(err, fault) || store.commits != 1 || committed.Acknowledged != acknowledged {
				t.Fatalf("activation acknowledged=%t commits=%d error=%v, want %t/1/post-commit fault", committed.Acknowledged, store.commits, err, acknowledged)
			}
		})
	}
}

func TestInMemoryPublicationCannotCommitFlowActivation(t *testing.T) {
	source, bus := topologyOperationFixture(t)
	identity := runtimeflowidentity.Derive(source, "workers", "alpha")
	plan := runtimepipeline.FlowInstanceActivationPlan{
		Identity: identity,
		Readiness: runtimepipeline.DynamicFlowRuntimeReadinessPlan{
			Identity: identity, RunID: busInternalTestRunID, BundleHash: bus.sourceArtifactFact.BundleHash(),
			WorkflowVersion: source.WorkflowVersion(), ExecutionMode: "live",
		},
		OccurredAt: time.Now().UTC(),
		Instance: runtimepipeline.WorkflowInstance{
			InstanceID: identity.InstanceID, StorageRef: identity.InstancePath, EntityType: "test_entity",
		},
	}
	event := eventtest.RunCreatingRootIngress(uuid.NewString(), "work.started", "gateway", "", json.RawMessage(`{}`), 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := events.NewConnectEvaluationLedger(nil)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := events.NewNoDeliverySettlement(events.EventWriteNormalPublication, events.NoDeliveryDeclaredConsumerNoPlan, ledger)
	if err != nil {
		t.Fatal(err)
	}
	command := PublicationCommand{Commit: CommitPublishRequest{Event: admitted, RouteSettlement: settlement}, Activations: []runtimepipeline.FlowInstanceActivationPlan{plan}}
	if err := command.Validate(); err != nil {
		t.Fatalf("valid activation publication fixture: %v", err)
	}
	committed, err := (InMemoryEventStore{}).CommitPublication(context.Background(), command)
	if err == nil || !strings.Contains(err.Error(), "flow instance activation requires a durable selected store") || len(committed.Activations) != 0 {
		t.Fatalf("in-memory activation committed %#v with error %v", committed, err)
	}
}

func (*publicationAcknowledgementProbeStore) LookupAPIEventPublication(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	return apiidempotency.Completion{}, false, nil
}

type publicationAcknowledgementProbe struct {
	persisted  atomic.Int32
	dispatched atomic.Int32
	started    chan struct{}
	release    <-chan struct{}
	process    atomic.Pointer[worklifetime.Process]
}

func (p *publicationAcknowledgementProbe) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if process, ok := worklifetime.ProcessFromContext(ctx); ok {
		p.process.Store(process)
	}
	switch signal.Kind {
	case lifecycleprobe.EventPersisted:
		p.persisted.Add(1)
	case lifecycleprobe.PostCommitDispatchStarted:
		p.dispatched.Add(1)
		if p.started != nil {
			select {
			case p.started <- struct{}{}:
			default:
			}
		}
		if p.release != nil {
			select {
			case <-p.release:
			case <-ctx.Done():
			}
		}
	}
}

func TestPublicationPostCommitErrorStillDispatchesAcknowledgedResult(t *testing.T) {
	fault := errors.New("post-commit publication fault")
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "unacknowledged", true: "acknowledged"}[acknowledged], func(t *testing.T) {
			store := &publicationAcknowledgementProbeStore{acknowledged: acknowledged, err: fault}
			probe := &publicationAcknowledgementProbe{}
			bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
			if err != nil {
				t.Fatal(err)
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType("task.requested"), "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
			err = bus.Publish(testAuthorActivityContext(context.Background()), event)
			if !errors.Is(err, fault) {
				t.Fatalf("Publish error = %v, want post-commit fault", err)
			}
			want := 0
			if acknowledged {
				want = 1
			}
			if store.commits != 1 || probe.persisted.Load() != int32(want) || probe.dispatched.Load() != int32(want) {
				t.Fatalf("commits/persisted/dispatched = %d/%d/%d, want 1/%d/%d", store.commits, probe.persisted.Load(), probe.dispatched.Load(), want, want)
			}
		})
	}
}

func TestPublicationAcknowledgedErrorDispatchesAfterRequestCancellation(t *testing.T) {
	fault := errors.New("post-commit cleanup after cancellation")
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancel()
	store := &publicationAcknowledgementProbeStore{acknowledged: true, err: fault, cancel: cancel}
	probe := &publicationAcknowledgementProbe{}
	bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), events.EventType("task.requested"), "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
	if err := bus.Publish(ctx, event); !errors.Is(err, fault) {
		t.Fatalf("Publish error = %v, want post-commit fault", err)
	}
	if probe.persisted.Load() != 1 || probe.dispatched.Load() != 1 {
		t.Fatalf("persisted/dispatched = %d/%d, want 1/1", probe.persisted.Load(), probe.dispatched.Load())
	}
}

func TestAPIEventPostCommitErrorRetainsCompletionAndDispatchesAcknowledgedResult(t *testing.T) {
	fault := errors.New("post-commit API publication fault")
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "unacknowledged", true: "acknowledged"}[acknowledged], func(t *testing.T) {
			store := &publicationAcknowledgementProbeStore{acknowledged: acknowledged, err: fault}
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			probe := &publicationAcknowledgementProbe{started: make(chan struct{}, 1), release: release}
			source, endpoint := acknowledgedRootInputEndpoint(t)
			bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, TestLifecycleProbe: probe, Durable: DurableTestDependencyProjection(store)})
			if err != nil {
				t.Fatal(err)
			}
			eventID := uuid.NewString()
			event := eventtest.ExistingRunRootIngress(eventID, events.EventType("task.requested"), "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
			store.construction = runtimepipeline.FlowConstructionPublicationEvidence{Identity: ConstructedFlowInstanceIdentityFixture(source, ".", event.RunID(), event.RunID())}
			store.observations = []runtimepipeline.FlowInstanceObservation{constructionIndexObservation(t, source, event.RunID(), store.construction.Identity, "")}
			completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
			actual, replay, err := bus.PublishAPIEventAcknowledged(testAuthorActivityContext(context.Background()), event, &endpoint, apiidempotency.Request{Method: "event.publish"}, completion)
			if !errors.Is(err, fault) || replay {
				t.Fatalf("API publication replay=%t error=%v, want post-commit fault", replay, err)
			}
			want := 0
			if acknowledged {
				want = 1
				if actual.ResourceID != eventID {
					t.Fatalf("completion = %#v, want %s", actual, eventID)
				}
				select {
				case <-probe.started:
				case <-time.After(2 * time.Second):
					t.Fatal("acknowledged API publication did not dispatch")
				}
				process := probe.process.Load()
				if process == nil || process.PublicCompletionCount() != 1 {
					t.Fatal("post-ACK error or worker spawn released the held responsibility")
				}
			} else if actual.ResourceID != "" {
				t.Fatalf("unacknowledged completion = %#v", actual)
			}
			if store.commits != 1 || probe.persisted.Load() != int32(want) || probe.dispatched.Load() != int32(want) {
				t.Fatalf("commits/persisted/dispatched = %d/%d/%d, want 1/%d/%d", store.commits, probe.persisted.Load(), probe.dispatched.Load(), want, want)
			}
			close(release)
			if err := bus.workOwner.WaitForQuiescence(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := bus.workOwner.(*worklifetime.RuntimeOccurrence).RetireAndWait(t.Context()); err != nil {
				t.Fatal(err)
			}
			if process := probe.process.Load(); process != nil {
				if _, err := process.Join(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if process := probe.process.Load(); process != nil && process.PublicCompletionCount() != 0 {
				t.Fatal("terminal disposition retained the public reservation")
			}
		})
	}
}

func TestPublicCompletionExactOccurrenceSurvivesRuntimeFence(t *testing.T) {
	for _, variant := range []string{"runtime", "standing", "selected", "manager-standing"} {
		t.Run(variant, func(t *testing.T) {
			process := worklifetime.NewProcess()
			source, endpoint := acknowledgedRootInputEndpoint(t)
			store := &publicationAcknowledgementProbeStore{acknowledged: true}
			probe := &publicationAcknowledgementProbe{started: make(chan struct{}, 1)}
			runtime, err := process.NewRuntime(t.Context(), worklifetime.RuntimeIdentity{RuntimeInstanceID: authorActivityTestRuntimeInstanceID, BundleHash: authorActivityTestSourceArtifactFact.BundleHash()})
			if err != nil {
				t.Fatal(err)
			}
			bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source, WorkOwner: runtime, TestLifecycleProbe: probe, Durable: DurableTestDependencyProjection(store)})
			if err != nil {
				t.Fatal(err)
			}
			var owner worklifetime.Occurrence = runtime
			var retireChild func()
			switch variant {
			case "standing", "manager-standing":
				standing, err := runtime.NewStanding(t.Context(), worklifetime.StandingIdentity{ServiceID: "service", RunID: "standing-run", Generation: 1})
				if err != nil {
					t.Fatal(err)
				}
				owner = standing
				retireChild = func() { _ = standing.RetireAndWait(context.Background()) }
				if variant == "manager-standing" {
					manager, err := worklifetime.NewManagerRunOccurrence(t.Context(), runtime, worklifetime.ManagerRunIdentity{Generation: 1})
					if err != nil {
						t.Fatal(err)
					}
					seed, err := manager.Begin(t.Context(), standing)
					if err != nil {
						t.Fatal(err)
					}
					owner, _ = worklifetime.OccurrenceFromContext(seed.Context())
					if err := seed.Done(); err != nil {
						t.Fatal(err)
					}
					retireChild = func() {
						_ = manager.RetireAndWait(context.Background())
						_ = standing.RetireAndWait(context.Background())
					}
				}
			case "selected":
				selected, err := process.NewSelectedFork(t.Context(), worklifetime.SelectedForkIdentity{ExecutionID: "execution", RunID: "selected-run", Generation: 1})
				if err != nil {
					t.Fatal(err)
				}
				owner = selected
				retireChild = func() { _ = selected.RetireAndWait(context.Background()) }
			}
			t.Cleanup(func() {
				if retireChild != nil {
					retireChild()
				}
				_, _ = runtime.RetireAndWait(context.Background())
				_, _ = process.Join(context.Background())
			})
			store.afterAPICommit = func() {
				if err := runtime.Fence(); err != nil {
					t.Error(err)
				}
			}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "task.requested", "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
			store.construction = runtimepipeline.FlowConstructionPublicationEvidence{Identity: ConstructedFlowInstanceIdentityFixture(source, ".", event.RunID(), event.RunID())}
			store.observations = []runtimepipeline.FlowInstanceObservation{constructionIndexObservation(t, source, event.RunID(), store.construction.Identity, "")}
			ctx := worklifetime.WithOccurrence(testAuthorActivityContext(t.Context()), owner)
			if _, replay, err := bus.PublishAPIEventAcknowledged(ctx, event, &endpoint, apiidempotency.Request{Method: "event.publish"}, apiidempotency.Completion{ResourceID: event.ID(), Response: json.RawMessage(`{"ok":true}`)}); err != nil || replay {
				t.Fatalf("acknowledged exact occurrence: replay=%t err=%v", replay, err)
			}
			select {
			case <-probe.started:
			case <-time.After(2 * time.Second):
				t.Fatal("post-ACK runtime re-admission lost accepted work")
			}
			if err := owner.WaitForQuiescence(t.Context()); err != nil {
				t.Fatal(err)
			}
			if retireChild != nil {
				retireChild()
			}
			if _, err := runtime.RetireAndWait(t.Context()); err != nil {
				t.Fatal(err)
			}
			if _, err := process.Join(t.Context()); err != nil {
				t.Fatal(err)
			}
			if process.PublicCompletionCount() != 0 {
				t.Fatal("exact occurrence retained its slot after disposition")
			}
		})
	}
}
