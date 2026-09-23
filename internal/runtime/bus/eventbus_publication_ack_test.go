package bus

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
)

type publicationAcknowledgementProbeStore struct {
	InMemoryEventStore
	acknowledged bool
	replay       bool
	err          error
	cancel       context.CancelFunc
	commits      int
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

func TestAPIEventReplayReleaseErrorUsesReplayProofWithoutNewAcknowledgement(t *testing.T) {
	fault := errors.New("API replay authority release fault")
	store := &publicationAcknowledgementProbeStore{replay: true, err: fault}
	probe := &publicationAcknowledgementProbe{}
	bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
	if err != nil {
		t.Fatal(err)
	}
	eventID := uuid.NewString()
	event := eventtest.ExistingRunRootIngress(eventID, events.EventType("task.requested"), "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
	completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
	actual, replayed, err := bus.PublishAPIEventAcknowledged(testAuthorActivityContext(context.Background()), event, nil, apiidempotency.Request{Method: "event.publish"}, completion)
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
	return runtimepipeline.CommittedFlowInstanceActivation{Plan: command.Plan, Acknowledged: true}, s.fault
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
			bus.durable.ActiveFlows = &topologyOperationDescriptors{}
			store := &flowActivationAcknowledgementProbeStore{acknowledged: acknowledged, fault: fault}
			bus.store = store
			identity := runtimeflowidentity.Derive(source, "workers", "alpha")
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

func (*publicationAcknowledgementProbeStore) LookupAPIEventPublication(context.Context, apiidempotency.Request) (apiidempotency.Completion, bool, error) {
	return apiidempotency.Completion{}, false, nil
}

type publicationAcknowledgementProbe struct {
	persisted  atomic.Int32
	dispatched atomic.Int32
	started    chan struct{}
}

func (p *publicationAcknowledgementProbe) NotifyLifecycle(_ context.Context, signal lifecycleprobe.Signal) {
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
			probe := &publicationAcknowledgementProbe{started: make(chan struct{}, 1)}
			bus, err := newScopedTestEventBus(store, EventBusOptions{TestLifecycleProbe: probe})
			if err != nil {
				t.Fatal(err)
			}
			eventID := uuid.NewString()
			event := eventtest.ExistingRunRootIngress(eventID, events.EventType("task.requested"), "provider", "", json.RawMessage(`{}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
			completion := apiidempotency.Completion{ResourceID: eventID, Response: json.RawMessage(`{"event_id":"` + eventID + `"}`)}
			actual, replay, err := bus.PublishAPIEventAcknowledged(testAuthorActivityContext(context.Background()), event, nil, apiidempotency.Request{Method: "event.publish"}, completion)
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
			} else if actual.ResourceID != "" {
				t.Fatalf("unacknowledged completion = %#v", actual)
			}
			if store.commits != 1 || probe.persisted.Load() != int32(want) || probe.dispatched.Load() != int32(want) {
				t.Fatalf("commits/persisted/dispatched = %d/%d/%d, want 1/%d/%d", store.commits, probe.persisted.Load(), probe.dispatched.Load(), want, want)
			}
		})
	}
}
