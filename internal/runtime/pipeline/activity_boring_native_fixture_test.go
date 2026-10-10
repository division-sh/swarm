package pipeline

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/engine"
)

type nativeActivityBoringFixtureForTest struct {
	native *PipelineDeliveryNativeFixtureForTest
	pc     *PipelineCoordinator
	bus    *nativeActivityBoringBusForTest
	ctx    context.Context
}

type nativeActivityBoringBusForTest struct {
	*nativePipelineDeliveryBusObservationForTest
	fixture                     *PipelineDeliveryNativeFixtureForTest
	handleActivityRequests      bool
	beforeActivityRequestHandle func(context.Context, events.Event) error
}

func (b *nativeActivityBoringBusForTest) EngineDispatcher() engine.PostCommitDispatcher { return b }

func (b *nativeActivityBoringBusForTest) DispatchPostCommit(ctx context.Context, intents []engine.EmitIntent) error {
	for _, intent := range intents {
		if intent.Event.Type() != activityRequestEventType {
			continue
		}
		request, err := b.fixture.PublishedEvent(ctx, intent.Event.ID())
		if err != nil {
			return err
		}
		if b.beforeActivityRequestHandle != nil {
			if err := b.beforeActivityRequestHandle(ctx, request); err != nil {
				return err
			}
		}
		if !b.handleActivityRequests {
			// A named pre-dispatch crash cut still invokes the original dispatcher
			// with cancelled admission, so it releases its own staged work.
			cancelled, cancel := context.WithCancel(ctx)
			cancel()
			return b.nativePipelineDeliveryBusObservationForTest.DispatchPostCommit(cancelled, intents)
		}
	}
	return b.nativePipelineDeliveryBusObservationForTest.DispatchPostCommit(ctx, intents)
}

func nativeActivityBoringFixture(t *testing.T, backend, server string, handle bool, open pipelineDeliveryNativeOpenerForTest) nativeActivityBoringFixtureForTest {
	t.Helper()
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, activityBoringFullFlowBundle(t, server), open)
	result := nativeActivityBoringFixtureForTest{native: fixture, pc: pc, ctx: ctx}
	result.installBus(t, handle)
	return result
}

func (f *nativeActivityBoringFixtureForTest) installBus(t *testing.T, handle bool) {
	f.bus = &nativeActivityBoringBusForTest{nativePipelineDeliveryBusObservationForTest: observeNativePipelineDeliveryBusForTest(t, f.pc), fixture: f.native, handleActivityRequests: handle}
	f.pc.bus = f.bus
}

func (f nativeActivityBoringFixtureForTest) reopen(t *testing.T, handle bool) nativeActivityBoringFixtureForTest {
	t.Helper()
	next := f.native.ReopenExecution()
	module := f.pc.module
	pc := next.NewCoordinator(PipelineCoordinatorOptions{Module: module})
	result := nativeActivityBoringFixtureForTest{native: next, pc: pc, ctx: correlation.WithRunID(next.Context, correlation.RunIDFromContext(f.ctx))}
	result.installBus(t, handle)
	return result
}

func seedNativeActivityBoringSourceFlowForTest(t *testing.T, fixture nativeActivityBoringFixtureForTest, event events.Event) context.Context {
	t.Helper()
	ctx := correlation.WithRunID(fixture.ctx, event.RunID())
	if err := fixture.native.RequireRun(ctx, event.RunID()); err != nil {
		t.Fatal(err)
	}
	instance := materializedWorkflowInstanceForSource(t, fixture.pc.SemanticSource(), ctx, WorkflowInstance{InstanceID: event.EntityID(), StorageRef: event.TargetRoute().FlowInstance, EntityID: event.EntityID(), WorkflowName: "research", WorkflowVersion: fixture.pc.SemanticSource().WorkflowVersion(), CurrentState: "pending", EnteredStageAt: event.CreatedAt(), CreatedAt: event.CreatedAt(), Fields: map[string]any{"marker": event.EntityID()}, EntityType: "test_entity"})
	if err := fixture.native.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	route := activityBoringNodeRoute(event, "scanner")
	if err := fixture.native.PublishNode(ctx, event, route); err != nil {
		t.Fatal(err)
	}
	return withWorkflowNodeDeliveryRoute(ctx, route)
}

func nativeActivityBoringSourceEventForTest(entity, run, url string) events.Event {
	event := newActivityBoringSourceEvent(entity, run, url)
	target := events.RouteIdentity{FlowID: "research", FlowInstance: "research/" + entity, EntityID: entity}
	return eventtest.ExistingRunRootIngressWithRoutingSource(event.ID(), "research/source.requested", "source", event.TaskID(), event.Payload(), event.ChainDepth(), run, events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), testWorkflowRoutingSource(target.FlowID, target.FlowInstance, target.EntityID), event.CreatedAt())
}

func assertNativeActivityBoringEventCountForTest(t *testing.T, fixture nativeActivityBoringFixtureForTest, event string, want int) {
	t.Helper()
	if got := fixture.native.EventIDCount(fixture.ctx, event); got != want {
		t.Fatalf("native activity event %s count=%d, want %d", event, got, want)
	}
}

func assertNativeActivityBoringDeliveryForTest(t *testing.T, fixture nativeActivityBoringFixtureForTest, event events.Event) {
	t.Helper()
	snapshot, err := fixture.native.NodeDeliverySnapshot(fixture.ctx, event.RunID(), event.ID(), mustActivityBoringNode("scanner").Key())
	if err != nil || string(snapshot.Status) != "delivered" || snapshot.ClaimVersion != 1 {
		t.Fatalf("native source delivery settlement=%+v/%v", snapshot, err)
	}
}
