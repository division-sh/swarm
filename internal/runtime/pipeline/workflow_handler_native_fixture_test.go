package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type WorkflowHandlerNativeFixtureForTest struct {
	Runs interface {
		runtimerunlifecycle.OperationOwner
		runtimerunlifecycle.CandidateStore
		LoadRunOrigin(context.Context, string) (runtimerunlifecycle.RunOrigin, error)
	}
	Persistence           WorkflowPersistence
	FreshProjection       func() WorkflowPersistence
	Context               context.Context
	RequireRun            func(context.Context, string) error
	Construct             func(context.Context, WorkflowInstance) error
	NewCoordinator        func(PipelineCoordinatorOptions) *PipelineCoordinator
	Publish               func(context.Context, events.Event, events.DeliveryRoute)
	PublishDirect         func(context.Context, events.Event)
	PublishedEvent        func(context.Context, string) (events.Event, error)
	PhysicalCounts        func(context.Context) (WorkflowEnginePhysicalCountsForTest, error)
	ConflictingEntityType func(context.Context, string, string) (int64, error)
	MissingHeader         func(context.Context, string, string) (int64, error)
	MissingFields         func(context.Context, string, string) (int64, error)
	Draining              func(context.Context, string, string) (int64, error)
	Terminated            func(context.Context, string, string, time.Time) (int64, error)
	ApplicationStorage    func(context.Context) (json.RawMessage, error)
}

// Observe the entire execution window while retaining the original planner and dispatcher.
type nativeHandlerDispatchObservationForTest struct {
	Bus
	EngineMutationPublicationPlanner
	publishes []events.Event
}

func (b *nativeHandlerDispatchObservationForTest) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return b
}

func (b *nativeHandlerDispatchObservationForTest) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if err := b.Bus.EngineDispatcher().DispatchPostCommit(ctx, intents); err != nil {
		return err
	}
	for _, intent := range intents {
		b.publishes = append(b.publishes, cloneEvent(intent.Event))
	}
	return nil
}

func (b *nativeHandlerDispatchObservationForTest) publishedCount() int { return len(b.publishes) }
func (b *nativeHandlerDispatchObservationForTest) publishedEvent(index int) events.Event {
	return b.publishes[index]
}

type nativeDispatchProbeForTest struct {
	Bus
	calls int
	err   error
}

func (p *nativeDispatchProbeForTest) EngineDispatcher() runtimeengine.PostCommitDispatcher { return p }
func (p *nativeDispatchProbeForTest) DispatchPostCommit(context.Context, []runtimeengine.EmitIntent) error {
	p.calls++
	return p.err
}

func TestNativeHandlerDispatchObservationDoesNotInventSuccessfulHandoff(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			var refusal error
			if failed {
				refusal = errors.New("native dispatcher refused handoff")
			}
			probe := &nativeDispatchProbeForTest{err: refusal}
			observer := &nativeHandlerDispatchObservationForTest{Bus: probe}
			event := eventtest.ExistingRunRootIngress(uuid.NewString(), "custom.emitted", "", "", []byte(`{"label":"done"}`), 0, uuid.NewString(), events.EventEnvelope{}, time.Now().UTC())
			err := observer.DispatchPostCommit(context.Background(), []runtimeengine.EmitIntent{{Event: event}})
			if !errors.Is(err, refusal) || probe.calls != 1 {
				t.Fatalf("real dispatch not preserved: calls=%d err=%v", probe.calls, err)
			}
			if failed {
				if observer.publishedCount() != 0 {
					t.Fatal("failed native handoff became successful dispatch evidence")
				}
			} else if observer.publishedCount() != 1 || !reflect.DeepEqual(observer.publishedEvent(0), event) {
				t.Fatal("observer changed the successful native handoff")
			}
		})
	}
}

func nativeHandlerEngineExistingEntityForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest, entityID string) (WorkflowHandlerNativeFixtureForTest, *PipelineCoordinator, context.Context, *nativeHandlerDispatchObservationForTest) {
	t.Helper()
	return nativeHandlerEngineExistingEntityWithModuleForTest(t, open, handlerEngineProjectNodeModule(t), entityID, nil)
}

func nativeHandlerEngineExistingEntityWithModuleForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest, module WorkflowModule, entityID string, fields map[string]any) (WorkflowHandlerNativeFixtureForTest, *PipelineCoordinator, context.Context, *nativeHandlerDispatchObservationForTest) {
	t.Helper()
	fixture := open(t, module.SemanticSource())
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: module})
	ctx := correlation.WithRunID(fixture.Context, entityID)
	if err := fixture.RequireRun(ctx, entityID); err != nil {
		t.Fatal(err)
	}
	run := correlation.RunIDFromContext(ctx)
	if err := fixture.Construct(ctx, materializedWorkflowInstanceForTest(WorkflowInstance{
		InstanceID: run, StorageRef: run, EntityID: entityID, WorkflowName: ".",
		WorkflowVersion: pc.SemanticSource().WorkflowVersion(), CurrentState: "queued",
		Fields: cloneMap(fields), EntityType: "test_entity",
	})); err != nil {
		t.Fatal(err)
	}
	planner, ok := pc.bus.(EngineMutationPublicationPlanner)
	if !ok {
		t.Fatal("native handler observation requires the original mutation publication planner")
	}
	observer := &nativeHandlerDispatchObservationForTest{Bus: pc.bus, EngineMutationPublicationPlanner: planner}
	pc.bus = observer
	return fixture, pc, ctx, observer
}

func executeNativeHandlerEngineWithHandoffForTest(t *testing.T, fixture WorkflowHandlerNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, observer *nativeHandlerDispatchObservationForTest, node identity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler, trigger workflowTriggerContext, deferFollowUp ...bool) (contractHandlerExecutionResult, error) {
	t.Helper()
	run := correlation.RunIDFromContext(ctx)
	instance, found, readErr := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, run))
	if readErr != nil || !found || instance.CurrentState != string(trigger.State.Stage) || !reflect.DeepEqual(cloneMap(instance.Fields), cloneMap(trigger.State.Metadata)) {
		t.Fatalf("native handler pre-state changed original metadata/stage: found=%t err=%v got=%+v want=%+v", found, readErr, instance, trigger.State)
	}
	target := events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: trigger.Event.EntityID()}
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), trigger.Event.Type(), trigger.Event.SourceAgent(), trigger.Event.TaskID(), trigger.Event.Payload(), trigger.Event.ChainDepth(), run,
		handlerTestWorkflowEnvelope(".", run, trigger.Event.EntityID()), testWorkflowRoutingSource(".", run, trigger.Event.EntityID()), trigger.Event.CreatedAt())
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	event = admitted.Event()
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(target)}
	fixture.Publish(ctx, event, route)
	trigger.Event = eventtest.TargetRouted(event, target)
	claimCtx, stop := claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, trigger.Event, route)
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	result, err := pc.executeNodeContractHandler(claimCtx, node, handler, trigger, false, deferFollowUp...)
	for _, intent := range result.FollowUp.Emissions {
		persisted, readErr := fixture.PublishedEvent(ctx, intent.Event.ID())
		if readErr != nil || !reflect.DeepEqual(persisted, intent.Event) {
			t.Fatalf("committed handler emission differs from original selected receipt: err=%v got=%+v want=%+v", readErr, persisted, intent.Event)
		}
	}
	if len(deferFollowUp) == 0 && result.Committed {
		err = errors.Join(err, dispatchNativeHandlerFollowUpForTest(pc, ctx, observer, result.FollowUp))
	}
	return result, err
}

func dispatchNativeHandlerFollowUpForTest(pc *PipelineCoordinator, ctx context.Context, observer *nativeHandlerDispatchObservationForTest, followUp handlerCommittedFollowUp) error {
	if pc.bus != observer {
		return fmt.Errorf("handler dispatch observation must cover the entire execution window")
	}
	return pc.transferCommittedHandlerFollowUp(ctx, followUp, nil)
}

type WorkflowEnginePhysicalCountsForTest struct {
	EntityStates, ConstructedHeaders, MutationJournal int64
}

func declarativeEmitContractSourceForTest(t *testing.T, emittedEvents string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: test\nstages:\n  queued: {}\n",
		"entities.yaml": "test_entity:\n  legacy_entity: text?\n",
		"nodes.yaml":    "node-a:\n  execution_type: system_node\n",
		"events.yaml":   emittedEvents + "custom.trigger:\n  reason: text\n  score: float\nbatch.submitted:\n  items: '[json]?'\n",
	})
}

func TestNativeDeclarativeEmissionSourcePreservesBusinessCatalogAndHostileField(t *testing.T) {
	for _, tc := range []struct {
		name, document string
		properties     map[string]runtimecontracts.EventFieldSpec
		required       []string
	}{
		{"custom.emitted", "custom.emitted:\n  label: text\n", map[string]runtimecontracts.EventFieldSpec{"label": {Type: "text"}}, []string{"label"}},
		{"guard.failed", "guard.failed:\n", nil, nil},
		{"guard.failed", "guard.failed:\n  score: float\n  reason: text\n", map[string]runtimecontracts.EventFieldSpec{"score": {Type: "float"}, "reason": {Type: "text"}}, []string{"reason", "score"}},
	} {
		t.Run(tc.document, func(t *testing.T) {
			bundle := declarativeEmitContractSourceForTest(t, tc.document)
			entry := bundle.Events[tc.name]
			required := append([]string(nil), entry.Payload.Required...)
			sort.Strings(required)
			if len(entry.Payload.Properties) != len(tc.properties) || (len(tc.properties) > 0 && !reflect.DeepEqual(entry.Payload.Properties, tc.properties)) || !reflect.DeepEqual(required, tc.required) {
				t.Fatalf("authored emission catalog changed: got=%+v want=%+v/%v", entry, tc.properties, tc.required)
			}
			input := bundle.Events["custom.trigger"]
			if len(input.Payload.Properties) != 2 || input.Payload.Properties["score"].Type != "float" || input.Payload.Properties["reason"].Type != "text" || len(input.Payload.Required) != 2 {
				t.Fatal("original trigger business contract changed")
			}
			batch := bundle.Events["batch.submitted"]
			if len(batch.Payload.Properties) != 1 || batch.Payload.Properties["items"].Type != "[json]" || len(batch.Payload.Required) != 0 {
				t.Fatal("original optional batch catalog changed")
			}
			if bundle.SourceArtifact == nil || !reflect.DeepEqual(bundle.Events, bundle.FlowTree.Root.Events) || bundle.RootEntities["test_entity"].Fields["legacy_entity"].Type != "text" {
				t.Fatal("source does not own complete catalog and hostile persisted field")
			}
		})
	}
}

func nativeWorkflowEnginePhysicalCountsForTest(t *testing.T, fixture WorkflowHandlerNativeFixtureForTest, ctx context.Context) map[string]int64 {
	t.Helper()
	counts, err := fixture.PhysicalCounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return map[string]int64{"entity_state": counts.EntityStates, "flow_instances": counts.ConstructedHeaders, "entity_mutations": counts.MutationJournal}
}

func nativeWorkflowHandlerRunContextForTest(t *testing.T, fixture WorkflowHandlerNativeFixtureForTest) context.Context {
	t.Helper()
	runID := uuid.NewString()
	ctx := correlation.WithRunID(fixture.Context, runID)
	if err := fixture.RequireRun(ctx, runID); err != nil {
		t.Fatal(err)
	}
	return ctx
}

func claimNativeWorkflowHandlerPublicationForTest(t *testing.T, pc *PipelineCoordinator, ctx context.Context, event events.Event, route events.DeliveryRoute) (context.Context, func() error) {
	t.Helper()
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := pc.deliveryStore.Snapshot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	result, err := pc.deliveryStore.ClaimDelivery(ctx, snapshot.Authority, event, route)
	if err != nil {
		t.Fatal(err)
	}
	claimed, ok := result.Acquired()
	if !ok {
		t.Fatalf("native handler claim = %s, want acquired", result.Disposition)
	}
	if err := claimed.Claim.Validate(); err != nil {
		t.Fatal(err)
	}
	if claimed.Snapshot.EventID != event.ID() || claimed.Snapshot.Route.Target != route.Target || claimed.Snapshot.Route.Recipient != route.Recipient || !claimed.Snapshot.Route.Context.Empty() {
		t.Fatal("native handler claim differs from published event/route")
	}
	ctx = deliverylifecycle.WithClaim(ctx, claimed.Claim)
	heartbeat, err := deliverylifecycle.StartClaimHeartbeatFromClaim(ctx, pc.workOwner, pc.deliveryStore, claimed.Claim, result.Renewal)
	if err != nil {
		t.Fatal(err)
	}
	return withWorkflowNodeDeliveryRoute(heartbeat.Context(), route), heartbeat.Stop
}

func executeNativeWorkflowScenarioHandlerForTest(t *testing.T, fixture WorkflowHandlerNativeFixtureForTest, pc *PipelineCoordinator, ctx context.Context, node identity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler, event events.Event) (contractHandlerExecutionResult, error) {
	t.Helper()
	route, found := deliverylifecycle.RouteFromContext(ctx)
	if !found {
		t.Fatal("native scenario handler requires its exact receiver route")
	}
	admitted, err := events.AdmitForPublish(event, events.AdmissionOptions{Now: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	event = admitted.Event()
	fixture.Publish(ctx, event, route)
	event = eventtest.TargetRouted(event, route.Target.Route())
	ctx, stop := claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, event, route)
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("native scenario claim cleanup: %v", err)
		}
	}()
	return pc.executeNodeContractHandler(ctx, node, handler, workflowTriggerContext{Event: event}, false)
}
