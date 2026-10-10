package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type nativeExactWorkflowJoinHarness struct {
	t                      *testing.T
	fixture                *PipelineDeliveryNativeFixtureForTest
	pc                     *PipelineCoordinator
	store                  *workflowInstanceStore
	ctx                    context.Context
	bundle                 *runtimecontracts.WorkflowContractBundle
	source                 exactWorkflowJoinSource
	schedules              *recordingGenericScheduleWakeupOwner
	mutations              *nativePipelineLifecycleMutationObservationForTest
	bus                    *nativePipelineDeliveryBusObservationForTest
	flowID, path, entityID string
	route                  flowidentity.Route
}

func nativeWorkflowJoinLoopBundleForTest(t *testing.T, mode string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	files := workflowJoinLifecycleFixtureFiles(false, mode)
	for _, prefix := range []string{"", "orders/"} {
		files[prefix+"events.yaml"] = strings.Replace(files[prefix+"events.yaml"], "manual.abort:\n", "manual.abort:\n  revision_id: text\n", 1)
		files[prefix+"nodes.yaml"] = strings.Replace(files[prefix+"nodes.yaml"], "    manual.abort:\n      advances_to: dispatching", "    manual.abort:\n      loop: {admit: revision, from: awaiting}\n      advances_to: dispatching", 1)
	}
	return loadWorkflowTempBundle(t, files)
}

func newNativeExactWorkflowJoinHarness(t *testing.T, backend, flowID, initial string, members []any, bundle *runtimecontracts.WorkflowContractBundle, open pipelineDeliveryNativeOpenerForTest) *nativeExactWorkflowJoinHarness {
	t.Helper()
	if bundle == nil {
		bundle = workflowJoinLifecycleBundle(t)
	}
	source := exactWorkflowJoinSource{Source: semanticview.Wrap(bundle), nodeFlowID: flowID}
	for _, plan := range bundle.Semantics.Joins {
		if plan.Node.FlowPath() == pipelineDeclarationFlowPath(flowID) {
			source.plans = append(source.plans, plan)
		}
	}
	h := &nativeExactWorkflowJoinHarness{t: t, bundle: bundle, source: source, schedules: &recordingGenericScheduleWakeupOwner{}, flowID: flowID}
	h.fixture = open(t, backend, source)
	h.ctx = correlation.WithRunID(h.fixture.Context, uuid.NewString())
	if err := h.fixture.RequireRun(h.ctx, correlation.RunIDFromContext(h.ctx)); err != nil {
		t.Fatal(err)
	}
	h.installCoordinator()
	h.path = correlation.RunIDFromContext(h.ctx)
	flow := pipelineDeclarationFlowPath(flowID)
	fields := map[string]any{"expected": append([]any{}, members...)}
	if flowID != "" {
		key := uuid.NewString()
		h.path = flowID + "/" + key
		fields["instance_key"] = key
	}
	h.entityID = FlowInstanceEntityID(h.path)
	h.route = testRunScopedWorkflowInstanceFromContext(h.ctx, h.path).Route
	instance := materializedWorkflowInstanceForSource(t, source, h.ctx, WorkflowInstance{
		InstanceID: h.route.InstanceID, StorageRef: h.path, WorkflowName: flow, WorkflowVersion: bundle.WorkflowVersion(),
		EntityID: h.entityID, CurrentState: initial, EnteredStageAt: time.Now().UTC(), Fields: fields, EntityType: "test_entity",
	})
	if err := h.fixture.Construct(h.ctx, instance); err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *nativeExactWorkflowJoinHarness) installCoordinator() {
	h.t.Helper()
	nodes, err := LoadWorkflowNodes(h.source)
	if err != nil {
		h.t.Fatal(err)
	}
	guards := NewContractGuardRegistry(h.source)
	h.pc = h.fixture.NewCoordinator(PipelineCoordinatorOptions{Module: &pipelineFixtureWorkflowModule{source: h.source, workflowNodes: nodes, guardRegistry: guards}, GenericSchedules: h.schedules})
	h.store = h.pc.workflowStore
	if h.mutations == nil {
		h.mutations = &nativePipelineLifecycleMutationObservationForTest{}
	}
	h.mutations.WorkflowEngineMutationOwner = h.store.engineMutations
	h.store.engineMutations = h.mutations
	if h.bus == nil {
		h.bus = &nativePipelineDeliveryBusObservationForTest{}
	}
	h.bus.Bus = h.pc.bus
	h.bus.EngineMutationPublicationPlanner = h.pc.bus.(EngineMutationPublicationPlanner)
	h.pc.bus = h.bus
}

func (h *nativeExactWorkflowJoinHarness) restart() {
	h.t.Helper()
	h.fixture = h.fixture.ReopenExecution()
	run := correlation.RunIDFromContext(h.ctx)
	h.ctx = correlation.WithRunID(h.fixture.Context, run)
	h.installCoordinator()
}

func (h *nativeExactWorkflowJoinHarness) envelope() events.EventEnvelope {
	return events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: pipelineDeclarationFlowPath(h.flowID), FlowInstance: h.path, EntityID: h.entityID})
}

func (h *nativeExactWorkflowJoinHarness) instance() WorkflowInstance {
	h.t.Helper()
	instance, found, err := h.fixture.Persistence.LoadWorkflowInstance(h.ctx, testRunScopedWorkflowRoute(h.ctx, h.route))
	if err != nil || !found {
		h.t.Fatalf("native join projection: found=%t error=%v", found, err)
	}
	return instance
}

func (h *nativeExactWorkflowJoinHarness) armInitial() genericschedule.Activation {
	h.t.Helper()
	if err := applyTestInitialEntryEffect(h.ctx, h.pc, h.route, h.entityID); err != nil {
		h.t.Fatal(err)
	}
	return h.armedSchedule()
}

func (h *nativeExactWorkflowJoinHarness) armedSchedule() genericschedule.Activation {
	h.t.Helper()
	upserts, _ := h.mutations.schedules()
	if len(upserts) != 1 {
		h.t.Fatalf("native initial join schedules=%d, want one", len(upserts))
	}
	_, ref, found := timeridentity.ParseJoinHandle(parsePayloadMap(genericSchedulePayloadForTest(h.t, upserts[0])))
	if !found || ref.FlowPath() != pipelineDeclarationFlowPath(h.flowID) || upserts[0].Command.RoutingSource.Route().FlowID != h.flowID {
		h.t.Fatalf("native armed declaration=%#v schedule=%#v", ref, upserts[0])
	}
	return upserts[0]
}

func (h *nativeExactWorkflowJoinHarness) activation() joinruntime.Activation {
	h.t.Helper()
	carrier, err := workflowInstanceStateCarrier(h.instance())
	if err != nil {
		h.t.Fatal(err)
	}
	node := pipelineNode(h.t, h.flowID, "join-node")
	activation, found, err := joinruntime.Load(carrier.StateBuckets, node, workflowJoinActivationKey(h.t, carrier.StateBuckets, node))
	if err != nil || !found {
		h.t.Fatalf("native join activation: found=%t error=%v", found, err)
	}
	return activation
}

func (h *nativeExactWorkflowJoinHarness) transition(next, name string) {
	h.t.Helper()
	event := nativeWorkflowJoinEventForTest(h.ctx, pipelineDeclarationFlowPath(h.flowID), h.path, h.entityID, name, []byte(`{}`), time.Now().UTC())
	dispatchNativeWorkflowJoinEventForTest(h.t, h.fixture, h.pc, h.ctx, event, "dispatcher")
	if got := h.instance().CurrentState; got != next {
		h.t.Fatalf("native transition=%s, want %s", got, next)
	}
}

func (h *nativeExactWorkflowJoinHarness) scheduleEvent(schedule genericschedule.Activation, id string) events.Event {
	return workflowJoinScheduleEventForTest(h.t, id, schedule, correlation.RunIDFromContext(h.ctx), h.envelope(), time.Now().UTC())
}

func (h *nativeExactWorkflowJoinHarness) fire(event events.Event) (contractHandlerExecutionResult, error) {
	h.t.Helper()
	return executeNativeResolvedJoinForTest(h.t, h.fixture, h.pc, h.ctx, event, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(h.t, h.pc, h.ctx, h.route, h.entityID)})
}

func (h *nativeExactWorkflowJoinHarness) startLoop() loopruntime.Activation {
	h.t.Helper()
	node := pipelineNode(h.t, h.flowID, "observer")
	handler := h.source.ExecutableNodeEventHandlers(node)["loop.start"]
	event := nativeWorkflowJoinEventForTest(h.ctx, pipelineDeclarationFlowPath(h.flowID), h.path, h.entityID, "loop.start", []byte(`{}`), time.Now().UTC())
	if _, err := executeNativePublishedWorkflowJoinForTest(h.t, h.fixture, h.mutations, h.pc, h.ctx, node, handler, workflowTriggerContext{Event: event, State: mustCurrentWorkflowState(h.t, h.pc, h.ctx, h.route, h.entityID), HandlerEventKey: "loop.start"}); err != nil {
		h.t.Fatal(err)
	}
	carrier, err := workflowInstanceStateCarrier(h.instance())
	if err != nil {
		h.t.Fatal(err)
	}
	loop, found, err := loopruntime.Load(carrier.StateBuckets, pipelineDeclarationFlowPath(h.flowID), "revision")
	if err != nil || !found {
		h.t.Fatalf("native loop start: found=%t error=%v", found, err)
	}
	return loop
}

func (h *nativeExactWorkflowJoinHarness) replay(event events.Event) error {
	h.t.Helper()
	recipient, _, _, _, err := ResolveWorkflowJoinOccurrenceDeliveryTarget(h.source, event)
	if err != nil {
		return err
	}
	before, err := h.fixture.NodeDeliverySnapshot(h.ctx, event.RunID(), event.ID(), recipient.ID())
	if err != nil {
		return err
	}
	outcomes, err := h.fixture.Store.Outcomes(h.ctx, before.DeliveryID)
	if err != nil {
		return err
	}
	counts := h.fixture.Transactions()
	if err := h.fixture.PublishNode(h.ctx, event, before.Route); err != nil {
		return err
	}
	after, err := h.fixture.Store.Snapshot(h.ctx, before.DeliveryID)
	if err != nil {
		return err
	}
	afterOutcomes, err := h.fixture.Store.Outcomes(h.ctx, before.DeliveryID)
	if err != nil {
		return err
	}
	afterCounts := h.fixture.Transactions()
	if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(outcomes, afterOutcomes) || afterCounts.Claims != counts.Claims || afterCounts.Settlements != counts.Settlements {
		return fmt.Errorf("native publication replay repeated claim, settlement or changed the stored result")
	}
	return nil
}
