package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
)

type flowActivationRouteStore interface {
	UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error
	DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error
}

type flowActivationTestBus struct {
	addedPaths         []string
	addedRouteRequests []runtimebus.FlowInstanceRouteMaterializationRequest
	removedPairs       []string
	published          []events.Event
	runtimeLogs        []runtimepipeline.RuntimeLogEntry
	unsubscribed       []string
	removeErr          error
	routeStore         flowActivationRouteStore
}

type flowActivationTestRouteStore struct {
	statusByPath map[string]string
}

type flowActivationTestInstanceStore struct {
	creates          []runtimepipeline.WorkflowInstance
	upserts          []runtimepipeline.WorkflowInstance
	terminatedPaths  []string
	terminatedAtSeen []time.Time
	byStorageRef     map[string]runtimepipeline.WorkflowInstance
}

type flowActivationTestStore struct {
	upserts      []PersistedAgent
	terminated   []string
	terminateErr error
}

func newFlowActivationManager(bus Bus, instances flowInstancePersistence, stores ...ManagerPersistence) *AgentManager {
	return NewAgentManagerWithOptions(bus, nil, AgentManagerOptions{
		WorkflowInstances: instances,
	}, stores...)
}

func (s *flowActivationTestRouteStore) UpsertFlowInstanceRoute(_ context.Context, route runtimebus.FlowInstanceRouteRecord) error {
	if s.statusByPath == nil {
		s.statusByPath = map[string]string{}
	}
	s.statusByPath[strings.TrimSpace(route.Identity.InstancePath)] = "active"
	return nil
}

func (s *flowActivationTestRouteStore) DeleteFlowInstanceRoute(_ context.Context, identity runtimeflowidentity.Route) error {
	if s.statusByPath == nil {
		s.statusByPath = map[string]string{}
	}
	s.statusByPath[strings.TrimSpace(identity.InstancePath)] = "inactive"
	return nil
}

func (s *flowActivationTestInstanceStore) Upsert(_ context.Context, instance runtimepipeline.WorkflowInstance) error {
	s.upserts = append(s.upserts, instance)
	s.storeInstance(instance)
	return nil
}

func (s *flowActivationTestInstanceStore) Create(_ context.Context, instance runtimepipeline.WorkflowInstance) error {
	if s.byStorageRef == nil {
		s.byStorageRef = map[string]runtimepipeline.WorkflowInstance{}
	}
	ref := strings.TrimSpace(instance.StorageRef)
	if ref != "" {
		if _, ok := s.byStorageRef[ref]; ok {
			return fmt.Errorf("flow instance already exists: %s", ref)
		}
	}
	s.creates = append(s.creates, instance)
	s.storeInstance(instance)
	return nil
}

func (s *flowActivationTestInstanceStore) storeInstance(instance runtimepipeline.WorkflowInstance) {
	if s.byStorageRef == nil {
		s.byStorageRef = map[string]runtimepipeline.WorkflowInstance{}
	}
	stored := instance
	stored.StorageRef = strings.TrimSpace(stored.StorageRef)
	if stored.StorageRef != "" {
		stored.Status = "active"
		s.byStorageRef[stored.StorageRef] = stored
	}
}

func (s *flowActivationTestInstanceStore) MarkTerminated(_ context.Context, storageRef string, terminatedAt time.Time) error {
	s.terminatedPaths = append(s.terminatedPaths, strings.TrimSpace(storageRef))
	s.terminatedAtSeen = append(s.terminatedAtSeen, terminatedAt)
	if s.byStorageRef != nil {
		instance := s.byStorageRef[strings.TrimSpace(storageRef)]
		instance.Status = "terminated"
		instance.TerminatedAt = terminatedAt
		s.byStorageRef[strings.TrimSpace(storageRef)] = instance
	}
	return nil
}

func (s *flowActivationTestInstanceStore) Load(_ context.Context, instanceID string) (runtimepipeline.WorkflowInstance, bool, error) {
	if s.byStorageRef == nil {
		return runtimepipeline.WorkflowInstance{}, false, nil
	}
	instance, ok := s.byStorageRef[strings.TrimSpace(instanceID)]
	return instance, ok, nil
}

func (s *flowActivationTestStore) UpsertAgent(_ context.Context, rec PersistedAgent) error {
	s.upserts = append(s.upserts, rec)
	return nil
}

func (*flowActivationTestStore) LoadAgents(context.Context) ([]PersistedAgent, error) {
	return nil, nil
}
func (s *flowActivationTestStore) MarkAgentTerminated(_ context.Context, agentID string) error {
	s.terminated = append(s.terminated, strings.TrimSpace(agentID))
	return s.terminateErr
}
func (*flowActivationTestStore) EnsureEntitySchema(context.Context, string) error { return nil }
func (*flowActivationTestStore) UpsertEventReceipt(context.Context, string, string, ReceiptStatus, string) error {
	return nil
}
func (*flowActivationTestStore) ListPendingEventsForAgent(context.Context, string, time.Time, int) ([]events.Event, error) {
	return nil, nil
}
func (*flowActivationTestStore) ListPendingSubscribedEvents(context.Context, string, []events.EventType, time.Time, int) ([]events.Event, error) {
	return nil, nil
}

func (b *flowActivationTestBus) Publish(_ context.Context, evt events.Event) error {
	b.published = append(b.published, evt)
	return nil
}

func (*flowActivationTestBus) PublishDirect(context.Context, events.Event, []string) error {
	return nil
}
func (*flowActivationTestBus) PublishPersistedRecipients(context.Context, events.Event, []string) error {
	return nil
}
func (*flowActivationTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (b *flowActivationTestBus) Unsubscribe(agentID string) {
	b.unsubscribed = append(b.unsubscribed, agentID)
}
func (*flowActivationTestBus) Store() runtimebus.EventStore { return nil }
func (*flowActivationTestBus) ResetInMemoryState() error    { return nil }
func (b *flowActivationTestBus) LogRuntime(_ context.Context, entry runtimepipeline.RuntimeLogEntry) error {
	b.runtimeLogs = append(b.runtimeLogs, entry)
	return nil
}

func (b *flowActivationTestBus) AddFlowInstanceRoute(req runtimebus.FlowInstanceRouteMaterializationRequest) error {
	req = req.Normalized()
	identity := req.Identity
	b.addedPaths = append(b.addedPaths, identity.InstancePath)
	b.addedRouteRequests = append(b.addedRouteRequests, req)
	if b.routeStore != nil {
		return b.routeStore.UpsertFlowInstanceRoute(context.Background(), runtimebus.FlowInstanceRouteRecord{
			Identity:       identity,
			EventPattern:   identity.InstancePath + "/task.started",
			SubscriberType: "agent",
			SubscriberID:   "reviewer-" + identity.InstanceID,
			SourceFlow:     identity.ScopeKey,
		})
	}
	return nil
}

func (b *flowActivationTestBus) RemoveFlowInstanceRoute(identity runtimeflowidentity.Route) error {
	b.removedPairs = append(b.removedPairs, identity.ScopeKey+"/"+identity.InstanceID)
	if b.removeErr != nil {
		return b.removeErr
	}
	if b.routeStore != nil {
		return b.routeStore.DeleteFlowInstanceRoute(context.Background(), identity)
	}
	return nil
}

type flowActivationStubAgent struct{ id string }

func (a flowActivationStubAgent) ID() string                      { return a.id }
func (flowActivationStubAgent) Type() string                      { return "generic" }
func (flowActivationStubAgent) Subscriptions() []events.EventType { return nil }
func (flowActivationStubAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}

func testFlowBundle(autoEmit string) *runtimecontracts.WorkflowContractBundle {
	reviewFlow := &runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{ID: "review"},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.started": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{},
				},
			},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"reviewer": {
				ID:            "reviewer-{instance_id}",
				Type:          "generic",
				Role:          "reviewer",
				Subscriptions: []string{"task.started"},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*reviewFlow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": reviewFlow,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"review": {
				Mode: "template",
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{Events: []string{"task.started"}},
				},
				AutoEmitOnCreate: runtimecontracts.AutoEmitOnCreateContract{Event: autoEmit},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}
}

func testFlowBundleWithAutoEmitEntry(autoEmit string, entry runtimecontracts.EventCatalogEntry) *runtimecontracts.WorkflowContractBundle {
	bundle := testFlowBundle(autoEmit)
	reviewFlow := bundle.FlowTree.ByID["review"]
	if reviewFlow == nil {
		return bundle
	}
	if reviewFlow.Events == nil {
		reviewFlow.Events = map[string]runtimecontracts.EventCatalogEntry{}
	}
	reviewFlow.Events[strings.TrimSpace(autoEmit)] = entry
	return bundle
}

func testNestedFlowBundle() *runtimecontracts.WorkflowContractBundle {
	grandchild := &runtimecontracts.FlowContractView{
		Path:  "child/grandchild",
		Paths: runtimecontracts.FlowContractPaths{ID: "grandchild", Flow: "grandchild"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {
				ID:            "worker-{instance_id}",
				Type:          "generic",
				Role:          "worker",
				Subscriptions: []string{"micro.started"},
			},
		},
	}
	child := &runtimecontracts.FlowContractView{
		Path:     "child",
		Paths:    runtimecontracts.FlowContractPaths{ID: "child", Flow: "child"},
		Children: []runtimecontracts.FlowContractView{*grandchild},
	}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*child},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"child":      child,
				"grandchild": grandchild,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"grandchild": {
				Mode: "template",
				Pins: runtimecontracts.FlowPins{
					Inputs: runtimecontracts.FlowInputPins{Events: []string{"micro.started"}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{Version: "v-test"},
	}
}

func testStaticFlowBundle() *runtimecontracts.WorkflowContractBundle {
	analysisFlow := &runtimecontracts.FlowContractView{
		Path:  "analyzer-flow",
		Paths: runtimecontracts.FlowContractPaths{ID: "analyzer-flow"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"analyzer": {
				Type:          "generic",
				Role:          "analyzer",
				Subscriptions: []string{"analysis.requested"},
				EmitEvents:    []string{"analysis.done"},
			},
		},
	}
	return &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{*analysisFlow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"analyzer-flow": analysisFlow,
			},
		},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"analyzer-flow": {
				RequiredAgents: []runtimecontracts.FlowRequiredAgent{{
					Role:         "analyzer",
					SubscribesTo: []string{"analysis.requested"},
					Emits:        []string{"analysis.done"},
				}},
				Pins: runtimecontracts.FlowPins{
					Inputs:  runtimecontracts.FlowInputPins{Events: []string{"analysis.requested"}},
					Outputs: runtimecontracts.FlowOutputPins{Events: []string{"analysis.done"}},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			Version: "v-test",
			FlowAgents: map[string][]runtimecontracts.FlowRequiredAgent{
				"analyzer-flow": {{
					Role:         "analyzer",
					SubscribesTo: []string{"analysis.requested"},
					Emits:        []string{"analysis.done"},
				}},
			},
			FlowInputs: map[string][]string{
				"analyzer-flow": {"analysis.requested"},
			},
			FlowOutputs: map[string][]string{
				"analyzer-flow": {"analysis.done"},
			},
			FlowPrefix: map[string]string{
				"analyzer-flow": "analyzer-flow",
			},
		},
	}
}

func testActivationRequest(bundle *runtimecontracts.WorkflowContractBundle, templateID, instanceID, sourceEntityID, flowPath string) runtimepipeline.FlowInstanceActivationRequest {
	instance := runtimeflowidentity.Stored(
		semanticview.Wrap(bundle),
		templateID,
		flowPath,
		instanceID,
		runtimepipeline.FlowInstanceEntityID(flowPath),
		sourceEntityID,
	)
	return runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		Instance:       instance,
	}
}

func testFlowActivationTriggerEvent(eventID string) events.Event {
	return eventtest.RootIngress(strings.TrimSpace(eventID),
		events.EventType("spawn.requested"),
		"spawner", "", json.RawMessage(`{}`), 0, "", "", events.EventEnvelope{}, time.Now().UTC())

}

func decodeFlowActivationEventPayload(t *testing.T, event events.Event) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(event.Payload(), &payload); err != nil {
		t.Fatalf("decode event payload: %v", err)
	}
	if payload == nil {
		payload = map[string]any{}
	}
	return payload
}

func findPublishedFlowActivationEvent(t *testing.T, bus *flowActivationTestBus, eventType string) events.Event {
	t.Helper()
	for _, event := range bus.published {
		if string(event.Type()) == eventType {
			return event
		}
	}
	t.Fatalf("published events = %#v, want %s", bus.published, eventType)
	return eventtest.RootIngress("", events.EventType(""), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
}

func TestActivateFlowInstanceAddsDerivedRouteTableInstance(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.InitialState = "queued"
	err := am.ActivateFlowInstance(context.Background(), req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.addedPaths) != 1 || bus.addedPaths[0] != "review/inst-1" {
		t.Fatalf("added paths = %#v, want [review/inst-1]", bus.addedPaths)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
		t.Fatal("expected activated flow agent config")
	}
	cfg, _ := am.GetAgentConfig("reviewer-inst-1")
	if got := strings.TrimSpace(cfg.EntityID); got != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("agent entity_id = %q, want %q", got, runtimepipeline.FlowInstanceEntityID("review/inst-1"))
	}
}

func TestActivateFlowInstancePassesActivationConfigToRouteMaterialization(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"vertical_id": "11111111-1111-4111-8111-111111111111",
	}
	if err := am.ActivateFlowInstance(context.Background(), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.addedRouteRequests) != 1 {
		t.Fatalf("route materialization requests = %#v, want one", bus.addedRouteRequests)
	}
	got := bus.addedRouteRequests[0].ActivationVariables["vertical_id"]
	if got != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("route activation variable vertical_id = %q, want config value", got)
	}
	if got := bus.addedRouteRequests[0].ActivationVariables["instance_id"]; got != "inst-1" {
		t.Fatalf("route activation variable instance_id = %q, want inst-1", got)
	}
}

func TestActivateFlowInstanceUsesSameBuiltinsForAgentAndRouteMaterialization(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("")
	bundle.FlowTree.ByID["review"].Agents["reviewer"] = runtimecontracts.AgentRegistryEntry{
		ID:            "reviewer-{flow_instance_path}",
		Type:          "generic",
		Role:          "reviewer",
		Subscriptions: []string{"task.started"},
	}

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"flow_instance_path": "wrong-config-path",
		"flow_scope_key":     "wrong-config-scope",
		"instance_id":        "wrong-config-instance",
		"template_id":        "wrong-config-template",
	}
	if err := am.ActivateFlowInstance(context.Background(), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if _, ok := am.GetAgentConfig("reviewer-review/inst-1"); !ok {
		t.Fatalf("expected flow agent rendered with built-in flow_instance_path, configs=%#v", am.agentCfg)
	}
	if len(bus.addedRouteRequests) != 1 {
		t.Fatalf("route materialization requests = %#v, want one", bus.addedRouteRequests)
	}
	vars := bus.addedRouteRequests[0].ActivationVariables
	if got := vars["flow_instance_path"]; got != "review/inst-1" {
		t.Fatalf("route activation variable flow_instance_path = %q, want review/inst-1", got)
	}
	if got := vars["flow_scope_key"]; got != "review" {
		t.Fatalf("route activation variable flow_scope_key = %q, want review", got)
	}
	if got := vars["instance_id"]; got != "inst-1" {
		t.Fatalf("route activation variable instance_id = %q, want inst-1", got)
	}
	if got := vars["template_id"]; got != "review" {
		t.Fatalf("route activation variable template_id = %q, want review", got)
	}
}

func TestActivateFlowInstancePublishesAutoEmitEvent(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("task.started")
	const runID = "11111111-1111-1111-1111-111111111115"
	const triggerEventID = "33333333-3333-3333-3333-333333333333"
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(triggerEventID)

	err := am.ActivateFlowInstance(ctx, req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	var autoEmit *events.Event
	for idx := range bus.published {
		if string(bus.published[idx].Type()) == "review/inst-1/task.started" {
			autoEmit = &bus.published[idx]
			break
		}
	}
	if autoEmit == nil {
		t.Fatalf("published events = %#v, want review/inst-1/task.started", bus.published)
	}
	if got := autoEmit.EntityID(); got != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("published entity_id = %q, want %q", got, runtimepipeline.FlowInstanceEntityID("review/inst-1"))
	}
	if got := strings.TrimSpace(autoEmit.RunID()); got != runID {
		t.Fatalf("published run_id = %q, want active run %q", got, runID)
	}
	if got := strings.TrimSpace(autoEmit.ParentEventID()); got != triggerEventID {
		t.Fatalf("published parent_event_id = %q, want trigger event %q", got, triggerEventID)
	}
	if got, _ := autoEmit.ContextMap("")["source_event_id"].(string); got != triggerEventID {
		t.Fatalf("event context source_event_id = %q, want trigger event %q", got, triggerEventID)
	}
}

func TestActivateFlowInstanceAutoEmitDoesNotInventLineageWithoutTrigger(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("task.started")

	if err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/task.started")
	if got := strings.TrimSpace(event.ParentEventID()); got != "" {
		t.Fatalf("parent_event_id = %q, want empty without concrete trigger", got)
	}
	if _, ok := event.ContextMap("")["source_event_id"]; ok {
		t.Fatalf("event context included source_event_id without concrete trigger: %#v", event.ContextMap(""))
	}
}

func TestActivateFlowInstanceAutoEmitPublishesConfigPayloadWithoutActivationContext(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry("component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"component_id":   {Type: "string"},
				"component_type": {Type: "string"},
			},
		},
		Required: []string{"component_id", "component_type"},
	})
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"component_id":   "component-1",
		"component_type": "api",
	}

	if err := am.ActivateFlowInstance(context.Background(), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/component.scaffold.start")
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["component_id"]; got != "component-1" {
		t.Fatalf("component_id payload = %#v, want component-1", got)
	}
	if got := payload["component_type"]; got != "api" {
		t.Fatalf("component_type payload = %#v, want api", got)
	}
	for _, key := range []string{"instance_id", "template_id", "flow_path", "parent_entity_id"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("auto-emit payload included activation context %q: %#v", key, payload)
		}
	}
	if got := event.EntityID(); got != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("auto-emit entity_id = %q, want flow instance entity", got)
	}
	if got := event.FlowInstance(); got != "review/inst-1" {
		t.Fatalf("auto-emit flow_instance = %q, want review/inst-1", got)
	}
}

func TestActivateFlowInstanceAutoEmitKeepsPayloadSourceEventIDNonAuthoritative(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry("component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"source_event_id": {Type: "string"},
			},
		},
		Required: []string{"source_event_id"},
	})
	const triggerEventID = "44444444-4444-4444-4444-444444444444"
	const payloadSourceEventID = "business-payload-source"
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(triggerEventID)
	req.Config = map[string]any{"source_event_id": payloadSourceEventID}

	if err := am.ActivateFlowInstance(context.Background(), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/component.scaffold.start")
	if got := strings.TrimSpace(event.ParentEventID()); got != triggerEventID {
		t.Fatalf("parent_event_id = %q, want trigger event %q", got, triggerEventID)
	}
	if got, _ := event.ContextMap("")["source_event_id"].(string); got != triggerEventID {
		t.Fatalf("event context source_event_id = %q, want trigger event %q", got, triggerEventID)
	}
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["source_event_id"]; got != payloadSourceEventID {
		t.Fatalf("payload source_event_id = %#v, want business payload value %q", got, payloadSourceEventID)
	}
}

func TestActivateFlowInstanceQueuedAutoEmitUsesProjectedConfigPayload(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry("component.scaffold.start", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"component_id": {Type: "string"},
			},
		},
		Required: []string{"component_id"},
	})
	postCommit := make([]func(), 0, 1)
	ctx := runtimepipeline.WithPipelinePostCommitActions(context.Background(), &postCommit)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{"component_id": "component-1"}

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("auto-emit published before post-commit flush: %#v", bus.published)
	}
	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/component.scaffold.start")
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["component_id"]; got != "component-1" {
		t.Fatalf("component_id payload = %#v, want component-1", got)
	}
	for _, key := range []string{"instance_id", "template_id", "flow_path", "parent_entity_id"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("queued auto-emit payload included activation context %q: %#v", key, payload)
		}
	}
}

func TestActivateFlowInstanceAutoEmitAllowsDeclaredTemplateIDBusinessField(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundleWithAutoEmitEntry("repo.template.selected", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"template_id": {Type: "string"},
			},
		},
		Required: []string{"template_id"},
	})
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{"template_id": "application-basic-v1"}

	if err := am.ActivateFlowInstance(context.Background(), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	event := findPublishedFlowActivationEvent(t, bus, "review/inst-1/repo.template.selected")
	payload := decodeFlowActivationEventPayload(t, event)
	if got := payload["template_id"]; got != "application-basic-v1" {
		t.Fatalf("template_id payload = %#v, want config business value", got)
	}
}

func TestActivateFlowInstanceQueuesAutoEmitUntilPostCommitWhenAvailable(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("task.started")
	const runID = "22222222-2222-2222-2222-222222222215"
	const triggerEventID = "55555555-5555-5555-5555-555555555555"
	postCommit := make([]func(), 0, 1)
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	ctx = runtimepipeline.WithPipelinePostCommitActions(ctx, &postCommit)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.TriggerEvent = testFlowActivationTriggerEvent(triggerEventID)

	err := am.ActivateFlowInstance(ctx, req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("auto-emit published before post-commit flush: %#v", bus.published)
	}
	if len(postCommit) != 1 {
		t.Fatalf("post-commit actions = %d, want 1", len(postCommit))
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if len(bus.published) != 1 {
		t.Fatalf("published events after post-commit = %d, want 1", len(bus.published))
	}
	if got := string(bus.published[0].Type()); got != "review/inst-1/task.started" {
		t.Fatalf("auto-emitted type = %q, want review/inst-1/task.started", got)
	}
	if got := strings.TrimSpace(bus.published[0].RunID()); got != runID {
		t.Fatalf("auto-emitted run_id = %q, want active run %q", got, runID)
	}
	if got := strings.TrimSpace(bus.published[0].ParentEventID()); got != triggerEventID {
		t.Fatalf("auto-emitted parent_event_id = %q, want trigger event %q", got, triggerEventID)
	}
}

func TestActivateFlowInstanceRejectsDuplicateInstanceIDBeforeSideEffects(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	store := &flowActivationTestStore{}
	am := newFlowActivationManager(bus, instances, store)
	bundle := testFlowBundle("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")

	if err := am.ActivateFlowInstance(context.Background(), req); err != nil {
		t.Fatalf("first ActivateFlowInstance: %v", err)
	}
	firstCreates := len(instances.creates)
	firstRoutes := len(bus.addedPaths)
	firstPublished := len(bus.published)
	firstAgents := len(store.upserts)

	err := am.ActivateFlowInstance(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "flow instance already exists: review/inst-1") {
		t.Fatalf("duplicate ActivateFlowInstance error = %v, want already-exists failure", err)
	}
	if len(instances.creates) != firstCreates {
		t.Fatalf("creates = %d, want unchanged %d", len(instances.creates), firstCreates)
	}
	if len(bus.addedPaths) != firstRoutes {
		t.Fatalf("added paths = %#v, want unchanged route side effects", bus.addedPaths)
	}
	if len(bus.published) != firstPublished {
		t.Fatalf("published events = %#v, want unchanged auto-emit side effects", bus.published)
	}
	if len(store.upserts) != firstAgents {
		t.Fatalf("persisted agents = %#v, want unchanged agent side effects", store.upserts)
	}
}

func TestActivateFlowInstanceFailsClosedOnAutoEmitMissingRequiredField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(bus, instances)
	bundle := testFlowBundleWithAutoEmitEntry("task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"reason": {Type: "string"},
			},
		},
		Required: []string{"reason"},
	})

	err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err == nil || !strings.Contains(err.Error(), "auto-emit task.started") || !strings.Contains(err.Error(), "reason is required") {
		t.Fatalf("ActivateFlowInstance error = %v, want missing required auto-emit schema failure", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none", bus.published)
	}
	if len(bus.runtimeLogs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", bus.runtimeLogs)
	}
	if len(instances.creates) != 0 {
		t.Fatalf("instance creates = %#v, want none", instances.creates)
	}
	if len(bus.addedPaths) != 0 {
		t.Fatalf("added paths = %#v, want none", bus.addedPaths)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("unexpected activated agent config after auto-emit schema failure")
	}
}

func TestActivateFlowInstanceQueuedAutoEmitFailsClosedOnUndeclaredConfigField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(bus, instances)
	bundle := testFlowBundle("task.started")
	postCommit := make([]func(), 0, 1)
	ctx := runtimepipeline.WithPipelinePostCommitActions(context.Background(), &postCommit)
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"unexpected": "value",
	}

	err := am.ActivateFlowInstance(ctx, req)
	if err == nil || !strings.Contains(err.Error(), "auto-emit task.started") || !strings.Contains(err.Error(), "unexpected is not allowed") {
		t.Fatalf("ActivateFlowInstance error = %v, want undeclared auto-emit schema failure", err)
	}
	if len(postCommit) != 0 {
		t.Fatalf("post-commit actions = %d, want 0", len(postCommit))
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none", bus.published)
	}
	if len(bus.runtimeLogs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", bus.runtimeLogs)
	}
	if len(instances.creates) != 0 {
		t.Fatalf("instance creates = %#v, want none", instances.creates)
	}
	if len(bus.addedPaths) != 0 {
		t.Fatalf("added paths = %#v, want none", bus.addedPaths)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("unexpected activated agent config after queued auto-emit schema failure")
	}
}

func TestActivateFlowInstanceAutoEmitFailsClosedOnUndeclaredEnvelopeLikeConfigField(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(bus, instances)
	bundle := testFlowBundle("task.started")
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"entity_id": "business-value",
	}

	err := am.ActivateFlowInstance(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "auto-emit task.started") || !strings.Contains(err.Error(), "entity_id is not allowed") {
		t.Fatalf("ActivateFlowInstance error = %v, want undeclared envelope-like config field failure", err)
	}
	if len(bus.published) != 0 {
		t.Fatalf("published events = %#v, want none", bus.published)
	}
	if len(instances.creates) != 0 {
		t.Fatalf("instance creates = %#v, want none", instances.creates)
	}
}

func TestValidateAutoEmitPayload_RejectsListTypeViolation(t *testing.T) {
	bundle := testFlowBundleWithAutoEmitEntry("task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"instance_id":      {Type: "string"},
				"template_id":      {Type: "string"},
				"flow_path":        {Type: "string"},
				"parent_entity_id": {Type: "string"},
				"sources":          {Type: "[SourceID]"},
			},
		},
		Required: []string{"instance_id", "template_id", "flow_path", "parent_entity_id", "sources"},
	})
	bundle.RootTypes = runtimecontracts.TypeCatalogDocument{
		Scalars: map[string]runtimecontracts.ScalarTypeDecl{
			"SourceID": {Base: "text"},
		},
	}

	err := validateAutoEmitPayload(semanticview.Wrap(bundle), "review", "task.started", map[string]any{
		"instance_id":      "inst-1",
		"template_id":      "review",
		"flow_path":        "review/inst-1",
		"parent_entity_id": "ent-parent",
		"sources":          "not-a-list",
	})
	if err == nil {
		t.Fatal("expected list-type auto-emit failure")
	}
	if !strings.Contains(err.Error(), "$.sources must be array") {
		t.Fatalf("validateAutoEmitPayload error = %v, want list-type detail", err)
	}
}

func TestValidateAutoEmitPayload_AllowsNamedTypeThroughCanonicalSchema(t *testing.T) {
	bundle := testFlowBundleWithAutoEmitEntry("task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"instance_id":      {Type: "string"},
				"template_id":      {Type: "string"},
				"flow_path":        {Type: "string"},
				"parent_entity_id": {Type: "string"},
				"details":          {Type: "ReviewDetails"},
			},
		},
		Required: []string{"instance_id", "template_id", "flow_path", "parent_entity_id", "details"},
	})
	bundle.RootTypes = runtimecontracts.TypeCatalogDocument{
		Types: map[string]runtimecontracts.NamedTypeDecl{
			"ReviewDetails": {
				Fields: map[string]runtimecontracts.TypeFieldSpec{
					"summary": {Type: "text"},
				},
			},
		},
	}

	err := validateAutoEmitPayload(semanticview.Wrap(bundle), "review", "task.started", map[string]any{
		"instance_id":      "inst-1",
		"template_id":      "review",
		"flow_path":        "review/inst-1",
		"parent_entity_id": "ent-parent",
		"details":          map[string]any{"summary": "ready"},
	})
	if err != nil {
		t.Fatalf("validateAutoEmitPayload valid named type: %v", err)
	}

	err = validateAutoEmitPayload(semanticview.Wrap(bundle), "review", "task.started", map[string]any{
		"instance_id":      "inst-1",
		"template_id":      "review",
		"flow_path":        "review/inst-1",
		"parent_entity_id": "ent-parent",
		"details":          "not-object",
	})
	if err == nil {
		t.Fatal("expected named-type auto-emit violation")
	}
	if !strings.Contains(err.Error(), "$.details must be object") {
		t.Fatalf("validateAutoEmitPayload error = %v, want named-type detail", err)
	}
}

func TestNormalizedStaticFlowEmitEvents_ExternalizesLocalEvents(t *testing.T) {
	got := normalizedStaticFlowEmitEvents(
		[]string{"analysis.done", "shared.event"},
		nil,
		map[string]struct{}{"analysis.done": {}},
		"analyzer-flow",
	)
	if len(got) != 2 || got[0] != "analyzer-flow/analysis.done" || got[1] != "shared.event" {
		t.Fatalf("normalizedStaticFlowEmitEvents = %#v", got)
	}
}

func TestNormalizedFlowAgentEmitEvents_ExternalizesInstanceLocalEvents(t *testing.T) {
	got := normalizedFlowAgentEmitEvents(
		[]string{"task.started", "shared.event"},
		nil,
		map[string]struct{}{"task.started": {}},
		"review/inst-1",
		"review",
		"inst-1",
	)
	if len(got) != 2 || got[0] != "review/inst-1/task.started" || got[1] != "shared.event" {
		t.Fatalf("normalizedFlowAgentEmitEvents = %#v", got)
	}
}

func TestActivateFlowInstancePersistsFlowInstanceConfig(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(bus, instances)
	bundle := testFlowBundleWithAutoEmitEntry("task.started", runtimecontracts.EventCatalogEntry{
		Payload: runtimecontracts.EventPayloadSpec{
			Properties: map[string]runtimecontracts.EventFieldSpec{
				"name":     {Type: "string"},
				"priority": {Type: "integer"},
			},
		},
	})

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"name":     "alpha",
		"priority": 1,
	}
	err := am.ActivateFlowInstance(context.Background(), req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(instances.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(instances.creates))
	}
	got := instances.creates[0]
	if got.StorageRef != "review/inst-1" {
		t.Fatalf("storage_ref = %q, want review/inst-1", got.StorageRef)
	}
	if got.Metadata["entity_id"] != runtimepipeline.FlowInstanceEntityID("review/inst-1") {
		t.Fatalf("metadata entity_id = %#v, want %q", got.Metadata["entity_id"], runtimepipeline.FlowInstanceEntityID("review/inst-1"))
	}
	if got.Config["name"] != "alpha" {
		t.Fatalf("config name = %#v, want alpha", got.Config["name"])
	}
	if got.Config["priority"] != 1 {
		t.Fatalf("config priority = %#v, want 1", got.Config["priority"])
	}
}

func TestActivateFlowInstancePersistsFullParentRouteMetadata(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(bus, instances)
	bundle := testFlowBundle("")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-legacy", "review/inst-1")
	req.Instance.ParentRoute = runtimeflowidentity.ParentRoute{
		FlowID:       "operating",
		FlowInstance: "operating/root",
		EntityID:     "parent-ent",
	}
	if err := am.ActivateFlowInstance(context.Background(), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(instances.creates) != 1 {
		t.Fatalf("creates = %d, want 1", len(instances.creates))
	}
	metadata := instances.creates[0].Metadata
	if got := metadata["parent_flow_id"]; got != "operating" {
		t.Fatalf("parent_flow_id = %#v, want operating", got)
	}
	if got := metadata["parent_flow_instance"]; got != "operating/root" {
		t.Fatalf("parent_flow_instance = %#v, want operating/root", got)
	}
	if got := metadata["parent_entity_id"]; got != "parent-ent" {
		t.Fatalf("parent_entity_id = %#v, want parent-ent", got)
	}
}

func TestActivateFlowInstanceResolvesAgentPermissions(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("")
	reviewFlow := bundle.FlowTree.ByID["review"]
	reviewFlow.Policy = runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"permission_bundles": {
			Value: map[string]any{
				"ops": map[string]any{
					"permissions": []any{"agent_fire"},
				},
			},
		},
	}}
	bundle.FlowTree.Root.Children[0].Policy = reviewFlow.Policy
	entry := reviewFlow.Agents["reviewer"]
	entry.PermissionsBundle = "ops"
	entry.Permissions = []string{"schedule"}
	reviewFlow.Agents["reviewer"] = entry
	bundle.FlowTree.Root.Children[0].Agents["reviewer"] = entry

	err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	cfg, ok := am.GetAgentConfig("reviewer-inst-1")
	if !ok {
		t.Fatal("expected activated flow agent config")
	}
	if len(cfg.Permissions) != 2 || cfg.Permissions[0] != "agent_fire" || cfg.Permissions[1] != "schedule" {
		t.Fatalf("permissions = %#v, want [agent_fire schedule]", cfg.Permissions)
	}
}

func TestDeactivateFlowInstanceRemovesAgentsAndRoutes(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(bus, instances)
	bundle := testFlowBundle("")

	err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := am.DeactivateFlowInstance(context.Background(), "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("expected flow agent teardown")
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed pairs = %#v, want [review/inst-1]", bus.removedPairs)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "review/inst-1" {
		t.Fatalf("terminated paths = %#v, want [review/inst-1]", instances.terminatedPaths)
	}
	if len(instances.terminatedAtSeen) != 1 || instances.terminatedAtSeen[0].IsZero() {
		t.Fatalf("terminated_at seen = %#v, want one non-zero timestamp", instances.terminatedAtSeen)
	}
}

func TestDeactivateFlowInstanceQueuesTerminalSideEffectsUntilPostCommitWhenAvailable(t *testing.T) {
	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	instances := &flowActivationTestInstanceStore{}
	managerStore := &flowActivationTestStore{}
	am := newFlowActivationManager(bus, instances, managerStore)
	bundle := testFlowBundle("")

	if err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	postCommit := make([]func(), 0, 1)
	ctx := runtimepipeline.WithPipelinePostCommitActions(context.Background(), &postCommit)
	if err := am.DeactivateFlowInstance(ctx, "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}

	if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
		t.Fatal("flow agent was torn down before post-commit flush")
	}
	if len(bus.unsubscribed) != 0 {
		t.Fatalf("unsubscribed before post-commit flush = %#v, want none", bus.unsubscribed)
	}
	if len(managerStore.terminated) != 0 {
		t.Fatalf("agent terminations before post-commit flush = %#v, want none", managerStore.terminated)
	}
	if len(bus.removedPairs) != 0 {
		t.Fatalf("removed routes before post-commit flush = %#v, want none", bus.removedPairs)
	}
	if got := routeStore.statusByPath["review/inst-1"]; got != "active" {
		t.Fatalf("route status before post-commit flush = %q, want active", got)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "review/inst-1" {
		t.Fatalf("flow instance terminal state = %#v, want committed owner entry", instances.terminatedPaths)
	}
	if len(postCommit) != 1 {
		t.Fatalf("post-commit actions = %d, want 1", len(postCommit))
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("expected flow agent teardown after post-commit flush")
	}
	if len(bus.unsubscribed) != 1 || bus.unsubscribed[0] != "reviewer-inst-1" {
		t.Fatalf("unsubscribed after post-commit flush = %#v, want reviewer-inst-1", bus.unsubscribed)
	}
	if len(managerStore.terminated) != 1 || managerStore.terminated[0] != "reviewer-inst-1" {
		t.Fatalf("agent terminations after post-commit flush = %#v, want reviewer-inst-1", managerStore.terminated)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed routes after post-commit flush = %#v, want [review/inst-1]", bus.removedPairs)
	}
	if got := routeStore.statusByPath["review/inst-1"]; got != "inactive" {
		t.Fatalf("route status after post-commit flush = %q, want inactive", got)
	}
	if len(bus.runtimeLogs) != 0 {
		t.Fatalf("runtime logs = %#v, want none", bus.runtimeLogs)
	}
}

func TestDeactivateFlowInstanceLogsPostCommitAgentFailureWithoutRouteRemoval(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	managerStore := &flowActivationTestStore{terminateErr: errors.New("agent terminate failed")}
	am := newFlowActivationManager(bus, instances, managerStore)
	bundle := testFlowBundle("")

	if err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	postCommit := make([]func(), 0, 1)
	ctx := runtimepipeline.WithPipelinePostCommitActions(context.Background(), &postCommit)
	if err := am.DeactivateFlowInstance(ctx, "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance returned pre-commit error: %v", err)
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if len(bus.runtimeLogs) != 1 {
		t.Fatalf("runtime logs = %#v, want one post-commit failure log", bus.runtimeLogs)
	}
	log := bus.runtimeLogs[0]
	if log.Action != "terminal_flow_instance_side_effects_failed" || log.Level != "warn" {
		t.Fatalf("runtime log = %#v, want warning terminal_flow_instance_side_effects_failed", log)
	}
	if !strings.Contains(log.Error, "agent terminate failed") {
		t.Fatalf("runtime log error = %q, want agent termination failure", log.Error)
	}
	if len(bus.removedPairs) != 0 {
		t.Fatalf("removed routes after agent failure = %#v, want no route removal", bus.removedPairs)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "review/inst-1" {
		t.Fatalf("flow terminal state = %#v, want preserved terminal transition", instances.terminatedPaths)
	}
}

func TestDeactivateFlowInstanceLogsPostCommitRouteFailureAfterAgentTeardown(t *testing.T) {
	bus := &flowActivationTestBus{removeErr: errors.New("route removal failed")}
	instances := &flowActivationTestInstanceStore{}
	managerStore := &flowActivationTestStore{}
	am := newFlowActivationManager(bus, instances, managerStore)
	bundle := testFlowBundle("")

	if err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	postCommit := make([]func(), 0, 1)
	ctx := runtimepipeline.WithPipelinePostCommitActions(context.Background(), &postCommit)
	if err := am.DeactivateFlowInstance(ctx, "review", "inst-1", "review/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance returned pre-commit error: %v", err)
	}

	runtimepipeline.FlushPipelinePostCommitActions(postCommit)
	if len(bus.runtimeLogs) != 1 {
		t.Fatalf("runtime logs = %#v, want one post-commit failure log", bus.runtimeLogs)
	}
	log := bus.runtimeLogs[0]
	if log.Action != "terminal_flow_instance_side_effects_failed" || log.Level != "warn" {
		t.Fatalf("runtime log = %#v, want warning terminal_flow_instance_side_effects_failed", log)
	}
	if !strings.Contains(log.Error, "route removal failed") {
		t.Fatalf("runtime log error = %q, want route removal failure", log.Error)
	}
	if len(managerStore.terminated) != 1 || managerStore.terminated[0] != "reviewer-inst-1" {
		t.Fatalf("agent terminations after route failure = %#v, want reviewer-inst-1", managerStore.terminated)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed routes after route failure = %#v, want one route attempt", bus.removedPairs)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "review/inst-1" {
		t.Fatalf("flow terminal state = %#v, want preserved terminal transition", instances.terminatedPaths)
	}
}

func TestDeactivateFlowInstanceUsesExactResolvedFlowPathForNestedTemplate(t *testing.T) {
	bus := &flowActivationTestBus{}
	instances := &flowActivationTestInstanceStore{}
	am := newFlowActivationManager(bus, instances)
	bundle := testNestedFlowBundle()

	err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "grandchild", "inst-1", "ent-1", "child/grandchild/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := am.DeactivateFlowInstance(context.Background(), "grandchild", "inst-1", "child/grandchild/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "child/grandchild/inst-1" {
		t.Fatalf("removed pairs = %#v, want [child/grandchild/inst-1]", bus.removedPairs)
	}
	if len(instances.terminatedPaths) != 1 || instances.terminatedPaths[0] != "child/grandchild/inst-1" {
		t.Fatalf("terminated paths = %#v, want [child/grandchild/inst-1]", instances.terminatedPaths)
	}
}

func TestDeactivateFlowInstanceModel_PersistsTerminalStateInFlowInstances(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	const runID = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	store := runtimepipeline.NewWorkflowInstanceStore(db)
	am := newFlowActivationManager(bus, store)
	bundle := testFlowBundle("")
	const subjectID = "11111111-1111-1111-1111-111111111111"
	req := testActivationRequest(bundle, "review", "inst-1", subjectID, "review/inst-1")

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := store.Mutate(ctx, req.Instance.EntityID, func(instance *runtimepipeline.WorkflowInstance) {
		instance.CurrentState = "completed"
	}); err != nil {
		t.Fatalf("Mutate: %v", err)
	}
	am.mu.Lock()
	am.agents["shared-subject-agent"] = flowActivationStubAgent{id: "shared-subject-agent"}
	am.agentCfg["shared-subject-agent"] = models.AgentConfig{
		ID:       "shared-subject-agent",
		EntityID: req.Instance.EntityID,
		FlowPath: "review/other-inst",
	}
	am.mu.Unlock()

	if err := am.DeactivateFlowInstanceModel(ctx, runtimepipeline.FlowInstanceDeactivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		Instance:       req.Instance,
		FinalState:     "completed",
	}); err != nil {
		t.Fatalf("DeactivateFlowInstanceModel: %v", err)
	}

	var (
		status       string
		terminatedAt time.Time
	)
	if err := db.QueryRowContext(context.Background(), `
		SELECT status, terminated_at
		FROM flow_instances
		WHERE instance_id = $1
	`, "review/inst-1").Scan(&status, &terminatedAt); err != nil {
		t.Fatalf("query flow_instances: %v", err)
	}
	if strings.TrimSpace(status) != "terminated" {
		t.Fatalf("flow_instances.status = %q, want terminated", status)
	}
	if terminatedAt.IsZero() {
		t.Fatal("flow_instances.terminated_at is zero")
	}
	routeStatus := routeStore.statusByPath["review/inst-1"]
	if strings.TrimSpace(routeStatus) != "inactive" {
		t.Fatalf("routing_rules.status = %q, want inactive", routeStatus)
	}

	instance, ok, err := store.Load(ctx, req.Instance.EntityID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "completed" {
		t.Fatalf("current_state = %q, want completed", got)
	}
	if strings.TrimSpace(instance.Status) != "terminated" {
		t.Fatalf("loaded workflow instance status = %q, want terminated", instance.Status)
	}
	if instance.TerminatedAt.IsZero() {
		t.Fatal("loaded workflow instance terminated_at is zero")
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("expected flow-scoped agent teardown")
	}
	if _, ok := am.GetAgentConfig("shared-subject-agent"); !ok {
		t.Fatal("expected unrelated flow agent to remain active")
	}
}

func TestDeactivateFlowInstanceModel_PostCommitSideEffectsFollowTerminalCommit(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	const runID = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status)
		VALUES ($1::uuid, 'running')
		ON CONFLICT (run_id) DO NOTHING
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	managerStore := &flowActivationTestStore{}
	store := runtimepipeline.NewWorkflowInstanceStore(db)
	am := newFlowActivationManager(bus, store, managerStore)
	bundle := testFlowBundle("")
	const subjectID = "22222222-2222-2222-2222-222222222222"
	req := testActivationRequest(bundle, "review", "inst-1", subjectID, "review/inst-1")

	if err := am.ActivateFlowInstance(ctx, req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := store.RunPipelineMutation(ctx, func(txctx context.Context) error {
		if err := store.Mutate(txctx, req.Instance.EntityID, func(instance *runtimepipeline.WorkflowInstance) {
			instance.CurrentState = "completed"
		}); err != nil {
			return err
		}
		if err := am.DeactivateFlowInstanceModel(txctx, runtimepipeline.FlowInstanceDeactivationRequest{
			ContractBundle: semanticview.Wrap(bundle),
			Instance:       req.Instance,
			FinalState:     "completed",
		}); err != nil {
			return err
		}
		if _, ok := am.GetAgentConfig("reviewer-inst-1"); !ok {
			t.Fatal("flow agent was torn down before terminal transaction commit")
		}
		if len(managerStore.terminated) != 0 || len(bus.unsubscribed) != 0 || len(bus.removedPairs) != 0 {
			t.Fatalf("side effects before commit: terminated=%#v unsubscribed=%#v routes=%#v", managerStore.terminated, bus.unsubscribed, bus.removedPairs)
		}
		if got := routeStore.statusByPath["review/inst-1"]; got != "active" {
			t.Fatalf("route status before commit = %q, want active", got)
		}
		var externalStatus string
		if err := db.QueryRowContext(context.Background(), `
			SELECT status
			FROM flow_instances
			WHERE instance_id = $1
		`, "review/inst-1").Scan(&externalStatus); err != nil {
			t.Fatalf("external flow_instances status before commit: %v", err)
		}
		if strings.TrimSpace(externalStatus) != "active" {
			t.Fatalf("external flow_instances status before commit = %q, want active", externalStatus)
		}
		return nil
	}); err != nil {
		t.Fatalf("RunPipelineMutation: %v", err)
	}

	var status string
	if err := db.QueryRowContext(context.Background(), `
		SELECT status
		FROM flow_instances
		WHERE instance_id = $1
	`, "review/inst-1").Scan(&status); err != nil {
		t.Fatalf("external flow_instances status after commit: %v", err)
	}
	if strings.TrimSpace(status) != "terminated" {
		t.Fatalf("external flow_instances status after commit = %q, want terminated", status)
	}
	if _, ok := am.GetAgentConfig("reviewer-inst-1"); ok {
		t.Fatal("expected flow agent teardown after terminal transaction commit")
	}
	if len(managerStore.terminated) != 1 || managerStore.terminated[0] != "reviewer-inst-1" {
		t.Fatalf("agent terminations after commit = %#v, want reviewer-inst-1", managerStore.terminated)
	}
	if len(bus.unsubscribed) != 1 || bus.unsubscribed[0] != "reviewer-inst-1" {
		t.Fatalf("unsubscribed after commit = %#v, want reviewer-inst-1", bus.unsubscribed)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "review/inst-1" {
		t.Fatalf("removed routes after commit = %#v, want review/inst-1", bus.removedPairs)
	}
	if got := routeStore.statusByPath["review/inst-1"]; got != "inactive" {
		t.Fatalf("route status after commit = %q, want inactive", got)
	}
}

func TestBuildFlowAgentConfig_ExternalizesLocalSubscriptionsFromExactFlowPath(t *testing.T) {
	cfg, err := buildFlowAgentConfig(
		semanticview.Wrap(testNestedFlowBundle()),
		"grandchild",
		"inst-1",
		"ent-1",
		"child/grandchild/inst-1",
		"worker",
		runtimecontracts.AgentRegistryEntry{
			ID:            "worker-{instance_id}",
			Type:          "generic",
			Role:          "worker",
			Subscriptions: []string{"micro.started"},
		},
		map[string]string{"instance_id": "inst-1"},
		map[string]struct{}{"micro.started": {}},
		map[string]any{},
	)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "child/grandchild/inst-1/micro.started" {
		t.Fatalf("subscriptions = %#v, want [child/grandchild/inst-1/micro.started]", cfg.Subscriptions)
	}
}

func TestEnsureStaticFlowRequiredAgentsRegistersStaticFlowSubscriptions(t *testing.T) {
	bus := &flowActivationTestBus{}
	store := &flowActivationTestStore{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{}, store)
	bundle := testStaticFlowBundle()

	if err := am.EnsureStaticFlowRequiredAgents(context.Background(), semanticview.Wrap(bundle)); err != nil {
		t.Fatalf("EnsureStaticFlowRequiredAgents: %v", err)
	}
	cfg, ok := am.GetAgentConfig("analyzer")
	if !ok {
		t.Fatal("expected static flow required agent config")
	}
	if got := cfg.Mode; got != "analyzer-flow" {
		t.Fatalf("mode = %q, want analyzer-flow", got)
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "analyzer-flow/analysis.requested" {
		t.Fatalf("subscriptions = %#v, want [analyzer-flow/analysis.requested]", cfg.Subscriptions)
	}
	if got := cfg.EmitEvents; len(got) != 1 || got[0] != "analyzer-flow/analysis.done" {
		t.Fatalf("emit_events = %#v, want [analyzer-flow/analysis.done]", got)
	}
	if len(store.upserts) != 1 || store.upserts[0].Config.ID != "analyzer" {
		t.Fatalf("persisted agents = %#v, want analyzer", store.upserts)
	}
}

func TestEnsureStaticFlowRequiredAgentsInfersFromOmittedRequiredAgents(t *testing.T) {
	bus := &flowActivationTestBus{}
	store := &flowActivationTestStore{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{}, store)
	bundle := testStaticFlowBundle()
	schema := bundle.FlowSchemas["analyzer-flow"]
	schema.RequiredAgents = nil
	schema.RequiredAgentsDeclared = false
	bundle.FlowSchemas["analyzer-flow"] = schema
	bundle.Semantics.FlowAgents = map[string][]runtimecontracts.FlowRequiredAgent{}
	bundle.Semantics.FlowAgentFacts = map[string][]runtimecontracts.RequiredAgentFact{}

	if err := am.EnsureStaticFlowRequiredAgents(context.Background(), semanticview.Wrap(bundle)); err != nil {
		t.Fatalf("EnsureStaticFlowRequiredAgents: %v", err)
	}
	cfg, ok := am.GetAgentConfig("analyzer")
	if !ok {
		t.Fatal("expected inferred static flow required agent config")
	}
	if len(cfg.Subscriptions) != 1 || cfg.Subscriptions[0] != "analyzer-flow/analysis.requested" {
		t.Fatalf("subscriptions = %#v, want [analyzer-flow/analysis.requested]", cfg.Subscriptions)
	}
	if len(store.upserts) != 1 || store.upserts[0].Config.ID != "analyzer" {
		t.Fatalf("persisted agents = %#v, want analyzer", store.upserts)
	}
}

func TestStaticRequiredAgentsForScopeRejectsRoleFallbackWithoutMapKey(t *testing.T) {
	records, err := staticRequiredAgentsForScope(nil, "analysis", "analysis", map[string]runtimecontracts.AgentRegistryEntry{
		"worker-alias": {
			ID:            "worker",
			Role:          "worker",
			Subscriptions: []string{"analysis.requested"},
			EmitEvents:    []string{"analysis.done"},
		},
	}, []runtimecontracts.FlowRequiredAgent{{
		Role:         "worker",
		SubscribesTo: []string{"analysis.requested"},
		Emits:        []string{"analysis.done"},
	}})

	if err == nil || !strings.Contains(err.Error(), `required agent "worker"`) {
		t.Fatalf("expected required-agent map-key error, records=%#v err=%v", records, err)
	}
}

func TestEnsureStaticAgentsForScopeRegistersRootAndFlowSubscriptions(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Version: "v-test",
			FlowPrefix: map[string]string{
				"ops-flow": "ops-flow",
			},
		},
	})

	rootAgents := map[string]runtimecontracts.AgentRegistryEntry{
		"test-agent": {
			ID:            "test-agent",
			Type:          "generic",
			Role:          "test-agent",
			Subscriptions: []string{"task.assigned"},
			EmitEvents:    []string{"task.completed"},
		},
	}
	if err := am.ensureStaticAgentsForScope(context.Background(), source, "", "", rootAgents); err != nil {
		t.Fatalf("ensureStaticAgentsForScope(root): %v", err)
	}
	flowAgents := map[string]runtimecontracts.AgentRegistryEntry{
		"operator": {
			ID:            "operator",
			Type:          "generic",
			Role:          "operator",
			Subscriptions: []string{"work.requested"},
			EmitEvents:    []string{"work.completed"},
		},
	}
	if err := am.ensureStaticAgentsForScope(context.Background(), source, "ops-flow", "ops-flow", flowAgents); err != nil {
		t.Fatalf("ensureStaticAgentsForScope(flow): %v", err)
	}

	rootCfg, ok := am.GetAgentConfig("test-agent")
	if !ok {
		t.Fatal("expected root static agent config")
	}
	if len(rootCfg.Subscriptions) != 1 || rootCfg.Subscriptions[0] != "task.assigned" {
		t.Fatalf("root subscriptions = %#v, want [task.assigned]", rootCfg.Subscriptions)
	}

	flowCfg, ok := am.GetAgentConfig("operator")
	if !ok {
		t.Fatal("expected flow static agent config")
	}
	if len(flowCfg.Subscriptions) != 1 || flowCfg.Subscriptions[0] != "ops-flow/work.requested" {
		t.Fatalf("flow subscriptions = %#v, want [ops-flow/work.requested]", flowCfg.Subscriptions)
	}
}

func TestEnsureStaticAgents_PackageBackedFlowOwnedAgentsCarryCanonicalFlowPath(t *testing.T) {
	source := loadPackageBackedStaticAgentSource(t)
	bus := &flowActivationTestBus{}
	store := &flowActivationTestStore{}
	var captured []models.AgentConfig
	am := NewAgentManagerWithOptions(bus, func(cfg models.AgentConfig) (Agent, error) {
		if _, err := sessions.ValidateAgentSessionScopeConfig(cfg); err != nil {
			return nil, err
		}
		captured = append(captured, cfg)
		return flowActivationStubAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{}, store)

	if err := am.EnsureStaticAgents(context.Background(), source); err != nil {
		t.Fatalf("EnsureStaticAgents: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("captured agents = %#v, want 1", captured)
	}
	if captured[0].FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", captured[0].FlowPath)
	}
	if captured[0].Mode != "support" {
		t.Fatalf("Mode = %q, want support", captured[0].Mode)
	}
	if captured[0].ID != "backend-{vertical_id}" {
		t.Fatalf("ID = %q, want backend-{vertical_id}", captured[0].ID)
	}
	if len(store.upserts) != 1 || store.upserts[0].Config.FlowPath != "support" {
		t.Fatalf("persisted agents = %#v, want support flow path", store.upserts)
	}
}

func TestEnsureStaticAgents_SoleParentFlowPackageAgentsStartWithOwningFlowPath(t *testing.T) {
	source := loadSoleParentFlowStaticAgentSource(t)
	bus := &flowActivationTestBus{}
	store := &flowActivationTestStore{}
	var captured []models.AgentConfig
	am := NewAgentManagerWithOptions(bus, func(cfg models.AgentConfig) (Agent, error) {
		if _, err := sessions.ValidateAgentSessionScopeConfig(cfg); err != nil {
			return nil, err
		}
		captured = append(captured, cfg)
		return flowActivationStubAgent{id: cfg.ID}, nil
	}, AgentManagerOptions{}, store)

	if err := am.EnsureStaticAgents(context.Background(), source); err != nil {
		t.Fatalf("EnsureStaticAgents: %v", err)
	}
	if len(captured) != 1 {
		t.Fatalf("captured agents = %#v, want 1", captured)
	}
	if captured[0].FlowPath != "support" {
		t.Fatalf("FlowPath = %q, want support", captured[0].FlowPath)
	}
	if captured[0].Mode != "support" {
		t.Fatalf("Mode = %q, want support", captured[0].Mode)
	}
	if captured[0].ID != "backend-{vertical_id}" {
		t.Fatalf("ID = %q, want backend-{vertical_id}", captured[0].ID)
	}
}

func TestActivateFlowInstanceFailsWithoutWorkflowInstanceStore(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := NewAgentManager(bus, nil)

	err := am.ActivateFlowInstance(context.Background(), testActivationRequest(testFlowBundle(""), "review", "inst-1", "ent-1", "review/inst-1"))
	if err == nil || !strings.Contains(err.Error(), "workflow instance store is required") {
		t.Fatalf("ActivateFlowInstance err = %v, want workflow instance store error", err)
	}
}

func loadPackageBackedStaticAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
backend:
  id: backend-{vertical_id}
  type: generic
  role: backend
  model: regular
  mode: session
  subscriptions:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func loadSoleParentFlowStaticAgentSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
packages:
  - path: extras
flows:
  - id: support
    flow: support
    mode: static
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
states:
  - waiting
  - done
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "extras", "package.yaml"), `
name: extras
version: "1.0.0"
flows: []
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "extras", "agents.yaml"), `
backend:
  id: backend-{vertical_id}
  type: generic
  role: backend
  model: regular
  mode: session
  subscriptions:
    - support/item.created
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeFlowActivationFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func TestBuildFlowAgentConfig_PassesContractToolsAndEmitEvents(t *testing.T) {
	cfg, err := buildFlowAgentConfig(
		semanticview.Wrap(testFlowBundle("")),
		"review",
		"inst-1",
		"ent-1",
		"review/inst-1",
		"reviewer",
		runtimecontracts.AgentRegistryEntry{
			ID:              "reviewer-{instance_id}",
			Type:            "generic",
			Role:            "reviewer",
			Tools:           []string{"schedule", "check_status"},
			NativeTools:     map[string]any{"bash": true, "file_io": true},
			EmitEvents:      []string{"task.completed", "task.completed", "review.failed"},
			MaxTurnsPerTask: 7,
		},
		map[string]string{"instance_id": "inst-1"},
		map[string]struct{}{"task.completed": {}, "review.failed": {}},
		map[string]any{},
	)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	if got := cfg.MaxTurnsPerTask; got != 7 {
		t.Fatalf("max_turns_per_task = %d, want 7", got)
	}
	if got := cfg.Tools; len(got) != 2 || got[0] != "check_status" || got[1] != "schedule" {
		t.Fatalf("tools = %#v, want [check_status schedule]", got)
	}
	if got := cfg.EmitEvents; len(got) != 2 || got[0] != "review/inst-1/review.failed" || got[1] != "review/inst-1/task.completed" {
		t.Fatalf("emit_events = %#v, want [review/inst-1/review.failed review/inst-1/task.completed]", got)
	}
	if !cfg.NativeTools.Bash || !cfg.NativeTools.FileIO {
		t.Fatalf("native_tools = %#v, want bash/file_io true", cfg.NativeTools)
	}
}

func TestStaticAndTemplateAgentMaterializationConsumeEffectivePlatformDefaults(t *testing.T) {
	source := loadAgentPlatformDefaultsMaterializationSource(t)

	staticScope, ok := source.FlowScopeByID("static_support")
	if !ok {
		t.Fatal("static_support flow scope missing")
	}
	staticEntry := staticScope.Agents["worker"]
	staticCfg, err := buildStaticFlowAgentConfig(source, "static_support", "static_support", "worker", staticEntry, staticFlowLocalEventSet(staticScope.Agents))
	if err != nil {
		t.Fatalf("buildStaticFlowAgentConfig: %v", err)
	}
	assertMaterializedAgentPlatformDefaults(t, staticCfg)

	templateScope, ok := source.FlowScopeByID("template_support")
	if !ok {
		t.Fatal("template_support flow scope missing")
	}
	templateEntry := templateScope.Agents["worker"]
	templateCfg, err := buildFlowAgentConfig(
		source,
		"template_support",
		"inst-1",
		"entity-1",
		"template_support/inst-1",
		"worker",
		templateEntry,
		map[string]string{"instance_id": "inst-1"},
		staticFlowLocalEventSet(templateScope.Agents),
		map[string]any{},
	)
	if err != nil {
		t.Fatalf("buildFlowAgentConfig: %v", err)
	}
	assertMaterializedAgentPlatformDefaults(t, templateCfg)
}

func assertMaterializedAgentPlatformDefaults(t *testing.T, cfg models.AgentConfig) {
	t.Helper()
	if cfg.Type != runtimecontracts.DefaultAgentType {
		t.Fatalf("Type = %q, want %q", cfg.Type, runtimecontracts.DefaultAgentType)
	}
	if cfg.ConversationMode != runtimecontracts.DefaultAgentMode {
		t.Fatalf("ConversationMode = %q, want %q", cfg.ConversationMode, runtimecontracts.DefaultAgentMode)
	}
	if cfg.SessionScope != "" {
		t.Fatalf("SessionScope = %q, want empty for task default", cfg.SessionScope)
	}
	if cfg.MaxTurnsPerTask != runtimecontracts.DefaultAgentMaxTurnsPerTask {
		t.Fatalf("MaxTurnsPerTask = %d, want %d", cfg.MaxTurnsPerTask, runtimecontracts.DefaultAgentMaxTurnsPerTask)
	}
	if cfg.WorkspaceClass != "" {
		t.Fatalf("WorkspaceClass = %q, want empty default", cfg.WorkspaceClass)
	}
}

func loadAgentPlatformDefaultsMaterializationSource(t *testing.T) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()

	writeFlowActivationFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: agent-defaults-materialization
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: static_support
    flow: static_support
    mode: static
  - id: template_support
    flow: template_support
    mode: template
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: agent-defaults-materialization\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	for _, flowID := range []string{"static_support", "template_support"} {
		writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), "name: "+flowID+"\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", flowID, "tools.yaml"), "{}\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), flowID+".requested:\n  entity_id: string\n")
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "static_support", "agents.yaml"), `
worker:
  model: regular
  subscriptions:
    - static_support.requested
  emit_events: []
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "template_support", "agents.yaml"), `
worker:
  id: worker-{instance_id}
  model: regular
  subscriptions:
    - template_support.requested
  emit_events: []
`)

	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}
