package manager

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	models "swarm/internal/runtime/core/actors"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
	"swarm/internal/runtime/sessions"
	"swarm/internal/testutil"
)

type flowActivationRouteStore interface {
	UpsertFlowInstanceRoute(ctx context.Context, route runtimebus.FlowInstanceRouteRecord) error
	DeleteFlowInstanceRoute(ctx context.Context, identity runtimeflowidentity.Route) error
}

type flowActivationTestBus struct {
	addedPaths   []string
	removedPairs []string
	published    []events.Event
	routeStore   flowActivationRouteStore
}

type flowActivationTestRouteStore struct {
	statusByPath map[string]string
}

type flowActivationTestInstanceStore struct {
	upserts          []runtimepipeline.WorkflowInstance
	terminatedPaths  []string
	terminatedAtSeen []time.Time
	byStorageRef     map[string]runtimepipeline.WorkflowInstance
}

type flowActivationTestStore struct {
	upserts []PersistedAgent
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
	if s.byStorageRef == nil {
		s.byStorageRef = map[string]runtimepipeline.WorkflowInstance{}
	}
	stored := instance
	stored.StorageRef = strings.TrimSpace(stored.StorageRef)
	if stored.StorageRef != "" {
		stored.Status = "active"
		s.byStorageRef[stored.StorageRef] = stored
	}
	return nil
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
func (*flowActivationTestStore) MarkAgentTerminated(context.Context, string) error { return nil }
func (*flowActivationTestStore) EnsureEntitySchema(context.Context, string) error  { return nil }
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
func (*flowActivationTestBus) Unsubscribe(string)           {}
func (*flowActivationTestBus) Store() runtimebus.EventStore { return nil }
func (*flowActivationTestBus) ResetInMemoryState() error    { return nil }
func (*flowActivationTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) error {
	return nil
}

func (b *flowActivationTestBus) AddFlowInstanceRoute(_ runtimecontracts.SystemNodeContract, identity runtimeflowidentity.Route) error {
	b.addedPaths = append(b.addedPaths, identity.InstancePath)
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
		sourceEntityID,
	)
	return runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		Instance:       instance,
	}
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

func TestActivateFlowInstancePublishesAutoEmitEvent(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("task.started")

	err := am.ActivateFlowInstance(context.Background(), testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	var autoEmit *events.Event
	for idx := range bus.published {
		if string(bus.published[idx].Type) == "review/inst-1/task.started" {
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
}

func TestActivateFlowInstanceQueuesAutoEmitUntilPostCommitWhenAvailable(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := newFlowActivationManager(bus, &flowActivationTestInstanceStore{})
	bundle := testFlowBundle("task.started")
	postCommit := make([]func(), 0, 1)
	ctx := runtimepipeline.WithPipelinePostCommitActions(context.Background(), &postCommit)

	err := am.ActivateFlowInstance(ctx, testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1"))
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
	if got := string(bus.published[0].Type); got != "review/inst-1/task.started" {
		t.Fatalf("auto-emitted type = %q, want review/inst-1/task.started", got)
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
	bundle := testFlowBundle("task.started")

	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	req.Config = map[string]any{
		"name":     "alpha",
		"priority": 1,
	}
	err := am.ActivateFlowInstance(context.Background(), req)
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if len(instances.upserts) != 1 {
		t.Fatalf("upserts = %d, want 1", len(instances.upserts))
	}
	got := instances.upserts[0]
	if got.StorageRef != "review/inst-1" {
		t.Fatalf("storage_ref = %q, want review/inst-1", got.StorageRef)
	}
	if got.SubjectID != "ent-1" {
		t.Fatalf("subject_id = %q, want ent-1", got.SubjectID)
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

	routeStore := &flowActivationTestRouteStore{}
	bus := &flowActivationTestBus{routeStore: routeStore}
	store := runtimepipeline.NewWorkflowInstanceStore(db)
	am := newFlowActivationManager(bus, store)
	bundle := testFlowBundle("")
	const subjectID = "11111111-1111-1111-1111-111111111111"
	req := testActivationRequest(bundle, "review", "inst-1", subjectID, "review/inst-1")

	if err := am.ActivateFlowInstance(context.Background(), req); err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := store.Mutate(context.Background(), req.Instance.EntityID, func(instance *runtimepipeline.WorkflowInstance) {
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

	if err := am.DeactivateFlowInstanceModel(context.Background(), runtimepipeline.FlowInstanceDeactivationRequest{
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

	instance, ok, err := store.Load(context.Background(), req.Instance.EntityID)
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
platform_version: ">=1.0.0"
entity_schema:
  groups:
    - name: item
      fields:
        - name: item_id
          type: string
          primary: true
flows:
  - id: support
    flow: support
    mode: static
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  payload:
    properties:
      entity_id:
        type: string
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
  payload:
    properties:
      entity_id:
        type: string
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), `
backend:
  id: backend-{vertical_id}
  type: generic
  role: backend
  model_tier: sonnet
  conversation_mode: session
  session_scope: flow
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
platform_version: ">=1.0.0"
packages:
  - path: extras
entity_schema:
  groups:
    - name: item
      fields:
        - name: item_id
          type: string
          primary: true
flows:
  - id: support
    flow: support
    mode: static
`)
	writeFlowActivationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFlowActivationFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  payload:
    properties:
      entity_id:
        type: string
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
  payload:
    properties:
      entity_id:
        type: string
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
  model_tier: sonnet
  conversation_mode: session
  session_scope: flow
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
			ToolsTier2:      []string{"schedule", "check_status"},
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
