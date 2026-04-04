package manager

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

type flowActivationTestBus struct {
	addedPaths   []string
	removedPairs []string
	published    []events.Event
}

type flowActivationTestInstanceStore struct {
	upserts []runtimepipeline.WorkflowInstance
}

type flowActivationTestStore struct {
	upserts []PersistedAgent
}

func (s *flowActivationTestInstanceStore) Upsert(_ context.Context, instance runtimepipeline.WorkflowInstance) error {
	s.upserts = append(s.upserts, instance)
	return nil
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
func (*flowActivationTestBus) Subscribe(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}
func (*flowActivationTestBus) Unsubscribe(string)                                          {}
func (*flowActivationTestBus) Store() runtimebus.EventStore                                { return nil }
func (*flowActivationTestBus) ResetInMemoryState() error                                   { return nil }
func (*flowActivationTestBus) LogRuntime(context.Context, runtimepipeline.RuntimeLogEntry) {}

func (b *flowActivationTestBus) AddFlowInstance(_ runtimecontracts.SystemNodeContract, instancePath string) error {
	b.addedPaths = append(b.addedPaths, instancePath)
	return nil
}

func (b *flowActivationTestBus) RemoveFlowInstance(templateID, instanceID string) error {
	b.removedPairs = append(b.removedPairs, templateID+"/"+instanceID)
	return nil
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

func TestActivateFlowInstanceAddsDerivedRouteTableInstance(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := NewAgentManager(bus, nil)
	bundle := testFlowBundle("")

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
		InitialState:   "queued",
	})
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
	am := NewAgentManager(bus, nil)
	bundle := testFlowBundle("task.started")

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
	})
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
	am := NewAgentManager(bus, nil)
	bundle := testFlowBundle("task.started")
	postCommit := make([]func(), 0, 1)
	ctx := runtimepipeline.WithPipelinePostCommitActions(context.Background(), &postCommit)

	err := am.ActivateFlowInstance(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
	})
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
	am := NewAgentManager(bus, nil)
	am.SetWorkflowInstanceStore(instances)
	bundle := testFlowBundle("task.started")

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
		Config: map[string]any{
			"name":     "alpha",
			"priority": 1,
		},
	})
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
	am := NewAgentManager(bus, nil)
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

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
	})
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
	am := NewAgentManager(bus, nil)
	bundle := testFlowBundle("")

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "review",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "review/inst-1",
	})
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
}

func TestDeactivateFlowInstanceUsesExactResolvedFlowPathForNestedTemplate(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := NewAgentManager(bus, nil)
	bundle := testNestedFlowBundle()

	err := am.ActivateFlowInstance(context.Background(), runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: semanticview.Wrap(bundle),
		TemplateID:     "grandchild",
		InstanceID:     "inst-1",
		EntityID:       "ent-1",
		FlowPath:       "child/grandchild/inst-1",
	})
	if err != nil {
		t.Fatalf("ActivateFlowInstance: %v", err)
	}
	if err := am.DeactivateFlowInstance(context.Background(), "grandchild", "inst-1", "child/grandchild/inst-1", "ent-1"); err != nil {
		t.Fatalf("DeactivateFlowInstance: %v", err)
	}
	if len(bus.removedPairs) != 1 || bus.removedPairs[0] != "child/grandchild/inst-1" {
		t.Fatalf("removed pairs = %#v, want [child/grandchild/inst-1]", bus.removedPairs)
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
	am := NewAgentManager(bus, nil, store)
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
	var payload map[string]any
	if err := json.Unmarshal(cfg.Config, &payload); err != nil {
		t.Fatalf("json.Unmarshal(config): %v", err)
	}
	if got := anySliceToStrings(payload["emit_events"]); len(got) != 1 || got[0] != "analyzer-flow/analysis.done" {
		t.Fatalf("emit_events = %#v, want [analyzer-flow/analysis.done]", got)
	}
	if len(store.upserts) != 1 || store.upserts[0].Config.ID != "analyzer" {
		t.Fatalf("persisted agents = %#v, want analyzer", store.upserts)
	}
}

func TestEnsureStaticAgentsForScopeRegistersRootAndFlowSubscriptions(t *testing.T) {
	bus := &flowActivationTestBus{}
	am := NewAgentManager(bus, nil)
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
	var payload map[string]any
	if err := json.Unmarshal(cfg.Config, &payload); err != nil {
		t.Fatalf("json.Unmarshal(config): %v", err)
	}
	if got := payload["max_turns_per_task"]; got != float64(7) {
		t.Fatalf("max_turns_per_task = %#v, want 7", got)
	}
	if got := anySliceToStrings(payload["tools"]); len(got) != 2 || got[0] != "schedule" || got[1] != "check_status" {
		t.Fatalf("tools = %#v, want [schedule check_status]", got)
	}
	if got := anySliceToStrings(payload["emit_events"]); len(got) != 2 || got[0] != "review/inst-1/task.completed" || got[1] != "review/inst-1/review.failed" {
		t.Fatalf("emit_events = %#v, want [review/inst-1/task.completed review/inst-1/review.failed]", got)
	}
	nativeTools, ok := payload["native_tools"].(map[string]any)
	if !ok || nativeTools["bash"] != true || nativeTools["file_io"] != true {
		t.Fatalf("native_tools = %#v, want bash/file_io true", payload["native_tools"])
	}
}

func anySliceToStrings(raw any) []string {
	items, ok := raw.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
