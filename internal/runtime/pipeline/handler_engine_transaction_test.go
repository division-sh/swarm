package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

func executeNodeContractHandlerWithHandoff(t *testing.T, pc *PipelineCoordinator, ctx context.Context, node runtimeidentity.ExecutableNode, handler runtimecontracts.SystemNodeEventHandler, trigger workflowTriggerContext, preview bool, deferFollowUp ...bool) (contractHandlerExecutionResult, error) {
	t.Helper()
	result, err := pc.executeNodeContractHandler(ctx, node, handler, trigger, preview, deferFollowUp...)
	if !preview && len(deferFollowUp) == 0 && result.Committed {
		err = errors.Join(err, pc.transferCommittedHandlerFollowUp(ctx, result.FollowUp, nil))
	}
	return result, err
}

func handlerTestRootIngress(id string, eventType events.EventType, sourceAgent, taskID string, payload json.RawMessage, chainDepth int, runID, parentEventID string, envelope events.EventEnvelope, createdAt time.Time) events.Event {
	if strings.TrimSpace(runID) == "" {
		runID = testPipelineRunID
	}
	source := eventtest.RootRoutingSource(runID)
	if route := envelope.Source.Normalized(); !route.Empty() {
		source = testWorkflowRoutingSource(route.FlowID, route.FlowInstance, route.EntityID)
	}
	candidate := eventtest.RunCreatingRootIngressWithRoutingSource(id, eventType, sourceAgent, taskID, payload, chainDepth, runID, parentEventID, envelope, source, createdAt)
	admitted, err := events.AdmitForPublish(candidate, events.AdmissionOptions{Now: time.Now().UTC()})
	if err != nil {
		panic(err)
	}
	return admitted.Event()
}

func handlerTestWorkflowEnvelope(flowID, flowInstance, entityID string) events.EventEnvelope {
	envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
	envelope = events.EnvelopeForFlowInstance(envelope, flowInstance)
	return events.EnvelopeForSourceRoute(envelope, events.RouteIdentity{
		FlowID:       flowID,
		FlowInstance: flowInstance,
		EntityID:     entityID,
	})
}

func handlerTestWorkflowModuleWithBundle(bundle *runtimecontracts.WorkflowContractBundle, flowID string, nodeIDs ...string) WorkflowModule {
	if bundle != nil && bundle.FlowTree.Root != nil {
		cloned := *bundle
		return &previewWorkflowModule{bundle: &cloned}
	}
	nodes := make(map[string]runtimecontracts.SystemNodeContract, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		node := runtimecontracts.SystemNodeContract{}
		if bundle != nil {
			if authored, ok := bundle.Nodes[nodeID]; ok {
				node = authored
			}
		}
		nodes[nodeID] = node
	}
	flow := runtimecontracts.FlowContractView{
		Paths:  runtimecontracts.FlowContractPaths{FlowPath: "."},
		Schema: runtimecontracts.FlowSchemaDocument{Name: flowID},
		Nodes:  nodes,
		Path:   ".",
	}
	if bundle != nil {
		flow.Events = bundle.Events
	}
	if bundle == nil {
		bundle = &runtimecontracts.WorkflowContractBundle{}
	} else {
		cloned := *bundle
		bundle = &cloned
	}
	bundle.FlowTree = flowmodel.Tree[runtimecontracts.FlowContractView]{
		Root: &flow,
		ByID: map[string]*runtimecontracts.FlowContractView{".": &flow},
	}
	if bundle.RootSchema == nil {
		bundle.RootSchema = &flow.Schema
	}
	if strings.TrimSpace(bundle.Semantics.Name) == "" {
		bundle.Semantics.Name = flowID
	}
	if strings.TrimSpace(bundle.Semantics.Version) == "" {
		bundle.Semantics.Version = "1"
	}
	return &previewWorkflowModule{bundle: bundle}
}

func canonicalPreviewWorkflowModuleForTest(module *previewWorkflowModule) *previewWorkflowModule {
	if module == nil || module.bundle == nil || module.bundle.FlowTree.Root != nil {
		return module
	}
	nodeIDs := make([]string, 0, len(module.bundle.Nodes))
	for nodeID := range module.bundle.Nodes {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)
	rooted := handlerTestWorkflowModuleWithBundle(module.bundle, module.bundle.Semantics.Name, nodeIDs...).(*previewWorkflowModule)
	rooted.workflowNodes = module.workflowNodes
	rooted.guardRegistry = module.guardRegistry
	return rooted
}

type recordingPipelineBus struct {
	mu                    sync.Mutex
	publishes             []events.Event
	publishContexts       []events.DeliveryContext
	runtimeLogs           []RuntimeLogEntry
	outboxIntents         []runtimeengine.EmitIntent
	publishErr            error
	publishInMutationHook func(context.Context, events.Event) error
	directPublishes       []events.Event
	directRecipients      [][]string
	directContexts        []events.DeliveryContext
	directInMutation      []bool
	outboxErr             error
	finalizeErr           error
	runtimeLogErr         error
}

func handlerEngineProjectNodeModule(t *testing.T) *previewWorkflowModule {
	t.Helper()
	module := &previewWorkflowModule{bundle: loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: handler-engine-test\nstages:\n  queued: {}\n",
		"entities.yaml": "test_entity: {}\n",
		"events.yaml":   "custom.trigger:\ncustom.emitted:\n  summary: EmitSummary?\n  flags: EmitFlags?\n  label: text?\n  kill_reason_missing: boolean?\n",
		"types.yaml":    "types:\n  EmitSummary:\n    entity_id: text\n    stage: text\n  EmitFlags:\n    ready: boolean\n",
		"nodes.yaml":    "node-a:\n  execution_type: system_node\n",
	})}
	return module
}

type recordingPipelineDispatcher struct {
	bus *recordingPipelineBus
}

func (b *recordingPipelineBus) Publish(ctx context.Context, evt events.Event) error {
	if b.publishErr != nil {
		return b.publishErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.publishes = append(b.publishes, evt)
	b.publishContexts = append(b.publishContexts, events.DeliveryContextFromContext(ctx))
	return nil
}

func (*recordingPipelineBus) SubscribeInternal(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (b *recordingPipelineBus) PublishDirect(ctx context.Context, evt events.Event, recipients []string) error {
	if b.publishErr != nil {
		return b.publishErr
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.directPublishes = append(b.directPublishes, evt)
	b.directRecipients = append(b.directRecipients, append([]string(nil), recipients...))
	b.directContexts = append(b.directContexts, events.DeliveryContextFromContext(ctx))
	b.directInMutation = append(b.directInMutation, false)
	return nil
}
func (*recordingPipelineBus) ResolveSubscribedRecipients(string) []string { return nil }
func (b *recordingPipelineBus) LogRuntime(_ context.Context, entry RuntimeLogEntry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.runtimeLogs = append(b.runtimeLogs, entry)
	return b.runtimeLogErr
}
func (b *recordingPipelineBus) EngineDispatcher() runtimeengine.PostCommitDispatcher {
	return recordingPipelineDispatcher{bus: b}
}

func (d recordingPipelineDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	for _, intent := range intents {
		intentCtx := events.WithDeliveryContext(ctx, intent.Context)
		if len(intent.Recipients) > 0 {
			if err := d.bus.PublishDirect(intentCtx, intent.Event, intent.Recipients); err != nil {
				return err
			}
			continue
		}
		if err := d.bus.Publish(intentCtx, intent.Event); err != nil {
			return err
		}
	}
	return nil
}

func (b *recordingPipelineBus) publishedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.publishes)
}

func (b *recordingPipelineBus) publishedEvent(i int) events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.publishes[i]
}

func (b *recordingPipelineBus) outboxCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.outboxIntents)
}

func (b *recordingPipelineBus) runtimeLogEntries() []RuntimeLogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]RuntimeLogEntry, len(b.runtimeLogs))
	copy(out, b.runtimeLogs)
	return out
}

func TestLogComputeModuleReplayEvidenceEmitsRuntimeLogCarrier(t *testing.T) {
	bus := &recordingPipelineBus{}
	evt := eventtest.PersistedProjection(
		"evt-1",
		events.EventType("render.requested"),
		"tester",
		"",
		json.RawMessage(`{"component":"api"}`),
		0,
		eventtest.UUID("persisted-projection-run"),
		"",
		events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: "ent-1"}),
		time.Now().UTC(),
	)
	trace := runtimeengine.ComputeModuleTrace{
		ModuleID:     "structured_renderer",
		RowID:        "render_bundle",
		Kind:         "wasm",
		ABI:          computemodule.ABI,
		Entry:        computemodule.DefaultEntry,
		Digest:       "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InputHash:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Outcome:      computemodule.ReplayOutcomeSuccess,
		OutputHash:   "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		FuelConsumed: 7,
		Limits: computemodule.ReplayLimits{
			Fuel:        100,
			MemoryPages: 17,
			OutputBytes: 1024,
		},
		Engine: "wasmtime-go:v46.0.0",
		Arch:   "arm64",
	}
	logComputeModuleReplayEvidence(testAuthorActivityContext(t, context.Background()), bus, "renderer", evt, []runtimeengine.ComputeModuleTrace{trace})
	logs := bus.runtimeLogEntries()
	if len(logs) != 1 {
		t.Fatalf("runtime logs = %#v, want one replay evidence carrier", logs)
	}
	entry := logs[0]
	if entry.Component != "compute_module" || entry.Action != computemodule.ReplayEvidenceAction || entry.EventID != "evt-1" || entry.EntityID != "ent-1" {
		t.Fatalf("runtime log entry = %#v, want compute_module replay action/event/entity", entry)
	}
	detail, ok := entry.Detail.(map[string]any)
	if !ok {
		t.Fatalf("detail = %#v, want map", entry.Detail)
	}
	envelopes, err := computemodule.DecodeReplayEvidenceDetail(detail)
	if err != nil {
		t.Fatalf("DecodeReplayEvidenceDetail: %v", err)
	}
	if len(envelopes) != 1 || envelopes[0].Normalized() != trace.Normalized() {
		t.Fatalf("decoded envelopes = %#v, want %#v", envelopes, trace.Normalized())
	}
}

func VerifyNativeExecuteNodeContractHandlerLogsComputeModuleReplayEvidenceBeforeFailureReturnForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	source := pipelineSourceWithStructuredRendererModule(t, map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []any{"missing"},
		"properties": map[string]any{
			"missing": map[string]any{"type": "string"},
		},
	})
	bundle, found := semanticview.Bundle(source)
	if !found {
		t.Fatal("native renderer requires its compiled artifact")
	}
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	bus := observeNativePipelineDeliveryBusForTest(t, pc)
	run := runtimecorrelation.RunIDFromContext(ctx)
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "render.requested", "operator", "", mustJSON(map[string]any{
		"component": "api", "owner": "platform", "language": "go",
		"files": []any{"main.go", "README.md", "service.yaml"},
	}), 0, run, events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{FlowID: ".", FlowInstance: run, EntityID: run}), time.Now().UTC())
	ctx, state := prepareNativeConstructorHandlerDeliveryForTest(t, fixture, pc, ctx, ".", "node-a", event)
	_, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a"), runtimecontracts.SystemNodeEventHandler{
		Compute: &runtimecontracts.ComputeSpec{
			Operation: runtimecontracts.ComputeOpModule,
			StoreAs:   "computed.rendered_bundle",
			Module: &runtimecontracts.ComputeModuleSpec{
				RowID:  "render_bundle",
				Module: "structured_renderer",
				Into:   "computed.rendered_bundle",
				Input: map[string]string{
					"component": "payload.component",
					"owner":     "payload.owner",
					"language":  "payload.language",
					"files":     "payload.files",
				},
			},
		},
	}, workflowTriggerContext{
		Event: event, State: state,
	})
	if err == nil {
		t.Fatal("executeNodeContractHandler error = nil, want output-schema failure")
	}
	var moduleErr *computemodule.Error
	if !errors.As(err, &moduleErr) || moduleErr.Code != computemodule.CodeABI {
		t.Fatalf("executeNodeContractHandler error = %#v, want compute module ABI failure", err)
	}
	logs := bus.runtimeLogEntries()
	if len(logs) != 2 || logs[0].Component != "compute_module" || logs[0].Action != computemodule.ReplayEvidenceAction || logs[1].Action != "handler_error" || logs[1].Failure == nil {
		t.Fatalf("runtime logs = %#v, want one compute replay before native handler failure settlement", logs)
	}
	detail, ok := logs[0].Detail.(map[string]any)
	if !ok {
		t.Fatalf("detail = %#v, want map", logs[0].Detail)
	}
	envelopes, err := computemodule.DecodeReplayEvidenceDetail(detail)
	if err != nil {
		t.Fatalf("DecodeReplayEvidenceDetail: %v", err)
	}
	if len(envelopes) != 1 {
		t.Fatalf("decoded envelopes = %#v, want one failure envelope", envelopes)
	}
	trace := envelopes[0].Normalized()
	if trace.Outcome != computemodule.ReplayOutcomeFailure || trace.ErrorCode != string(computemodule.CodeABI) || trace.OutputHash != "" {
		t.Fatalf("failure trace = %#v, want ABI failure outcome without output hash", trace)
	}
}

func pipelineSourceWithStructuredRendererModule(t *testing.T, outputSchema map[string]any) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	raw, err := os.ReadFile(filepath.Join("..", "computemodule", "testdata", "structured_renderer.wasm"))
	if err != nil {
		t.Fatal(err)
	}
	modulePath := filepath.Join(root, "modules", "structured_renderer.wasm")
	if err := os.MkdirAll(filepath.Dir(modulePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(modulePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "schema.yaml"), []byte("name: render\nstages:\n  ready: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "nodes.yaml"), []byte("node-a:\n  execution_type: system_node\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	module := runtimecontracts.PolicyModule{
		Kind:   "wasm",
		Path:   "modules/structured_renderer.wasm",
		ABI:    computemodule.ABI,
		Entry:  computemodule.DefaultEntry,
		Digest: "sha256:" + hex.EncodeToString(sum[:]),
		InputSchema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"required":             []any{"component", "owner", "language", "files"},
			"properties": map[string]any{
				"component": map[string]any{"type": "string"},
				"owner":     map[string]any{"type": "string"},
				"language":  map[string]any{"type": "string"},
				"files": map[string]any{
					"type":  "array",
					"items": map[string]any{"type": "string"},
				},
			},
		},
		OutputSchema: outputSchema,
		Limits: runtimecontracts.PolicyModuleLimits{
			Gas:         5_000_000,
			MemoryPages: 17,
			OutputBytes: 1024,
		},
	}
	declaration := map[string]any{
		"handler_type": module.Kind, "path": module.Path, "abi": module.ABI, "entry": module.Entry, "digest": module.Digest,
		"input_schema": module.InputSchema, "output_schema": module.OutputSchema,
		"limits": map[string]any{"gas": module.Limits.Gas, "memory_pages": module.Limits.MemoryPages, "output_bytes": module.Limits.OutputBytes},
	}
	wire, err := json.Marshal(map[string]any{"structured_renderer": declaration})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "tools.yaml"), wire, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "entities.yaml"), []byte("render_entity: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte("render.requested:\n  component: text\n  owner: text\n  language: text\n  files: list<text>\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	artifact, err := sourceartifact.AdmitDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleFromArtifact("../../..", artifact, "", runtimecontracts.WorkflowContractLoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func TestPipelineCoordinatorPublish_ReturnsBusPublishError(t *testing.T) {
	wantErr := errors.New("bus publish failed")
	pc := &PipelineCoordinator{
		bus: &recordingPipelineBus{publishErr: wantErr},
	}

	inbound := handlerTestRootIngress(uuid.NewString(), events.EventType("custom.received"), "gateway", "task-publish-error", []byte(`{}`), 0, uuid.NewString(), "", events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"), time.Now().UTC())
	ctx := runtimecorrelation.WithInboundEvent(testAuthorActivityContext(t, context.Background()), inbound)
	err := pc.publish(ctx, "custom.emitted", "ent-1", map[string]any{"ok": true})
	if !errors.Is(err, wantErr) {
		t.Fatalf("publish error = %v, want %v", err, wantErr)
	}
}

func VerifyExecuteNodeContractHandlerUsesConstructedEntityIdentityForWritesAndEmitForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "name: runtime-test\n",
		"scoring/schema.yaml": `name: scoring
stages:
  queued: {}
`,
		"scoring/entities.yaml": `
subject:
  name: text
`,
		"scoring/events.yaml": "custom.emitted:\n  label: text\n",
		"scoring/nodes.yaml": `
node-a:
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	module := canonicalPreviewWorkflowModuleForTest(&previewWorkflowModule{bundle: bundle})
	fixture := open(t, module.SemanticSource())
	pc := fixture.NewCoordinator(PipelineCoordinatorOptions{Module: module})
	ctx := runtimecorrelation.WithRunID(fixture.Context, testPipelineRunID)
	if err := fixture.RequireRun(ctx, testPipelineRunID); err != nil {
		t.Fatal(err)
	}
	instance := constructedScenarioInstanceForTest(t, module.SemanticSource(), ctx, "scoring")
	if err := fixture.Construct(ctx, instance); err != nil {
		t.Fatal(err)
	}
	before, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef))
	if err != nil || !found || before.EntityID != instance.EntityID || before.CurrentState != instance.CurrentState || !reflect.DeepEqual(cloneMap(before.Fields), cloneMap(instance.Fields)) || before.Revision != 1 {
		t.Fatalf("native child construction changed authored pre-state: found=%t err=%v stored=%+v", found, err, before)
	}
	planner, ok := pc.bus.(EngineMutationPublicationPlanner)
	if !ok {
		t.Fatal("native child handler requires the original publication planner")
	}
	bus := &nativeHandlerDispatchObservationForTest{Bus: pc.bus, EngineMutationPublicationPlanner: planner}
	pc.bus = bus
	event := eventtest.ExistingRunRootIngressWithRoutingSource(uuid.NewString(), "custom.trigger", "", "", nil, 0, testPipelineRunID, events.EventEnvelope{}, eventtest.RootRoutingSource(testPipelineRunID), time.Now().UTC())
	node := pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a")
	route := events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "scoring", FlowInstance: instance.StorageRef, EntityID: instance.EntityID}),
	}
	fixture.Publish(ctx, event, route)
	event = eventtest.TargetRouted(event, route.Target.Route())
	ctx, stop := claimNativeWorkflowHandlerPublicationForTest(t, pc, ctx, event, route)
	defer func() {
		if err := stop(); err != nil {
			t.Error(err)
		}
	}()
	state := mustCurrentWorkflowState(t, pc, ctx, runtimeflowidentity.StoredRoute("scoring", instance.InstanceID, instance.StorageRef), instance.EntityID)

	result, err := executeNodeContractHandlerWithHandoff(t, pc, ctx, node, runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "name", Value: runtimecontracts.LiteralExpression("Updated Entity")},
			},
		},
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted", Fields: map[string]runtimecontracts.ExpressionValue{
			"label": runtimecontracts.CELExpression(`has(entity.name) ? entity.name : "missing"`),
		}},
	}, workflowTriggerContext{
		Event: event, State: state,
	}, false)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	if got := bus.publishedEvent(0).EntityID(); got != FlowInstanceEntityID("scoring") {
		t.Fatalf("emitted entity_id=%s, want constructed scoring identity", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil || payload["label"] != "Updated Entity" {
		t.Fatalf("updated constructed field did not reach emit.fields: payload=%#v err=%v", payload, err)
	}
	stored, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef))
	if err != nil || !found || stored.EntityID != FlowInstanceEntityID("scoring") || stored.Fields["name"] != "Updated Entity" || stored.Revision != before.Revision+1 {
		t.Fatalf("constructed child write was not durable: found=%t err=%v stored=%+v", found, err, stored)
	}
	persisted, err := fixture.PublishedEvent(ctx, bus.publishedEvent(0).ID())
	if err != nil || !reflect.DeepEqual(persisted, bus.publishedEvent(0)) {
		t.Fatalf("child emission diverged from original selected receipt: err=%v persisted=%+v", err, persisted)
	}
}

func VerifyNativeExecuteNodeContractHandlerRejectsMissingWriteSourceBeforeEmitForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, nativeEmitPersistenceBundleForTest(t), open)
	bus := observeNativePipelineDeliveryBusForTest(t, pc)
	instance := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
	runID, entityID := runtimecorrelation.RunIDFromContext(ctx), instance.EntityID
	evt := nativeWorkflowJoinEventForTest(ctx, ".", runID, entityID, "research.completed", mustJSON(map[string]any{}), time.Now().UTC())
	node := pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a")
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(evt.Envelope().Target)}
	if err := fixture.PublishNode(ctx, evt, route); err != nil {
		t.Fatal(err)
	}
	ctx = withWorkflowNodeDeliveryRoute(ctx, route)
	_, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "business_brief"},
			},
			SourceEvent: "research.completed",
		},
		Emit:       runtimecontracts.EmitSpec{Event: "spec.requested"},
		AdvancesTo: "mvp_speccing",
	}, workflowTriggerContext{
		Event: evt,

		State: WorkflowState{
			EntityID: entityID,
			Stage:    WorkflowStateID("researching"),
			Metadata: map[string]any{},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "business_brief") || !strings.Contains(err.Error(), "absent") {
		t.Fatalf("executeNodeContractHandler error = %v, want missing source rejection", err)
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("published count = %d, want 0 after missing source rejection", got)
	}

	instance, ok, loadErr := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
	if loadErr != nil {
		t.Fatalf("load workflow instance: %v", loadErr)
	}
	if !ok {
		t.Fatal("expected seeded workflow instance to remain available")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "researching" {
		t.Fatalf("current_state = %q, want researching after rejected mutation", got)
	}
	if _, ok := instance.Fields["business_brief"]; ok {
		t.Fatalf("missing source created business_brief: %#v", instance.Fields)
	}
}

func TestVerifyPreparedWorkflowEmitPersistenceRejectsMissingExpectedField(t *testing.T) {
	err := verifyPreparedWorkflowEmitPersistence(WorkflowInstance{}, runtimeengine.EmitPersistencePrerequisites{
		Fields: []runtimeengine.EmitPersistenceFieldPrerequisite{{Field: "business_brief"}},
	})
	if !errors.Is(err, runtimeengine.ErrEmitPersistencePrerequisite) {
		t.Fatalf("missing expected field error = %v", err)
	}
}

func VerifyNativeExecuteNodeContractHandlerPublishesAfterPersistencePrerequisiteFieldSucceedsForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, nativeEmitPersistenceBundleForTest(t), open)
	bus := observeNativePipelineDeliveryBusForTest(t, pc)
	instance := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
	runID, entityID := runtimecorrelation.RunIDFromContext(ctx), instance.EntityID
	evt := nativeWorkflowJoinEventForTest(ctx, ".", runID, entityID, "research.completed", mustJSON(map[string]any{"business_brief": map[string]any{"summary": "validated"}}), time.Now().UTC())
	node := pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a")
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(evt.Envelope().Target)}
	if err := fixture.PublishNode(ctx, evt, route); err != nil {
		t.Fatal(err)
	}
	ctx = withWorkflowNodeDeliveryRoute(ctx, route)
	result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, node, runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "business_brief"},
			},
			SourceEvent: "research.completed",
		},
		Emit:       runtimecontracts.EmitSpec{Event: "spec.requested"},
		AdvancesTo: "mvp_speccing",
	}, workflowTriggerContext{
		Event: evt,

		State: WorkflowState{
			EntityID: entityID,
			Stage:    WorkflowStateID("researching"),
			Metadata: map[string]any{},
		},
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("published count = %d, want 1", got)
	}
	if got := string(bus.publishedEvent(0).Type()); got != "spec.requested" {
		t.Fatalf("published type = %q, want spec.requested", got)
	}

	instance, ok, loadErr := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, runID))
	if loadErr != nil {
		t.Fatalf("load workflow instance: %v", loadErr)
	}
	if !ok {
		t.Fatal("expected workflow instance to persist")
	}
	if got := strings.TrimSpace(instance.CurrentState); got != "mvp_speccing" {
		t.Fatalf("current_state = %q, want mvp_speccing", got)
	}
	brief, ok := instance.Fields["business_brief"].(map[string]any)
	if !ok || brief["summary"] != "validated" {
		t.Fatalf("business_brief = %#v, want persisted payload", instance.Fields["business_brief"])
	}
}

func handlerDataAccumulationModule(t *testing.T) *previewWorkflowModule {
	t.Helper()
	return &previewWorkflowModule{bundle: loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: validation\nstages:\n  queued: {}\n",
		"entities.yaml": "test_entity:\n  revision_count: integer\n  kill_reason: text\n  kill_reason_missing: boolean\n",
		"events.yaml":   "validation.spec_requested:\n",
		"nodes.yaml":    "node-a:\n  execution_type: system_node\n",
	})}
}

func VerifyExecuteNodeContractHandlerPersistsArithmeticDataAccumulationExpressionForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	const entityID = testPipelineRunID
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, open, handlerDataAccumulationModule(t), entityID, map[string]any{"revision_count": 0})
	trigger := handlerTestRootIngress("", events.EventType("validation.spec_requested"), "", "", nil, 0, testPipelineRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{})
	_, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "revision_count", Value: runtimecontracts.CELExpression("entity.revision_count + 1")},
			},
		},
	}, workflowTriggerContext{
		Event: trigger,
		State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(testPipelineRunID), entityID),
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}

	instance, ok, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, testPipelineRunID))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after declarative write")
	}
	switch got := instance.Fields["revision_count"].(type) {
	case int:
		if got != 1 {
			t.Fatalf("revision_count = %d, want 1", got)
		}
	case float64:
		if got != 1 {
			t.Fatalf("revision_count = %v, want 1", got)
		}
	case int64:
		if got != 1 {
			t.Fatalf("revision_count = %v, want 1", got)
		}
	default:
		t.Fatalf("revision_count = %#v (%T), want 1", instance.Fields["revision_count"], instance.Fields["revision_count"])
	}
}

func VerifyExecuteNodeContractHandlerFailsClosedOnDataAccumulationCELRuntimeErrorForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	const entityID = testPipelineRunID
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, open, handlerDataAccumulationModule(t), entityID, nil)
	trigger := handlerTestRootIngress("", events.EventType("validation.spec_requested"), "", "", nil, 0, testPipelineRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{})
	_, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "revision_count", Value: runtimecontracts.CELExpression("entity.revision_count + payload.missing_delta")},
			},
		},
	}, workflowTriggerContext{
		Event: trigger,
		State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(testPipelineRunID), entityID),
	})
	if err == nil {
		t.Fatal("expected executeNodeContractHandler to fail on data accumulation CEL runtime error")
	}
	if !strings.Contains(err.Error(), "data_accumulation target revision_count") && !strings.Contains(err.Error(), "data_accumulation target entity.revision_count") {
		t.Fatalf("error = %v, want data_accumulation target context", err)
	}

	instance, ok, loadErr := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, testPipelineRunID))
	if loadErr != nil {
		t.Fatalf("load workflow instance: %v", loadErr)
	}
	if !ok {
		t.Fatal("expected workflow instance to remain available")
	}
	if _, exists := instance.Fields["revision_count"]; exists {
		t.Fatalf("revision_count unexpectedly persisted after CEL runtime error: %#v", instance.Fields["revision_count"])
	}
}

func VerifyExecuteNodeContractHandlerPersistsExplicitAbsenceDecisionForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	const entityID = testPipelineRunID
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, open, handlerDataAccumulationModule(t), entityID, nil)
	trigger := handlerTestRootIngress("", events.EventType("validation.spec_requested"), "", "", nil, 0, testPipelineRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{})
	_, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{
				{TargetField: "kill_reason_missing", Value: runtimecontracts.CELExpression("!has(entity.kill_reason)")},
			},
		},
	}, workflowTriggerContext{
		Event: trigger,
		State: mustCurrentWorkflowState(t, pc, ctx, testWorkflowInstanceRoute(testPipelineRunID), entityID),
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}

	instance, ok, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, testPipelineRunID))
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("workflow instance missing after declarative write")
	}
	if got := instance.Fields["kill_reason_missing"]; got != true {
		t.Fatalf("kill_reason_missing = %#v, want true for unassigned field", got)
	}
}

func TestHandlerExecutionStateSnapshotCreateEntityIncludesInitialStateAndDefaults(t *testing.T) {
	snapshot, err := handlerExecutionStateSnapshot(runtimecontracts.SystemNodeEventHandler{CreateEntity: true}, "ent-child", WorkflowState{
		EntityID: "ent-parent",
		Stage:    WorkflowStateID("queued"),
		Metadata: map[string]any{
			"revision_count": 0,
			"is_duplicate":   false,
		},
	}, "validation", "v-test")
	if err != nil {
		t.Fatalf("handlerExecutionStateSnapshot: %v", err)
	}

	if got := snapshot.EntityID.String(); got != "ent-child" {
		t.Fatalf("snapshot entity_id = %q, want ent-child", got)
	}
	if snapshot.CurrentState != "queued" {
		t.Fatalf("snapshot current_state = %q, want queued", snapshot.CurrentState)
	}
	if snapshot.WorkflowName != "validation" {
		t.Fatalf("snapshot workflow_name = %q, want validation", snapshot.WorkflowName)
	}
	if snapshot.WorkflowVersion != "v-test" {
		t.Fatalf("snapshot workflow_version = %q, want v-test", snapshot.WorkflowVersion)
	}
	if snapshot.Fields == nil {
		t.Fatal("snapshot fields = nil, want persisted fields")
	}
	if got := snapshot.Fields["revision_count"]; got != 0 {
		t.Fatalf("snapshot revision_count = %#v, want 0", got)
	}
	if got := snapshot.Fields["is_duplicate"]; got != false {
		t.Fatalf("snapshot is_duplicate = %#v, want false", got)
	}
}

func VerifyNativeExecuteNodeContractHandlerCreateEntityPersistsSchemaInitialValuesBeforeGuardReadsForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "name: runtime-test\n",
		"validation/schema.yaml": `name: validation
stages:
  queued: {}
`,
		"validation/entities.yaml": `
validation_entity:
  revision_count:
    type: integer
    initial: 0
  kill_reason: text
`,
		"validation/events.yaml": "candidate.discovered:\nentity.created:\n  revision_count: integer\n",
		"validation/nodes.yaml": `
node-a:
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	bus := &nativePipelineDeliveryBusObservationForTest{Bus: pc.bus, EngineMutationPublicationPlanner: pc.bus.(EngineMutationPublicationPlanner)}
	pc.bus = bus
	trigger := nativeWorkflowJoinEventForTest(ctx, "validation", "validation", FlowInstanceEntityID("validation"), "candidate.discovered", nil, time.Now().UTC())
	ctx, state := prepareNativeConstructorHandlerDeliveryForTest(t, fixture, pc, ctx, "validation", "node-a", trigger)
	result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a"), runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{Check: `entity.revision_count == 0 && !has(entity.kill_reason)`},
		Emit: runtimecontracts.EmitSpec{
			Event: "entity.created",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"revision_count": runtimecontracts.CELExpression("entity.revision_count"),
			},
		},
	}, workflowTriggerContext{
		Event: trigger,
		State: state,
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	emitted := bus.publishedEvent(0)
	entityID := emitted.EntityID()
	if entityID == "" {
		t.Fatal("expected emitted event to carry created entity id")
	}
	var payload map[string]any
	if err := json.Unmarshal(emitted.Payload(), &payload); err != nil {
		t.Fatalf("unmarshal emitted payload: %v", err)
	}
	if got := payload["revision_count"]; got != float64(0) && got != 0 {
		t.Fatalf("emitted payload revision_count = %#v, want 0", got)
	}

	instance, ok, err := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, emitted.FlowInstance()))
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected created entity to persist")
	}
	if got := instance.Fields["revision_count"]; got != int64(0) {
		t.Fatalf("persisted revision_count = %#v, want 0", got)
	}
	if _, present := instance.Fields["kill_reason"]; present {
		t.Fatalf("creation synthesized an uninitialized field: %#v", instance.Fields)
	}
	assertNativeCreatedChildFlowIdentityCoherentForTest(t, fixture, ctx, "validation", entityID, emitted, instance)

	initialMutations := map[string][3]string{}
	for _, row := range fixture.MutationHistory(ctx, runtimecorrelation.RunIDFromContext(ctx), entityID) {
		if row.WriterType == "platform" && row.WriterID == "entity_initial_value" {
			initialMutations[row.Domain+":"+row.Path] = [3]string{row.WriterType, row.WriterID, row.HandlerStep}
		}
	}
	if got, ok := initialMutations["authored_field:revision_count"]; !ok {
		t.Fatalf("expected initial-value mutation for revision_count, got %#v", initialMutations)
	} else if got[2] != "create_entity" {
		t.Fatalf("revision_count initial mutation metadata = %#v, want handler_step create_entity", got)
	}
}

func VerifyNativeExecuteNodeContractHandlerQueryEntitiesGuardUsesWorkflowContextForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "name: runtime-test\n",
		"validation/schema.yaml": `name: validation
instance: validation_id
stages:
  queued: {}
`,
		"validation/entities.yaml": `
validation_request:
  validation_id: uuid
  request_id: text?
`,
		"validation/events.yaml": `
request.received:
  request_id: text
request.accepted:
  request_id: text
`,
		"validation/nodes.yaml": `
node-a:
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	bus := observeNativePipelineDeliveryBusForTest(t, pc)
	const otherRunID = "88888888-8888-8888-8888-888888888888"
	if err := fixture.RequireRun(ctx, otherRunID); err != nil {
		t.Fatal(err)
	}
	otherCtx := runtimecorrelation.WithRunID(fixture.Context, otherRunID)
	constructNativeQueryEntitiesGuardInstanceForTest(t, fixture, pc, ctx, "11111111-1111-1111-1111-111111111111", "req-existing")
	constructNativeQueryEntitiesGuardInstanceForTest(t, fixture, pc, otherCtx, "22222222-2222-2222-2222-222222222222", "req-cross-run")

	handler := runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{Check: `query_entities(request_id == payload.request_id).count == 0`},
		Emit: runtimecontracts.EmitSpec{
			Event: "request.accepted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"request_id": runtimecontracts.RefExpression("payload.request_id"),
			},
		},
	}
	runHandler := func(entityID, requestID string) error {
		instance := constructNativeQueryEntitiesGuardInstanceForTest(t, fixture, pc, ctx, entityID, "")
		event := nativeWorkflowJoinEventForTest(ctx, "validation", instance.StorageRef, instance.EntityID, "request.received", mustJSON(map[string]any{"request_id": requestID}), time.Now().UTC())
		node := pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a")
		route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(event.Envelope().Target)}
		if err := fixture.PublishNode(ctx, event, route); err != nil {
			return err
		}
		deliveryCtx := withWorkflowNodeDeliveryRoute(ctx, route)
		state := mustCurrentWorkflowState(t, pc, deliveryCtx, runtimeflowidentity.StoredRoute("validation", instance.InstanceID, instance.StorageRef), instance.EntityID)
		_, err := executeNativeClaimedPipelineHandlerForTest(t, pc, deliveryCtx, node, handler, workflowTriggerContext{
			Event: event,
			State: state,
		})
		return err
	}

	if err := runHandler("33333333-3333-3333-3333-333333333333", "req-new"); err != nil {
		t.Fatalf("execute handler for new request: %v", err)
	}
	if err := runHandler("44444444-4444-4444-4444-444444444444", "req-cross-run"); err != nil {
		t.Fatalf("execute handler for cross-run request: %v", err)
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("published count after accepted requests = %d, want 2", got)
	}
	if err := runHandler("55555555-5555-5555-5555-555555555555", "req-existing"); err != nil {
		t.Fatalf("execute handler for duplicate request: %v", err)
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("published count after duplicate request = %d, want unchanged 2", got)
	}
}

func VerifyNativeExecuteNodeContractHandlerCreateEntityPersistsNonValidationChildFlowIdentityForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "name: runtime-test\n",
		"review/schema.yaml": `name: review
stages:
  queued: {}
`,
		"review/entities.yaml": `
review_entity:
  status:
    type: text
    initial: pending
`,
		"review/events.yaml": "candidate.ready:\nreview.created:\n  status: text\n",
		"review/nodes.yaml": `
node-a:
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	bus := &nativePipelineDeliveryBusObservationForTest{Bus: pc.bus, EngineMutationPublicationPlanner: pc.bus.(EngineMutationPublicationPlanner)}
	pc.bus = bus
	trigger := nativeWorkflowJoinEventForTest(ctx, "review", "review", FlowInstanceEntityID("review"), "candidate.ready", nil, time.Now().UTC())
	ctx, state := prepareNativeConstructorHandlerDeliveryForTest(t, fixture, pc, ctx, "review", "node-a", trigger)
	result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a"), runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{Check: `entity.status == "pending"`},
		Emit: runtimecontracts.EmitSpec{
			Event: "review.created",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"status": runtimecontracts.CELExpression("entity.status"),
			},
		},
	}, workflowTriggerContext{
		Event: trigger,
		State: state,
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	emitted := bus.publishedEvent(0)
	entityID := emitted.EntityID()
	if entityID == "" {
		t.Fatal("expected emitted event to carry created entity id")
	}
	instance, ok, err := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, emitted.FlowInstance()))
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected created entity to persist")
	}
	if got := instance.Fields["status"]; got != "pending" {
		t.Fatalf("persisted status = %#v, want pending", got)
	}
	assertNativeCreatedChildFlowIdentityCoherentForTest(t, fixture, ctx, "review", entityID, emitted, instance)
}

func VerifyNativeExecuteNodeContractHandlerCreateEntityAllowsLaterClearOfSchemaInitialValueForTest(t *testing.T, backend string, open pipelineDeliveryNativeOpenerForTest) {
	source := loadWorkflowTempSource(t, map[string]string{

		"schema.yaml": "name: runtime-test\n",
		"validation/schema.yaml": `name: validation
stages:
  queued: {}
`,
		"validation/entities.yaml": `
validation_entity:
  revision_count:
    type: integer?
    initial: 0
`,
		"validation/events.yaml": "candidate.discovered:\nentity.created:\n",
		"validation/nodes.yaml": `
node-a:
  execution_type: system_node
`,
	})
	bundle, ok := semanticview.Bundle(source)
	if !ok {
		t.Fatal("expected temp workflow bundle")
	}
	fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
	bus := &nativePipelineDeliveryBusObservationForTest{Bus: pc.bus, EngineMutationPublicationPlanner: pc.bus.(EngineMutationPublicationPlanner)}
	pc.bus = bus
	trigger := nativeWorkflowJoinEventForTest(ctx, "validation", "validation", FlowInstanceEntityID("validation"), "candidate.discovered", nil, time.Now().UTC())
	ctx, state := prepareNativeConstructorHandlerDeliveryForTest(t, fixture, pc, ctx, "validation", "node-a", trigger)
	result, err := executeNativeClaimedPipelineHandlerForTest(t, pc, ctx, pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a"), runtimecontracts.SystemNodeEventHandler{
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{Writes: []runtimecontracts.WorkflowDataWrite{
			{Operation: runtimecontracts.WorkflowDataOperationClear, TargetRef: "entity.revision_count"},
		}},
		Emit: runtimecontracts.EmitSpec{Event: "entity.created"},
	}, workflowTriggerContext{
		Event: trigger,
		State: state,
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	emitted := bus.publishedEvent(0)
	entityID := emitted.EntityID()
	if entityID == "" {
		t.Fatal("expected emitted event to carry created entity id")
	}

	instance, ok, err := pc.workflowStore.Load(ctx, testRunScopedWorkflowInstanceFromContext(ctx, emitted.FlowInstance()))
	if err != nil {
		t.Fatalf("workflowStore.Load: %v", err)
	}
	if !ok {
		t.Fatal("expected created entity to persist")
	}
	if _, ok := instance.Fields["revision_count"]; ok {
		t.Fatalf("persisted revision_count = %#v, want field cleared", instance.Fields["revision_count"])
	}

	var sawInitial bool
	for _, row := range fixture.MutationHistory(ctx, runtimecorrelation.RunIDFromContext(ctx), entityID) {
		if row.Domain == "authored_field" && row.Path == "revision_count" && row.WriterType == "platform" && row.WriterID == "entity_initial_value" && row.HandlerStep == "create_entity" {
			sawInitial = true
		}
	}
	if !sawInitial {
		t.Fatal("expected initial-value mutation for revision_count before later clear")
	}
}

func VerifyNativeExecuteNodeContractHandlerReturnsTerminalRejectForTerminalEntityForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			bundle := loadWorkflowTempBundle(t, map[string]string{
				"schema.yaml":   "name: demo\nstages:\n  queued: {}\n  done: {final: true}\n",
				"entities.yaml": "test_entity: {}\n",
				"nodes.yaml":    "node-a:\n  execution_type: system_node\n  event_handlers:\n    finish:\n      advances_to: done\n    custom.trigger: {}\n",
				"events.yaml":   "finish:\ncustom.trigger:\n",
			})
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			instance := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
			finish := nativeWorkflowJoinEventForTest(ctx, ".", instance.StorageRef, instance.EntityID, "finish", []byte(`{}`), time.Now().UTC())
			dispatchNativeWorkflowJoinEventForTest(t, fixture, pc, ctx, finish, "node-a")
			instance, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef))
			if err != nil || !found || instance.CurrentState != "done" {
				t.Fatalf("real terminal control: %#v/%t/%v", instance, found, err)
			}
			event := nativeWorkflowJoinEventForTest(ctx, ".", instance.StorageRef, instance.EntityID, "custom.trigger", []byte(`{}`), time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "node-a")), Target: events.MustExistingEntityTarget(event.TargetRoute())}
			if err := fixture.PublishNode(ctx, event, route); err != nil {
				t.Fatal(err)
			}
			ctx = withWorkflowNodeDeliveryRoute(ctx, route)
			state := mustCurrentWorkflowState(t, pc, ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef).Route, instance.EntityID)
			counts := fixture.Transactions()
			result, err := executeNodeContractHandlerWithHandoff(t, pc, ctx, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{}, workflowTriggerContext{Event: event, State: state}, false)
			var refusal *TerminalReceiverError
			if !errors.As(err, &refusal) || refusal.Stage != "done" || refusal.FlowID != "." || result.Committed {
				t.Fatalf("terminal target admission result=%#v err=%v, want exact terminal refusal before mutation", result, err)
			}
			after, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, instance.StorageRef))
			if err != nil || !found || !reflect.DeepEqual(instance, after) || fixture.Transactions().Claims != counts.Claims || bus.publishedCount() != 0 {
				t.Fatalf("terminal refusal changed state, acquired a claim or published: %#v/%t/%v", after, found, err)
			}
		})
	}
}

func VerifyNativeExecuteNodeHandlerPlanResult_NestedPackageRootConnectDoesNotAuthorizeRepositoryRootHandlerForTest(t *testing.T, open pipelineDeliveryNativeOpenerForTest) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			source := loadWorkflowFixtureSource(t, "test-nested-three-levels")
			bundle, ok := semanticview.Bundle(source)
			if !ok {
				t.Fatal("expected workflow fixture bundle")
			}
			fixture, pc, ctx := nativePilotPipelineForTest(t, backend, bundle, open)
			bus := observeNativePipelineDeliveryBusForTest(t, pc)
			root := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, ".")
			child := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, "child")
			grandchild := constructNativePipelineScenarioForTest(t, fixture, pc, ctx, "child/grandchild")
			if child.CurrentState != "waiting" {
				t.Fatalf("compiled preconditions: child=%s grandchild=%s", child.CurrentState, grandchild.CurrentState)
			}
			target := events.RouteIdentity{FlowID: ".", FlowInstance: root.StorageRef, EntityID: root.EntityID}
			envelope := events.EnvelopeForTargetRoute(events.EnvelopeForEntityID(events.EventEnvelope{}, grandchild.EntityID), target)
			unstamped := eventtest.ExistingRunRootIngress("", "child/grandchild/micro.done", "operator", "", nil, 0, runtimecorrelation.RunIDFromContext(ctx), envelope, time.Time{})
			if consume, handled, err := pc.workflowNodeInterceptPolicy(ctx, string(unstamped.Type()), unstamped); err != nil || handled || consume {
				t.Fatalf("unstamped intercept: handled=%t consume=%t error=%v", handled, consume, err)
			}
			evt := eventtest.ExistingRunRootIngress(uuid.NewString(), unstamped.Type(), "operator", "", nil, 0, runtimecorrelation.RunIDFromContext(ctx), envelope, time.Now().UTC())
			route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(pipelineNode(t, ".", "root-collector")), Target: events.MustExistingEntityTarget(target)}
			if err := fixture.PublishNode(ctx, evt, route); err != nil {
				t.Fatal(err)
			}
			handled, err := pc.dispatchWorkflowNodeEventResult(withWorkflowNodeDeliveryRoute(ctx, route), evt)
			if err == nil || !strings.Contains(err.Error(), "stamped connect claim") || handled {
				t.Fatalf("nested authoring delivery: handled=%t error=%v, want stamped connect claim refusal", handled, err)
			}
			stored, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, child.StorageRef))
			if err != nil || !found || stored.CurrentState != "waiting" {
				t.Fatalf("unstamped delivery changed child: found=%t instance=%#v error=%v", found, stored, err)
			}
			storedGrandchild, found, err := fixture.Persistence.LoadWorkflowInstance(ctx, testRunScopedWorkflowInstanceFromContext(ctx, grandchild.StorageRef))
			if err != nil || !found || !reflect.DeepEqual(grandchild, storedGrandchild) {
				t.Fatalf("unstamped delivery changed the constructed descendant: found=%t error=%v", found, err)
			}
			if got := bus.publishedCount(); got != 0 {
				t.Fatalf("unstamped delivery published=%d, want zero", got)
			}
		})
	}
}

func TestExecuteNodeContractHandlerRejectsAmbiguousHandlerTopLevelEmitWithRules(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module:         handlerEngineProjectNodeModule(t),
	}

	_, err := executeNodeContractHandlerWithHandoff(t, pc, testPipelineCoordinatorRunContext(t, pc), pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "default.emitted"},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{ID: "pick-rule", Condition: "true", Emit: runtimecontracts.EmitSpec{Event: "rule.emitted"}},
		},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),

		State: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err == nil {
		t.Fatal("expected ambiguous handler-level emit config to be rejected")
	}
	if !strings.Contains(err.Error(), "handler-top-level emit is only allowed on single-emit handlers") {
		t.Fatalf("error = %v", err)
	}
}

func TestExecuteNodeContractHandlerRejectsAmbiguousHandlerTopLevelEmitWithRulesWithoutRuleEmit(t *testing.T) {
	bus := &recordingPipelineBus{}
	pc := &PipelineCoordinator{
		bus:            bus,
		expressionEval: newWorkflowExpressionEvaluator(),
		entityLocks:    map[string]*sync.Mutex{},
		module:         handlerEngineProjectNodeModule(t),
	}

	_, err := executeNodeContractHandlerWithHandoff(t, pc, testPipelineCoordinatorRunContext(t, pc), pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "default.emitted"},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{ID: "pick-rule", Condition: "true", AdvancesTo: "done"},
		},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "ent-1"),
			time.Time{},
		),

		State: WorkflowState{EntityID: "ent-1", Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, false)
	if err == nil {
		t.Fatal("expected ambiguous handler-level emit config to be rejected")
	}
	if !strings.Contains(err.Error(), "handler-top-level emit is only allowed on single-emit handlers") {
		t.Fatalf("error = %v", err)
	}
}

func declarativeEmitContractTestBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return declarativeEmitContractSourceForTest(t, "custom.emitted:\n  label: text\n")
}

func additiveOnSuccessContractBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: test\nstages:\n  queued: {}\n",
		"entities.yaml": "test_entity: {}\n",
		"nodes.yaml":    "node-a:\n  execution_type: system_node\n",
		"events.yaml":   "rule.emitted:\nhandler.succeeded:\n",
	})
}

func rulesEmitTemplateContractBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	return loadWorkflowTempBundle(t, map[string]string{
		"schema.yaml":   "name: test\nstages:\n  queued: {}\n",
		"entities.yaml": "test_entity: {}\n",
		"nodes.yaml":    "node-a:\n  execution_type: system_node\n",
		"events.yaml":   "account.scored:\n  account_id: text\n  score: float\naccount.bucketed:\n  account_id: text\n  score: float\n  bucket: text\n",
	})
}

func VerifyExecuteNodeContractHandlerUsesTypedEnvelopeIdentityOverPayloadForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	entityID := eventtest.UUID("env-ent")
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityForTest(t, open, entityID)

	result, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a"), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			[]byte(`{"entity_id":"payload-ent"}`),
			0,
			"",
			"",
			events.EventEnvelope{EntityID: entityID},
			time.Now().UTC(),
		),
		State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	if got := bus.publishedEvent(0).EntityID(); got != entityID {
		t.Fatalf("emitted event entity_id = %q, want %q", got, entityID)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("unmarshal emitted payload: %v", err)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("emitted payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
}
func VerifyExecuteNodeContractHandlerOnSuccessRulesEmitsBothInOrderForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	module := handlerTestWorkflowModuleWithBundle(additiveOnSuccessContractBundle(t), ".", "node-a")
	entityID := eventtest.UUID("ent-1")
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, open, module, entityID, nil)

	result, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		OnSuccess: runtimecontracts.HandlerOnSuccessSpec{Emit: runtimecontracts.EmitSpec{Event: "handler.succeeded"}},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{ID: "pick-rule", Condition: "true", Emit: runtimecontracts.EmitSpec{Event: "rule.emitted"}},
		},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{}),
		State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 2 {
		t.Fatalf("bus published count = %d, want 2", got)
	}
	if got := []events.EventType{bus.publishes[0].Type(), bus.publishes[1].Type()}; !reflect.DeepEqual(got, []events.EventType{"rule.emitted", "handler.succeeded"}) {
		t.Fatalf("published order = %#v", got)
	}
}
func VerifyExecuteNodeContractHandlerRulesEmitTemplatePublishesOneMergedEventForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	module := handlerTestWorkflowModuleWithBundle(rulesEmitTemplateContractBundle(t), ".", "node-a")
	entityID := eventtest.UUID("ent-1")
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, open, module, entityID, nil)

	result, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{
			Event: "account.bucketed",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"account_id": runtimecontracts.CELExpression("payload.account_id"),
				"score":      runtimecontracts.CELExpression("payload.score"),
			},
		},
		Rules: []runtimecontracts.HandlerRuleEntry{
			{
				ID:        "high",
				Condition: "payload.score >= 80.0",
				Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
					"bucket": runtimecontracts.CELExpression(`"high"`),
				}},
			},
			{
				ID:        "medium",
				Condition: "payload.score >= 40.0",
				Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
					"bucket": runtimecontracts.CELExpression(`"medium"`),
				}},
			},
			{
				ID:        "low",
				Condition: "else",
				Emit: runtimecontracts.EmitSpec{Fields: map[string]runtimecontracts.ExpressionValue{
					"bucket": runtimecontracts.CELExpression(`"low"`),
				}},
			},
		},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress(
			"",
			events.EventType("account.scored"),
			"",
			"",
			mustJSON(map[string]any{"account_id": "acct-1", "score": 91}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),
		State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	emitted := bus.publishedEvent(0)
	if got := emitted.Type(); got != events.EventType("account.bucketed") {
		t.Fatalf("published event = %q, want account.bucketed", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(emitted.Payload(), &payload); err != nil {
		t.Fatalf("json.Unmarshal payload: %v", err)
	}
	if got := payload["account_id"]; got != "acct-1" {
		t.Fatalf("account_id = %#v, want acct-1", got)
	}
	if got := payload["bucket"]; got != "high" {
		t.Fatalf("bucket = %#v, want high", got)
	}
	if got := int(payload["score"].(float64)); got != 91 {
		t.Fatalf("score = %#v, want 91", payload["score"])
	}
}

func VerifyExecuteNodeContractHandler_UsesEmitFieldsAsOnlyBusinessPayloadSourceForTest(t *testing.T, open func(*testing.T, semanticview.Source, bool) WorkflowHandlerNativeFixtureForTest) {
	entityID := eventtest.UUID("ent-1")
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, func(t *testing.T, source semanticview.Source) WorkflowHandlerNativeFixtureForTest {
		return open(t, source, false)
	}, handlerTestWorkflowModuleWithBundle(declarativeEmitContractTestBundle(t), ".", "node-a"), entityID, map[string]any{"legacy_entity": "should-not-pass"})

	_, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{
			Event: "custom.emitted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"label": runtimecontracts.CELExpression(`"done"`),
			},
		},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1", "legacy": "should-not-pass"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),

		State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{"legacy_entity": "should-not-pass"}},
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := payload["label"]; got != "done" {
		t.Fatalf("payload.label = %#v, want done", got)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
	if _, ok := payload["trigger_event_type"]; ok {
		t.Fatalf("payload must not carry envelope trigger_event_type: %#v", payload["trigger_event_type"])
	}
	if _, ok := payload["current_state"]; ok {
		t.Fatalf("payload must not carry envelope current_state: %#v", payload["current_state"])
	}
	if _, ok := payload["legacy"]; ok {
		t.Fatalf("legacy trigger payload leaked into emitted payload: %#v", payload["legacy"])
	}
	if _, ok := payload["legacy_entity"]; ok {
		t.Fatalf("entity metadata leaked into emitted payload: %#v", payload["legacy_entity"])
	}
	if got := bus.publishedEvent(0).EntityID(); got != entityID {
		t.Fatalf("emitted event entity_id = %q, want ent-1", got)
	}
	if got := string(bus.publishedEvent(0).Type()); got != "custom.emitted" {
		t.Fatalf("emitted event type = %q, want custom.emitted", got)
	}
}

func VerifyExecuteNodeContractHandler_GuardEscalateUsesOnlyRuntimeOwnedEnvelopeForTest(t *testing.T, open func(*testing.T, semanticview.Source, bool) WorkflowHandlerNativeFixtureForTest) {
	entityID := eventtest.UUID("ent-1")
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, func(t *testing.T, source semanticview.Source) WorkflowHandlerNativeFixtureForTest {
		return open(t, source, false)
	}, handlerTestWorkflowModuleWithBundle(declarativeEmitContractSourceForTest(t, "guard.failed:\n"), ".", "node-a"), entityID, map[string]any{"legacy_entity": "should-not-pass"})

	_, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{
			Check:  "payload.score >= 70.0",
			OnFail: "escalate:guard.failed",
		},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1", "score": 50, "legacy": "should-not-pass"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),

		State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{"legacy_entity": "should-not-pass"}},
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
	if _, ok := payload["trigger_event_type"]; ok {
		t.Fatalf("payload must not carry envelope trigger_event_type: %#v", payload["trigger_event_type"])
	}
	if _, ok := payload["current_state"]; ok {
		t.Fatalf("payload must not carry envelope current_state: %#v", payload["current_state"])
	}
	if _, ok := payload["score"]; ok {
		t.Fatalf("guard escalation leaked trigger payload into emitted payload: %#v", payload["score"])
	}
	if _, ok := payload["legacy"]; ok {
		t.Fatalf("guard escalation leaked legacy trigger payload into emitted payload: %#v", payload["legacy"])
	}
	if got := bus.publishedEvent(0).EntityID(); got != entityID {
		t.Fatalf("guard escalation event entity_id = %q, want ent-1", got)
	}
	if _, ok := payload["legacy_entity"]; ok {
		t.Fatalf("guard escalation leaked entity metadata into emitted payload: %#v", payload["legacy_entity"])
	}
}

func VerifyExecuteNodeContractHandler_GuardEscalateObjectFieldsUseExplicitPayloadOnlyForTest(t *testing.T, open func(*testing.T, semanticview.Source, bool) WorkflowHandlerNativeFixtureForTest) {
	entityID := eventtest.UUID("ent-1")
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, func(t *testing.T, source semanticview.Source) WorkflowHandlerNativeFixtureForTest {
		return open(t, source, false)
	}, handlerTestWorkflowModuleWithBundle(declarativeEmitContractSourceForTest(t, "guard.failed:\n  score: float\n  reason: text\n"), ".", "node-a"), entityID, map[string]any{"legacy_entity": "should-not-pass"})

	_, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		Guard: &runtimecontracts.GuardSpec{
			Check: "payload.score >= 70.0",
			OnFailSpec: runtimecontracts.GuardFailureSpec{
				Action: runtimecontracts.GuardFailureActionEscalate,
				Escalation: runtimecontracts.EmitSpec{
					Event: "guard.failed",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"score":  runtimecontracts.CELExpression("payload.score"),
						"reason": runtimecontracts.CELExpression(`"score_below_threshold"`),
					},
				},
			},
		},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1", "score": 50, "legacy": "should-not-pass"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),
		State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{"legacy_entity": "should-not-pass"}},
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if got := payload["score"]; got != float64(50) {
		t.Fatalf("guard escalation score payload = %#v, want 50", got)
	}
	if got := payload["reason"]; got != "score_below_threshold" {
		t.Fatalf("guard escalation reason payload = %#v, want score_below_threshold", got)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
	if _, ok := payload["legacy"]; ok {
		t.Fatalf("guard escalation leaked unmapped trigger payload: %#v", payload["legacy"])
	}
	if _, ok := payload["legacy_entity"]; ok {
		t.Fatalf("guard escalation leaked entity metadata: %#v", payload["legacy_entity"])
	}
}

func VerifyExecuteNodeContractHandler_RejectsUndeclaredBusinessPayloadAcrossImmediateEmitSitesForTest(t *testing.T, open func(*testing.T, semanticview.Source, bool) WorkflowHandlerNativeFixtureForTest) {
	entityID := eventtest.UUID("ent-1")
	tests := []struct {
		name    string
		event   events.Event
		state   WorkflowState
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{
			name:  "handler top level",
			event: handlerTestRootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{}),
			state: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
			handler: runtimecontracts.SystemNodeEventHandler{
				Emit: runtimecontracts.EmitSpec{
					Event: "custom.emitted",
					Fields: map[string]runtimecontracts.ExpressionValue{
						"label": runtimecontracts.CELExpression(`"ok"`),
						"extra": runtimecontracts.CELExpression(`"bad"`),
					},
				},
			},
		},
		{
			name:  "rules",
			event: handlerTestRootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{}),
			state: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
			handler: runtimecontracts.SystemNodeEventHandler{
				Rules: []runtimecontracts.HandlerRuleEntry{{
					ID:        "pick",
					Condition: "true",
					Emit: runtimecontracts.EmitSpec{
						Event: "custom.emitted",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"label": runtimecontracts.CELExpression(`"ok"`),
							"extra": runtimecontracts.CELExpression(`"bad"`),
						},
					},
				}},
			},
		},
		{
			name:  "on_complete",
			event: handlerTestRootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{}),
			state: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
			handler: runtimecontracts.SystemNodeEventHandler{
				OnComplete: []runtimecontracts.HandlerRuleEntry{{
					ID:        "complete",
					Condition: "true",
					Emit: runtimecontracts.EmitSpec{
						Event: "custom.emitted",
						Fields: map[string]runtimecontracts.ExpressionValue{
							"label": runtimecontracts.CELExpression(`"ok"`),
							"extra": runtimecontracts.CELExpression(`"bad"`),
						},
					},
				}},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityWithModuleForTest(t, func(t *testing.T, source semanticview.Source) WorkflowHandlerNativeFixtureForTest {
				return open(t, source, true)
			}, handlerTestWorkflowModuleWithBundle(declarativeEmitContractTestBundle(t), ".", "node-a"), entityID, nil)
			_, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), tc.handler, workflowTriggerContext{
				Event: tc.event,
				State: tc.state,
			})
			if err == nil {
				t.Fatal("expected undeclared business payload to fail closed")
			}
			if !errors.Is(err, runtimeengine.ErrEmitPayloadContractViolation) {
				t.Fatalf("error = %v, want %v", err, runtimeengine.ErrEmitPayloadContractViolation)
			}
			if got := bus.publishedCount(); got != 0 {
				t.Fatalf("bus published count = %d, want 0", got)
			}
		})
	}
}

func VerifyExecuteNodeContractHandlerDefersCommittedEmissionsForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	entityID := eventtest.UUID("ent-1")
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityForTest(t, open, entityID)

	result, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineNode(t, ".", "node-a"), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "custom.emitted"},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress("", events.EventType("custom.trigger"), "", "", nil, 0, "", "", events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), time.Time{}),
		State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	}, true)
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if !result.Handled {
		t.Fatal("expected handled result")
	}
	if !result.Committed || len(result.FollowUp.Emissions) != 1 {
		t.Fatalf("committed fixture lost deferred follow-up: committed=%t emissions=%d", result.Committed, len(result.FollowUp.Emissions))
	}
	if got := bus.publishedCount(); got != 0 {
		t.Fatalf("bus published count = %d, want 0 before deferred dispatch", got)
	}
	if err := dispatchNativeHandlerFollowUpForTest(pc, ctx, bus, result.FollowUp); err != nil {
		t.Fatalf("dispatch committed follow-up: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1 after deferred dispatch", got)
	}
}

func VerifyExecuteNodeContractHandlerAppliesEmitFieldsToEmittedEventForTest(t *testing.T, open func(*testing.T, semanticview.Source) WorkflowHandlerNativeFixtureForTest) {
	entityID := eventtest.UUID("ent-1")
	fixture, pc, ctx, bus := nativeHandlerEngineExistingEntityForTest(t, open, entityID)

	_, err := executeNativeHandlerEngineWithHandoffForTest(t, fixture, pc, ctx, bus, pipelineOnlySourceNode(t, pc.SemanticSource(), "node-a"), runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{
			Event: "custom.emitted",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"summary.entity_id": runtimecontracts.CELExpression("_entity.id"),
				"summary.stage":     runtimecontracts.CELExpression("_entity.current_state"),
				"flags.ready":       runtimecontracts.CELExpression("true"),
				"label":             runtimecontracts.CELExpression(`"done"`),
			},
		},
	}, workflowTriggerContext{
		Event: handlerTestRootIngress(
			"",
			events.EventType("custom.trigger"),
			"",
			"",
			mustJSON(map[string]any{"entity_id": "ent-1"}),
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
			time.Time{},
		),

		State: WorkflowState{EntityID: entityID, Stage: WorkflowStateID("queued"), Metadata: map[string]any{}},
	})
	if err != nil {
		t.Fatalf("executeNodeContractHandler: %v", err)
	}
	if got := bus.publishedCount(); got != 1 {
		t.Fatalf("bus published count = %d, want 1", got)
	}
	var payload map[string]any
	if err := json.Unmarshal(bus.publishedEvent(0).Payload(), &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	summary, _ := payload["summary"].(map[string]any)
	if got := summary["entity_id"]; got != entityID {
		t.Fatalf("payload.summary.entity_id = %#v, want ent-1", got)
	}
	if got := summary["stage"]; got != "queued" {
		t.Fatalf("payload.summary.stage = %#v, want queued", got)
	}
	flags, _ := payload["flags"].(map[string]any)
	if got := flags["ready"]; got != true {
		t.Fatalf("payload.flags.ready = %#v, want true", got)
	}
	if got := payload["label"]; got != "done" {
		t.Fatalf("payload.label = %#v, want done", got)
	}
	if _, ok := payload["entity_id"]; ok {
		t.Fatalf("payload must not carry envelope entity_id: %#v", payload["entity_id"])
	}
}
