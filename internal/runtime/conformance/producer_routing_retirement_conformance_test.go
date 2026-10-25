package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/checkoutsource"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/yamlsource"
	"gopkg.in/yaml.v3"
)

func TestProducerRoutingCanonicalConsumerManifestationsExecute(t *testing.T) {
	cases := []struct {
		id           string
		fixture      string
		flowPath     string
		flowID       string
		flowInstance string
		nodeID       string
		trigger      string
		emitted      string
		runtimeEvent string
		disposition  string
		payload      map[string]any
	}{
		{id: "B001", fixture: "internal/runtime/testdata/generic-swarm-bundle", flowID: "delivery", flowInstance: "delivery/fixture-instance", nodeID: "delivery-node", trigger: "timer.item.timeout", emitted: "item.completed", runtimeEvent: "delivery/fixture-instance/item.completed", disposition: "same_flow"},
		{id: "B002", fixture: "internal/runtime/testdata/generic-swarm-bundle", flowID: "intake", flowInstance: "intake", nodeID: "intake-router", trigger: "item.created", emitted: "item.processed", runtimeEvent: "intake/item.processed", disposition: "same_flow", payload: map[string]any{"item_id": "item-1", "items": []any{map[string]any{"id": "line-1", "score": float64(75)}}}},
		{id: "B103", fixture: "tests/tier5-flow-lifecycle/test-auto-emit-on-create", flowPath: "worker", flowID: "worker", flowInstance: "worker/worker-001", nodeID: "worker", trigger: "auto.started", emitted: "auto.processed", runtimeEvent: "worker/worker-001/auto.processed", disposition: "same_flow"},
		{id: "B151", fixture: "tests/tier8-boot-verification/test-boot-event-cycle", nodeID: "test-node", trigger: "cycle.ping", emitted: "cycle.pong", disposition: "same_flow"},
		{id: "B152", fixture: "tests/tier8-boot-verification/test-boot-event-cycle", nodeID: "test-node", trigger: "cycle.pong", emitted: "cycle.ping", disposition: "same_flow"},
		{id: "B164", fixture: "tests/tier8-boot-verification/test-boot-permission-tool-mismatch", nodeID: "test-node", trigger: "task.requested", emitted: "task.completed", disposition: "same_flow"},
		{id: "B171", fixture: "tests/tier8-boot-verification/test-boot-prompt-stub", nodeID: "stub-agent-node", trigger: "task.requested", emitted: "task.completed", disposition: "external", payload: map[string]any{"task_type": "proof"}},
		{id: "B173", fixture: "tests/tier8-boot-verification/test-boot-self-emit", nodeID: "test-node", trigger: "loop.event", emitted: "loop.event", disposition: "same_flow"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			bundle := loadProducerRoutingFixture(t, tc.fixture)
			if tc.id == "B171" {
				setProducerRoutingProofEmitField(t, bundle, tc.nodeID, tc.trigger, "result", runtimecontracts.LiteralExpression("complete"))
			}
			source := semanticview.Wrap(bundle)
			payload, err := json.Marshal(tc.payload)
			if err != nil {
				t.Fatal(err)
			}
			const runID = "00000000-0000-0000-0000-000000000001"
			entityID := runID
			if tc.flowID != "" {
				entityID = runtimeflowidentity.EntityID(tc.flowInstance)
			}
			envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, entityID)
			if tc.flowID != "" {
				envelope = events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
					FlowID: tc.flowID, FlowInstance: tc.flowInstance, EntityID: entityID,
				})
			}
			routingSource := eventtest.RootRoutingSource("00000000-0000-0000-0000-000000000001")
			if tc.flowID != "" {
				if scope, ok := source.FlowScopeByID(tc.flowID); ok && scope.Mode == "template" {
					routingSource = eventtest.ConcreteTemplateRoutingSource(tc.flowID, tc.flowInstance, entityID)
				} else {
					routingSource = eventtest.StaticFlowRoutingSource(tc.flowID, tc.flowInstance, entityID)
				}
			}
			previewCtx := context.Background()
			node := conformanceNode(t, tc.flowID, tc.nodeID)
			if tc.flowPath != "" {
				node = conformanceFlowPathNode(t, tc.flowPath, tc.nodeID)
			}
			flowID, instancePath := tc.flowID, tc.flowInstance
			if flowID == "" {
				flowID, instancePath = ".", runID
			}
			previewCtx = runtimedelivery.WithRoute(previewCtx, events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: flowID, FlowInstance: instancePath, EntityID: entityID,
				}),
			})
			preview, err := runtimepipeline.PreviewContractHandlerExecution(
				previewCtx, bundle, node,
				eventtest.RunCreatingRootIngressWithRoutingSource(
					"event-"+strings.ToLower(tc.id), events.EventType(tc.trigger), "fixture-proof", "", payload, 0,
					"00000000-0000-0000-0000-000000000001", "", envelope, routingSource, time.Now().UTC(),
				),
				producerRoutingPreviewSnapshot(t, source, tc.flowID, instancePath, entityID), nil,
			)
			if err != nil {
				t.Fatalf("execute emitting handler: %v", err)
			}
			wantRuntimeEvent := strings.TrimSpace(tc.runtimeEvent)
			if wantRuntimeEvent == "" {
				wantRuntimeEvent = tc.emitted
			}
			if tc.id == "B002" {
				plans := source.FanOutPlansForHandler(node, tc.trigger)
				if preview.FanOutCount != 1 || len(preview.Emits) != 0 || len(plans) != 1 || plans[0].Emit.EventType() != tc.emitted {
					t.Fatalf("deferred fan-out preview = count:%d eager:%v plans:%#v, want one compiled %q obligation and no eager event", preview.FanOutCount, preview.Emits, plans, tc.emitted)
				}
			} else if !containsProducerRoutingValue(preview.Emits, wantRuntimeEvent) {
				t.Fatalf("preview emits = %v, want %q", preview.Emits, wantRuntimeEvent)
			}
			switch tc.disposition {
			case "same_flow":
				consumers := semanticview.BuildAuthoredEventEndpointCensus(source).MatchingConsumers(tc.flowID, tc.emitted)
				if len(consumers) == 0 {
					t.Fatalf("%s has no typed same-flow consumer", tc.emitted)
				}
				if runtimepinrouting.PinDeclaredOutput(source, tc.flowID, tc.emitted) {
					classification := runtimepinrouting.ClassifyOutputConsumer(source, tc.flowID, tc.emitted)
					if !classification.Has(runtimepinrouting.OutputConsumerSameFlow) {
						t.Fatalf("output consumer classification = %#v, want same-flow", classification)
					}
				}
			case "external":
				// The historical external disposition is now a public root export,
				// not an authored promise of an external consumer.
				classification := runtimepinrouting.ClassifyOutputConsumer(source, tc.flowID, tc.emitted)
				if !classification.Has(runtimepinrouting.OutputConsumerRootExport) {
					t.Fatalf("%s lacks a compiled public root output: %#v", tc.emitted, classification)
				}
			default:
				t.Fatalf("unsupported disposition %q", tc.disposition)
			}
		})
	}
}

func TestProducerRoutingRetiredIntentFixturesFailClosed(t *testing.T) {
	for _, tc := range []struct {
		id      string
		fixture string
	}{
		{id: "B169", fixture: "tests/tier8-boot-verification/test-boot-prompt-ref-stub"},
		{id: "B170", fixture: "tests/tier8-boot-verification/test-boot-prompt-ref"},
	} {
		t.Run(tc.id, func(t *testing.T) {
			repoRoot := canonicalrouting.RepoRoot(t)
			_, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
				repoRoot,
				filepath.Join(repoRoot, tc.fixture),
				runtimecontracts.DefaultPlatformSpecFile(repoRoot),
			)
			if err == nil || !strings.Contains(err.Error(), `agent field "prompt_ref"`) {
				t.Fatalf("load error = %v, want retired prompt_ref teaching rejection", err)
			}
		})
	}
}

func setProducerRoutingProofEmitField(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle, nodeID, trigger, field string, value runtimecontracts.ExpressionValue) {
	t.Helper()
	node, ok := bundle.Nodes[nodeID]
	if !ok {
		t.Fatalf("node %s missing", nodeID)
	}
	handler, ok := node.EventHandlers[trigger]
	if !ok {
		t.Fatalf("handler %s/%s missing", nodeID, trigger)
	}
	if handler.Emit.Fields == nil {
		handler.Emit.Fields = map[string]runtimecontracts.ExpressionValue{}
	}
	handler.Emit.Fields[field] = value
	node.EventHandlers[trigger] = handler
	bundle.Nodes[nodeID] = node
	if handlers := bundle.Semantics.NodeHandlers[nodeID]; handlers != nil {
		handlers[trigger] = handler
	}
}

func TestProducerRoutingRetirementExcludedFixturesExecuteCanonicalOutput(t *testing.T) {
	cases := []struct {
		id          string
		fixture     string
		nodeID      string
		trigger     string
		payload     map[string]any
		wantEmitted string
	}{
		{id: "B045", fixture: "tests/tier11-flow-composition/test-child-flow-absolute-path", nodeID: "listener", trigger: "task.done", wantEmitted: "work.finished"},
		{id: "B078", fixture: "tests/tier11-flow-composition/test-tool-override", nodeID: "root-node", trigger: "child.done", wantEmitted: "task.done"},
		{id: "B081", fixture: "tests/tier11-flow-composition/test-wildcard-deep-subscription", nodeID: "collector", trigger: "task.done", wantEmitted: "pipeline.complete"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			bundle := loadProducerRoutingFixture(t, tc.fixture)
			payload, err := json.Marshal(tc.payload)
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			node := conformanceNode(t, "", tc.nodeID)
			previewCtx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(node),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: ".", FlowInstance: "00000000-0000-0000-0000-000000000001", EntityID: "00000000-0000-0000-0000-000000000001",
				}),
			})
			preview, err := runtimepipeline.PreviewContractHandlerExecution(
				previewCtx,
				bundle,
				node,
				eventtest.RunCreatingRootIngress(
					"event-"+strings.ToLower(tc.id), events.EventType(tc.trigger), "fixture-harness", "", payload, 0,
					"00000000-0000-0000-0000-000000000001", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "00000000-0000-0000-0000-000000000001"), time.Now().UTC(),
				),
				producerRoutingPreviewSnapshot(t, source, "", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000001"),
				nil,
			)
			if err != nil {
				t.Fatalf("preview canonical output: %v", err)
			}
			if !containsProducerRoutingValue(preview.Emits, tc.wantEmitted) {
				t.Fatalf("preview emits = %v, want %q", preview.Emits, tc.wantEmitted)
			}
		})
	}
}

// These are compiled-handler component proofs with an explicit constructed
// snapshot, not public construction or persistence qualification.
func producerRoutingPreviewSnapshot(t *testing.T, source semanticview.Source, flowID, path, entityID string) runtimeengine.StateSnapshot {
	t.Helper()
	name := flowID
	if name == "" {
		name = "."
	}
	control := runtimeengine.StateControl{FlowPath: path, StorageRef: path, InstanceID: runtimeflowidentity.LogicalInstanceID(path)}
	var fields map[string]any
	if contract, found := entityruntime.ResolveForFlow(source, flowID); found {
		control.EntityType = contract.EntityType
		var err error
		fields, err = entityruntime.InitialValues(contract)
		if err != nil {
			t.Fatal(err)
		}
	}
	return runtimeengine.StateSnapshot{
		EntityID: identity.NormalizeEntityID(entityID), WorkflowName: name, WorkflowVersion: source.WorkflowVersion(),
		CurrentState: source.FlowInitialStage(name), EnteredStateAt: time.Now().UTC(),
		StateCarrier: runtimeengine.NewStateCarrierWithOwners(fields, nil, control, nil, nil),
	}
}

func TestProducerRoutingRetirementExcludedDeadOutputs(t *testing.T) {
	cases := []struct {
		id    string
		path  string
		event string
	}{
		{id: "B080", path: "tests/tier11-flow-composition/test-wildcard-deep-subscription/child/nodes.yaml", event: "work.finished"},
		{id: "B099", path: "tests/tier4-cross-entity/test-create-entity/child-flow/nodes.yaml", event: "child.done"},
		{id: "B191", path: "tests/tier9-composition-patterns/test-compose-multi-emit-cross-flow/tracker/nodes.yaml", event: "task.recorded"},
	}
	repoRoot := canonicalrouting.RepoRoot(t)
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			raw := readProducerRoutingProofYAML(t, filepath.Join(repoRoot, tc.path))
			if containsProducerRoutingValue(emittedEventsInYAML(raw), tc.event) {
				t.Fatalf("dead event %q is still emitted", tc.event)
			}
			pins := admittedProducerRoutingOutputPins(t, filepath.Join(repoRoot, filepath.Dir(tc.path), "schema.yaml"))
			if err := retiredProducerRoutingOutputError(pins, tc.event); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCheckedYAMLRejectsAllProducerRoutingAuthority(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	for _, path := range retiredProducerRoutingFiles(t, repoRoot) {
		t.Errorf("%s retains retired emit.target or emit.broadcast", path)
	}
}

func retiredProducerRoutingFiles(t *testing.T, repoRoot string) []string {
	t.Helper()
	var retired []string
	err := checkoutsource.WalkDir(repoRoot, repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if (entry.Name() == "nodes.yaml" || entry.Name() == "schema.yaml") && hasRetiredProducerRoutingFile(t, path) {
			retired = append(retired, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return retired
}

func TestProducerRoutingRetirementGuardIgnoresNestedLiteralEmitMap(t *testing.T) {
	raw := []byte("worker:\n  request:\n    emit:\n      event: task.done\n      fields:\n        config:\n          literal:\n            emit:\n              broadcast: true\n")
	retired, err := hasRetiredProducerRoutingYAML(raw)
	if err != nil {
		t.Fatal(err)
	}
	if retired {
		t.Fatal("nested literal payload was classified as retired routing")
	}
}

func TestProducerRoutingRetirementGuardCoversSchemaOwnedEmitSites(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
	}{
		{
			name: "loop escape",
			raw:  "name: loop\nloops:\n  retry:\n    escape:\n      emit: {event: retry.exhausted, broadcast: true}\n",
		},
		{
			name: "stage gate outcome",
			raw:  "name: gate\nstages:\n  waiting:\n    gate:\n      outcomes:\n        approve:\n          emit: {event: review.approved, target: sender}\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			retired, err := hasRetiredProducerRoutingYAML([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if !retired {
				t.Fatal("schema-owned retired routing was not classified")
			}
		})
	}
}

func readProducerRoutingProofYAML(t testing.TB, path string, out ...any) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) && isOptionalProducerRoutingProofFile(path) {
		return map[string]any{}
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if len(out) > 0 {
		if err := yaml.Unmarshal(raw, out[0]); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		return nil
	}
	var document map[string]any
	if err := yaml.Unmarshal(raw, &document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return document
}

func emittedEventsInYAML(document map[string]any) []string {
	var eventsFound []string
	var walk func(any)
	walk = func(value any) {
		switch typed := value.(type) {
		case map[string]any:
			for key, child := range typed {
				if key == "emit" {
					switch emit := child.(type) {
					case string:
						eventsFound = append(eventsFound, strings.TrimSpace(emit))
					case map[string]any:
						if eventType, ok := emit["event"].(string); ok {
							eventsFound = append(eventsFound, strings.TrimSpace(eventType))
						}
					}
				}
				walk(child)
			}
		case []any:
			for _, child := range typed {
				walk(child)
			}
		}
	}
	walk(document)
	sort.Strings(eventsFound)
	return eventsFound
}

func admittedProducerRoutingOutputPins(t testing.TB, path string) map[string]struct{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pins, err := producerRoutingOutputPins(data)
	if err != nil {
		t.Fatalf("admit output evidence %s: %v", path, err)
	}
	return pins
}

func producerRoutingOutputPins(data []byte) (map[string]struct{}, error) {
	snapshot, err := yamlsource.Load(data)
	if err != nil {
		return nil, err
	}
	schema, err := runtimecontracts.AdmitFlowSchemaValue(snapshot.Document("schema.yaml").Root())
	if err != nil {
		return nil, err
	}
	pins := map[string]struct{}{}
	for _, pin := range schema.Pins.Outputs.EventPins {
		pins[pin.Event] = struct{}{}
	}
	return pins, nil
}

func retiredProducerRoutingOutputError(pins map[string]struct{}, event string) error {
	if _, exists := pins[event]; exists {
		return fmt.Errorf("removed event %q still has an output pin", event)
	}
	return nil
}

func TestProducerRoutingLedgerDetectsRestoredScalarOutput(t *testing.T) {
	for _, restored := range []bool{false, true} {
		source := "name: negative-proof\noutputs: [work.retained]\n"
		if restored {
			source = strings.Replace(source, "[work.retained]", "[work.retained, work.removed]", 1)
		}
		pins, err := producerRoutingOutputPins([]byte(source))
		if err != nil {
			t.Fatal(err)
		}
		if detected := retiredProducerRoutingOutputError(pins, "work.removed") != nil; detected != restored {
			t.Fatalf("restored scalar output detected=%t want=%t", detected, restored)
		}
	}
	for _, alternate := range []string{
		"outputs: {events: [work.removed]}",
		"outputs: [{event: work.removed, sink: harness}]",
		"outputs: [{name: work.removed}]",
	} {
		if _, err := producerRoutingOutputPins([]byte(alternate)); err == nil {
			t.Fatalf("ledger admitted alternate output spelling %s", alternate)
		}
	}
}

func hasRetiredProducerRoutingYAML(raw []byte) (bool, error) {
	var document yaml.Node
	if err := yaml.Unmarshal(raw, &document); err != nil {
		return false, err
	}
	for _, emit := range producerRoutingProofEmitNodes(&document) {
		for index := 0; index+1 < len(emit.Content); index += 2 {
			switch strings.TrimSpace(emit.Content[index].Value) {
			case "target", "broadcast":
				return true, nil
			}
		}
	}
	return false, nil
}

func producerRoutingProofEmitNodes(document *yaml.Node) []*yaml.Node {
	root := document
	if root != nil && root.Kind == yaml.DocumentNode && len(root.Content) == 1 {
		root = root.Content[0]
	}
	if root == nil || root.Kind != yaml.MappingNode {
		return nil
	}
	var emits []*yaml.Node
	for index := 1; index < len(root.Content); index += 2 {
		node := root.Content[index]
		handlers := producerRoutingProofMappingValue(node, "event_handlers")
		if handlers == nil || handlers.Kind != yaml.MappingNode {
			continue
		}
		for handlerIndex := 1; handlerIndex < len(handlers.Content); handlerIndex += 2 {
			handler := handlers.Content[handlerIndex]
			emits = appendProducerRoutingProofEmit(emits, handler)
			emits = appendProducerRoutingProofEmit(emits, producerRoutingProofMappingValue(handler, "on_success"))
			for _, key := range []string{"rules", "on_complete"} {
				emits = appendProducerRoutingProofRuleEmits(emits, producerRoutingProofMappingValue(handler, key))
			}
			emits = appendProducerRoutingProofEmit(emits, producerRoutingProofMappingValue(handler, "fan_out"))
			join := producerRoutingProofMappingValue(handler, "join")
			emits = appendProducerRoutingProofEmit(emits, producerRoutingProofMappingValue(join, "on_complete"))
			emits = appendProducerRoutingProofEmit(emits, producerRoutingProofMappingValue(join, "timeout"))
		}
	}
	for _, loop := range producerRoutingProofMappingValues(producerRoutingProofMappingValue(root, "loops")) {
		emits = appendProducerRoutingProofEmit(emits, producerRoutingProofMappingValue(loop, "escape"))
	}
	for _, stage := range producerRoutingProofMappingValues(producerRoutingProofMappingValue(root, "stages")) {
		gate := producerRoutingProofMappingValue(stage, "gate")
		for _, outcome := range producerRoutingProofMappingValues(producerRoutingProofMappingValue(gate, "outcomes")) {
			emits = appendProducerRoutingProofEmit(emits, outcome)
		}
	}
	return emits
}

func appendProducerRoutingProofRuleEmits(emits []*yaml.Node, rules *yaml.Node) []*yaml.Node {
	if rules == nil {
		return emits
	}
	if rules.Kind == yaml.SequenceNode {
		for _, rule := range rules.Content {
			emits = appendProducerRoutingProofEmit(emits, rule)
			emits = appendProducerRoutingProofEmit(emits, producerRoutingProofMappingValue(rule, "fan_out"))
		}
		return emits
	}
	if rules.Kind == yaml.MappingNode {
		for _, rule := range producerRoutingProofMappingValues(rules) {
			emits = appendProducerRoutingProofEmit(emits, rule)
			emits = appendProducerRoutingProofEmit(emits, producerRoutingProofMappingValue(rule, "fan_out"))
		}
	}
	return emits
}

func appendProducerRoutingProofEmit(emits []*yaml.Node, owner *yaml.Node) []*yaml.Node {
	emit := producerRoutingProofMappingValue(owner, "emit")
	if emit != nil && emit.Kind == yaml.MappingNode {
		return append(emits, emit)
	}
	return emits
}

func producerRoutingProofMappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(node.Content); index += 2 {
		if strings.TrimSpace(node.Content[index].Value) == key {
			return node.Content[index+1]
		}
	}
	return nil
}

func producerRoutingProofMappingValues(node *yaml.Node) []*yaml.Node {
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	values := make([]*yaml.Node, 0, len(node.Content)/2)
	for index := 1; index < len(node.Content); index += 2 {
		values = append(values, node.Content[index])
	}
	return values
}

func hasRetiredProducerRoutingFile(t testing.TB, path string) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) && isOptionalProducerRoutingProofFile(path) {
		return false
	}
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	retired, err := hasRetiredProducerRoutingYAML(raw)
	if err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return retired
}

func isOptionalProducerRoutingProofFile(path string) bool {
	switch filepath.Base(path) {
	case "agents.yaml", "state.yaml", "events.yaml", "nodes.yaml", "policy.yaml", "tools.yaml", "types.yaml":
		return true
	default:
		return false
	}
}

func repositoryTestEntrypoints(t testing.TB, repoRoot string) map[string]struct{} {
	t.Helper()
	out := map[string]struct{}{}
	err := checkoutsource.WalkDir(repoRoot, repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(entry.Name(), "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if ok && strings.HasPrefix(function.Name.Name, "Test") {
				out[function.Name.Name] = struct{}{}
				ast.Inspect(function.Body, func(node ast.Node) bool {
					literal, ok := node.(*ast.BasicLit)
					if !ok || literal.Kind != token.STRING {
						return true
					}
					value := strings.Trim(literal.Value, "`\"")
					if len(value) == 4 && value[0] == 'B' {
						out[function.Name.Name+"/"+value] = struct{}{}
					}
					return true
				})
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan test entrypoints: %v", err)
	}
	return out
}

func TestProducerRoutingProofCensusRejectsForeignOnlyEntrypoint(t *testing.T) {
	root := t.TempDir()
	foreign := filepath.Join(root, "internal", "review-nested")
	if err := os.MkdirAll(foreign, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(foreign, ".git"), []byte("gitdir: elsewhere\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	proof := []byte("package proof\nimport \"testing\"\nfunc TestRoutingBoundaryProof(t *testing.T) {}\n")
	if err := os.WriteFile(filepath.Join(foreign, "foreign_test.go"), proof, 0o644); err != nil {
		t.Fatal(err)
	}
	check := func(want bool) {
		t.Helper()
		_, found := repositoryTestEntrypoints(t, root)["TestRoutingBoundaryProof"]
		if found != want {
			t.Fatalf("routing proof credited = %v, want %v", found, want)
		}
	}
	check(false)
	current := filepath.Join(root, "current_test.go")
	if err := os.WriteFile(current, proof, 0o644); err != nil {
		t.Fatal(err)
	}
	check(true)
	if err := os.Remove(current); err != nil {
		t.Fatal(err)
	}
	check(false)
}

func loadProducerRoutingFixture(t testing.TB, relative string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := canonicalrouting.RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot, filepath.Join(repoRoot, relative), runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		if diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err); ok {
			t.Fatalf("load %s: %v: %s", relative, err, diagnostic.RawCause)
		}
		t.Fatalf("load %s: %v", relative, err)
	}
	return bundle
}

func containsProducerRoutingValue(values []string, want string) bool {
	want = strings.TrimSpace(want)
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}
