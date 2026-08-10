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

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"gopkg.in/yaml.v3"
)

type producerRoutingRetirementLedger struct {
	SchemaVersion           string                               `yaml:"schema_version"`
	AuditedHead             string                               `yaml:"audited_head"`
	HistoricalSourceCommit  string                               `yaml:"historical_source_commit"`
	HistoricalRemovalCommit string                               `yaml:"historical_removal_commit"`
	HistoricalMergeCommit   string                               `yaml:"historical_merge_commit"`
	Rows                    []producerRoutingRetirementLedgerRow `yaml:"rows"`
}

type producerRoutingRetirementLedgerRow struct {
	ID          string `yaml:"id"`
	Path        string `yaml:"path"`
	Event       string `yaml:"event"`
	Disposition string `yaml:"disposition"`
	Proof       string `yaml:"proof"`
}

func TestProducerRoutingRetirementLedger(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	ledger := loadProducerRoutingRetirementLedger(t)
	if ledger.SchemaVersion != "producer-routing-retirement/v1" || len(ledger.Rows) != 197 {
		t.Fatalf("ledger identity/count = %q/%d, want producer-routing-retirement/v1/197", ledger.SchemaVersion, len(ledger.Rows))
	}
	if ledger.HistoricalSourceCommit != "8629f242491f569e6facdd5a0cff8959263f03ef^" ||
		ledger.HistoricalRemovalCommit != "8629f242491f569e6facdd5a0cff8959263f03ef" ||
		ledger.HistoricalMergeCommit != "0bd15742df7367bbfba7ca5f5639fcfb3b71ba4c" {
		t.Fatalf("historical evidence is not commit-qualified: %#v", ledger)
	}
	testEntrypoints := repositoryTestEntrypoints(t, repoRoot)
	wantDispositionCounts := map[string]int{
		"harness": 94, "negative_removal": 39, "dead_removal": 46,
		"same_flow": 7, "external": 3, "historical_connect": 8,
	}
	gotDispositionCounts := map[string]int{}
	seen := map[string]struct{}{}
	for index, row := range ledger.Rows {
		wantID := fmt.Sprintf("B%03d", index+1)
		if row.ID != wantID {
			t.Fatalf("ledger row %d id = %q, want %q", index, row.ID, wantID)
		}
		if _, duplicate := seen[row.ID]; duplicate {
			t.Fatalf("duplicate ledger id %q", row.ID)
		}
		seen[row.ID] = struct{}{}
		gotDispositionCounts[row.Disposition]++
		row := row
		t.Run(row.ID, func(t *testing.T) {
			baseProof := strings.Split(strings.TrimSpace(row.Proof), "/")[0]
			if _, exists := testEntrypoints[baseProof]; !exists {
				t.Fatalf("proof %q does not identify an actual TestXxx entrypoint", row.Proof)
			}
			if row.Disposition == "same_flow" || row.Disposition == "external" {
				if _, exists := testEntrypoints[strings.TrimSpace(row.Proof)]; !exists {
					t.Fatalf("canonical consumer proof %q does not identify an exact executable subtest", row.Proof)
				}
			}
			path := filepath.Join(repoRoot, filepath.FromSlash(row.Path))
			raw := readProducerRoutingProofYAML(t, path)
			if hasRetiredProducerRoutingFile(t, path) {
				t.Fatalf("%s retains retired producer routing", row.Path)
			}
			emits := emittedEventsInYAML(raw)
			schemaPath := filepath.Join(filepath.Dir(path), "schema.yaml")
			outputSinks := outputPinSinksInYAML(readProducerRoutingProofYAML(t, schemaPath))
			switch row.Disposition {
			case "harness":
				if outputSinks[row.Event] != "harness" || !containsProducerRoutingValue(emits, row.Event) {
					t.Fatalf("harness migration event=%q emits=%v sinks=%v", row.Event, emits, outputSinks)
				}
			case "negative_removal", "dead_removal":
				if _, exists := outputSinks[row.Event]; exists {
					t.Fatalf("removed event %q still has an output pin", row.Event)
				}
				// These two emits are the direct evidence for their original negative
				// classifications; only their unrelated output authority was removed.
				if row.ID != "B154" && row.ID != "B167" && containsProducerRoutingValue(emits, row.Event) {
					t.Fatalf("removed event %q is still emitted", row.Event)
				}
			case "same_flow", "external":
				if !containsProducerRoutingValue(emits, row.Event) {
					t.Fatalf("retained canonical-consumer event %q is no longer emitted", row.Event)
				}
			case "historical_connect":
				// Current source is only collateral proof; source evidence is pinned
				// by the commit-qualified ledger header above.
			default:
				t.Fatalf("unknown disposition %q", row.Disposition)
			}
		})
	}
	if fmt.Sprint(gotDispositionCounts) != fmt.Sprint(wantDispositionCounts) {
		t.Fatalf("disposition counts = %v, want %v", gotDispositionCounts, wantDispositionCounts)
	}
}

func TestProducerRoutingCanonicalConsumerManifestationsExecute(t *testing.T) {
	cases := []struct {
		id           string
		fixture      string
		flowID       string
		flowInstance string
		nodeID       string
		trigger      string
		emitted      string
		runtimeEvent string
		disposition  string
		payload      map[string]any
	}{
		{id: "B001", fixture: "internal/runtime/testdata/generic-swarm-bundle", flowID: "delivery", flowInstance: "delivery/fixture-instance", nodeID: "delivery-node", trigger: "timer.item.timeout", emitted: "item.completed", runtimeEvent: "delivery/item.completed", disposition: "same_flow"},
		{id: "B002", fixture: "internal/runtime/testdata/generic-swarm-bundle", flowID: "intake", flowInstance: "intake", nodeID: "intake-router", trigger: "item.created", emitted: "item.processed", runtimeEvent: "intake/item.processed", disposition: "same_flow", payload: map[string]any{"item_id": "item-1", "items": []any{map[string]any{"id": "line-1"}}}},
		{id: "B103", fixture: "tests/tier5-flow-lifecycle/test-auto-emit-on-create", flowID: "worker", flowInstance: "worker/worker-001", nodeID: "worker", trigger: "auto.started", emitted: "auto.processed", runtimeEvent: "worker/auto.processed", disposition: "same_flow"},
		{id: "B151", fixture: "tests/tier8-boot-verification/test-boot-event-cycle", nodeID: "test-node", trigger: "cycle.ping", emitted: "cycle.pong", disposition: "same_flow"},
		{id: "B152", fixture: "tests/tier8-boot-verification/test-boot-event-cycle", nodeID: "test-node", trigger: "cycle.pong", emitted: "cycle.ping", disposition: "same_flow"},
		{id: "B164", fixture: "tests/tier8-boot-verification/test-boot-permission-tool-mismatch", nodeID: "test-node", trigger: "task.requested", emitted: "task.completed", disposition: "same_flow"},
		{id: "B169", fixture: "tests/tier8-boot-verification/test-boot-prompt-ref-stub", nodeID: "prompt-ref-stub-node", trigger: "task.requested", emitted: "task.completed", disposition: "external"},
		{id: "B170", fixture: "tests/tier8-boot-verification/test-boot-prompt-ref", nodeID: "prompt-ref-node", trigger: "task.requested", emitted: "task.completed", disposition: "external"},
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
			envelope := events.EnvelopeForEntityID(events.EventEnvelope{}, "fixture-entity")
			if tc.flowID != "" {
				envelope = events.EnvelopeForSourceRoute(events.EventEnvelope{}, events.RouteIdentity{
					FlowID: tc.flowID, FlowInstance: tc.flowInstance, EntityID: "fixture-entity",
				})
			}
			previewCtx := context.Background()
			if tc.flowID != "" {
				previewCtx = runtimedelivery.WithRoute(previewCtx, events.DeliveryRoute{
					Recipient: events.MustNodeDeliveryRecipient(tc.nodeID),
					Target: events.MustExistingEntityTarget(events.RouteIdentity{
						FlowID: tc.flowID, FlowInstance: tc.flowInstance, EntityID: "fixture-entity",
					}),
				})
			}
			preview, err := runtimepipeline.PreviewContractHandlerExecution(
				previewCtx, bundle, tc.nodeID,
				eventtest.RunCreatingRootIngress(
					"event-"+strings.ToLower(tc.id), events.EventType(tc.trigger), "fixture-proof", "", payload, 0,
					"00000000-0000-0000-0000-000000000001", "", envelope, time.Now().UTC(),
				),
				runtimepipeline.WorkflowState{Stage: runtimepipeline.NormalizeWorkflowStateID(source.FlowInitialStage(tc.flowID))}, nil,
			)
			if err != nil {
				t.Fatalf("execute emitting handler: %v", err)
			}
			wantRuntimeEvent := strings.TrimSpace(tc.runtimeEvent)
			if wantRuntimeEvent == "" {
				wantRuntimeEvent = tc.emitted
			}
			if !containsProducerRoutingValue(preview.Emits, wantRuntimeEvent) {
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
				entry, _, ok := source.ResolveFlowEventCatalogEntry(tc.flowID, tc.emitted)
				if !ok || entry.AcceptedConsumerBoundary() != runtimecontracts.EventConsumerBoundaryExternal {
					t.Fatalf("%s lacks typed accepted external boundary", tc.emitted)
				}
			default:
				t.Fatalf("unsupported disposition %q", tc.disposition)
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
		{id: "B081", fixture: "tests/tier11-flow-composition/test-wildcard-deep-subscription", nodeID: "collector", trigger: "child/task.done", wantEmitted: "pipeline.complete"},
		{id: "B100", fixture: "tests/tier4-cross-entity/test-create-entity", nodeID: "test-node", trigger: "entity.create_requested", payload: map[string]any{"child_id": "child-001"}, wantEmitted: "entity.created"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			bundle := loadProducerRoutingFixture(t, tc.fixture)
			if tc.id == "B100" {
				node := bundle.Nodes[tc.nodeID]
				handler := node.EventHandlers[tc.trigger]
				handler.Action = runtimecontracts.ActionSpec{}
				node.EventHandlers[tc.trigger] = handler
				bundle.Nodes[tc.nodeID] = node
			}
			payload, err := json.Marshal(tc.payload)
			if err != nil {
				t.Fatal(err)
			}
			source := semanticview.Wrap(bundle)
			previewCtx := runtimedelivery.WithRoute(context.Background(), events.DeliveryRoute{
				Recipient: events.MustNodeDeliveryRecipient(tc.nodeID),
				Target: events.MustExistingEntityTarget(events.RouteIdentity{
					FlowID: source.WorkflowName(), FlowInstance: "00000000-0000-0000-0000-000000000001", EntityID: "fixture-entity",
				}),
			})
			preview, err := runtimepipeline.PreviewContractHandlerExecution(
				previewCtx,
				bundle,
				tc.nodeID,
				eventtest.RunCreatingRootIngress(
					"event-"+strings.ToLower(tc.id), events.EventType(tc.trigger), "fixture-harness", "", payload, 0,
					"00000000-0000-0000-0000-000000000001", "", events.EnvelopeForEntityID(events.EventEnvelope{}, "fixture-entity"), time.Now().UTC(),
				),
				runtimepipeline.WorkflowState{Stage: runtimepipeline.NormalizeWorkflowStateID(source.WorkflowInitialStage())},
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

func TestProducerRoutingRetirementExcludedDeadOutputs(t *testing.T) {
	cases := []struct {
		id    string
		path  string
		event string
	}{
		{id: "B080", path: "tests/tier11-flow-composition/test-wildcard-deep-subscription/flows/child/nodes.yaml", event: "work.finished"},
		{id: "B099", path: "tests/tier4-cross-entity/test-create-entity/flows/child-flow/nodes.yaml", event: "child.done"},
		{id: "B191", path: "tests/tier9-composition-patterns/test-compose-multi-emit-cross-flow/flows/tracker/nodes.yaml", event: "task.recorded"},
	}
	repoRoot := canonicalrouting.RepoRoot(t)
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			raw := readProducerRoutingProofYAML(t, filepath.Join(repoRoot, tc.path))
			if containsProducerRoutingValue(emittedEventsInYAML(raw), tc.event) {
				t.Fatalf("dead event %q is still emitted", tc.event)
			}
			schema := readProducerRoutingProofYAML(t, filepath.Join(repoRoot, filepath.Dir(tc.path), "schema.yaml"))
			if _, exists := outputPinSinksInYAML(schema)[tc.event]; exists {
				t.Fatalf("dead event %q still has an output pin", tc.event)
			}
		})
	}
}

func TestCheckedYAMLRejectsAllProducerRoutingAuthority(t *testing.T) {
	repoRoot := canonicalrouting.RepoRoot(t)
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if (entry.Name() == "nodes.yaml" || entry.Name() == "schema.yaml") && hasRetiredProducerRoutingFile(t, path) {
			t.Errorf("%s retains retired emit.target or emit.broadcast", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestProducerRoutingRetirementGuardIgnoresNestedLiteralEmitMap(t *testing.T) {
	raw := []byte("worker:\n  id: worker\n  event_handlers:\n    request:\n      emit:\n        event: task.done\n        fields:\n          config:\n            literal:\n              emit:\n                broadcast: true\n")
	retired, err := runtimecontracts.HasRetiredProducerRoutingYAML(raw)
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
			retired, err := runtimecontracts.HasRetiredProducerRoutingYAML([]byte(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if !retired {
				t.Fatal("schema-owned retired routing was not classified")
			}
		})
	}
}

func loadProducerRoutingRetirementLedger(t testing.TB) producerRoutingRetirementLedger {
	t.Helper()
	var ledger producerRoutingRetirementLedger
	readProducerRoutingProofYAML(t, filepath.Join("testdata", "producer_routing_retirement_ledger.yaml"), &ledger)
	return ledger
}

func readProducerRoutingProofYAML(t testing.TB, path string, out ...any) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
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

func outputPinSinksInYAML(document map[string]any) map[string]string {
	out := map[string]string{}
	pins, _ := document["pins"].(map[string]any)
	outputs, _ := pins["outputs"].(map[string]any)
	eventsList, _ := outputs["events"].([]any)
	for _, value := range eventsList {
		switch pin := value.(type) {
		case string:
			out[strings.TrimSpace(pin)] = ""
		case map[string]any:
			eventType, _ := pin["event"].(string)
			if strings.TrimSpace(eventType) == "" {
				eventType, _ = pin["name"].(string)
			}
			sink, _ := pin["sink"].(string)
			out[strings.TrimSpace(eventType)] = strings.TrimSpace(sink)
		}
	}
	return out
}

func hasRetiredProducerRoutingFile(t testing.TB, path string) bool {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	retired, err := runtimecontracts.HasRetiredProducerRoutingYAML(raw)
	if err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return retired
}

func repositoryTestEntrypoints(t testing.TB, repoRoot string) map[string]struct{} {
	t.Helper()
	out := map[string]struct{}{}
	err := filepath.WalkDir(repoRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "vendor" {
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
