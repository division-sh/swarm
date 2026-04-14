package bootverify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func TestRun_MapsMissingToolToToolResolutionWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-tool-missing")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "tool_resolution", "nonexistent_tool") {
		t.Fatalf("expected tool_resolution warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForBuiltinRuntimeToolReference(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", Tools: []string{"schedule"}, Permissions: []string{"schedule"}},
	}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "tool_resolution", "schedule") {
		t.Fatalf("unexpected tool_resolution warning for builtin runtime tool, got %#v", report.Warnings())
	}
}

func TestRun_MapsMissingDiscoveredMCPToolToToolResolutionWarning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		switch req["method"] {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
				},
			})
		case "notifications/initialized":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"result":  map[string]any{},
			})
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tools": []map[string]any{{
						"name": "ping",
					}},
				},
			})
		default:
			t.Fatalf("unexpected mcp method %v", req["method"])
		}
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				Tools: []string{"infra.missing"},
			},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport": "http",
						"url":       server.URL,
						"prefix":    "infra",
					},
				},
			},
		}},
	})

	report := Run(context.Background(), source, Options{CheckMCPReachable: true})

	if !reportContains(report.Warnings(), "tool_resolution", "infra.missing") {
		t.Fatalf("expected tool_resolution warning for undiscovered mcp tool, got %#v", report.Warnings())
	}
}

func TestRun_MapsMissingContractMCPToolToToolResolutionWarning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		switch req["method"] {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
				},
			})
		case "notifications/initialized":
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": map[string]any{}})
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tools": []map[string]any{{
						"name": "ping",
					}},
				},
			})
		default:
			t.Fatalf("unexpected mcp method %v", req["method"])
		}
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {Tools: []string{"infra.missing"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"infra.missing": {HandlerType: "mcp"},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport": "http",
						"url":       server.URL,
						"prefix":    "infra",
					},
				},
			},
		}},
	})

	report := Run(context.Background(), source, Options{CheckMCPReachable: true})

	if !reportContains(report.Warnings(), "tool_resolution", "infra.missing") {
		t.Fatalf("expected tool_resolution warning for undiscovered contract mcp tool, got %#v", report.Warnings())
	}
}

func TestRun_MapsEventNoSchemaToEventChainIntegrityWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-event-no-schema")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "event_chain_integrity", "orphan.event") {
		t.Fatalf("expected event_chain_integrity warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsEventNoConsumerToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-event-no-consumer")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "event_consumer_exists", "orphan.unconsumed") {
		t.Fatalf("expected event_consumer_exists warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsEventNoProducerToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-event-no-producer")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "event_producer_exists", "task.requested") {
		t.Fatalf("expected event_producer_exists warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForEventConsumerExistsWhenCatalogDeclaresConsumerMetadata(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"producer": {
					"task.start": {Emits: runtimecontracts.EventEmission{Single: "task.done"}},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"producer": {
				SubscribesTo: []string{"task.start"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.start": {
						Emits: runtimecontracts.EventEmission{Single: "task.done"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.start": {Source: "external"},
			"task.done":  {ConsumerType: []string{"dashboard"}},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if reportContains(report.Warnings(), "event_consumer_exists", "task.done") {
		t.Fatalf("unexpected event_consumer_exists warning with consumer metadata, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForEventProducerExistsWhenCatalogDeclaresExternalOrPlannedSource(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		entry runtimecontracts.EventCatalogEntry
	}{
		{
			name:  "external source",
			entry: runtimecontracts.EventCatalogEntry{Source: "external system"},
		},
		{
			name:  "planned status",
			entry: runtimecontracts.EventCatalogEntry{Status: "planned"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := &runtimecontracts.WorkflowContractBundle{
				Platform: runtimecontracts.PlatformSpecDocument{},
				Semantics: runtimecontracts.WorkflowSemanticView{
					NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
						"consumer": {
							"task.requested": {},
						},
					},
				},
				Nodes: map[string]runtimecontracts.SystemNodeContract{
					"consumer": {
						SubscribesTo: []string{"task.requested"},
						EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
							"task.requested": {},
						},
					},
				},
				Events: map[string]runtimecontracts.EventCatalogEntry{
					"task.requested": tc.entry,
				},
			}
			bundle.Platform.Platform.Name = "test"
			bundle.Platform.Platform.Version = "1.0.0"
			source := semanticview.Wrap(bundle)

			report := Run(context.Background(), source, Options{})

			if reportContains(report.Warnings(), "event_producer_exists", "task.requested") {
				t.Fatalf("unexpected event_producer_exists warning for %s, got %#v", tc.name, report.Warnings())
			}
		})
	}
}

func TestRun_MapsMissingPromptToPromptExistsWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-prompt-missing")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "prompt_exists", "promptless-agent") {
		t.Fatalf("expected prompt_exists warning, got %#v", report.Warnings())
	}
}

func TestEventProducedExternallyLocal_AllowsAnnotatedSourceText(t *testing.T) {
	t.Parallel()

	entry := runtimecontracts.EventCatalogEntry{Source: "platform (timer system)"}
	if !eventProducedExternallyLocal(entry) {
		t.Fatal("expected platform source annotation to count as externally produced")
	}
}

func TestRun_ReportsRecordEvidenceMissingEvidenceTarget(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-a": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.completed": {
						Action: runtimecontracts.ActionSpec{ID: "record_evidence"},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "record_evidence is missing evidence_target") {
		t.Fatalf("expected handler_field_compliance error, got %#v", report.Errors())
	}
}

func TestRun_MapsPromptStubToPromptExistsWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-prompt-stub")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "prompt_exists", "TODO") {
		t.Fatalf("expected prompt_exists warning for stub, got %#v", report.Warnings())
	}
}

func TestRun_MapsPolicyConflictToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-policy-conflict")

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Warnings(), "policy_conflict_detection", "max_retries") {
		t.Fatalf("expected policy_conflict_detection warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsConditionPolicyToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-condition-policy")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "condition_policy_alignment", "policy.nonexistent_key") {
		t.Fatalf("expected condition_policy_alignment warning, got %#v", report.Warnings())
	}
}

func TestRun_AllowsNestedConditionPolicyReferencePresentInResolvedPolicy(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	nodeID, eventType, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("expected at least one handler")
	}
	handler.Guard = &runtimecontracts.GuardSpec{Check: "policy.retry.max_attempts > 0"}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	bundle.Policy = runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"retry": {
			Value: map[string]any{
				"max_attempts": 3,
			},
		},
	}}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "condition_policy_alignment", "policy.retry.max_attempts") {
		t.Fatalf("expected nested policy reference to be accepted, got %#v", report.Warnings())
	}
}

func TestRun_MapsRequiredAgentMismatchToNamedError(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-required-agent-missing")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "required_agents_match", "missing-agent") {
		t.Fatalf("expected required_agents_match error, got %#v", report.Errors())
	}
}

func TestRun_ReportsRequiredAgentSubscriptionMismatchForTemplateFlow(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	schema := bundle.FlowSchemas["child"]
	schema.Mode = "template"
	schema.RequiredAgents = []runtimecontracts.FlowRequiredAgent{{
		Role:         "worker",
		SubscribesTo: []string{"work.completed"},
		Emits:        []string{"work.completed"},
	}}
	bundle.FlowSchemas["child"] = schema
	bundle.Semantics.FlowAgents["child"] = append([]runtimecontracts.FlowRequiredAgent(nil), schema.RequiredAgents...)
	bundle.Agents["worker"] = runtimecontracts.AgentRegistryEntry{
		ID:               "worker",
		Role:             "worker",
		ModelTier:        "small",
		ConversationMode: "task",
		Subscriptions:    []string{"work.requested"},
		EmitEvents:       []string{"work.completed"},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "required_agents_match", "subscriptions mismatch") {
		t.Fatalf("expected template-flow required_agents_match subscriptions error, got %#v", report.Errors())
	}
}

func TestRun_ReportsRootRequiredAgentSubscriptionMismatch(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	if bundle.RootSchema == nil {
		bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{}
	}
	bundle.RootSchema.RequiredAgents = []runtimecontracts.FlowRequiredAgent{{
		Role:         "worker",
		SubscribesTo: []string{"task.completed"},
		Emits:        []string{"task.completed"},
	}}
	bundle.Agents["worker"] = runtimecontracts.AgentRegistryEntry{
		ID:               "worker",
		Role:             "worker",
		ModelTier:        "small",
		ConversationMode: "task",
		Subscriptions:    []string{"task.requested"},
		EmitEvents:       []string{"task.completed"},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "required_agents_match", "root required agent worker subscriptions mismatch") {
		t.Fatalf("expected root required_agents_match subscriptions error, got %#v", report.Errors())
	}
}

func TestRun_MapsConditionPayloadMismatchToNamedError(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-condition-payload-mismatch")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "condition_payload_alignment", "payload.nonexistent_field") {
		t.Fatalf("expected condition_payload_alignment error, got %#v", report.Errors())
	}
}

func TestRun_AllowsNestedConditionPayloadReferenceWithinEventPayloadSchema(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	nodeID, eventType, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("expected at least one handler")
	}
	handler.Guard = &runtimecontracts.GuardSpec{Check: `payload.task.id != ""`}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	entry := bundle.Events[eventType]
	entry.Payload.Properties = map[string]runtimecontracts.EventFieldSpec{
		"task": {Type: "object"},
	}
	bundle.Events[eventType] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "condition_payload_alignment", "payload.task.id") {
		t.Fatalf("expected nested payload reference to be accepted, got %#v", report.Errors())
	}
}

func TestRun_MapsPayloadCoverageMismatchToNamedError(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-payload-mismatch")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "payload_field_coverage", "writes 'foo'") {
		t.Fatalf("expected payload_field_coverage error, got %#v", report.Errors())
	}
}

func TestRun_MapsConfigFromPayloadMismatchToNamedError(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	bundle.Semantics.HandlerTransitions = []runtimecontracts.HandlerTransitionSemantic{{
		ID:        "transition-1",
		EventType: "task.requested",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			SourceEvent: "other.event",
		},
	}}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "config_from_payload_alignment", "source_event other.event does not match handler event task.requested") {
		t.Fatalf("expected config_from_payload_alignment error, got %#v", report.Errors())
	}
}

func TestRun_AllowsFanOutDerivedAccumulationSourceEvent(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	bundle.Semantics.HandlerTransitions = []runtimecontracts.HandlerTransitionSemantic{{
		ID:        "transition-1",
		EventType: "task.requested",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			SourceEvent: "fan_out.child_completed",
		},
	}}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "config_from_payload_alignment", "fan_out.child_completed") {
		t.Fatalf("expected fan_out source_event to be accepted, got %#v", report.Errors())
	}
}

func TestRun_MapsStateMachineMismatchToNamedError(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-state-machine-invalid")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "state_machine_coherence", "bogus_state") {
		t.Fatalf("expected state_machine_coherence error, got %#v", report.Errors())
	}
}

func TestRun_WarnsWhenDeclaredStateIsUnreachable(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "semantic_drift_unreachable_state", "declares state review but no transition path from initial_state waiting reaches review") {
		t.Fatalf("expected semantic_drift_unreachable_state warning, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "semantic_drift_unreachable_state", "Reachable states: active, done, waiting") {
		t.Fatalf("expected reachable-state summary, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "semantic_drift_unreachable_state", "Unreachable states: review") {
		t.Fatalf("expected unreachable-state summary, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnWhenOnCompleteBranchReachesDeclaredState(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	node := bundle.Nodes["support-node"]
	handler := node.EventHandlers["ticket.closed"]
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition:  "true",
		AdvancesTo: "review",
	}}
	node.EventHandlers["ticket.closed"] = handler
	bundle.Nodes["support-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "semantic_drift_unreachable_state", "review") {
		t.Fatalf("unexpected semantic_drift_unreachable_state warning when on_complete reaches review, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnWhenRuleBranchReachesDeclaredState(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	node := bundle.Nodes["support-node"]
	handler := node.EventHandlers["ticket.closed"]
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition:  "true",
		AdvancesTo: "review",
	}}
	node.EventHandlers["ticket.closed"] = handler
	bundle.Nodes["support-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "semantic_drift_unreachable_state", "review") {
		t.Fatalf("unexpected semantic_drift_unreachable_state warning when rules reach review, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotUseAccumulateOnCompleteAsReachabilityProof(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	node := bundle.Nodes["support-node"]
	handler := node.EventHandlers["ticket.closed"]
	handler.Accumulate = &runtimecontracts.AccumulateSpec{
		OnComplete: []runtimecontracts.HandlerRuleEntry{{
			Condition:  "true",
			AdvancesTo: "review",
		}},
	}
	node.EventHandlers["ticket.closed"] = handler
	bundle.Nodes["support-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Warnings(), "semantic_drift_unreachable_state", "review") {
		t.Fatalf("expected semantic_drift_unreachable_state warning when only accumulate.on_complete reaches review, got %#v", report.Warnings())
	}
}

func TestRun_PreservesStateMachineCoherenceErrorWhenInvalidTargetExists(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	node := bundle.Nodes["support-node"]
	handler := node.EventHandlers["ticket.closed"]
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition:  "true",
		AdvancesTo: "review",
	}}
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition:  "true",
		AdvancesTo: "bogus_state",
	}}
	node.EventHandlers["ticket.closed"] = handler
	bundle.Nodes["support-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "state_machine_coherence", "bogus_state") {
		t.Fatalf("expected state_machine_coherence error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "semantic_drift_unreachable_state", "review") {
		t.Fatalf("unexpected semantic_drift_unreachable_state warning when declared states remain reachable, got %#v", report.Warnings())
	}
}

func TestRun_MapsDialectDualToNamedError(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	nodeID, eventType, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("expected at least one handler")
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{Condition: "true"}}
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{Condition: "true"}}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "dialect_compliance", "declares both on_complete and rules") {
		t.Fatalf("expected dialect_compliance error, got %#v", report.Errors())
	}
}

func TestRun_MapsInvalidFieldDetectionToNamedError(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"flow-a": {
				InitialState: "pending",
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{},
		Tools:  map[string]runtimecontracts.ToolSchemaEntry{},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.0.0"
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "invalid_field_detection", "flow schema flow-a missing required field name") {
		t.Fatalf("expected invalid_field_detection error, got %#v", report.Errors())
	}
}

func TestRun_RejectsRootAgentFlowSessionScope(t *testing.T) {
	root := writeSessionScopeValidationFixture(t, `
root-flow:
  id: root-flow
  model_tier: sonnet
  conversation_mode: session
  session_scope: flow
  subscriptions:
    - item.created
`, "", "")

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	if !reportContains(report.Errors(), "invalid_field_detection", "session_scope flow requires flow-scoped declaration") {
		t.Fatalf("expected session_scope flow declaration error, got %#v", report.Errors())
	}
}

func TestRun_RejectsEntitySessionScopeInStatelessFlow(t *testing.T) {
	root := writeSessionScopeValidationFixture(t, "{}\n", `
name: support
`, `
entity-agent:
  id: entity-agent
  model_tier: sonnet
  conversation_mode: session_per_entity
  session_scope: entity
  subscriptions:
    - item.created
`)

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	if !reportContains(report.Errors(), "invalid_field_detection", "session_scope entity requires stateful flow support") {
		t.Fatalf("expected stateful flow session_scope error, got %#v", report.Errors())
	}
}

func TestRun_AcceptsExplicitSessionScopeDeclarations(t *testing.T) {
	root := writeSessionScopeValidationFixture(t, `
root-global:
  id: root-global
  model_tier: sonnet
  conversation_mode: session
  session_scope: global
  subscriptions:
    - item.created
`, `
name: support
initial_state: waiting
states:
  - waiting
  - done
`, `
flow-agent:
  id: flow-agent
  model_tier: sonnet
  conversation_mode: session
  session_scope: flow
  subscriptions:
    - support/item.created
entity-agent:
  id: entity-agent
  model_tier: sonnet
  conversation_mode: session_per_entity
  session_scope: entity
  subscriptions:
    - support/item.created
`)

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	for _, finding := range report.Errors() {
		if finding.CheckID == "invalid_field_detection" && strings.Contains(finding.Message, "session_scope") {
			t.Fatalf("unexpected session_scope error: %#v", report.Errors())
		}
	}
}

func TestRun_AcceptsPackageBackedFlowSessionScopeDeclarations(t *testing.T) {
	root := writePackageBackedSessionScopeValidationFixture(t, `
name: support
initial_state: waiting
states:
  - waiting
  - done
`, `
flow-agent:
  id: flow-agent
  model_tier: sonnet
  conversation_mode: session
  session_scope: flow
  subscriptions:
    - support/item.created
entity-agent:
  id: entity-agent
  model_tier: sonnet
  conversation_mode: session_per_entity
  session_scope: entity
  subscriptions:
    - support/item.created
`)

	source := loadSessionScopeValidationFixture(t, root)
	report := Run(context.Background(), source, Options{})

	for _, finding := range report.Errors() {
		if finding.CheckID == "invalid_field_detection" && strings.Contains(finding.Message, "session_scope") {
			t.Fatalf("unexpected package-backed session_scope error: %#v", report.Errors())
		}
	}
}

func TestRun_RejectsPackageBackedEntitySessionScopeInStatelessFlow(t *testing.T) {
	root := writePackageBackedSessionScopeValidationFixture(t, `
name: support
`, `
entity-agent:
  id: entity-agent
  model_tier: sonnet
  conversation_mode: session_per_entity
  session_scope: entity
  subscriptions:
    - support/item.created
`)

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	if !reportContains(report.Errors(), "invalid_field_detection", "session_scope entity requires stateful flow support") {
		t.Fatalf("expected stateful flow session_scope error for package-backed flow, got %#v", report.Errors())
	}
}

func TestRun_MapsHandlerFieldComplianceToNamedError(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	nodeID, eventType, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("expected at least one handler")
	}
	node := bundle.Nodes[nodeID]
	handler.Action = runtimecontracts.ActionSpec{ID: "missing.handler.action"}
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "action missing.handler.action is not executable") {
		t.Fatalf("expected handler_field_compliance error, got %#v", report.Errors())
	}
}

func TestRun_MapsSelfEmitToEventCycleDetection(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-self-emit")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "event_cycle_detection", "emits its own trigger event") {
		t.Fatalf("expected event_cycle_detection error, got %#v", report.Errors())
	}
}

func TestRun_MapsSelfEmitToEventCycleDetectionForFlowLocalHandlers(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-local-events"))
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Emits = runtimecontracts.EventEmission{Single: eventType}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "event_cycle_detection", "emits its own trigger event") {
		t.Fatalf("expected event_cycle_detection error for flow-local handler, got %#v", report.Errors())
	}
}

func TestRun_ReportsSemanticModelMultiHopEventCycle(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-self-emit")
	bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{
		"node-a": {
			"task.a": {Emits: runtimecontracts.EventEmission{Single: "task.b"}},
		},
		"node-b": {
			"task.b": {Emits: runtimecontracts.EventEmission{Single: "task.a"}},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"node-a": {
			ID:           "node-a",
			SubscribesTo: []string{"task.a"},
			Produces:     []string{"task.b"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"task.a": {Emits: runtimecontracts.EventEmission{Single: "task.b"}},
			},
		},
		"node-b": {
			ID:           "node-b",
			SubscribesTo: []string{"task.b"},
			Produces:     []string{"task.a"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"task.b": {Emits: runtimecontracts.EventEmission{Single: "task.a"}},
			},
		},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"task.a": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"entity_id": {Type: "string"}}}},
		"task.b": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"entity_id": {Type: "string"}}}},
	}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "event_cycle_detection", "EVENT-CYCLE") {
		t.Fatalf("expected semantic-model event_cycle_detection error, got %#v", report.Errors())
	}
}

func TestRun_MapsBareConditionToConditionExpressionValidation(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-bare-condition")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected validation errors, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "condition_expression_validation", "missing required prefix") {
		t.Fatalf("expected condition_expression_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsUnsupportedGuardOnFail(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Guard: &runtimecontracts.GuardSpec{
							Check:  "entity.entity_id != null",
							OnFail: "explode",
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "condition_expression_validation", `unsupported guard on_fail action "explode"`) {
		t.Fatalf("expected unsupported on_fail error, got %#v", report.Errors())
	}
}

func TestRun_RejectsMalformedConditionCELAfterRecognizedPrefix(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Guard: &runtimecontracts.GuardSpec{Check: "entity.entity_id =="},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "condition_expression_validation", `CEL parse failed for "entity.entity_id =="`) {
		t.Fatalf("expected CEL parse failure, got %#v", report.Errors())
	}
}

func TestRun_RejectsFanOutNamespaceInGuardConditions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Guard: &runtimecontracts.GuardSpec{Check: "fan_out.count > 0"},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "condition_expression_validation", "fan_out.count > 0") {
		t.Fatalf("expected fan_out guard condition to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsItemNamespaceOutsideFilterConditions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Rules: []runtimecontracts.HandlerRuleEntry{{
							Condition: "item.score > 0",
						}},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "condition_expression_validation", "item.score > 0") {
		t.Fatalf("expected rule item condition to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsAccumulatedNamespaceInDataAccumulationExpressions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			EntitySchema: runtimecontracts.EntitySchema{
				Groups: []runtimecontracts.EntitySchemaGroup{{
					Name: "tracking",
					Fields: []runtimecontracts.EntitySchemaField{
						{Name: "expected_count", Type: "integer"},
					},
				}},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
							Writes: []runtimecontracts.WorkflowDataWrite{
								{TargetField: "expected_count", Value: runtimecontracts.CELExpression("accumulated.size()")},
							},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "data_accumulation_expression_validation", "accumulated.size()") {
		t.Fatalf("expected accumulated namespace in data_accumulation expression to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsAccumulatedNamespaceInPayloadTransformExpressions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						PayloadTransform: &runtimecontracts.PayloadTransformSpec{
							Fields: map[string]string{
								"bad": "accumulated.size()",
							},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "payload_transform_expression_validation", "accumulated.size()") {
		t.Fatalf("expected accumulated namespace in payload_transform expression to fail validation, got %#v", report.Errors())
	}
}

func TestRun_PreservesPermissionMismatchWarningsDuringMigration(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-permission-tool-mismatch")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "agent_permission_validation", "lookup_data") {
		t.Fatalf("expected agent_permission_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsReservedPlatformNamespaceToNamedCheck(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	bundle.Events["platform.forbidden"] = runtimecontracts.EventCatalogEntry{}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "platform_namespace_violation", "reserved platform.* namespace") {
		t.Fatalf("expected platform_namespace_violation error, got %#v", report.Errors())
	}
}

func TestRun_MapsReservedPlatformNamespaceInAgentEmitEventsToNamedCheck(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	agent := bundle.Agents["intake-agent"]
	agent.EmitEvents = []string{"platform.forbidden"}
	bundle.Agents["intake-agent"] = agent
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "platform_namespace_violation", "emit_events references reserved platform.* namespace") {
		t.Fatalf("expected platform_namespace_violation error, got %#v", report.Errors())
	}
}

func TestRun_MapsInvalidNativeToolsToNamedCheck(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	agent := bundle.Agents["intake-agent"]
	agent.NativeTools = map[string]any{"mystery_tool": true, "bash": "yes"}
	bundle.Agents["intake-agent"] = agent
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "native_tools_valid", "native_tools.mystery_tool") {
		t.Fatalf("expected native_tools_valid error for unknown capability, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "native_tools_valid", "native_tools.bash must be boolean") {
		t.Fatalf("expected native_tools_valid error for non-boolean value, got %#v", report.Errors())
	}
}

func TestRun_MapsProducesDriftToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-produces-drift")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "produces_drift", "outside produces list") {
		t.Fatalf("expected produces_drift warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForProducesDriftWhenDeclaredEventsMatchEmits(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(bootverifyDeclarationDriftBundle()), Options{})

	if reportContains(report.Warnings(), "produces_drift", "outside produces list") {
		t.Fatalf("unexpected produces_drift warning for matching declaration/emission, got %#v", report.Warnings())
	}
}

func TestRun_MapsPhantomProducesToNamedWarning(t *testing.T) {
	bundle := bootverifyDeclarationDriftBundle()
	bundle.Nodes["producer"] = runtimecontracts.SystemNodeContract{
		Produces: []string{"task.done", "task.unused"},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"task.start": {Emits: runtimecontracts.EventEmission{Single: "task.done"}},
		},
		SubscribesTo: []string{"task.start"},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "phantom_produces", "task.unused") {
		t.Fatalf("expected phantom_produces warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForPhantomProducesWhenDeclaredEventsMatchEmits(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(bootverifyDeclarationDriftBundle()), Options{})

	if reportContains(report.Warnings(), "phantom_produces", "no handler emits") {
		t.Fatalf("unexpected phantom_produces warning for matching declaration/emission, got %#v", report.Warnings())
	}
}

func TestRun_WarnsWhenPayloadTransformOmitsRequiredEmittedField(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.PayloadTransform = &runtimecontracts.PayloadTransformSpec{
		Fields: map[string]string{
			"geography": "payload.geography",
			"mode":      "payload.mode",
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("expected payload completeness warning for scan_id, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "semantic_drift_payload_completeness", "payload_transform: present") {
		t.Fatalf("expected payload completeness warning to mention payload_transform presence, got %#v", report.Warnings())
	}
}

func TestRun_WarnsWithoutPayloadTransformEvenWhenContextSuggestsPassthrough(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	entry := bundle.Events["market_research.scan_assigned"]
	entry.Required = []string{"entity_id", "scan_id"}
	bundle.Events["market_research.scan_assigned"] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("expected payload completeness warning for scan_id, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "semantic_drift_payload_completeness", "payload_transform: absent") {
		t.Fatalf("expected payload completeness warning to mention missing payload_transform, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "semantic_drift_payload_completeness", "trigger schema declares scan_id: yes (required)") {
		t.Fatalf("expected payload completeness warning to mention trigger schema context, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "semantic_drift_payload_completeness", "entity schema declares scan_id: yes") {
		t.Fatalf("expected payload completeness warning to mention entity schema context, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnWhenTransformAndPlatformForcedFieldsCoverRequiredPayload(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.PayloadTransform = &runtimecontracts.PayloadTransformSpec{
		Fields: map[string]string{
			"scan_id":   "payload.scan_id",
			"geography": "payload.geography",
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("unexpected payload completeness warning when transform covers required fields, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnWhenNormalizedTransformTargetsCoverRequiredPayload(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		body      string
		transform *runtimecontracts.PayloadTransformSpec
	}{
		{
			name: "mappings",
			body: "payload_transform:\n  mappings:\n    scan_id: payload.scan_id\n",
		},
		{
			name: "entries",
			transform: &runtimecontracts.PayloadTransformSpec{
				Entries: []runtimecontracts.TransformSpec{{
					Target: "scan_id",
					Value:  runtimecontracts.CELExpression("payload.scan_id"),
				}},
			},
		},
		{
			name: "shorthand",
			body: "payload_transform:\n  scan_id: payload.scan_id\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := bootverifyPayloadCompletenessBundle()
			node := bundle.Nodes["dispatcher"]
			handler := node.EventHandlers["scan.corpus_dispatch"]
			if tc.transform != nil {
				handler.PayloadTransform = tc.transform
			} else {
				handler.PayloadTransform = mustPayloadCompletenessTransform(t, tc.body)
			}
			node.EventHandlers["scan.corpus_dispatch"] = handler
			bundle.Nodes["dispatcher"] = node
			bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if reportContains(report.Warnings(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
				t.Fatalf("unexpected payload completeness warning for %s transform form, got %#v", tc.name, report.Warnings())
			}
		})
	}
}

func TestRun_DoesNotWarnWhenOnlyPlatformForcedFieldsAreRequired(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	entry := bundle.Events["market_research.scan_assigned"]
	entry.Required = []string{"entity_id", "current_state"}
	bundle.Events["market_research.scan_assigned"] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "semantic_drift_payload_completeness", "not statically provable") {
		t.Fatalf("unexpected payload completeness warning for platform-forced-only schema, got %#v", report.Warnings())
	}
}

func TestRun_ReportsInputPinWiringWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-missing-pin")

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("expected input_pin_wiring warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForExternalInputPinWithoutEmitter(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	entry := bundle.Events["task.feedback"]
	entry.Source = "external"
	bundle.Events["task.feedback"] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("unexpected input_pin_wiring warning for external input, got %#v", report.Warnings())
	}
}

func TestRun_ConstrainsExternalInputProducerPathToConsumingScope(t *testing.T) {
	root := writeInputPinExternalScopeFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	var (
		externalCleared bool
		plainWarned     bool
	)
	for _, finding := range report.Warnings() {
		if finding.CheckID != "input_pin_wiring" || !strings.Contains(finding.Message, "ticket.ready") {
			continue
		}
		switch finding.Location {
		case "external_consumer":
			externalCleared = true
		case "plain_consumer":
			plainWarned = true
		}
	}
	if externalCleared {
		t.Fatalf("unexpected input_pin_wiring warning for external_consumer, got %#v", report.Warnings())
	}
	if !plainWarned {
		t.Fatalf("expected input_pin_wiring warning for plain_consumer, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForSiblingFlowOutputPinInputProducerPath(t *testing.T) {
	root := writeCrossFlowPinAmbiguityFixture(t, false)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	bundle.Semantics.FlowOutputs["producer_b"] = nil

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "ticket.ready") {
		t.Fatalf("unexpected input_pin_wiring warning for sibling output pin proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForRootAgentEmitInputProducerPath(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Agents["lifecycle-coordinator"] = runtimecontracts.AgentRegistryEntry{
		ID:         "lifecycle-coordinator",
		EmitEvents: []string{"task.feedback"},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("unexpected input_pin_wiring warning for root agent emit proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForRootNodeHandlerEmitInputProducerPath(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	node := bundle.Nodes["dispatcher"]
	node.EventHandlers["task.requested"] = runtimecontracts.SystemNodeEventHandler{
		Emits: runtimecontracts.EventEmission{Single: "child/task.feedback"},
	}
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["task.requested"] = node.EventHandlers["task.requested"]

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("unexpected input_pin_wiring warning for root handler emit proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForPlatformEventCatalogInputProducerPath(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Platform.PlatformEvents.Catalog = map[string]yaml.Node{
		"platform.runtime_log": {},
	}
	renameFlowHandlerEvent(t, bundle, "child", "worker", "task.feedback", "platform.runtime_log", runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		AdvancesTo:   "done",
		Emits:        runtimecontracts.EventEmission{Single: "task.result"},
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "platform.runtime_log") {
		t.Fatalf("unexpected input_pin_wiring warning for platform event proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForSameFlowTimerInputProducerPath(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	node := bundle.Nodes["worker"]
	node.Timers = append(node.Timers, runtimecontracts.WorkflowTimerContract{
		ID:     "feedback-timeout",
		Event:  "task.feedback",
		FlowID: "child",
		NodeID: "worker",
	})
	bundle.Nodes["worker"] = node
	bundle.Semantics.Timers = append(bundle.Semantics.Timers, runtimecontracts.WorkflowTimerContract{
		ID:     "feedback-timeout",
		Event:  "task.feedback",
		FlowID: "child",
		NodeID: "worker",
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("unexpected input_pin_wiring warning for same-flow timer proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotUseProducesOrPlannedAsInputProducerPathProof(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*runtimecontracts.WorkflowContractBundle)
	}{
		{
			name: "produces only",
			mutate: func(bundle *runtimecontracts.WorkflowContractBundle) {
				node := bundle.Nodes["dispatcher"]
				node.Produces = append(node.Produces, "child/task.feedback")
				bundle.Nodes["dispatcher"] = node
			},
		},
		{
			name: "planned status",
			mutate: func(bundle *runtimecontracts.WorkflowContractBundle) {
				entry := bundle.Events["task.feedback"]
				entry.Status = "planned"
				bundle.Events["task.feedback"] = entry
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
			tc.mutate(bundle)

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
				t.Fatalf("expected input_pin_wiring warning for %s, got %#v", tc.name, report.Warnings())
			}
		})
	}
}

func TestRun_ReportsConflictingWritePinOwners(t *testing.T) {
	root := writeCrossFlowPinAmbiguityFixture(t, false)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
	bundle.Semantics.FlowWrites["producer_a"] = []string{"ticket.status"}
	bundle.Semantics.FlowWrites["producer_b"] = []string{"ticket.status"}
	bundle.Semantics.WritePinOwners["ticket.status"] = []string{"producer_a", "producer_b"}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "write_pin_ownership_validation", "ticket.status") {
		t.Fatalf("expected write_pin_ownership_validation error, got %#v", report.Errors())
	}
}

func TestRun_DoesNotWarnForLocalizedCrossFlowEventRouting(t *testing.T) {
	root := writeLocalizedEventRoutingFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "event_producer_exists", "consumer/ticket.ready") {
		t.Fatalf("unexpected event_producer_exists warning for localized flow input, got %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "event_consumer_exists", "producer/ticket.ready") {
		t.Fatalf("unexpected event_consumer_exists warning for localized producer output, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForFlowLocalEmittedEventsWithOwningFlowSchemas(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-local-events"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "event_chain_integrity", "child/child.internal") {
		t.Fatalf("unexpected event_chain_integrity warning for child/child.internal, got %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "event_chain_integrity", "child/child.done") {
		t.Fatalf("unexpected event_chain_integrity warning for child/child.done, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForFlowOwnedAgentEmissionsDeclaredAsFlowOutputs(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-required-agents-child"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "event_consumer_exists", "analysis.done") {
		t.Fatalf("unexpected event_consumer_exists warning for analysis.done flow output, got %#v", report.Warnings())
	}
}

func TestRun_ReportsExpressionFieldReferenceWarning(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = false
	handler.Condition = "entity.missing_score >= 70"
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Warnings(), "expression_field_reference_validation", "entity.missing_score") {
		t.Fatalf("expected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "expression_field_reference_validation", "did you mean accumulated.filter()?") {
		t.Fatalf("expected accumulated.filter hint, got %#v", report.Warnings())
	}
}

func TestRun_RejectsCreateEntityConditionReferenceToFieldClearedLaterInSameHandler(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "tracking",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "revision_count", Type: "integer"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Condition = "entity.revision_count > 0"
	handler.Clear = &runtimecontracts.ClearSpec{Target: "revision_count"}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsGuardReferenceToSparseFieldEvenWhenSameHandlerWritesFieldLater(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.missing_score >= 70"}
	handler.DataAccumulation.Writes = append(handler.DataAccumulation.Writes, runtimecontracts.WorkflowDataWrite{
		TargetField: "missing_score",
		SourceField: "score",
	})
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.missing_score") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestHandlerEntityFieldWriters_TracksSetsGateAndClearTargets(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		SetsGate: &runtimecontracts.GateSpec{Name: "approved", Value: true},
		Clear: &runtimecontracts.ClearSpec{
			Target:  "revision_count",
			Targets: []string{"entity.base_score"},
		},
	}

	writers := handlerEntityFieldWriters(handler)
	if _, ok := writers["gates"]; !ok {
		t.Fatalf("gates missing from handler writers: %#v", writers)
	}
	if _, ok := writers["revision_count"]; !ok {
		t.Fatalf("revision_count missing from handler writers: %#v", writers)
	}
	if _, ok := writers["base_score"]; !ok {
		t.Fatalf("base_score missing from handler writers: %#v", writers)
	}
}

func TestRun_RejectsFilterReferenceToSparseFieldEvenWhenSameHandlerComputesFieldLater(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Filter = &runtimecontracts.FilterSpec{
		Source:    "payload.items",
		ItemsFrom: "payload.items",
		Condition: "entity.filtered_score >= 70",
		StoreAs:   "entity.filtered_items",
	}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.filtered_score",
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.filtered_score") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsExpressionFieldReferenceErrorForDataAccumulationExpressionThatDependsOnSiblingWrite(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{
		{
			TargetField: "base_score",
			SourceField: "score",
		},
		{
			TargetField: "adjusted_score",
			Value:       runtimecontracts.CELExpression("entity.base_score + 1"),
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.base_score") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsExpressionFieldReferenceForSelfTargetEntityUpdate(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier9-composition-patterns", "test-compose-guard-counter-escalate"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_AllowsGuardReferenceToPersistedFieldEvenWhenHandlerWritesFieldLater(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = false
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.retry_count < 3"}
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "retry_count",
		Value:       runtimecontracts.CELExpression("entity.retry_count + 1"),
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_AllowsOnCompleteReferenceToPersistedFieldEvenWhenHandlerWritesFieldLater(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "validation_phase",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "revision_count", Type: "integer", Initial: 0},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "revision_count",
		Value:       runtimecontracts.CELExpression("entity.revision_count + 1"),
	}}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		ID:        "retry",
		Condition: "entity.revision_count < 3",
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_AllowsCreateEntityGuardReferenceToSchemaInitializedField(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "validation_phase",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "revision_count", Type: "integer", Initial: 0},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.revision_count == 0"}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsSparsePresenceChecksWithoutInitializer(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "validation_phase",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "kill_reason", Type: "text"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: "has(entity.kill_reason) || entity.kill_reason == null"}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.kill_reason") {
		t.Fatalf("unexpected sparse-field validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsHasGuardedTernaryReadWithoutInitializer(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "validation_phase",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "kill_reason", Type: "text"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: `has(entity.kill_reason) ? entity.kill_reason == "manual" : true`}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.kill_reason") {
		t.Fatalf("unexpected guarded ternary sparse-field validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsCreateEntityGuardReferenceToFieldInitializedOnlyBySameHandlerDataAccumulation(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "validation_phase",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "revision_count", Type: "integer"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.revision_count == 0"}
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "revision_count",
		Value:       runtimecontracts.LiteralExpression(0),
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityPayloadTransformReadOfSameHandlerTopLevelWrite(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "tracking",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "revision_count", Type: "integer"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "revision_count",
		Value:       runtimecontracts.LiteralExpression(0),
	}}
	handler.PayloadTransform = &runtimecontracts.PayloadTransformSpec{
		Fields: map[string]string{
			"revision_count": "entity.revision_count",
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsCreateEntityPayloadTransformReadOfRuleOnlyWrite(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "tracking",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "revision_count", Type: "integer"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.entity_id != null",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "revision_count",
				Value:       runtimecontracts.LiteralExpression(0),
			}},
		},
	}}
	handler.PayloadTransform = &runtimecontracts.PayloadTransformSpec{
		Fields: map[string]string{
			"revision_count": "entity.revision_count",
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsCreateEntityPayloadTransformReadOfRuleOnlyComputeOutput(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "tracking",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "revision_count", Type: "integer"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.entity_id != null",
		Compute: &runtimecontracts.ComputeSpec{
			Operation: runtimecontracts.ComputeOpCount,
			StoreAs:   "entity.revision_count",
		},
	}}
	handler.PayloadTransform = &runtimecontracts.PayloadTransformSpec{
		Fields: map[string]string{
			"revision_count": "entity.revision_count",
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityPayloadTransformReadWhenRuleAlsoWritesUnconditionallyAvailableField(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "tracking",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "revision_count", Type: "integer"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.revision_count",
	}
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.entity_id != null",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "revision_count",
				Value:       runtimecontracts.LiteralExpression(0),
			}},
		},
	}}
	handler.PayloadTransform = &runtimecontracts.PayloadTransformSpec{
		Fields: map[string]string{
			"revision_count": "entity.revision_count",
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_PreservesSiblingWriteErrorWhenSelfTargetUpdateExistsInSameHandler(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{
		{
			TargetField: "base_score",
			Value:       runtimecontracts.CELExpression("entity.base_score + 1"),
		},
		{
			TargetField: "adjusted_score",
			Value:       runtimecontracts.CELExpression("entity.base_score + 1"),
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.base_score") {
		t.Fatalf("expected mixed-case sibling-write error, got %#v", report.Errors())
	}
}

func TestRun_PreservesSiblingWriteErrorWhenGuardAlsoReadsSparseField(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.retry_count < 3"}
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{
		{
			TargetField: "retry_count",
			Value:       runtimecontracts.CELExpression("entity.retry_count + 1"),
		},
		{
			TargetField: "adjusted_score",
			Value:       runtimecontracts.CELExpression("entity.retry_count + entity.base_score"),
		},
		{
			TargetField: "base_score",
			SourceField: "score",
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("expected sparse-field guard error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.base_score") {
		t.Fatalf("expected sibling-write dependency error, got %#v", report.Errors())
	}
}

func TestRun_AllowsTopLevelDataAccumulationExpressionToReadRuleProducedField(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = false
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition: "payload.score >= 70",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "base_score",
				SourceField: "score",
			}},
		},
	}}
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "adjusted_score",
		Value:       runtimecontracts.CELExpression("entity.base_score + 1"),
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.base_score") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_SuppressesExpressionFieldReferenceFindingWhenComputeMakesFieldAvailableBeforeOnComplete(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.composite_score >= 0",
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.composite_score") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.composite_score") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_WarnsWhenCreateEntityComputeProofDependsOnDynamicExpectedFrom(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "tracking",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "expected_count", Type: "integer", Initial: 1},
				{Name: "composite_score", Type: "number"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Accumulate = &runtimecontracts.AccumulateSpec{ExpectedFrom: "entity.expected_count"}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.composite_score >= 0",
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.composite_score") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "expression_field_reference_validation", "entity.composite_score") {
		t.Fatalf("expected degraded compute/store_as warning, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "expression_field_reference_validation", "dynamic") {
		t.Fatalf("expected dynamic expected_from warning detail, got %#v", report.Warnings())
	}
}

func TestRun_AllowsCreateEntityComputeProofWhenExpectedFromIsNotDynamicEntityField(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Semantics.EntitySchema = runtimecontracts.EntitySchema{
		Groups: []runtimecontracts.EntitySchemaGroup{{
			Name: "tracking",
			Fields: []runtimecontracts.EntitySchemaField{
				{Name: "composite_score", Type: "number"},
			},
		}},
	}
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Accumulate = &runtimecontracts.AccumulateSpec{Threshold: 1}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.composite_score >= 0",
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.composite_score") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.composite_score") {
		t.Fatalf("unexpected degraded warning for non-dynamic expected_from, got %#v", report.Warnings())
	}
}

func TestRun_RequiresCreateEntityForStatefulInputPinHandlers(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	for nodeID, node := range flowView.Nodes {
		for eventType, handler := range node.EventHandlers {
			handler.CreateEntity = false
			writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)
		}
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("expected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityForStatefulInputPinHandlers(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	for nodeID, node := range flowView.Nodes {
		for eventType, handler := range node.EventHandlers {
			handler.CreateEntity = true
			writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)
		}
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsTemplateFlowInputPinHandlersWithoutCreateEntity(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	schema, ok := bundle.FlowSchemas[flowID]
	if !ok {
		t.Fatalf("flow schema %s missing", flowID)
	}
	schema.Mode = "template"
	bundle.FlowSchemas[flowID] = schema
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	for nodeID, node := range flowView.Nodes {
		for eventType, handler := range node.EventHandlers {
			handler.CreateEntity = false
			writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)
		}
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsCrossFlowPinAmbiguityWithoutEscapeHatch(t *testing.T) {
	root := writeCrossFlowPinAmbiguityFixture(t, false)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "cross_flow_pin_ambiguity_validation", "ticket.ready") {
		t.Fatalf("expected cross_flow_pin_ambiguity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCrossFlowPinAmbiguityWithScopedEscapeHatch(t *testing.T) {
	root := writeCrossFlowPinAmbiguityFixture(t, true)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, filepath.Join(repoRootForBootverifyTest(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "cross_flow_pin_ambiguity_validation", "ticket.ready") {
		t.Fatalf("unexpected cross_flow_pin_ambiguity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsStatelessFlowInputPinHandlersWithoutCreateEntity(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	schema, ok := bundle.FlowSchemas[flowID]
	if !ok {
		t.Fatalf("flow schema %s missing", flowID)
	}
	schema.InitialState = ""
	bundle.FlowSchemas[flowID] = schema
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	for nodeID, node := range flowView.Nodes {
		for eventType, handler := range node.EventHandlers {
			handler.CreateEntity = false
			writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)
		}
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsBackpropInputPinHandlersWithoutCreateEntity(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	var (
		nodeID    string
		eventType string
		handler   runtimecontracts.SystemNodeEventHandler
		found     bool
	)
	for candidateNodeID, node := range flowView.Nodes {
		for candidateEventType, candidateHandler := range node.EventHandlers {
			nodeID = candidateNodeID
			eventType = candidateEventType
			handler = candidateHandler
			found = true
			break
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal("expected child flow handler")
	}
	newEventType := "child.killed_backprop"
	handler.CreateEntity = false
	renameFlowHandlerEvent(t, bundle, flowID, nodeID, eventType, newEventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_UsesCompiledOwnersForEquivalentSingleNodePerEventRoutes(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	rootNode := bundle.Nodes["dispatcher"]
	rootNode.EventHandlers["child/task.feedback"] = runtimecontracts.SystemNodeEventHandler{}
	bundle.Nodes["dispatcher"] = rootNode
	bundle.Semantics.NodeHandlers["dispatcher"]["child/task.feedback"] = runtimecontracts.SystemNodeEventHandler{}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "single_node_per_event", "child/task.feedback") {
		t.Fatalf("expected single_node_per_event error, got %#v", report.Errors())
	}
}

func TestRun_ReportsExactDuplicateSingleNodePerEventOwnership(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	handler := bundle.Nodes["complete-task"].EventHandlers["task.requested"]
	addProjectHandler(t, bundle, "shadow-complete-task", "task.requested", handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "single_node_per_event", "task.requested") {
		t.Fatalf("expected single_node_per_event duplicate-owner error, got %#v", report.Errors())
	}
}

func TestRun_DoesNotReportSingleNodePerEventForWildcardOnlyOverlap(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	addProjectHandler(t, bundle, "dispatcher-shadow", "child/*", runtimecontracts.SystemNodeEventHandler{})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "single_node_per_event", "child/task.feedback") {
		t.Fatalf("unexpected single_node_per_event wildcard collision, got %#v", report.Errors())
	}
}

func TestRun_ReportsMissingTransitionTriggerEvent(t *testing.T) {
	bundle := bootverifyTransitionRuntimeOwnershipBundle()
	bundle.Semantics.Transitions[0].Trigger = "ticket.missing"

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "transition_reference_validation", "trigger ticket.missing missing from event catalog") {
		t.Fatalf("expected transition_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsTransitionOwnershipMismatch(t *testing.T) {
	bundle := bootverifyTransitionRuntimeOwnershipBundle()
	bundle.Semantics.Transitions[0].Node = "projector"

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "transition_ownership_validation", "workflow owner is projector") {
		t.Fatalf("expected transition_ownership_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsMissingSemanticHandlerForOwnedRuntimeEvent(t *testing.T) {
	bundle := bootverifyTransitionRuntimeOwnershipBundle()
	event := bundle.Events["ticket.opened"]
	event.OwningNode = "dispatcher"
	bundle.Events["ticket.opened"] = event

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "event_runtime_wiring_validation", "owning_node dispatcher missing semantic event_handler") {
		t.Fatalf("expected event_runtime_wiring_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsMissingRuntimeExecutorForOwnedRuntimeEvent(t *testing.T) {
	bundle := bootverifyTransitionRuntimeOwnershipBundle()
	bundle.Nodes["idle-owner"] = runtimecontracts.SystemNodeContract{ID: "idle-owner"}
	event := bundle.Events["ticket.audit"]
	event.RuntimeHandling = "projection"
	event.OwningNode = "idle-owner"
	bundle.Events["ticket.audit"] = event

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "event ticket.audit owning_node idle-owner has no runtime executor") {
		t.Fatalf("expected handler_field_compliance runtime executor error, got %#v", report.Errors())
	}
}

func TestBootCheckRegistry_HasSpecCheckCount(t *testing.T) {
	if got := len(bootCheckRegistry); got != 40 {
		t.Fatalf("bootCheckRegistry count = %d, want 40", got)
	}
	if got := len(supplementalChecks); got != 2 {
		t.Fatalf("supplementalChecks count = %d, want 2", got)
	}
}

func TestRun_ReportsErrorForUnprefixedTimerStartOn(t *testing.T) {
	root := writeTimerValidationFixture(t, "ticket.opened", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
	if !reportContains(report.Errors(), "timer_validation", "start_on") {
		t.Fatalf("expected timer_validation start_on error, got %#v", report.Errors())
	}
}

func TestRun_ReportsErrorForTimerCancelOnBoot(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.opened", "boot")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
	if !reportContains(report.Errors(), "timer_validation", "cancel_on") {
		t.Fatalf("expected timer_validation cancel_on error, got %#v", report.Errors())
	}
}

func TestRun_ReportsErrorForUnknownTimerTriggerState(t *testing.T) {
	root := writeTimerValidationFixture(t, "state:missing_state", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
	if !reportContains(report.Errors(), "timer_validation", "unknown state") {
		t.Fatalf("expected timer_validation unknown state error, got %#v", report.Errors())
	}
}

func TestRun_ReportsWarningForUnknownTimerTriggerEvent(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.unknown", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
	if !reportContains(report.Warnings(), "timer_validation", "unknown event") {
		t.Fatalf("expected timer_validation unknown event warning, got %#v", report.Warnings())
	}
}

func TestRun_ReportsErrorForTimerMissingOwner(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.opened", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle := loadFixtureBundleAt(t, repoRoot, root, platformSpec)
	bundle.Semantics.Timers[0].Owner = ""
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "timer_validation", "missing owner") {
		t.Fatalf("expected timer_validation missing owner error, got %#v", report.Errors())
	}
}

func TestRun_ReportsErrorForTimerOwnerMissingFromParticipants(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.opened", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle := loadFixtureBundleAt(t, repoRoot, root, platformSpec)
	bundle.Semantics.Timers[0].Owner = "missing-owner"
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "timer_validation", "owner missing-owner missing from participants") {
		t.Fatalf("expected timer_validation missing participant owner error, got %#v", report.Errors())
	}
}

func TestRun_ReportsErrorForTimerEventMissingFromCatalog(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.opened", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	bundle := loadFixtureBundleAt(t, repoRoot, root, platformSpec)
	bundle.Semantics.Timers[0].Event = "timer.missing"
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "timer_validation", "event timer.missing missing from event catalog") {
		t.Fatalf("expected timer_validation missing timer event error, got %#v", report.Errors())
	}
}

func repoRootForBootverifyTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func writeBootverifyFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func loadSessionScopeValidationFixture(t *testing.T, fixtureRoot string) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	return semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, fixtureRoot, platformSpec))
}

func writeSessionScopeValidationFixture(t *testing.T, rootAgents, flowSchema, flowAgents string) string {
	t.Helper()
	root := t.TempDir()
	flows := " []"
	if strings.TrimSpace(flowSchema) != "" || strings.TrimSpace(flowAgents) != "" {
		flows = "\n  - id: support\n    flow: support\n    mode: static"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
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
flows:`+flows+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  payload:
    properties:
      entity_id:
        type: string
`)
	if strings.TrimSpace(rootAgents) == "" {
		rootAgents = "{}\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), rootAgents)
	if strings.TrimSpace(flowSchema) != "" || strings.TrimSpace(flowAgents) != "" {
		if strings.TrimSpace(flowSchema) == "" {
			flowSchema = "name: support\n"
		}
		if strings.TrimSpace(flowAgents) == "" {
			flowAgents = "{}\n"
		}
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), flowSchema)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  payload:
    properties:
      entity_id:
        type: string
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), flowAgents)
	}
	return root
}

func writePackageBackedSessionScopeValidationFixture(t *testing.T, flowSchema, packageAgents string) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
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
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  payload:
    properties:
      entity_id:
        type: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), flowSchema)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  payload:
    properties:
      entity_id:
        type: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), packageAgents)
	return root
}

type timerValidationFixtureOptions struct {
	startOn           string
	cancelOn          string
	owner             string
	event             string
	includeTimerEvent bool
}

func writeTimerValidationFixture(t *testing.T, startOn, cancelOn string) string {
	return writeTimerValidationFixtureWithOptions(t, timerValidationFixtureOptions{
		startOn:           startOn,
		cancelOn:          cancelOn,
		owner:             "support-node",
		event:             "timer.reminder",
		includeTimerEvent: true,
	})
}

func writeTimerValidationFixtureWithOptions(t *testing.T, opts timerValidationFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	if strings.TrimSpace(opts.event) == "" {
		opts.event = "timer.reminder"
	}

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: timer-validation
version: "1.0.0"
platform: ">=1.6.0"
entity_schema:
  groups:
    - name: ticket
      fields:
        - name: ticket_id
          type: string
flows:
  - id: support
    flow: support
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: timer-validation\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	timerEventBlock := ""
	if opts.includeTimerEvent {
		timerEventBlock = strings.TrimSpace(`
` + opts.event + `:
  payload:
    properties:
      entity_id:
        type: string
`)
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
terminal_states: [done]
states: [waiting, active, done]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
ticket.opened:
  payload:
    properties:
      entity_id:
        type: string
ticket.closed:
  payload:
    properties:
      entity_id:
        type: string
`+timerEventBlock+`
`)
	timerBlock := `
    - id: reminder
      owner: ` + opts.owner + `
      event: ` + opts.event + `
      delay: 1m
      start_on: ` + opts.startOn + "\n"
	if strings.TrimSpace(opts.cancelOn) != "" {
		timerBlock += "      cancel_on: " + opts.cancelOn + "\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), `
support-node:
  id: support-node
  execution_type: system_node
  subscribes_to:
    - ticket.opened
    - ticket.closed
    - timer.reminder
  timers:
`+timerBlock+`  event_handlers:
    ticket.opened:
      create_entity: true
      advances_to: active
    ticket.closed:
      advances_to: done
    timer.reminder:
      advances_to: done
`)
	return root
}

func writeCrossFlowPinAmbiguityFixture(t *testing.T, scoped bool) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: pin-ambiguity
version: "1.0.0"
platform: ">=1.6.0"
entity_schema:
  groups:
    - name: item
      fields:
        - name: item_id
          type: string
flows:
  - id: producer_a
    flow: producer_a
    mode: static
  - id: producer_b
    flow: producer_b
    mode: static
  - id: consumer
    flow: consumer
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: pin-ambiguity\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	for _, flowID := range []string{"producer_a", "producer_b"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events: []
  outputs:
    events:
      - ticket.ready
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), `
ticket.ready:
  payload:
    properties:
      entity_id:
        type: string
`)
	}

	subscription := "ticket.ready"
	if scoped {
		subscription = "producer_a/ticket.ready"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
initial_state: waiting
terminal_states: [done]
states: [waiting, done]
pins:
  inputs:
    events:
      - ticket.ready
  outputs:
    events:
      - consumer.started
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
ticket.ready:
  payload:
    properties:
      entity_id:
        type: string
consumer.started:
  payload:
    properties:
      entity_id:
        type: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to:
    - `+subscription+`
  event_handlers:
    ticket.ready:
      create_entity: true
      advances_to: done
      emits: consumer.started
`)

	return root
}

func writeInputPinExternalScopeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: input-pin-external-scope
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: external_consumer
    flow: external_consumer
    mode: static
  - id: plain_consumer
    flow: plain_consumer
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: input-pin-external-scope\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	for _, flowID := range []string{"external_consumer", "plain_consumer"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events:
      - ticket.ready
  outputs:
    events: []
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
		entry := "ticket.ready:\n  payload:\n    entity_id: string\n"
		if flowID == "external_consumer" {
			entry = "ticket.ready:\n  _source: external (manual handoff)\n  payload:\n    entity_id: string\n"
		}
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), entry)
	}

	return root
}

func writeLocalizedEventRoutingFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: localized-event-routing
version: "1.0.0"
platform: ">=1.6.0"
entity_schema:
  groups:
    - name: item
      fields:
        - name: item_id
          type: string
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: localized-event-routing\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events: []
  outputs:
    events:
      - ticket.ready
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
ticket.ready:
  payload:
    properties:
      entity_id:
        type: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to:
    - start
  produces:
    - ticket.ready
  event_handlers:
    start:
      emits: ticket.ready
`)

	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
initial_state: waiting
terminal_states: [done]
states: [waiting, done]
pins:
  inputs:
    events:
      - ticket.ready
  outputs:
    events: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
ticket.ready:
  payload:
    properties:
      entity_id:
        type: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to:
    - ticket.ready
  event_handlers:
    ticket.ready:
      create_entity: true
      advances_to: done
`)

	return root
}

func writeStateReachabilityFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: state-reachability
version: "1.0.0"
platform: ">=1.6.0"
entity_schema:
  groups:
    - name: ticket
      fields:
        - name: ticket_id
          type: string
flows:
  - id: support
    flow: support
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: state-reachability\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
terminal_states: [done]
states: [waiting, active, review, done]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
ticket.opened:
  payload:
    entity_id: string
ticket.closed:
  payload:
    entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), `
support-node:
  id: support-node
  execution_type: system_node
  subscribes_to:
    - ticket.opened
    - ticket.closed
  event_handlers:
    ticket.opened:
      create_entity: true
      advances_to: active
    ticket.closed:
      advances_to: done
`)

	return root
}

func loadTier8Fixture(t *testing.T, fixture string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadTier8FixtureBundle(t, fixture))
}

func loadTier8FixtureBundle(t *testing.T, fixture string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", fixture)
	return loadFixtureBundleAt(t, repoRoot, fixtureRoot, platformSpec)
}

func loadFixtureBundle(t *testing.T, relativeRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	fixtureRoot := filepath.Join(repoRoot, relativeRoot)
	return loadFixtureBundleAt(t, repoRoot, fixtureRoot, platformSpec)
}

func loadFixtureBundleAt(t *testing.T, repoRoot, fixtureRoot, platformSpec string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func bootverifyTransitionRuntimeOwnershipBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Transitions: []runtimecontracts.WorkflowTransitionContract{{
				ID:      "ticket-open",
				Trigger: "ticket.created",
				Node:    "dispatcher",
				Actions: []string{"emit_opened"},
				Guards:  []string{"allow_ticket"},
			}},
			ActionByID: map[string]runtimecontracts.GuardActionEntry{
				"emit_opened": {
					ID:    "emit_opened",
					Emits: "ticket.opened",
				},
			},
			GuardByID: map[string]runtimecontracts.GuardActionEntry{
				"allow_ticket": {
					ID:    "allow_ticket",
					Check: "true",
				},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"dispatcher": {
					"ticket.created": {},
				},
				"projector": {
					"ticket.opened": {},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {
				ID:               "dispatcher",
				OwnedTransitions: []string{"ticket-open"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"ticket.created": {},
				},
			},
			"projector": {
				ID: "projector",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"ticket.opened": {},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"ticket.created": {
				RuntimeHandling: "consuming",
				OwningNode:      "dispatcher",
			},
			"ticket.opened": {
				RuntimeHandling: "projection",
				OwningNode:      "projector",
			},
			"ticket.audit": {},
		},
	}
}

func bootverifyDeclarationDriftBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"producer": {
					"task.start": {Emits: runtimecontracts.EventEmission{Single: "task.done"}},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"producer": {
				SubscribesTo: []string{"task.start"},
				Produces:     []string{"task.done"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.start": {Emits: runtimecontracts.EventEmission{Single: "task.done"}},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.start": {Source: "external"},
			"task.done":  {ConsumerType: []string{"dashboard"}},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	return bundle
}

func bootverifyPayloadCompletenessBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			EntitySchema: runtimecontracts.EntitySchema{
				Groups: []runtimecontracts.EntitySchemaGroup{{
					Name: "default",
					Fields: []runtimecontracts.EntitySchemaField{
						{Name: "scan_id"},
						{Name: "geography"},
					},
				}},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"dispatcher": {
					"scan.corpus_dispatch": {
						Emits: runtimecontracts.EventEmission{Single: "market_research.scan_assigned"},
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {
				SubscribesTo: []string{"scan.corpus_dispatch"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.corpus_dispatch": {
						Emits: runtimecontracts.EventEmission{Single: "market_research.scan_assigned"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.corpus_dispatch": {
				Source: "external",
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"scan_id":   {Type: "string"},
						"geography": {Type: "string"},
						"mode":      {Type: "string"},
					},
				},
				Required: []string{"scan_id", "geography"},
			},
			"market_research.scan_assigned": {
				ConsumerType: []string{"dashboard"},
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id":          {Type: "string"},
						"current_state":      {Type: "string"},
						"trigger_event_type": {Type: "string"},
						"scan_id":            {Type: "string"},
						"geography":          {Type: "string"},
					},
				},
				Required: []string{"entity_id", "scan_id"},
			},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	return bundle
}

func mustPayloadCompletenessTransform(t *testing.T, body string) *runtimecontracts.PayloadTransformSpec {
	t.Helper()

	var decoded struct {
		PayloadTransform runtimecontracts.PayloadTransformSpec `yaml:"payload_transform"`
	}
	if err := yaml.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("unmarshal payload_transform: %v", err)
	}
	return &decoded.PayloadTransform
}

func reportContains(items []Finding, checkID, contains string) bool {
	for _, item := range items {
		if item.CheckID == checkID && strings.Contains(item.Message, contains) {
			return true
		}
	}
	return false
}

func firstBundleHandler(bundle *runtimecontracts.WorkflowContractBundle) (string, string, runtimecontracts.SystemNodeEventHandler, bool) {
	for nodeID, node := range bundle.Nodes {
		for eventType, handler := range node.EventHandlers {
			return nodeID, eventType, handler, true
		}
	}
	return "", "", runtimecontracts.SystemNodeEventHandler{}, false
}

func firstFlowHandlerInFlowView(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) (string, string, string, runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	for _, view := range bundle.FlowViews() {
		flowID := strings.TrimSpace(view.Paths.ID)
		for nodeID, node := range view.Nodes {
			for eventType, handler := range node.EventHandlers {
				return flowID, nodeID, eventType, handler
			}
		}
	}
	t.Fatal("expected fixture to include at least one flow handler")
	return "", "", "", runtimecontracts.SystemNodeEventHandler{}
}

func writeFlowHandler(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	node := flowView.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	flowView.Nodes[nodeID] = node
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler
}

func renameFlowHandlerEvent(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, nodeID, oldEventType, newEventType string, handler runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	node := flowView.Nodes[nodeID]
	delete(node.EventHandlers, oldEventType)
	node.EventHandlers[newEventType] = handler
	flowView.Nodes[nodeID] = node
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	delete(bundle.Semantics.NodeHandlers[nodeID], oldEventType)
	bundle.Semantics.NodeHandlers[nodeID][newEventType] = handler
	if len(bundle.Semantics.FlowInputs[flowID]) > 0 {
		inputs := append([]string{}, bundle.Semantics.FlowInputs[flowID]...)
		for idx, eventType := range inputs {
			if strings.TrimSpace(eventType) == strings.TrimSpace(oldEventType) {
				inputs[idx] = newEventType
			}
		}
		bundle.Semantics.FlowInputs[flowID] = inputs
	}
}

func flowEventEntry(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, eventType string) runtimecontracts.EventCatalogEntry {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	entry, ok := flowView.Events[eventType]
	if !ok {
		t.Fatalf("flow %s event %s missing", flowID, eventType)
	}
	return entry
}

func writeFlowEventEntry(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, eventType string, entry runtimecontracts.EventCatalogEntry) {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	if flowView.Events == nil {
		flowView.Events = map[string]runtimecontracts.EventCatalogEntry{}
	}
	flowView.Events[eventType] = entry
}

func addProjectHandler(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	node := bundle.Nodes[nodeID]
	node.ID = nodeID
	node.ExecutionType = "system_node"
	if node.EventHandlers == nil {
		node.EventHandlers = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler
}
