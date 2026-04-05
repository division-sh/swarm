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

func TestRun_ReportsInputPinWiringWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-missing-pin")

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("expected input_pin_wiring warning, got %#v", report.Warnings())
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

func TestRun_ReportsExpressionFieldReferenceWarning(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
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

func TestRun_SuppressesExpressionFieldReferenceWarningWhenWriterExists(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Condition = "entity.missing_score >= 70"
	handler.DataAccumulation.Writes = append(handler.DataAccumulation.Writes, runtimecontracts.WorkflowDataWrite{
		TargetField: "missing_score",
		SourceField: "score",
	})
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.missing_score") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
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

func TestBootCheckRegistry_HasSpecCheckCount(t *testing.T) {
	if got := len(bootCheckRegistry); got != 36 {
		t.Fatalf("bootCheckRegistry count = %d, want 36", got)
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

func writeTimerValidationFixture(t *testing.T, startOn, cancelOn string) string {
	t.Helper()
	root := t.TempDir()

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
timer.reminder:
  payload:
    properties:
      entity_id:
        type: string
`)
	timerBlock := `
    - id: reminder
      event: timer.reminder
      delay: 1m
      start_on: ` + startOn + "\n"
	if strings.TrimSpace(cancelOn) != "" {
		timerBlock += "      cancel_on: " + cancelOn + "\n"
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
