package bootverify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	if !reportContains(report.Warnings(), "event_consumer_exists", "task.completed") {
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

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
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

func TestBootCheckRegistry_HasSpecCheckCount(t *testing.T) {
	if got := len(bootCheckRegistry); got != 32 {
		t.Fatalf("bootCheckRegistry count = %d, want 32", got)
	}
	if got := len(supplementalChecks); got != 3 {
		t.Fatalf("supplementalChecks count = %d, want 3", got)
	}
}

func TestRun_FlagsMissingMinimumRequiredFiles(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "impl.minimum_required_files", "no agents.yaml and no child flows") {
		t.Fatalf("expected impl.minimum_required_files error, got %#v", report.Errors())
	}
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
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixture, err)
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
