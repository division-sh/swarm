package pipeline

import (
	"path/filepath"
	"strings"
	"testing"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/semanticview"
)

func TestValidateWorkflowContractsDetailed_Tier8SemanticFixtures(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	platformSpec := filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
	cases := []struct {
		name         string
		fixture      string
		wantCategory string
		wantWarning  bool
		wantContains string
	}{
		{name: "condition payload mismatch", fixture: "test-boot-condition-payload-mismatch", wantCategory: "CONDITION-PAYLOAD", wantContains: "payload.nonexistent_field"},
		{name: "tool missing", fixture: "test-boot-tool-missing", wantCategory: "TOOL-MISSING", wantWarning: true, wantContains: "nonexistent_tool"},
		{name: "self emit", fixture: "test-boot-self-emit", wantCategory: "DIALECT-SELF-EMIT", wantContains: "loop.event"},
		{name: "event cycle", fixture: "test-boot-event-cycle", wantCategory: "EVENT-CYCLE", wantContains: "cycle.ping"},
		{name: "event without schema", fixture: "test-boot-event-no-schema", wantCategory: "EVENT-NO-SCHEMA", wantWarning: true, wantContains: "orphan.event"},
		{name: "permission tool mismatch", fixture: "test-boot-permission-tool-mismatch", wantCategory: "PERMISSION-MISMATCH", wantWarning: true, wantContains: "lookup_data"},
		{name: "prompt missing", fixture: "test-boot-prompt-missing", wantCategory: "PROMPT-MISSING", wantWarning: true, wantContains: "promptless-agent"},
		{name: "prompt stub", fixture: "test-boot-prompt-stub", wantCategory: "PROMPT-STUB", wantWarning: true, wantContains: "TODO"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadTier8BootFixture(t, repoRoot, platformSpec, tc.fixture)
			warnings, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
			if tc.wantWarning {
				if err != nil {
					t.Fatalf("expected warning, got error: %v", err)
				}
				if !workflowWarningContains(warnings, tc.wantCategory, tc.wantContains) {
					t.Fatalf("expected warning %s containing %q, got %#v", tc.wantCategory, tc.wantContains, warnings)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected validation error for %s", tc.wantCategory)
			}
			if !workflowValidationErrorContains(err, tc.wantContains) {
				t.Fatalf("expected validation error containing %q, got %v", tc.wantContains, err)
			}
		})
	}
}

func TestValidateWorkflowContractsDetailed_RejectsMissingRequiredAgentRole(t *testing.T) {
	bundle := newRequiredAgentValidationBundle()
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err == nil || !workflowValidationErrorContains(err, "missing-agent") {
		t.Fatalf("expected missing required agent error, got %v", err)
	}
}

func TestValidateWorkflowContractsDetailed_RejectsMissingRootRequiredAgentRole(t *testing.T) {
	bundle := newRequiredAgentValidationBundle()
	bundle.RootSchema = &runtimecontracts.FlowSchemaDocument{
		Name: "root",
		RequiredAgents: []runtimecontracts.FlowRequiredAgent{{
			Role:  "missing-root-agent",
			Emits: []string{"task.completed"},
		}},
	}
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err == nil || !workflowValidationErrorContains(err, "missing-root-agent") {
		t.Fatalf("expected missing root required agent error, got %v", err)
	}
}

func TestValidateWorkflowContractsDetailed_AllowsNodeWithoutProduces(t *testing.T) {
	bundle := newNodeProducesOptionalValidationBundle()
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("expected validator to allow omitted produces, got %v", err)
	}
}

func TestValidateWorkflowContractsDetailed_AllowsStatelessFlow(t *testing.T) {
	bundle := newStatelessFlowValidationBundle()
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("expected validator to allow stateless flow, got %v", err)
	}
}

func TestValidateWorkflowContractsDetailed_DefaultBundleDoesNotWarnOnKnownPermissionBundles(t *testing.T) {
	repoRoot := contractComplianceRepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractComplianceBundleRoot(t), runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	warnings, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("ValidateWorkflowContractsDetailed: %v", err)
	}
	for _, warning := range warnings {
		if warning.Category != "PERMISSION-MISMATCH" {
			continue
		}
		if strings.Contains(warning.Message, "permissions resolution failed") {
			t.Fatalf("unexpected permission resolution warning: %s", warning.Message)
		}
	}
}

func TestValidateWorkflowContractsDetailed_AllowsDeclaredMCPPrefixedTools(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"ops-agent": {
				ID:               "ops-agent",
				Role:             "ops",
				ModelTier:        "fast",
				ConversationMode: "react",
				Subscriptions:    []string{"task.requested"},
				ToolsTier2:       []string{"infra.ping"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{},
		Policy: runtimecontracts.PolicyDocument{
			Values: map[string]runtimecontracts.PolicyValue{
				"mcp_servers": {
					Value: map[string]any{
						"empire-infra": map[string]any{
							"transport": "stdio",
							"command":   "empire-infra-mcp",
							"prefix":    "infra",
						},
					},
				},
			},
		},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.0.0"
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err != nil {
		t.Fatalf("expected declared MCP-prefixed tool to validate, got %v", err)
	}
}

func TestValidateWorkflowContractsDetailed_RejectsUnknownNativeToolCapability(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"research-agent": {
				ID:               "research-agent",
				Role:             "research",
				ModelTier:        "fast",
				ConversationMode: "task",
				Subscriptions:    []string{"task.requested"},
				NativeTools:      map[string]any{"mystery_tool": true},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.0.0"
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err == nil || !workflowValidationErrorContains(err, "native_tools.mystery_tool") {
		t.Fatalf("expected unknown native_tools capability error, got %v", err)
	}
}

func TestValidateWorkflowContractsDetailed_RejectsNonBooleanNativeToolValue(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"research-agent": {
				ID:               "research-agent",
				Role:             "research",
				ModelTier:        "fast",
				ConversationMode: "task",
				Subscriptions:    []string{"task.requested"},
				NativeTools:      map[string]any{"bash": "yes"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.0.0"
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err == nil || !workflowValidationErrorContains(err, "native_tools.bash must be boolean") {
		t.Fatalf("expected non-boolean native_tools error, got %v", err)
	}
}

func TestValidateWorkflowContractsDetailed_RejectsReservedPlatformEventNamespace(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"research-agent": {
				ID:               "research-agent",
				Role:             "research",
				ModelTier:        "fast",
				ConversationMode: "task",
				Subscriptions:    []string{"task.requested"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"platform.bad_event": {},
			"task.requested":     {},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.0.0"
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err == nil || !workflowValidationErrorContains(err, "reserved platform.* namespace") {
		t.Fatalf("expected reserved platform namespace error, got %v", err)
	}
}

func TestValidateWorkflowContractsDetailed_RejectsReservedPlatformNamespaceInAgentEmitEvents(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"research-agent": {
				ID:               "research-agent",
				Role:             "research",
				ModelTier:        "fast",
				ConversationMode: "task",
				Subscriptions:    []string{"task.requested"},
				EmitEvents:       []string{"platform.bad_event"},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.0.0"
	_, err := ValidateWorkflowContractsDetailed(semanticview.Wrap(bundle))
	if err == nil || !workflowValidationErrorContains(err, "emit_events references reserved platform.* namespace") {
		t.Fatalf("expected reserved platform namespace emit_events error, got %v", err)
	}
}

func TestWorkflowPolicyConflictWarnings(t *testing.T) {
	projectScopes := []semanticview.ProjectScope{{
		Key: "root",
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"max_retries": {Value: 3},
		}},
	}}
	flowScopes := []semanticview.FlowScope{{
		ID: "sub-flow",
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"max_retries": {Value: 10},
		}},
	}}
	warnings := workflowPolicyConflictWarnings(projectScopes, flowScopes)
	if !workflowWarningContains(warnings, "POLICY-CONFLICT", "max_retries") {
		t.Fatalf("expected POLICY-CONFLICT warning, got %#v", warnings)
	}
}

func loadTier8BootFixture(t *testing.T, repoRoot, platformSpec, fixture string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", fixture)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixture, err)
	}
	return bundle
}

func newRequiredAgentValidationBundle() *runtimecontracts.WorkflowContractBundle {
	const flowID = "phase4-required-agent"
	schema := runtimecontracts.FlowSchemaDocument{
		Name:         flowID,
		InitialState: "pending",
		States:       []string{"pending", "done"},
		TerminalStates: []string{
			"done",
		},
		Pins: runtimecontracts.FlowPins{
			Inputs: runtimecontracts.FlowInputPins{
				Events: []string{"task.requested"},
			},
			Outputs: runtimecontracts.FlowOutputPins{
				Events: []string{"task.completed"},
			},
		},
		RequiredAgents: []runtimecontracts.FlowRequiredAgent{{
			Role:  "missing-agent",
			Emits: []string{"task.completed"},
		}},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			flowID: schema,
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {},
			"task.completed": {},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{},
		Tools:  map[string]runtimecontracts.ToolSchemaEntry{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInitial: map[string]string{
				flowID: schema.InitialState,
			},
			FlowStates: map[string][]string{
				flowID: append([]string{}, schema.States...),
			},
			FlowTerminal: map[string][]string{
				flowID: append([]string{}, schema.TerminalStates...),
			},
			FlowInputs: map[string][]string{
				flowID: append([]string{}, schema.Pins.Inputs.Events...),
			},
			FlowOutputs: map[string][]string{
				flowID: append([]string{}, schema.Pins.Outputs.Events...),
			},
			FlowReads: map[string][]string{
				flowID: append([]string{}, schema.Pins.Inputs.Reads...),
			},
			FlowWrites: map[string][]string{
				flowID: append([]string{}, schema.Pins.Outputs.Writes...),
			},
			FlowAgents: map[string][]runtimecontracts.FlowRequiredAgent{
				flowID: append([]runtimecontracts.FlowRequiredAgent{}, schema.RequiredAgents...),
			},
		},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.0.0"
	return bundle
}

func newNodeProducesOptionalValidationBundle() *runtimecontracts.WorkflowContractBundle {
	const flowID = "phase4-produces-optional"
	schema := runtimecontracts.FlowSchemaDocument{
		Name:         flowID,
		InitialState: "pending",
		States:       []string{"pending", "done"},
		TerminalStates: []string{
			"done",
		},
		Pins: runtimecontracts.FlowPins{
			Inputs: runtimecontracts.FlowInputPins{
				Events: []string{"task.requested"},
			},
			Outputs: runtimecontracts.FlowOutputPins{
				Events: []string{"task.completed"},
			},
		},
	}
	node := runtimecontracts.SystemNodeContract{
		ID:            "system-node",
		ExecutionType: "system",
		SubscribesTo:  []string{"task.requested"},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"task.requested": {
				AdvancesTo: "done",
				Emits:      runtimecontracts.EventEmission{Single: "task.completed"},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			flowID: schema,
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.requested": {},
			"task.completed": {},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"system-node": node,
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInitial: map[string]string{
				flowID: schema.InitialState,
			},
			FlowStates: map[string][]string{
				flowID: append([]string{}, schema.States...),
			},
			FlowTerminal: map[string][]string{
				flowID: append([]string{}, schema.TerminalStates...),
			},
			FlowInputs: map[string][]string{
				flowID: append([]string{}, schema.Pins.Inputs.Events...),
			},
			FlowOutputs: map[string][]string{
				flowID: append([]string{}, schema.Pins.Outputs.Events...),
			},
		},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.2.0"
	return bundle
}

func newStatelessFlowValidationBundle() *runtimecontracts.WorkflowContractBundle {
	const flowID = "phase4-stateless"
	schema := runtimecontracts.FlowSchemaDocument{
		Name:         flowID,
		InitialState: "",
		States:       []string{},
		Pins: runtimecontracts.FlowPins{
			Inputs: runtimecontracts.FlowInputPins{
				Events: []string{"intake.requested"},
			},
			Outputs: runtimecontracts.FlowOutputPins{
				Events: []string{"intake.completed"},
			},
		},
	}
	node := runtimecontracts.SystemNodeContract{
		ID:            "scan-node",
		ExecutionType: "system",
		SubscribesTo:  []string{"intake.requested"},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"intake.requested": {
				Emits: runtimecontracts.EventEmission{Single: "intake.completed"},
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			flowID: schema,
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"intake.requested": {},
			"intake.completed": {},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-node": node,
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			FlowInitial: map[string]string{
				flowID: schema.InitialState,
			},
			FlowStates: map[string][]string{
				flowID: append([]string{}, schema.States...),
			},
			FlowInputs: map[string][]string{
				flowID: append([]string{}, schema.Pins.Inputs.Events...),
			},
			FlowOutputs: map[string][]string{
				flowID: append([]string{}, schema.Pins.Outputs.Events...),
			},
		},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.2.0"
	return bundle
}

func workflowWarningContains(warnings []WorkflowContractWarning, category, contains string) bool {
	for _, warning := range warnings {
		if strings.TrimSpace(warning.Category) != strings.TrimSpace(category) {
			continue
		}
		if contains == "" || strings.Contains(warning.Message, contains) {
			return true
		}
	}
	return false
}
