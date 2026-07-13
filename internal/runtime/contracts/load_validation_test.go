package contracts

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestValidateWorkflowContractBundleLoadConstraintsRejectsOnCompleteAndRules(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)

	nodeID, eventType, handler, ok := firstLoadedWorkflowHandler(bundle)
	if !ok {
		t.Fatal("expected workflow handler")
	}
	handler.OnComplete = []HandlerRuleEntry{{Condition: "true"}}
	handler.Rules = []HandlerRuleEntry{{Condition: "else"}}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !errors.Is(err, ErrConflictingCompletion) {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsDeprecatedGuardFallback(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)

	nodeID, eventType, handler, ok := firstLoadedWorkflowHandler(bundle)
	if !ok {
		t.Fatal("expected workflow handler")
	}
	handler.Guard = &GuardSpec{ID: "legacy_guard_only"}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !errors.Is(err, ErrDeprecatedGuardFallback) {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsMultipleAuthoritativeOwners(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)

	bundle.Semantics.EventOwners["task.completed"] = []string{"node-a", "node-b"}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !errors.Is(err, ErrMultipleAuthoritativeOwners) {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsInvalidExecutionType(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)

	for nodeID, node := range bundle.Nodes {
		node.ExecutionType = "workflow_node"
		bundle.Nodes[nodeID] = node
		break
	}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !contractErrorContains(err, "unsupported execution_type") {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsAllowsMissingExecutionType(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)

	for nodeID, node := range bundle.Nodes {
		node.ExecutionType = ""
		bundle.Nodes[nodeID] = node
		break
	}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err != nil {
		t.Fatalf("unexpected load validation error for missing execution_type: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsNodeIDMismatch(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)

	var expected string
	for nodeID, node := range bundle.Nodes {
		node.ID = nodeID + "-alias"
		bundle.Nodes[nodeID] = node
		expected = nodeID
		break
	}
	if expected == "" {
		t.Fatal("expected at least one node")
	}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !contractErrorContains(err, "node id") || !contractErrorContains(err, "must match map key") {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsAllowsRenderedNodeIDTemplate(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)

	for nodeID, node := range bundle.Nodes {
		node.ID = nodeID + "-{instance_id}"
		bundle.Nodes[nodeID] = node
		break
	}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err != nil {
		t.Fatalf("unexpected load validation error for rendered node id template: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsUnsupportedHandlerAction(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)

	nodeID, eventType, handler, ok := firstLoadedWorkflowHandler(bundle)
	if !ok {
		t.Fatal("expected workflow handler")
	}
	handler.Action = ActionSpec{ID: "increment_revision_count"}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !contractErrorContains(err, "action increment_revision_count is not in platform spec") {
		t.Fatalf("unexpected load validation error: %v", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsInvalidSchemaRefinements(t *testing.T) {
	minLength := 1
	bundle := &WorkflowContractBundle{
		Events: map[string]EventCatalogEntry{
			"deploy.requested": {
				Payload: EventPayloadSpec{
					Properties: map[string]EventFieldSpec{
						"score": {
							Type:        "integer",
							Refinements: SchemaRefinements{Pattern: "^[0-9]+$"},
						},
						"component": {Type: "text"},
						"owner": {
							Type:        "text",
							Refinements: SchemaRefinements{EqualTo: "missing"},
						},
					},
				},
			},
		},
		RootTypes: TypeCatalogDocument{
			Types: map[string]NamedTypeDecl{
				"Manifest": {
					Fields: map[string]TypeFieldSpec{
						"files": {
							Type:        "integer",
							Refinements: SchemaRefinements{Length: SchemaLengthRefinement{Min: &minLength}},
						},
					},
				},
			},
		},
		RootEntities: EntityContractsDocument{
			"deploy": {
				Fields: map[string]EntityFieldDecl{
					"created_at": {
						Type:        "timestamp",
						Refinements: SchemaRefinements{Pattern: "^2026-"},
					},
				},
			},
		},
	}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil {
		t.Fatal("expected invalid schema refinements to fail")
	}
	for _, want := range []string{
		"pattern refinement requires string/text type",
		"equal_to references undeclared sibling field",
		"length refinement requires string/text or list type",
		"root entities.deploy.created_at pattern refinement requires string/text type",
	} {
		if !contractErrorContains(err, want) {
			t.Fatalf("load validation error = %v, want %q", err, want)
		}
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRejectsMalformedPolicyValidationSet(t *testing.T) {
	pinCandidate := true
	flow := &FlowContractView{
		Paths: FlowContractPaths{ID: "deploy", Flow: "deploy"},
		Policy: PolicyDocument{Validation: map[string]PolicyValidationSet{
			"deploy_manifest": {
				Classes: map[string]PolicyValidationClass{"invalid": {Disposition: "deploy.manifest_invalid"}},
				Inputs:  map[string]string{"source_ref": "string", "manifest_source_ref": "number"},
				Rules: []PolicyValidationRule{{
					ID:           "VR-001",
					Class:        "invalid",
					Text:         "Manifest source ref must match request source ref.",
					PinCandidate: &pinCandidate,
					Check:        PolicyValidationCheck{Equal: &PolicyValidationEqualCheck{Left: "input.source_ref", Right: "input.manifest_source_ref"}},
				}},
			},
		}},
	}
	bundle := &WorkflowContractBundle{
		FlowSchemas: map[string]FlowSchemaDocument{"deploy": {}},
		FlowTree: FlowTree{
			Root: flow,
			ByID: map[string]*FlowContractView{
				"deploy": flow,
			},
		},
	}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !contractErrorContains(err, "compares input.source_ref type string with input.manifest_source_ref type number") {
		t.Fatalf("load validation error = %v, want validation input type mismatch", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRequiresPolicyValidationPinCandidate(t *testing.T) {
	flow := &FlowContractView{
		Paths: FlowContractPaths{ID: "deploy", Flow: "deploy"},
		Policy: PolicyDocument{Validation: map[string]PolicyValidationSet{
			"deploy_manifest": {
				Classes: map[string]PolicyValidationClass{"invalid": {Disposition: "deploy.manifest_invalid"}},
				Inputs:  map[string]string{"source_ref": "string", "manifest_source_ref": "string"},
				Rules: []PolicyValidationRule{{
					ID:    "VR-001",
					Class: "invalid",
					Text:  "Manifest source ref must match request source ref.",
					Check: PolicyValidationCheck{Equal: &PolicyValidationEqualCheck{Left: "input.source_ref", Right: "input.manifest_source_ref"}},
				}},
			},
		}},
	}
	bundle := &WorkflowContractBundle{
		FlowSchemas: map[string]FlowSchemaDocument{"deploy": {}},
		FlowTree: FlowTree{
			Root: flow,
			ByID: map[string]*FlowContractView{
				"deploy": flow,
			},
		},
	}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !contractErrorContains(err, "pin_candidate must be explicitly true or false") {
		t.Fatalf("load validation error = %v, want missing pin_candidate", err)
	}
}

func TestValidateWorkflowContractBundleLoadConstraintsRequiresValidateRowsToMapDeclaredInputs(t *testing.T) {
	pinCandidate := true
	validation := &ComputeValidationSpec{
		RowID: "validate_manifest",
		Set:   "deploy_manifest",
		Into:  "computed.validation.deploy_manifest",
		Input: map[string]string{
			"source_ref": "payload.source_ref",
		},
	}
	flow := &FlowContractView{
		Paths: FlowContractPaths{ID: "deploy", Flow: "deploy"},
		Policy: PolicyDocument{Validation: map[string]PolicyValidationSet{
			"deploy_manifest": {
				Classes: map[string]PolicyValidationClass{"invalid": {Disposition: "deploy.manifest_invalid"}},
				Inputs:  map[string]string{"source_ref": "string", "manifest_source_ref": "string"},
				Rules: []PolicyValidationRule{{
					ID:           "VR-001",
					Class:        "invalid",
					Text:         "Manifest source ref must match request source ref.",
					PinCandidate: &pinCandidate,
					Check:        PolicyValidationCheck{Equal: &PolicyValidationEqualCheck{Left: "input.source_ref", Right: "input.manifest_source_ref"}},
				}},
			},
		}},
	}
	bundle := &WorkflowContractBundle{
		FlowSchemas: map[string]FlowSchemaDocument{"deploy": {}},
		FlowTree: FlowTree{
			Root: flow,
			ByID: map[string]*FlowContractView{
				"deploy": flow,
			},
		},
		nodeSources: map[string]ContractItemSource{"deploy_node": {FlowID: "deploy"}},
		Nodes: map[string]SystemNodeContract{
			"deploy_node": {
				ID: "deploy_node",
				EventHandlers: map[string]SystemNodeEventHandler{
					"deploy.requested": {
						Rules: []HandlerRuleEntry{{
							ID:        "validate_manifest",
							PolicyRow: PolicySheetRowMetadata{Kind: PolicySheetRowKindValidate, Validation: validation},
							Compute: &ComputeSpec{
								Operation:  ComputeOpValidate,
								StoreAs:    "computed.validation.deploy_manifest",
								Validation: validation,
							},
						}},
					},
				},
			},
		},
	}

	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err == nil || !contractErrorContains(err, `validate.input missing required set input "manifest_source_ref"`) {
		t.Fatalf("load validation error = %v, want missing validate input", err)
	}
}

func TestEventFieldSpecDecodeRejectsMalformedSchemaRefinement(t *testing.T) {
	var field EventFieldSpec
	err := yaml.Unmarshal([]byte(`
type: text
pattern: "["
`), &field)
	if err == nil || !strings.Contains(err.Error(), "pattern") {
		t.Fatalf("EventFieldSpec decode error = %v, want pattern failure", err)
	}
}

func TestLoadWorkflowContractBundle_PreservesEvidenceTarget(t *testing.T) {
	bundle := loadCurrentWorkflowBundleForTest(t)
	for _, node := range bundle.Nodes {
		for _, handler := range node.EventHandlers {
			if strings.TrimSpace(handler.Action.ID) != "record_evidence" {
				continue
			}
			if strings.TrimSpace(handler.EvidenceTarget) == "" {
				t.Fatal("expected record_evidence handler to preserve evidence_target")
			}
			return
		}
	}
	t.Fatal("expected at least one record_evidence handler")
}

func TestLoadWorkflowContractBundleRejectsRetiredPublicNodeAndSchemaFields(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	platformSpec := DefaultPlatformSpecFile(repoRoot)
	tests := []struct {
		name        string
		schemaExtra string
		nodes       string
		wantErr     string
	}{
		{
			name:        "schema namespace is not public schema YAML",
			schemaExtra: "namespace: legacy\n",
			nodes:       "{}\n",
			wantErr:     "schema field \"namespace\" is not supported",
		},
		{
			name:        "node idempotency table is retired public YAML",
			schemaExtra: "",
			nodes: `
worker:
  id: worker
  idempotency_table: worker_idempotency
  event_handlers: {}
`,
			wantErr: "RETIRED",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writeFieldReconciliationBundle(t, root, tc.schemaExtra, tc.nodes)
			_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
			if err == nil || !contractErrorContains(err, tc.wantErr) {
				t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestLoadWorkflowContractBundleAllowsPublicNodeStateTable(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	root := t.TempDir()
	writeFieldReconciliationBundle(t, root, "", `
worker:
  id: worker
  state_table: worker_state
  state_schema:
    fields:
      count:
        type: integer
  event_handlers: {}
`)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	node, ok := bundle.Nodes["worker"]
	if !ok {
		t.Fatalf("worker node missing: %#v", bundle.Nodes)
	}
	if got, want := strings.TrimSpace(node.StateTable), "worker_state"; got != want {
		t.Fatalf("StateTable = %q, want %q", got, want)
	}
}

func TestLoadWorkflowContractBundleRejectsRetiredTimerDurationAlias(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	root := t.TempDir()
	writeFieldReconciliationBundle(t, root, "", `
worker:
  id: worker
  timers:
    - id: reminder
      event: timer.reminder
      delay_minutes: 5
  event_handlers: {}
`)
	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !contractErrorContains(err, "RETIRED") || !contractErrorContains(err, "delay_minutes") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want retired delay_minutes rejection", err)
	}
}

func writeFieldReconciliationBundle(t *testing.T, root, schemaExtra, nodes string) {
	t.Helper()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: field-reconciliation
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows: []
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: field-reconciliation\n"+schemaExtra)
	writeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), nodes)
}

func TestAgentRegistryEntryRejectsRetiredModelTierField(t *testing.T) {
	var entry AgentRegistryEntry
	err := yaml.Unmarshal([]byte(`
role: researcher
type: managed
model: regular
model_tier: sonnet
mode: task
subscriptions: [scan.requested]
`), &entry)
	if err == nil || !strings.Contains(err.Error(), "model_tier is retired") {
		t.Fatalf("yaml.Unmarshal error = %v, want retired model_tier rejection", err)
	}
}

func TestAgentRegistryEntryDerivesRuntimeScopeFromAuthoredMode(t *testing.T) {
	tests := []struct {
		name      string
		mode      string
		wantScope string
	}{
		{name: "task", mode: "task"},
		{name: "session", mode: "session", wantScope: "flow"},
		{name: "session_per_entity", mode: "session_per_entity", wantScope: "entity"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entry AgentRegistryEntry
			err := yaml.Unmarshal([]byte(`
role: researcher
type: managed
model: regular
mode: `+tt.mode+`
subscriptions: [scan.requested]
`), &entry)
			if err != nil {
				t.Fatalf("yaml.Unmarshal: %v", err)
			}
			if entry.Mode != tt.mode || entry.ConversationMode != tt.mode || entry.SessionScope != tt.wantScope {
				t.Fatalf("entry mode/scope = (%q, %q, %q), want (%q, %q, %q)", entry.Mode, entry.ConversationMode, entry.SessionScope, tt.mode, tt.mode, tt.wantScope)
			}
		})
	}
}

func TestEffectiveAgentRegistryEntryAppliesLayer1PlatformDefaults(t *testing.T) {
	var entry AgentRegistryEntry
	err := yaml.Unmarshal([]byte(`
role: researcher
model: regular
subscriptions: [scan.requested]
`), &entry)
	if err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	effective := EffectiveAgentRegistryEntry("researcher", entry)
	if effective.Type != DefaultAgentType {
		t.Fatalf("type = %q, want %q", effective.Type, DefaultAgentType)
	}
	if effective.Mode != DefaultAgentMode || effective.ConversationMode != DefaultAgentMode || effective.SessionScope != "" {
		t.Fatalf("mode/conversation/session = (%q, %q, %q), want task/task/empty", effective.Mode, effective.ConversationMode, effective.SessionScope)
	}
	if effective.MaxTurnsPerTask != DefaultAgentMaxTurnsPerTask {
		t.Fatalf("max_turns_per_task = %d, want %d", effective.MaxTurnsPerTask, DefaultAgentMaxTurnsPerTask)
	}
	if effective.WorkspaceClass != "" {
		t.Fatalf("workspace_class = %q, want empty", effective.WorkspaceClass)
	}
	for _, field := range []string{"type", "mode", "max_turns_per_task", "workspace_class"} {
		if got := effective.EffectiveSourceForField(field); got != AgentFieldSourcePlatformDefault {
			t.Fatalf("%s source = %q, want %q", field, got, AgentFieldSourcePlatformDefault)
		}
	}
	if got := effective.EffectiveSourceForField("model"); got != AgentFieldSourceAuthored {
		t.Fatalf("model source = %q, want %q", got, AgentFieldSourceAuthored)
	}
}

func TestAgentRegistryEntryRejectsExplicitInvalidLayer1Values(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		contains string
	}{
		{name: "empty_mode", body: "mode:\n", contains: "mode is required"},
		{name: "zero_max_turns", body: "max_turns_per_task: 0\n", contains: "max_turns_per_task must be positive"},
		{name: "negative_max_turns", body: "max_turns_per_task: -1\n", contains: "max_turns_per_task must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entry AgentRegistryEntry
			err := yaml.Unmarshal([]byte(`
role: researcher
model: regular
subscriptions: [scan.requested]
`+tt.body), &entry)
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tt.contains)
			}
		})
	}
}

func TestAgentRegistryEntryRejectsRetiredMemoryModeFieldsAndAliases(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		contains string
	}{
		{name: "conversation_mode", body: "conversation_mode: task\n", contains: "conversation_mode is retired"},
		{name: "session_scope", body: "mode: session\nsession_scope: flow\n", contains: "session_scope is runtime-derived from mode"},
		{name: "session_scope_authority", body: "mode: session\nsession_scope_authority: platform_internal\n", contains: "session_scope_authority is platform-internal"},
		{name: "mode_global", body: "mode: global\n", contains: "reserved"},
		{name: "mode_unknown", body: "mode: forever\n", contains: "invalid mode"},
		{name: "mode_stateless", body: "mode: stateless\n", contains: "retired"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entry AgentRegistryEntry
			err := yaml.Unmarshal([]byte(`
role: researcher
type: managed
model: regular
`+tt.body+`
subscriptions: [scan.requested]
`), &entry)
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tt.contains)
			}
		})
	}
}

func TestAgentRegistryEntryRejectsUnsupportedLayerSyntaxAndUnknownFields(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		contains string
	}{
		{name: "profile", body: "profile: cheap\n", contains: "reserved for future agent-defaults/profile support"},
		{name: "runtime_id_template", body: "runtime_id_template: worker-{entity_id}\n", contains: "reserved for future agent-defaults/profile support"},
		{name: "unknown", body: "surprise_field: true\n", contains: `agent field "surprise_field" is not supported.`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entry AgentRegistryEntry
			err := yaml.Unmarshal([]byte(`
role: researcher
model: regular
subscriptions: [scan.requested]
`+tt.body), &entry)
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tt.contains)
			}
		})
	}
}

func TestLoadWorkflowContractBundleAllowsAgentPromptInputs(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	root := t.TempDir()
	writeFieldReconciliationBundle(t, root, "", "{}\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), `
worker:
  model: regular
  prompt_inputs: [customer_name, order_type]
  subscriptions: [work.requested]
`)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	entry, ok := bundle.AgentEntry("worker")
	if !ok {
		t.Fatalf("worker agent missing: %#v", bundle.Agents)
	}
	if got, want := strings.Join(entry.PromptInputs, ","), "customer_name,order_type"; got != want {
		t.Fatalf("PromptInputs = %q, want %q", got, want)
	}
	if !entry.AuthoredFields["prompt_inputs"] {
		t.Fatalf("prompt_inputs authored field not recorded: %#v", entry.AuthoredFields)
	}
}

func TestValidateWorkflowCriteriaContractsAllowsFlowLocalCriteriaAndCitation(t *testing.T) {
	bundle := criteriaValidationTestBundle()
	err := validateWorkflowContractBundleLoadConstraints(bundle)
	if err != nil {
		t.Fatalf("validateWorkflowContractBundleLoadConstraints: %v", err)
	}
}

func TestValidateWorkflowCriteriaContractsRejectsInvalidCriteriaShapes(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*WorkflowContractBundle)
		wantError string
	}{
		{
			name: "duplicate rule id",
			mutate: func(bundle *WorkflowContractBundle) {
				flow := bundle.FlowTree.ByID["validation"]
				set := flow.Policy.Criteria["feasibility_exclusions"]
				set.Rules = append(set.Rules, PolicyCriteriaRule{ID: "FX-HARD-01", Class: "soft", Text: "Duplicate."})
				flow.Policy.Criteria["feasibility_exclusions"] = set
			},
			wantError: "duplicate stable criteria id",
		},
		{
			name: "bad policy param ref",
			mutate: func(bundle *WorkflowContractBundle) {
				flow := bundle.FlowTree.ByID["validation"]
				set := flow.Policy.Criteria["feasibility_exclusions"]
				set.Rules[0].Params = map[string]PolicyCriteriaParam{"max": {Value: "policy.missing"}}
				flow.Policy.Criteria["feasibility_exclusions"] = set
			},
			wantError: "references unknown policy scalar",
		},
		{
			name: "unknown agent criteria ref",
			mutate: func(bundle *WorkflowContractBundle) {
				agent := bundle.scopedAgents["validation::cto-agent"]
				agent.Criteria = []string{"missing"}
				bundle.scopedAgents["validation::cto-agent"] = agent
			},
			wantError: "criteria ref \"missing\" does not resolve",
		},
		{
			name: "missing allowed classes",
			mutate: func(bundle *WorkflowContractBundle) {
				flow := bundle.FlowTree.ByID["validation"]
				field := flow.Events["cto.spec_vetoed"].Payload.Properties["cites"]
				field.Citation.AllowedClasses = nil
				flow.Events["cto.spec_vetoed"].Payload.Properties["cites"] = field
				bundle.scopedEvents["validation::cto.spec_vetoed"] = flow.Events["cto.spec_vetoed"]
			},
			wantError: "allowed_classes is required",
		},
		{
			name: "undeclared allowed class",
			mutate: func(bundle *WorkflowContractBundle) {
				flow := bundle.FlowTree.ByID["validation"]
				field := flow.Events["cto.spec_vetoed"].Payload.Properties["cites"]
				field.Citation.AllowedClasses = []string{"allow"}
				flow.Events["cto.spec_vetoed"].Payload.Properties["cites"] = field
				bundle.scopedEvents["validation::cto.spec_vetoed"] = flow.Events["cto.spec_vetoed"]
			},
			wantError: "allowed class \"allow\" is not declared",
		},
		{
			name: "event citation criteria ref resolves without emitter",
			mutate: func(bundle *WorkflowContractBundle) {
				flow := bundle.FlowTree.ByID["validation"]
				field := flow.Events["cto.spec_vetoed"].Payload.Properties["cites"]
				field.Citation.Criteria = "missing"
				flow.Events["cto.spec_vetoed"].Payload.Properties["cites"] = field
				bundle.scopedEvents["validation::cto.spec_vetoed"] = flow.Events["cto.spec_vetoed"]
				bundle.scopedAgents = map[string]AgentRegistryEntry{}
				bundle.scopedAgentSources = map[string]ContractItemSource{}
			},
			wantError: "criteria set \"missing\" does not resolve in flow validation policy.criteria",
		},
		{
			name: "agent emits criteria event without declaring set",
			mutate: func(bundle *WorkflowContractBundle) {
				agent := bundle.scopedAgents["validation::cto-agent"]
				agent.Criteria = nil
				bundle.scopedAgents["validation::cto-agent"] = agent
			},
			wantError: "but the agent does not declare it",
		},
		{
			name: "project criteria is not a flow-local owner",
			mutate: func(bundle *WorkflowContractBundle) {
				bundle.projectContracts = map[string]ProjectContractView{
					"root": {
						Paths: ProjectPackagePaths{Key: "root"},
						Policy: PolicyDocument{Criteria: map[string]PolicyCriteriaSet{
							"root_criteria": criteriaValidationTestSet(),
						}},
					},
				}
			},
			wantError: "criteria must be declared in flow policy.yaml",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bundle := criteriaValidationTestBundle()
			tc.mutate(bundle)
			err := validateWorkflowContractBundleLoadConstraints(bundle)
			if err == nil || !contractErrorContains(err, tc.wantError) {
				t.Fatalf("validateWorkflowContractBundleLoadConstraints error = %v, want %q", err, tc.wantError)
			}
		})
	}
}

func TestAgentAndEventCriteriaCitationYAMLDecode(t *testing.T) {
	var agent AgentRegistryEntry
	if err := yaml.Unmarshal([]byte(`
role: cto
model: regular
subscriptions: [spec.review_requested]
emit_events: [cto.spec_vetoed]
criteria: [feasibility_exclusions]
`), &agent); err != nil {
		t.Fatalf("yaml.Unmarshal AgentRegistryEntry: %v", err)
	}
	if got := strings.Join(agent.Criteria, ","); got != "feasibility_exclusions" {
		t.Fatalf("agent criteria = %q, want feasibility_exclusions", got)
	}

	var event EventCatalogEntry
	if err := yaml.Unmarshal([]byte(`
reason: text
cites:
  type: "[text]"
  citation:
    criteria: feasibility_exclusions
    allowed_classes: [hard, soft]
`), &event); err != nil {
		t.Fatalf("yaml.Unmarshal EventCatalogEntry: %v", err)
	}
	field := event.Payload.Properties["cites"]
	if field.Citation.Criteria != "feasibility_exclusions" {
		t.Fatalf("citation criteria = %q, want feasibility_exclusions", field.Citation.Criteria)
	}
	if got := strings.Join(field.Citation.AllowedClasses, ","); got != "hard,soft" {
		t.Fatalf("allowed classes = %q, want hard,soft", got)
	}
}

func criteriaValidationTestBundle() *WorkflowContractBundle {
	flow := FlowContractView{
		Paths: FlowContractPaths{ID: "validation", Flow: "validation"},
		Policy: PolicyDocument{
			Values: map[string]PolicyValue{
				"max_features": {Value: 5},
			},
			Criteria: map[string]PolicyCriteriaSet{
				"feasibility_exclusions": criteriaValidationTestSet(),
			},
		},
		Events: map[string]EventCatalogEntry{
			"cto.spec_vetoed": {
				Payload: EventPayloadSpec{
					Type: "object",
					Properties: map[string]EventFieldSpec{
						"cites": {
							Type: "[text]",
							Citation: CriteriaCitation{
								Criteria:       "feasibility_exclusions",
								AllowedClasses: []string{"hard"},
							},
						},
					},
					Required: []string{"cites"},
				},
			},
		},
	}
	root := &FlowContractView{Children: []FlowContractView{flow}}
	flowPtr := &root.Children[0]
	agent := AgentRegistryEntry{
		Model:      "regular",
		Role:       "cto",
		EmitEvents: []string{"cto.spec_vetoed"},
		Criteria:   []string{"feasibility_exclusions"},
	}
	return &WorkflowContractBundle{
		FlowSchemas: map[string]FlowSchemaDocument{
			"validation": {Name: "validation"},
		},
		FlowTree: FlowTree{
			Root: root,
			ByID: map[string]*FlowContractView{
				"validation": flowPtr,
			},
		},
		scopedAgents: map[string]AgentRegistryEntry{
			"validation::cto-agent": agent,
		},
		scopedAgentSources: map[string]ContractItemSource{
			"validation::cto-agent": {FlowID: "validation", Layer: "flow", File: "flows/validation/agents.yaml"},
		},
		scopedEvents: map[string]EventCatalogEntry{
			"validation::cto.spec_vetoed": flow.Events["cto.spec_vetoed"],
		},
		scopedEventSources: map[string]ContractItemSource{
			"validation::cto.spec_vetoed": {FlowID: "validation", Layer: "flow", File: "flows/validation/events.yaml"},
		},
	}
}

func criteriaValidationTestSet() PolicyCriteriaSet {
	return PolicyCriteriaSet{
		Classes: map[string]PolicyCriteriaClass{
			"hard": {Disposition: "cto.spec_vetoed"},
			"soft": {Disposition: "cto.spec_revision_needed"},
		},
		Rules: []PolicyCriteriaRule{{
			ID:    "FX-HARD-01",
			Class: "hard",
			Text:  "Requires regulated real-time integration.",
			Params: map[string]PolicyCriteriaParam{
				"max_features": {Value: "policy.max_features"},
			},
		}, {
			ID:    "FX-SOFT-04",
			Class: "soft",
			Text:  "Missing MVP spec.",
		}},
	}
}

func TestLoadWorkflowContractBundleRejectsLayer2AgentDefaultsBlock(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	root := t.TempDir()
	writeFieldReconciliationBundle(t, root, "", "{}\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), `
agent_defaults:
  model: regular
worker:
  model: regular
  subscriptions: [work.requested]
`)
	_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, DefaultPlatformSpecFile(repoRoot))
	if err == nil || !strings.Contains(err.Error(), "agent_defaults") || !strings.Contains(err.Error(), "Layer 1 platform defaults") {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides error = %v, want agent_defaults Layer 1 rejection", err)
	}
}

func TestAgentRegistryEntryRejectsRetiredAuthoringAliases(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		contains string
	}{
		{name: "tools_tier2", body: "tools_tier2: [lookup_data]\n", contains: "tools_tier2 is retired"},
		{name: "subscriptions_bootstrap", body: "subscriptions_bootstrap: [scan.requested]\n", contains: "subscriptions_bootstrap is retired"},
		{name: "subscribes_to", body: "subscribes_to: [scan.requested]\n", contains: "subscribes_to is retired for agents.yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var entry AgentRegistryEntry
			err := yaml.Unmarshal([]byte(`
role: researcher
type: managed
model: regular
mode: task
subscriptions: [scan.requested]
`+tt.body), &entry)
			if err == nil || !strings.Contains(err.Error(), tt.contains) {
				t.Fatalf("yaml.Unmarshal error = %v, want %q", err, tt.contains)
			}
		})
	}
}

func contractRepoRoot(t *testing.T) string {
	t.Helper()
	return strings.TrimSpace(repoRootForContractsTest(t))
}

func loadCurrentWorkflowBundleForTest(t *testing.T) *WorkflowContractBundle {
	t.Helper()
	repoRoot := contractRepoRoot(t)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, currentWorkflowContractsDirForTest(t), DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return bundle
}

func firstLoadedWorkflowHandler(bundle *WorkflowContractBundle) (string, string, SystemNodeEventHandler, bool) {
	for nodeID, node := range bundle.Nodes {
		for eventType, handler := range node.EventHandlers {
			return nodeID, eventType, handler, true
		}
	}
	return "", "", SystemNodeEventHandler{}, false
}

func TestLoadWorkflowContractBundleRejectsTier8DialectFixtures(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	platformSpec := DefaultPlatformSpecFile(repoRoot)
	cases := []struct {
		name     string
		fixture  string
		contains string
	}{
		{name: "advances_to list", fixture: "test-boot-advances-to-list", contains: "DIALECT-ADV-LIST"},
		{name: "guard scalar", fixture: "test-boot-dialect-guard", contains: "DIALECT-GUARD"},
		{name: "on_complete dict", fixture: "test-boot-on-complete-dict", contains: "DIALECT-OC-ORDER"},
		{name: "undefined handler field", fixture: "test-boot-handler-field-undefined", contains: "handler field \"custom_logic\" is not supported"},
		{name: "deprecated handler field", fixture: "test-boot-deprecated-field", contains: "DEPRECATED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", tc.fixture)
			_, err := LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
			if err == nil || !contractErrorContains(err, tc.contains) {
				t.Fatalf("expected load error containing %q, got %v", tc.contains, err)
			}
		})
	}
}

func TestLoadWorkflowContractBundleAllowsSiblingFlowLocalAuthoritativeOwners(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated")
	platformSpec := DefaultPlatformSpecFile(repoRoot)

	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	if owners := bundle.RuntimeEventOwners("work.begin"); len(owners) != 0 {
		t.Fatalf("expected no authoritative owners for root work.begin, got %v", owners)
	}
	if owners := bundle.RuntimeEventOwners("flow-a/work.begin"); !hasAll(owners, "alpha-intake") || hasAny(owners, "beta-intake") {
		t.Fatalf("expected only alpha-intake to own flow-a/work.begin, got %v", owners)
	}
	if owners := bundle.RuntimeEventOwners("flow-b/work.begin"); !hasAll(owners, "beta-intake") || hasAny(owners, "alpha-intake") {
		t.Fatalf("expected only beta-intake to own flow-b/work.begin, got %v", owners)
	}
}

func TestLoadWorkflowContractBundleAllowsSiblingFlowLocalWildcardAuthoritativeOwners(t *testing.T) {
	repoRoot := contractRepoRoot(t)
	root := t.TempDir()
	writeFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: wildcard-owner-test
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: flow-a
    flow: flow-a
  - id: flow-b
    flow: flow-b
`)
	writeFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: wildcard-owner-test\n")
	writeFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-a", "package.yaml"), "name: flow-a\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-a", "schema.yaml"), `
name: flow-a
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-a", "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-a", "agents.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-a", "events.yaml"), `
task.done:
  entity_id: string
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-a", "nodes.yaml"), `
flow-a-wildcard:
  id: flow-a-wildcard
  execution_type: system_node
  subscribes_to: [task.*]
  event_handlers:
    task.*:
      advances_to: done
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-b", "package.yaml"), "name: flow-b\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-b", "schema.yaml"), `
name: flow-b
initial_state: active
terminal_states: [done]
states: [active, done]
pins:
  outputs:
    events: [task.done]
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-b", "policy.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-b", "agents.yaml"), "{}\n")
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-b", "events.yaml"), `
task.done:
  entity_id: string
`)
	writeFixtureFile(t, filepath.Join(root, "flows", "flow-b", "nodes.yaml"), `
flow-b-wildcard:
  id: flow-b-wildcard
  execution_type: system_node
  subscribes_to: [task.*]
  event_handlers:
    task.*:
      advances_to: done
`)
	platformSpec := DefaultPlatformSpecFile(repoRoot)
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	if owners := bundle.RuntimeEventOwners("task.done"); len(owners) != 0 {
		t.Fatalf("expected no authoritative owners for ambiguous root task.done, got %v", owners)
	}
	if owners := bundle.RuntimeEventOwners("flow-a/task.done"); !hasAll(owners, "flow-a-wildcard") || hasAny(owners, "flow-b-wildcard") {
		t.Fatalf("expected only flow-a-wildcard to own flow-a/task.done, got %v", owners)
	}
	if owners := bundle.RuntimeEventOwners("flow-b/task.done"); !hasAll(owners, "flow-b-wildcard") || hasAny(owners, "flow-a-wildcard") {
		t.Fatalf("expected only flow-b-wildcard to own flow-b/task.done, got %v", owners)
	}
}

func contractErrorContains(err error, substr string) bool {
	if err == nil || strings.TrimSpace(substr) == "" {
		return false
	}
	var verr *LoadValidationError
	if errors.As(err, &verr) {
		for _, item := range verr.Items {
			if item != nil && strings.Contains(item.Error(), substr) {
				return true
			}
		}
	}
	text := err.Error()
	return strings.Contains(text, substr)
}

func hasAll(values []string, wants ...string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[strings.TrimSpace(value)] = struct{}{}
	}
	for _, want := range wants {
		if _, ok := seen[strings.TrimSpace(want)]; !ok {
			return false
		}
	}
	return true
}

func hasAny(values []string, wants ...string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		seen[strings.TrimSpace(value)] = struct{}{}
	}
	for _, want := range wants {
		if _, ok := seen[strings.TrimSpace(want)]; ok {
			return true
		}
	}
	return false
}
