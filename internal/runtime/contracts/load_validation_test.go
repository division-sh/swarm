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
		{name: "undefined handler field", fixture: "test-boot-handler-field-undefined", contains: "UNDEFINED-FIELD"},
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
platform_version: ">=1.0.0"
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
