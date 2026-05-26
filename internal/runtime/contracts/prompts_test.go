package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
	flowmodel "swarm/internal/runtime/flowmodel"
)

func TestLoadPromptForAgent_UsesPromptRefAndWorkspaceRoleFallback(t *testing.T) {
	resolver := NewBundlePromptResolver(loadPromptTestBundle(t, repoRoot(t)))
	prompt, found, err := resolver.LoadPromptForAgent(models.AgentConfig{
		ID:   "cos-entity-1",
		Role: "ops_lead",
	}, "")
	if err != nil {
		t.Fatalf("LoadPromptForAgent: %v", err)
	}
	if !found {
		t.Fatal("expected prompt to be found")
	}
	if !strings.Contains(prompt, "{{team_name}}") {
		t.Fatalf("expected generic operations prompt template, got %q", prompt)
	}
}

func TestBundlePromptResolver_KeepsBundleStateIsolated(t *testing.T) {
	bundleA := loadPromptTestBundle(t, repoRoot(t))
	bundleA.Policy.Values = map[string]PolicyValue{
		"team_name": {Value: "alpha-team"},
	}
	bundleB := loadPromptTestBundle(t, repoRoot(t))
	bundleB.Policy.Values = map[string]PolicyValue{
		"team_name": {Value: "beta-team"},
	}

	promptA, found, err := NewBundlePromptResolver(bundleA).LoadPromptForAgent(models.AgentConfig{
		ID:   "ops-lead",
		Role: "ops_lead",
	}, "")
	if err != nil {
		t.Fatalf("resolver A LoadPromptForAgent: %v", err)
	}
	if !found {
		t.Fatal("expected prompt for resolver A")
	}
	promptB, found, err := NewBundlePromptResolver(bundleB).LoadPromptForAgent(models.AgentConfig{
		ID:   "ops-lead",
		Role: "ops_lead",
	}, "")
	if err != nil {
		t.Fatalf("resolver B LoadPromptForAgent: %v", err)
	}
	if !found {
		t.Fatal("expected prompt for resolver B")
	}
	if !strings.Contains(promptA, "alpha-team") {
		t.Fatalf("resolver A prompt = %q, want alpha-team", promptA)
	}
	if !strings.Contains(promptB, "beta-team") {
		t.Fatalf("resolver B prompt = %q, want beta-team", promptB)
	}
	if promptA == promptB {
		t.Fatalf("expected isolated prompt resolution, got identical prompts %q", promptA)
	}
}

func TestResolvePromptFileForContractAgent_UsesCanonicalCandidateSet(t *testing.T) {
	repo := repoRoot(t)
	root := writePromptTestBundle(t, repo)

	agentsPath := filepath.Join(root, "agents.yaml")
	agentsRaw, err := os.ReadFile(agentsPath)
	if err != nil {
		t.Fatalf("read %s: %v", agentsPath, err)
	}
	agentsRaw = append(agentsRaw, []byte(strings.TrimLeft(`
prompt-ref-agent:
  role: prompt_ref_role
  prompt_ref: shared-prompt
entry-id-agent:
  id: concrete-template
  role: entry_id_role
mode-agent:
  role: mode_role
workspace-role-agent:
  role: ops_lead
  workspace_class: factory
parent-agent:
  role: parent_role
`, "\n"))...)
	if err := os.WriteFile(agentsPath, agentsRaw, 0o644); err != nil {
		t.Fatalf("write %s: %v", agentsPath, err)
	}
	promptsDir := filepath.Join(root, "prompts")
	for name, content := range map[string]string{
		"shared-prompt.md":     "shared prompt\n",
		"concrete-template.md": "entry id prompt\n",
		"mode-agent.review.md": "mode prompt\n",
		"factory-ops-lead.md":  "workspace role prompt\n",
		"parent-agent.md":      "parent prompt\n",
	} {
		if err := os.WriteFile(filepath.Join(promptsDir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("write prompt %s: %v", name, err)
		}
	}

	bundle, err := LoadWorkflowContractBundleWithOverrides(repo, root, DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}

	for _, tc := range []struct {
		name    string
		agentID string
		mode    string
		want    string
	}{
		{name: "prompt_ref", agentID: "prompt-ref-agent", want: "shared-prompt.md"},
		{name: "entry_id", agentID: "entry-id-agent", want: "concrete-template.md"},
		{name: "mode_variant", agentID: "mode-agent", mode: "review", want: "mode-agent.review.md"},
		{name: "workspace_role", agentID: "workspace-role-agent", want: "factory-ops-lead.md"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			entry, ok := bundle.AgentEntry(tc.agentID)
			if !ok {
				t.Fatalf("AgentEntry(%s) missing", tc.agentID)
			}
			source, _ := bundle.AgentContractSource(tc.agentID)
			resolution, found, err := ResolvePromptFileForContractAgent(bundle, tc.agentID, entry, source, tc.mode)
			if err != nil {
				t.Fatalf("ResolvePromptFileForContractAgent: %v", err)
			}
			if !found {
				t.Fatal("expected prompt file to resolve")
			}
			if got := filepath.Base(resolution.Path); got != tc.want {
				t.Fatalf("resolved prompt = %s, want %s", got, tc.want)
			}
		})
	}

	resolution, found, err := NewBundlePromptResolver(bundle).ResolvePromptFileForAgent(models.AgentConfig{
		ID:          "parent-agent-shard-1",
		ParentAgent: "parent-agent",
		Role:        "parent_role",
	}, "")
	if err != nil {
		t.Fatalf("ResolvePromptFileForAgent shard parent: %v", err)
	}
	if !found {
		t.Fatal("expected shard child to resolve parent prompt")
	}
	if got := filepath.Base(resolution.Path); got != "parent-agent.md" {
		t.Fatalf("shard prompt = %s, want parent-agent.md", got)
	}
}

func TestPromptResolvedPolicy_UsesPackageAndFlowPrecedence(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Policy: PolicyDocument{Values: map[string]PolicyValue{
			"team_name": {Value: "root"},
			"priority":  {Value: "low"},
		}},
		PackageTree: []LoadedProjectPackage{
			{Key: "division"},
			{Key: "ops", ParentKey: "division"},
		},
		projectContracts: map[string]ProjectContractView{
			"division": {
				Policy: PolicyDocument{Values: map[string]PolicyValue{
					"team_name": {Value: "division"},
					"priority":  {Value: "medium"},
				}},
			},
			"ops": {
				Policy: PolicyDocument{Values: map[string]PolicyValue{
					"team_name": {Value: "ops"},
				}},
			},
		},
		FlowTree: flowmodel.Tree[FlowContractView]{
			Root: &FlowContractView{
				Policy: PolicyDocument{Values: map[string]PolicyValue{
					"team_name": {Value: "division"},
					"priority":  {Value: "medium"},
				}},
				Children: []FlowContractView{{
					Paths: FlowContractPaths{ID: "ops/research"},
					Policy: PolicyDocument{Values: map[string]PolicyValue{
						"team_name": {Value: "research"},
					}},
				}},
			},
			ByID: map[string]*FlowContractView{
				"ops/research": {
					Paths: FlowContractPaths{ID: "ops/research"},
					Policy: PolicyDocument{Values: map[string]PolicyValue{
						"team_name": {Value: "research"},
					}},
				},
			},
		},
	}

	projectPolicy := promptResolvedPolicy(bundle, ContractItemSource{PackageKey: "ops"})
	if got := projectPolicy.Values["team_name"].Value; got != "ops" {
		t.Fatalf("project team_name = %#v, want ops", got)
	}
	if got := projectPolicy.Values["priority"].Value; got != "medium" {
		t.Fatalf("project priority = %#v, want medium", got)
	}

	flowPolicy := promptResolvedPolicy(bundle, ContractItemSource{PackageKey: "ops", FlowID: "ops/research"})
	if got := flowPolicy.Values["team_name"].Value; got != "research" {
		t.Fatalf("flow team_name = %#v, want research", got)
	}
	if got := flowPolicy.Values["priority"].Value; got != "medium" {
		t.Fatalf("flow priority = %#v, want medium", got)
	}
}

func TestPromptVariableValues_UsesSpecResolutionOrder(t *testing.T) {
	bundle := &WorkflowContractBundle{
		Policy: PolicyDocument{Values: map[string]PolicyValue{
			"team_name":     {Value: "policy"},
			"customer_tier": {Value: "gold"},
			"current_date":  {Value: "policy-date"},
		}},
	}
	cfg := models.AgentConfig{
		ID:       "agent-42",
		FlowPath: "flows/demo/inst-1",
		Config: mustPromptJSON(t, map[string]any{
			"team_name": "instance",
			"fields": map[string]any{
				"team_name": "entity",
				"score":     7,
			},
		}),
	}

	vars := promptVariableValues(bundle, ContractItemSource{}, cfg)
	if got := vars["team_name"]; got != "instance" {
		t.Fatalf("team_name = %#v, want instance", got)
	}
	if got := vars["customer_tier"]; got != "gold" {
		t.Fatalf("customer_tier = %#v, want gold", got)
	}
	if got := vars["score"]; got != float64(7) {
		t.Fatalf("score = %#v, want 7", got)
	}
	if got := vars["current_date"]; got != "policy-date" {
		t.Fatalf("current_date = %#v, want policy-date", got)
	}
	if got := vars["agent_id"]; got != "agent-42" {
		t.Fatalf("agent_id = %#v, want agent-42", got)
	}
	if got := vars["flow_instance_path"]; got != "flows/demo/inst-1" {
		t.Fatalf("flow_instance_path = %#v, want flows/demo/inst-1", got)
	}
}

func mustPromptJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal prompt json: %v", err)
	}
	return raw
}
