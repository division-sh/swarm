package contracts

import (
	"encoding/json"
	"strings"
	"testing"

	models "swarm/internal/runtime/core/actors"
	flowmodel "swarm/internal/runtime/flowmodel"
)

func TestLoadPromptForAgent_UsesPromptRefAndWorkspaceRoleFallback(t *testing.T) {
	SetActivePromptBundle(loadPromptTestBundle(t, repoRoot(t)))
	prompt, found, err := LoadPromptForAgent(models.AgentConfig{
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
