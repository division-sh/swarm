package bootverify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

// The nil embedded implementations panic on any accidental deployment read.
type forbiddenStructuralCredentials struct{ runtimecredentials.Store }
type forbiddenStructuralManagedCredentials struct {
	runtimemanagedcredentials.Store
}

func TestStructuralValidationNeverObservesDeployment(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	source := semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {Tools: []string{"infra.remote"}, Model: "regular", WorkspaceClass: "missing-class"},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {Value: map[string]any{
				"infra": map[string]any{"transport": "http", "url": server.URL, "prefix": "infra"},
			}},
		}},
	})
	report := Run(context.Background(), source, Options{
		Purpose: StructuralValidation, CheckMCPReachable: true, ValidateModelResolution: true,
		Credentials: forbiddenStructuralCredentials{}, ManagedCredentials: forbiddenStructuralManagedCredentials{},
	})
	if calls.Load() != 0 {
		t.Fatalf("structural verification made %d MCP requests", calls.Load())
	}
	for _, finding := range report.Findings {
		switch finding.CheckID {
		case "credential_key_exists", "managed_credential_state", "mcp_server_reachable", "required_mcp_tool_availability":
			t.Fatalf("structural report contains deployment observation: %#v", finding)
		}
	}
	if !reportContains(report.Errors(), "workspace_class_exists", "missing-class") {
		t.Fatalf("structural workspace declaration check was lost: %#v", report.Findings)
	}
}

func TestStructuralValidationRejectsUnknownPurpose(t *testing.T) {
	report := Run(context.Background(), semanticviewtest.WrapRootAgents(&runtimecontracts.WorkflowContractBundle{}), Options{Purpose: ValidationPurpose(255)})
	if !reportContains(report.Errors(), "workflow_contract_validation", "invalid validation purpose") {
		t.Fatalf("invalid purpose accepted: %#v", report)
	}
}

func TestStructuralCheckPurposeCensus(t *testing.T) {
	// Pin the entire registry so new checks require a deliberate purpose decision.
	classified := map[string]string{
		"declared_agent_name_valid":               "structural",
		"event_metadata_authority":                "structural",
		"event_chain_integrity":                   "structural",
		"event_consumer_exists":                   "structural",
		"event_producer_exists":                   "structural",
		"legacy_qualified_subscription":           "structural",
		"semantic_drift_dead_event_schema":        "structural",
		"entity_writer_coverage":                  "structural",
		"payload_field_coverage":                  "structural",
		"entity_write_target_compliance":          "structural",
		"contained_state_operation_compliance":    "structural",
		"semantic_drift_payload_completeness":     "structural",
		"condition_payload_alignment":             "structural",
		"condition_policy_alignment":              "structural",
		"state_machine_coherence":                 "structural",
		"semantic_drift_unreachable_state":        "structural",
		"node_state_schema_typed_counterpart":     "structural",
		"accumulator_entity_projection":           "structural",
		"accumulator_handler_isolation":           "structural",
		"accumulator_input_producer_path":         "structural",
		"required_agents_match":                   "structural",
		"handler_field_compliance":                "structural",
		policySheetLookupCheckID:                  "structural",
		policySheetValidationCheckID:              "structural",
		computeModuleCheckID:                      "structural",
		fanOutValidationCheckID:                   "structural",
		joinValidationCheckID:                     "structural",
		loopValidationCheckID:                     "structural",
		collectionItemSemanticsCheckID:            "structural",
		stageGateValidationCheckID:                "structural",
		"tool_resolution":                         "mixed",
		"required_mcp_tool_availability":          "execution",
		"platform_tool_usage_hints":               "mixed",
		"generated_tool_schema_closure":           "structural",
		"intent_resolution":                       "structural",
		"produces_drift":                          "structural",
		"invalid_field_detection":                 "mixed",
		"policy_conflict_detection":               "structural",
		"event_cycle_detection":                   "structural",
		"dialect_compliance":                      "structural",
		"single_node_per_event":                   "structural",
		"config_from_payload_alignment":           "structural",
		"phantom_produces":                        "structural",
		"native_tools_valid":                      "structural",
		"mcp_server_reachable":                    "execution",
		"platform_namespace_violation":            "structural",
		"workspace_class_exists":                  "structural",
		"credential_key_exists":                   "execution",
		"agent_permission_validation":             "structural",
		"transition_reference_validation":         "structural",
		"condition_expression_validation":         "structural",
		"data_accumulation_expression_validation": "structural",
		"emit_field_expression_validation":        "structural",
		"executable_reader_expression_validation": "structural",
		"expression_field_reference_validation":   "structural",
		"entity_reader_coverage":                  "structural",
		"primary_entity_validation":               "structural",
		"template_instance_validation":            "structural",
		"singleton_coordinator_validation":        "structural",
		"cross_surface_named_type_use":            "structural",
		"transition_ownership_validation":         "structural",
		"event_runtime_wiring_validation":         "structural",
		"timer_validation":                        "structural",
		"write_pin_ownership_validation":          "structural",
		"gate_schema_validation":                  "structural",
		"composition_connect_validation":          "structural",
		"input_pin_wiring":                        "structural",
		"pin_target_resolution":                   "structural",
		"cross_flow_pin_ambiguity_validation":     "structural",
		"flow_boundary_create_entity_validation":  "structural",
		"flow_data_access_validation":             "structural",
		"impl.platform_metadata_validation":       "structural",
		"impl.deprecated_contract_alias":          "structural",
		"agent_prompt_lint_structural":            "structural",
	}
	seen := map[string]bool{}
	for _, registry := range [][]Check{bootCheckRegistry, supplementalChecks} {
		for _, check := range registry {
			if classified[check.ID] == "" || seen[check.ID] {
				t.Errorf("unclassified or duplicate check %q", check.ID)
			}
			seen[check.ID] = true
		}
	}
	for id := range classified {
		if !seen[id] {
			t.Errorf("retired check %q survives in purpose census", id)
		}
	}
}
