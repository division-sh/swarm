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
