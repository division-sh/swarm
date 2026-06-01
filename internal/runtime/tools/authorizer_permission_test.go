package tools

import (
	"context"
	"errors"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestToolAuthorizer_PermissionGatedTools(t *testing.T) {
	t.Run("agent fire allowed with permission", func(t *testing.T) {
		err := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID:          "ops-1",
			Permissions: []string{"agent_fire"},
		}, "agent_fire")
		if err != nil {
			t.Fatalf("expected agent_fire to be allowed: %v", err)
		}
	})

	t.Run("agent fire denied without permission", func(t *testing.T) {
		err := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID: "ops-2",
		}, "agent_fire")
		if err == nil || !errors.Is(err, ErrToolNotAllowed) {
			t.Fatalf("expected denial for agent_fire without permission, got %v", err)
		}
	})

	t.Run("schedule allowed with permission", func(t *testing.T) {
		err := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID:          "ops-3",
			Permissions: []string{"schedule"},
		}, "schedule")
		if err != nil {
			t.Fatalf("expected schedule to be allowed: %v", err)
		}
	})

	t.Run("schedule denied without permission", func(t *testing.T) {
		err := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID: "ops-4",
		}, "schedule")
		if err == nil || !errors.Is(err, ErrToolNotAllowed) {
			t.Fatalf("expected denial for schedule without permission, got %v", err)
		}
	})

	t.Run("universal tool bypasses permission tier", func(t *testing.T) {
		err := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID: "ops-5",
		}, "agent_message")
		if err != nil {
			t.Fatalf("expected universal tool to be allowed: %v", err)
		}
	})

	t.Run("actor config tier still applies to non-gated tools", func(t *testing.T) {
		err := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID: "ops-6",
		}, "workflow_custom_tool")
		if err == nil || !errors.Is(err, ErrToolNotAllowed) {
			t.Fatalf("expected unconstrained non-gated tool to be denied, got %v", err)
		}
	})

	t.Run("actor config still allows explicitly listed workflow tool", func(t *testing.T) {
		authErr := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID:    "ops-7",
			Tools: []string{"workflow_custom_tool"},
		}, "workflow_custom_tool")
		if authErr != nil {
			t.Fatalf("expected listed workflow tool to be allowed: %v", authErr)
		}
	})

	t.Run("provider native read allowed when file_io enabled", func(t *testing.T) {
		authErr := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID:          "ops-8",
			NativeTools: models.NativeToolConfig{FileIO: true},
		}, "Read")
		if authErr != nil {
			t.Fatalf("expected provider native Read to be allowed: %v", authErr)
		}
	})

	t.Run("actor config tool aliases are canonicalized", func(t *testing.T) {
		authErr := NewToolAuthorizer(nil, nil).Authorize(context.Background(), models.AgentConfig{
			ID:    "ops-9",
			Tools: []string{"Read"},
		}, "read_file")
		if authErr != nil {
			t.Fatalf("expected aliased configured tool to be allowed: %v", authErr)
		}
	})
}

func TestResolveAgentPermissions_ExpandsBundleAndDedupes(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"permission_bundles": {
				Value: map[string]any{
					"ops": map[string]any{
						"permissions": []any{"agent_fire", "schedule"},
					},
				},
			},
		}},
	})
	perms, err := ResolveAgentPermissions(source, "", runtimecontracts.AgentRegistryEntry{
		PermissionsBundle: "ops",
		Permissions:       []string{"agent_fire", "schedule"},
	})
	if err != nil {
		t.Fatalf("ResolveAgentPermissions: %v", err)
	}
	want := []string{"agent_fire", "schedule"}
	if len(perms) != len(want) {
		t.Fatalf("expected %v, got %v", want, perms)
	}
	for i := range want {
		if perms[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, perms)
		}
	}
}

func TestValidateAgentPermissions_ReportsToolPermissionMismatch(t *testing.T) {
	platform := runtimecontracts.PlatformSpecDocument{}
	platform.PermissionsModel.Permissions = append([]string(nil), defaultPlatformPermissions...)
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Platform: platform,
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"permission_bundles": {
				Value: map[string]any{
					"ops": map[string]any{
						"permissions": []any{"agent_fire"},
					},
				},
			},
		}},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"invalid-agent": {
				ID:         "invalid-agent",
				Role:       "operator",
				ToolsTier2: []string{"agent_fire"},
			},
			"valid-agent": {
				ID:                "valid-agent",
				Role:              "operator",
				PermissionsBundle: "ops",
				ToolsTier2:        []string{"agent_fire"},
			},
		},
	})

	agentCount, errs := ValidateAgentPermissions(source)
	if agentCount != 2 {
		t.Fatalf("expected 2 agents, got %d", agentCount)
	}
	if len(errs) != 1 {
		t.Fatalf("expected exactly one validation error, got %d: %v", len(errs), errs)
	}
	if !strings.Contains(errs[0].Error(), `agent invalid-agent declares tool agent_fire without required permission "agent_fire"`) {
		t.Fatalf("unexpected validation error: %v", errs[0])
	}
}

func TestValidateAgentPermissions_AcceptsToolDefinedExtensionPermission(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"researcher": {
				ID:          "researcher",
				Role:        "researcher",
				Permissions: []string{"external_api_access"},
				Tools:       []string{"lookup_data"},
			},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"lookup_data": {
				HandlerType: "http",
				Permission:  "external_api_access",
				HTTP: &runtimecontracts.HTTPToolSpec{
					Method: "GET",
					URL:    "https://example.com",
				},
			},
		},
	})

	agentCount, errs := ValidateAgentPermissions(source)
	if agentCount != 1 {
		t.Fatalf("expected 1 agent, got %d", agentCount)
	}
	if len(errs) != 0 {
		t.Fatalf("expected no validation errors, got %v", errs)
	}
}

func TestValidateAgentPermissions_DefaultWorkflowBundleDoesNotReportUnknownBundles(t *testing.T) {
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	contractsRoot := runtimecontracts.DefaultWorkflowContractsDir(repoRoot)
	if strings.TrimSpace(contractsRoot) == "" {
		t.Skip("no default workflow contracts dir")
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, runtimecontracts.DefaultPlatformSpecFile(repoRoot))
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	_, errs := ValidateAgentPermissions(semanticview.Wrap(bundle))
	for _, err := range errs {
		if strings.Contains(err.Error(), "unknown permissions_bundle") {
			t.Fatalf("unexpected bundle resolution error: %v", err)
		}
	}
}

func TestToolAuthorizer_ExplicitEmitEventsAllowEmitTool(t *testing.T) {
	registry := NewEmitRegistry(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"coord.done": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
				},
			},
		},
	}), nil)
	auth := NewToolAuthorizer(nil, func(actor models.AgentConfig, toolName string) toolAuthorizationDecision {
		return classifyToolAuthorization(actor, toolName, nil, registry)
	})
	err := auth.Authorize(context.Background(), models.AgentConfig{
		ID:         "coordinator-1",
		EmitEvents: []string{"coord.done"},
	}, "emit_coord_done")
	if err != nil {
		t.Fatalf("expected configured emit tool to be allowed: %v", err)
	}
}

func TestToolAuthorizer_ScopedEmitEventsAllowLocalEmitTool(t *testing.T) {
	registry := NewEmitRegistry(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"discovery/category.assessed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
				},
			},
		},
	}), nil)
	auth := NewToolAuthorizer(nil, func(actor models.AgentConfig, toolName string) toolAuthorizationDecision {
		return classifyToolAuthorization(actor, toolName, nil, registry)
	})
	err := auth.Authorize(context.Background(), models.AgentConfig{
		ID:         "market-research-agent",
		EmitEvents: []string{"discovery/category.assessed"},
	}, "emit_category_assessed")
	if err != nil {
		t.Fatalf("expected scoped configured emit tool to be allowed: %v", err)
	}
}

func TestToolAuthorizer_AllowsMCPPrefixedEmitToolAlias(t *testing.T) {
	registry := NewEmitRegistry(semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"market_research.scan_complete": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
				},
			},
		},
	}), nil)
	auth := NewToolAuthorizer(nil, func(actor models.AgentConfig, toolName string) toolAuthorizationDecision {
		return classifyToolAuthorization(actor, toolName, nil, registry)
	})
	err := auth.Authorize(context.Background(), models.AgentConfig{
		ID:         "market-research-agent",
		EmitEvents: []string{"market_research.scan_complete"},
	}, "mcp__runtime-tools__emit_market_research_scan_complete")
	if err != nil {
		t.Fatalf("expected MCP-prefixed emit tool alias to be allowed: %v", err)
	}
}
