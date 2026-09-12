package tools

import (
	"testing"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
)

func TestDeclarationConsumersRejectRoleOnlyActor(t *testing.T) {
	source, _ := loadFlowDataToolSource(t)
	exact := flowDataActor()
	if _, flow, ok := criteriaAgentContractDeclaration(source, exact); !ok || flow != "support" {
		t.Fatalf("exact criteria declaration missing: %q %v", flow, ok)
	}
	if root := actorFlowOwnershipRoot(source, exact); root != "support" {
		t.Fatalf("exact write owner = %q", root)
	}
	for _, id := range []string{"unknown", "", "factory-cto"} {
		actor := models.AgentConfig{ID: id, Role: exact.Role, FlowID: exact.FlowID, FlowPath: exact.FlowPath}
		if _, _, ok := criteriaAgentContractDeclaration(source, actor); ok {
			t.Errorf("unknown/retired actor %q borrowed criteria declaration", id)
		}
		if root := actorFlowOwnershipRoot(source, actor); root != "" {
			t.Errorf("unknown/retired actor %q borrowed write owner %q", id, root)
		}
	}
}

func TestWebSearchPolicyDoesNotInferDeclarationFromPath(t *testing.T) {
	source := rateLimitedNativeWebSearchSiblingFlowSource("https://provider.invalid")
	for _, id := range []string{"unknown", ""} {
		actor := models.AgentConfig{ID: id, Role: "alpha-agent", FlowPath: "alpha/instance-1"}
		if _, err := resolveWebSearchProviderConfigForActor(source, actor); err == nil {
			t.Error("path-only actor acquired scoped provider configuration")
		}
		actor.FlowID = "alpha"
		if _, err := resolveWebSearchProviderConfigForActor(source, actor); err != nil {
			t.Fatalf("explicit runtime flow policy was rejected: %v", err)
		}
		if decision := classifyToolAuthorization(actor, "web_search", nil); decision.allowed {
			t.Error("policy configuration granted a native capability")
		}
		actor.NativeTools.WebSearch = true
		if decision := classifyToolAuthorization(actor, "web_search", nil); !decision.allowed {
			t.Error("independent native tool configuration was revoked")
		}
		actor.FlowID = "missing"
		if _, err := resolveWebSearchProviderConfigForActor(source, actor); err == nil {
			t.Error("unknown configured flow acquired policy")
		}
	}
}
