package operatorsurface

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func TestOperatorAgentSummaryPublishesCanonicalMemoryFacts(t *testing.T) {
	memorySummary, err := operatorAgentSummaryFromPersisted(runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "memory-agent",
			Identity: operatorAgentProjectionTestIdentity(t, "memory-agent", "support/chat-1"),
			Role:     "worker",
			Type:     "managed",
			Model:    "cheap",
			Memory:   agentmemory.Authored(true),
			FlowPath: "support/chat-1",
		},
	}, operatorAgentProjection{LifecycleState: "active"}, 0)
	if err != nil {
		t.Fatalf("memory summary: %v", err)
	}
	if !memorySummary.Memory || memorySummary.MemorySource != string(agentmemory.SourceAuthored) {
		t.Fatalf("memory summary = enabled:%v source:%q, want authored true", memorySummary.Memory, memorySummary.MemorySource)
	}
	if memorySummary.FlowInstance != "support/chat-1" {
		t.Fatalf("FlowInstance = %q, want support/chat-1", memorySummary.FlowInstance)
	}
	assertOperatorAgentSummaryMemoryJSON(t, memorySummary, `"memory":true`, `"memory_source":"authored"`)

	defaultSummary, err := operatorAgentSummaryFromPersisted(runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "stateless-agent",
			Identity: operatorAgentProjectionTestIdentity(t, "stateless-agent", ""),
			Role:     "worker",
			Type:     "managed",
			Model:    "cheap",
			Memory:   agentmemory.PlatformDefault(),
		},
	}, operatorAgentProjection{}, 0)
	if err != nil {
		t.Fatalf("default summary: %v", err)
	}
	if defaultSummary.Memory || defaultSummary.MemorySource != string(agentmemory.SourcePlatformDefault) {
		t.Fatalf("default memory summary = enabled:%v source:%q, want platform-default false", defaultSummary.Memory, defaultSummary.MemorySource)
	}
	assertOperatorAgentSummaryMemoryJSON(t, defaultSummary, `"memory":false`, `"memory_source":"platform_default"`)
}

func operatorAgentProjectionTestIdentity(t *testing.T, agentID, flow string) agentidentity.Identity {
	t.Helper()
	name, err := agentidentity.RuntimeName(agentID, "operator-agent-projection-test")
	if err != nil {
		t.Fatal(err)
	}
	route := agentidentity.RootRoute()
	if flow != "" {
		route, err = agentidentity.PresentRoute("support", "chat-1", flow)
		if err != nil {
			t.Fatal(err)
		}
	}
	identity, err := agentidentity.New("11111111-1111-4111-8111-111111111114", name, route)
	if err != nil {
		t.Fatal(err)
	}
	return identity
}

func assertOperatorAgentSummaryMemoryJSON(t *testing.T, summary any, expected ...string) {
	t.Helper()
	raw, err := json.Marshal(summary)
	if err != nil {
		t.Fatalf("marshal summary: %v", err)
	}
	text := string(raw)
	for _, fragment := range expected {
		if !strings.Contains(text, fragment) {
			t.Fatalf("summary json = %s, want %s", text, fragment)
		}
	}
	for _, retired := range []string{"conversation_mode", "session_scope", `"mode"`} {
		if strings.Contains(text, retired) {
			t.Fatalf("summary json = %s, must not expose retired %s", text, retired)
		}
	}
}
