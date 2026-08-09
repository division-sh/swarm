package runtimepersistence

import (
	"slices"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/runfork"
)

func TestCanonicalConversationForkSandboxPolicyOwnsExactAvailableTools(t *testing.T) {
	policy := runfork.CanonicalConversationForkSandboxPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	prepared := runfork.ConversationForkChatPrepared{
		SandboxPolicy:  policy,
		AvailableTools: policy.AvailableToolNames(),
	}
	if err := prepared.ValidateSandboxPolicy(); err != nil {
		t.Fatalf("ValidateSandboxPolicy: %v", err)
	}

	mutated := prepared
	mutated.AvailableTools = append([]string(nil), prepared.AvailableTools...)
	mutated.AvailableTools = append(mutated.AvailableTools, "unplanned_tool")
	if err := mutated.ValidateSandboxPolicy(); err == nil {
		t.Fatal("sandbox policy accepted an unplanned available tool")
	}

	drifted := policy
	drifted.StubbedTools = append([]string(nil), policy.StubbedTools...)
	drifted.StubbedTools[0] = "different_tool"
	if err := drifted.Validate(); err == nil {
		t.Fatal("sandbox policy accepted a drifted stubbed tool set")
	}
	if slices.Equal(drifted.AvailableToolNames(), policy.AvailableToolNames()) {
		t.Fatal("drifted policy unexpectedly retained canonical available tools")
	}
}
