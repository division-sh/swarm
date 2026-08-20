package llm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
)

const (
	testAgentFrameBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testManagedModelAlias    = "frame-alias"
)

func testManagedSessionSeed(t testing.TB, identity agentidentity.Identity, role string, runtime Runtime) agentframe.SessionSeed {
	t.Helper()
	contract, ok := ProviderContractForRuntime(runtime)
	if !ok {
		t.Fatalf("runtime %T has no provider contract", runtime)
	}
	intent, err := agentintent.Resolve(
		agentintent.SourceInline,
		"inline",
		"agents.yaml#agents."+identity.AgentID()+".intent",
		"Complete the admitted business work using only authorized capabilities.",
	)
	if err != nil {
		t.Fatalf("resolve managed test intent: %v", err)
	}
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatalf("derive managed test prompt: %v", err)
	}
	providerPrompt, err := agentintent.AssembleProviderPrompt(intent, []string{}, prompt, agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatalf("assemble managed test provider prompt: %v", err)
	}
	return agentframe.SessionSeed{
		AgentIdentity:  identity,
		Role:           role,
		Intent:         intent,
		Criteria:       []string{},
		ProviderPrompt: providerPrompt,
		RuntimeMode:    contract.RuntimeMode,
		Provider:       contract.Provider,
		Transport:      string(contract.Transport),
		ModelAlias:     testManagedModelAlias,
		Model:          "test-model",
	}
}

func newTestManagedConversation(t testing.TB, agentID, flowInstance, role string, tools []ToolDefinition, memory agentmemory.Plan, maxTurns int, runtime Runtime) *Conversation {
	t.Helper()
	seed := testManagedSessionSeed(t, testAgentIdentity(agentID, flowInstance), role, runtime)
	conversation, err := NewManagedConversation(seed, "task-1", tools, memory, maxTurns, runtime)
	if err != nil {
		t.Fatalf("NewManagedConversation: %v", err)
	}
	return conversation
}

func testManagedEvent(agentID string) events.Event {
	return eventtest.ExistingRunRootIngress(
		eventtest.UUID("managed-frame-event:"+agentID),
		events.EventType("work.requested"),
		"operator",
		"task-1",
		json.RawMessage(`{"work":"admitted"}`),
		0,
		testMemoryRunID,
		events.EventEnvelope{},
		time.Unix(1, 0).UTC(),
	)
}

func testManagedConversationContext(t testing.TB, harness *effecttest.Harness, agentID, flowInstance, role string) context.Context {
	t.Helper()
	setEffectHarnessAgent(t, harness, agentID, flowInstance)
	identity := testAgentIdentity(agentID, flowInstance)
	ctx := harness.CompletionContext("managed-frame:" + agentID)
	ctx = models.WithActor(ctx, models.AgentConfig{
		ExecutionMode: "live",
		ID:            agentID,
		Identity:      identity,
		FlowPath:      identity.FlowInstance(),
		Role:          role,
		Memory:        testMemory(),
	})
	ctx = withTestMemory(ctx, agentID, flowInstance)
	fact, err := runtimecorrelation.NewPersistedBundleSourceFact(testAgentFrameBundleHash)
	if err != nil {
		t.Fatalf("test bundle source: %v", err)
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, fact)
	return llmTestWorkContext(t, ctx)
}

func requireManagedSettlementProviderSelection(t testing.TB, settlements []runtimeeffects.CompletionSettlement) {
	t.Helper()
	if len(settlements) == 0 {
		t.Fatal("managed completion produced no settlements")
	}
	for index, settlement := range settlements {
		if settlement.Usage.ResolvedModel != "test-model" || settlement.Spend.ModelAlias != testManagedModelAlias || settlement.Spend.ResolvedModel != "test-model" {
			t.Fatalf("managed settlement %d provider selection = usage %#v spend %#v", index, settlement.Usage, settlement.Spend)
		}
	}
}
