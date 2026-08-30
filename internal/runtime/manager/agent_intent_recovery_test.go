package manager

import (
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestSelectedStoreRecoveryReconstructsCanonicalPromptBeforeProviderFactory(t *testing.T) {
	intent := managerRecoveryIntent(t)
	source := managerRecoverySource(intent, nil)
	cfg := managerTestAgentConfig(models.AgentConfig{
		ID:       "worker",
		Identity: managerAgentIdentity("worker"),
		Role:     "worker",
		Model:    "regular",
		Intent:   intent,
	})
	cfg.Prompt = runtimeagentintent.DerivedPrompt{}

	providerCalls := 0
	am := newTestAgentManagerWithOptions(t, nil, func(got models.AgentConfig) (Agent, error) {
		providerCalls++
		prompt, err := got.DerivedSystemPrompt()
		if err != nil {
			t.Fatalf("provider received invalid prompt carrier: %v", err)
		}
		if prompt != intent.Content {
			t.Fatalf("provider prompt = %q, want exact canonical intent %q", prompt, intent.Content)
		}
		return newGenericAgent(got), nil
	}, AgentManagerOptions{SemanticSource: source})

	if err := bindCanonicalAgentPrompt(source, &cfg); err != nil {
		t.Fatalf("reconstruct selected-store prompt: %v", err)
	}
	if _, err := am.buildAgent(cfg); err != nil {
		t.Fatalf("build recovered provider agent: %v", err)
	}
	if providerCalls != 1 {
		t.Fatalf("provider factory calls = %d, want exactly 1", providerCalls)
	}
}

func TestSelectedStoreRecoveryRejectsCriteriaMismatchBeforeProviderFactory(t *testing.T) {
	intent := managerRecoveryIntent(t)
	source := managerRecoverySource(intent, []string{"quality"})
	cfg := managerTestAgentConfig(models.AgentConfig{
		ID:       "worker",
		Identity: managerAgentIdentity("worker"),
		Role:     "worker",
		Model:    "regular",
		Intent:   intent,
		Criteria: []string{"hostile-replacement"},
	})
	cfg.Prompt = runtimeagentintent.DerivedPrompt{}

	providerCalls := 0
	am := newTestAgentManagerWithOptions(t, nil, func(got models.AgentConfig) (Agent, error) {
		providerCalls++
		return newGenericAgent(got), nil
	}, AgentManagerOptions{SemanticSource: source})

	err := bindCanonicalAgentPrompt(source, &cfg)
	if err == nil {
		_, err = am.buildAgent(cfg)
	}
	if err == nil || !strings.Contains(err.Error(), "runtime refs must match contract agent criteria") {
		t.Fatalf("reconstruct selected-store prompt error = %v, want criteria mismatch rejection", err)
	}
	if providerCalls != 0 {
		t.Fatalf("provider factory calls = %d, want 0", providerCalls)
	}
}

func managerRecoveryIntent(t testing.TB) runtimeagentintent.Resolved {
	t.Helper()
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"review/agents.yaml#agents.worker.intent",
		"Perform only the declared review work.",
	)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}

func managerRecoverySource(intent runtimeagentintent.Resolved, criteria []string) semanticview.Source {
	flow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{FlowPath: "review"},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {
				ID:             "worker",
				Role:           "worker",
				ResolvedIntent: intent,
				Criteria:       append([]string(nil), criteria...),
			},
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		FlowTree: runtimecontracts.FlowTree{
			Root: &runtimecontracts.FlowContractView{Children: []runtimecontracts.FlowContractView{flow}},
			ByID: map[string]*runtimecontracts.FlowContractView{"review": &flow},
		},
	}
	return semanticview.Wrap(bundle)
}
