package manager

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestAgentFactoryRejectsAuthoredPromptAuthority(t *testing.T) {
	for _, raw := range []string{`{"system_prompt":"inert"}`, `{"nested":{"system_prompt":"inert"}}`, `{"nested":[{"system_prompt":"inert"}]}`} {
		cfg := managerTestAgentConfig(models.AgentConfig{ID: "worker", Role: "worker", Model: "regular"})
		cfg.Config = json.RawMessage(raw)
		calls := 0
		am := newTestAgentManager(t, nil, func(got models.AgentConfig) (Agent, error) { calls++; return newGenericAgent(got), nil })
		_, err := am.buildAgent(cfg)
		if err == nil || !strings.Contains(err.Error(), "is unsupported") || calls != 0 {
			t.Fatalf("authored prompt reached factory: calls=%d err=%v", calls, err)
		}
	}
}

func TestSelectedStoreRecoveryReconstructsCanonicalPromptBeforeProviderFactory(t *testing.T) {
	intent := managerRecoveryIntent(t)
	source := managerRecoverySource(t, intent, nil)
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
	source := managerRecoverySource(t, intent, []string{"quality"})
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

func managerRecoverySource(t *testing.T, intent runtimeagentintent.Resolved, criteria []string) semanticview.Source {
	t.Helper()
	root := filepath.Join(t.TempDir(), "recovery")
	for _, flow := range []string{".", "review"} {
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow, "manifest.yaml"), "version: 1.0.0\nplatform_version: '*'\n")
		writeFlowActivationFixtureFile(t, filepath.Join(root, flow, "schema.yaml"), "stages: []\n")
	}
	agent := fmt.Sprintf("worker:\n  role: worker\n  model: regular\n  intent: {inline: %q}\n", intent.Content)
	if len(criteria) != 0 {
		agent += "  criteria: [quality]\n"
		writeFlowActivationFixtureFile(t, filepath.Join(root, "review", "rules.yaml"), "quality:\n  criteria:\n    classes:\n      hard: {disposition: reject}\n    rules:\n      - {id: QUALITY-01, class: hard, text: Require declared quality.}\n")
	}
	writeFlowActivationFixtureFile(t, filepath.Join(root, "review", "agents.yaml"), agent)
	repo := filepath.Clean("../../..")
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}
