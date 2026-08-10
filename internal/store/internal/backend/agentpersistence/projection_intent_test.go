package agentpersistence

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestPersistedAgentProjectionRoundTripsExactIntentArtifact(t *testing.T) {
	cfg := persistedIntentTestAgent(t)
	projection, err := ProjectPersistedAgentConfig(cfg, "")
	if err != nil {
		t.Fatalf("ProjectPersistedAgentConfig: %v", err)
	}
	hydrated, err := HydratePersistedAgentConfig(projection)
	if err != nil {
		t.Fatalf("HydratePersistedAgentConfig: %v", err)
	}
	if !reflect.DeepEqual(hydrated.Intent, cfg.Intent) {
		t.Fatalf("hydrated intent = %#v, want %#v", hydrated.Intent, cfg.Intent)
	}
	if hydrated.SystemPrompt != cfg.SystemPrompt {
		t.Fatalf("hydrated derived system prompt = %q, want exact %q", hydrated.SystemPrompt, cfg.SystemPrompt)
	}
	if !reflect.DeepEqual(hydrated.Criteria, cfg.Criteria) {
		t.Fatalf("hydrated criteria = %#v, want %#v", hydrated.Criteria, cfg.Criteria)
	}
}

func TestPersistedAgentProjectionRejectsMissingOrTamperedIntent(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		cfg := persistedIntentTestAgent(t)
		cfg.Intent = runtimeagentintent.Resolved{}
		if _, err := ProjectPersistedAgentConfig(cfg, ""); err == nil || !strings.Contains(err.Error(), "resolved agent intent is required") {
			t.Fatalf("ProjectPersistedAgentConfig error = %v, want missing intent rejection", err)
		}
	})

	t.Run("tampered", func(t *testing.T) {
		projection, err := ProjectPersistedAgentConfig(persistedIntentTestAgent(t), "")
		if err != nil {
			t.Fatal(err)
		}
		var descriptor map[string]any
		if err := json.Unmarshal(projection.RuntimeDescriptor, &descriptor); err != nil {
			t.Fatal(err)
		}
		intent := descriptor["intent"].(map[string]any)
		intent["content_hash"] = "sha256:tampered"
		projection.RuntimeDescriptor, err = json.Marshal(descriptor)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := HydratePersistedAgentConfig(projection); err == nil || !strings.Contains(err.Error(), "content hash does not match") {
			t.Fatalf("HydratePersistedAgentConfig error = %v, want tamper rejection", err)
		}
	})
}

func persistedIntentTestAgent(t testing.TB) runtimeactors.AgentConfig {
	t.Helper()
	name, err := runtimeagentidentity.DeclaredName("worker", "swarm://global/worker")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := runtimeagentidentity.New(name, runtimeagentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	intent, err := runtimeagentintent.Resolve(
		runtimeagentintent.SourceInline,
		"inline",
		"agents.yaml#agents.worker.intent",
		"  exact business intent\n",
	)
	if err != nil {
		t.Fatal(err)
	}
	return runtimeactors.AgentConfig{
		ID:                 "worker",
		Identity:           identity,
		Type:               "worker",
		Role:               "worker",
		Model:              "regular",
		LLMBackend:         "claude_cli",
		ResolvedLLMBackend: "claude_cli",
		ExecutionMode:      runtimeeffects.ExecutionModeLive,
		Memory:             agentmemory.Authored(false),
		Intent:             intent,
		SystemPrompt:       intent.Content + "\n## Contract Criteria\nquality",
		Criteria:           []string{"quality"},
		Config:             json.RawMessage(`{}`),
	}
}
