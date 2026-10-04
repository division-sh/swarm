package runtime

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func TestDeclaredAgentModelAdmissionUsesConfiguredProfileWithoutRuntimeOrCredentials(t *testing.T) {
	source := compiledRuntimeValidationSource(t, &runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent": {ID: "agent", Model: "deployment-model"},
		},
	})
	for _, backend := range []string{llmselection.BackendAnthropic, llmselection.BackendClaudeCLI, llmselection.BackendOpenAICompatible, llmselection.BackendOpenAIResponses} {
		t.Run(backend, func(t *testing.T) {
			cfg := &config.Config{LLM: config.LLMConfig{Backend: backend}}
			cfg.LLM.Models = llmselection.ModelAliases{"deployment-model": {backend: "configured-provider-model"}}
			if err := ValidateDeclaredAgentModelAdmission(executionposture.Live, cfg, source); err != nil {
				t.Fatalf("static profile/model admission: %v", err)
			}
			cfg.LLM.Models = llmselection.ModelAliases{"deployment-model": {llmselection.BackendMock: "wrong-profile"}}
			if err := ValidateDeclaredAgentModelAdmission(executionposture.Live, cfg, source); err == nil || !strings.Contains(err.Error(), "does not resolve for backend") {
				t.Fatalf("wrong profile was admitted: %v", err)
			}
		})
	}
}

func TestDeclaredAgentModelAdmissionRequiresSourceAndAdmittedConfiguration(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	if err := ValidateDeclaredAgentModelAdmission(executionposture.Live, nil, source); err == nil {
		t.Fatal("missing admitted configuration was treated as a pass")
	}
	if err := ValidateDeclaredAgentModelAdmission(executionposture.Live, &config.Config{}, nil); err == nil {
		t.Fatal("missing source was treated as a pass")
	}
}
