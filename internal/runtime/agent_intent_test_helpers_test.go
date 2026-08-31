package runtime

import (
	"strings"
	"testing"

	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func runtimeTestAgentConfig(t testing.TB, cfg runtimeactors.AgentConfig) runtimeactors.AgentConfig {
	t.Helper()
	if strings.TrimSpace(cfg.FlowID) == "" && cfg.CanonicalFlowPath() == "" {
		cfg.FlowID = "."
	}
	if strings.TrimSpace(cfg.ResolvedLLMBackend) == "" {
		cfg.ResolvedLLMBackend = strings.TrimSpace(cfg.LLMBackend)
		if cfg.ResolvedLLMBackend == "" {
			cfg.ResolvedLLMBackend = "anthropic"
		}
	}
	if cfg.Intent.Empty() {
		resolved, err := runtimeagentintent.Resolve(
			runtimeagentintent.SourceInline,
			"inline",
			"agents.yaml#agents."+strings.TrimSpace(cfg.ID)+".intent",
			"Perform the runtime test agent's assigned work.",
		)
		if err != nil {
			t.Fatalf("resolve runtime test intent: %v", err)
		}
		cfg.Intent = resolved
	}
	if cfg.Prompt.Empty() {
		prompt, err := runtimeagentintent.IntentOnlyPrompt(cfg.Intent)
		if err != nil {
			t.Fatalf("derive runtime test prompt: %v", err)
		}
		cfg.Prompt = prompt
	}
	return cfg
}

func runtimeTestSubscriptionSource(eventNames ...string) semanticview.Source {
	events := make(map[string]runtimecontracts.EventCatalogEntry, len(eventNames))
	for _, eventName := range eventNames {
		events[eventName] = runtimecontracts.EventCatalogEntry{}
	}
	root := runtimecontracts.FlowContractView{Path: ".", Events: events}
	return semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &root,
			ByID: map[string]*runtimecontracts.FlowContractView{".": &root},
		},
	})
}
