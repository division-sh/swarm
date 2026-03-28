package tools

import (
	"context"
	"fmt"
	"strings"

	runtimecredentials "swarm/internal/runtime/credentials"
	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
)

func ValidateNativeToolBootConfig(ctx context.Context, source semanticview.Source, store runtimecredentials.Store, runtime llm.Runtime) ([]error, error) {
	if source == nil {
		return nil, nil
	}
	needsWebSearch := false
	for _, agent := range source.AgentEntries() {
		raw, ok := agent.NativeTools["web_search"]
		flag, isBool := raw.(bool)
		if ok && isBool && flag {
			needsWebSearch = true
			break
		}
	}
	if !needsWebSearch {
		return nil, nil
	}
	caps := llm.NativeToolCapabilities{}
	if provider, ok := runtime.(llm.NativeToolCapabilityProvider); ok && provider != nil {
		caps = provider.NativeToolCapabilities()
	}
	if caps.WebSearch {
		return nil, nil
	}
	if _, ok := semanticview.PolicyValueForFlow(source, "", "web_search_provider"); !ok {
		return []error{fmt.Errorf("native_tools.web_search is enabled but policy.web_search_provider is not configured")}, nil
	}
	cfg, err := resolveWebSearchProviderConfigFromSource(source)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.CredentialsKey) == "" || store == nil {
		return nil, nil
	}
	_, ok, err := store.Get(ctx, cfg.CredentialsKey)
	if err != nil {
		return nil, err
	}
	if ok {
		return nil, nil
	}
	return []error{fmt.Errorf("web_search provider credential %q is not present in the credential store", cfg.CredentialsKey)}, nil
}
