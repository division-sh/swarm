package tools

import (
	"context"
	"fmt"
	"sort"
	"strings"

	runtimecredentials "swarm/internal/runtime/credentials"
	llm "swarm/internal/runtime/llm"
	"swarm/internal/runtime/semanticview"
)

func ValidateNativeToolBootConfig(ctx context.Context, source semanticview.Source, store runtimecredentials.Store, runtime llm.Runtime) ([]error, error) {
	if source == nil {
		return nil, nil
	}
	enabled := enabledNativeCapabilities(source)
	if len(enabled) == 0 {
		return nil, nil
	}
	provider, ok := runtime.(llm.NativeToolCapabilityProvider)
	if !ok || provider == nil {
		return nil, fmt.Errorf("native tools are enabled but runtime does not expose native tool capabilities")
	}
	caps := provider.NativeToolCapabilities()
	unsupported := make([]string, 0, len(enabled))
	for _, capability := range enabled {
		if nativeToolCapabilitySupported(caps, capability) {
			continue
		}
		unsupported = append(unsupported, capability)
	}
	if len(unsupported) == 0 {
		return nil, nil
	}
	sort.Strings(unsupported)
	parts := make([]string, 0, len(unsupported))
	for _, capability := range unsupported {
		parts = append(parts, "native_tools."+capability)
	}
	return nil, fmt.Errorf("%s enabled but runtime does not support provider-native capability", strings.Join(parts, ", "))
}

func enabledNativeCapabilities(source semanticview.Source) []string {
	if source == nil {
		return nil
	}
	seen := map[string]struct{}{}
	for _, agent := range source.AgentEntries() {
		for capability, raw := range agent.NativeTools {
			capability = strings.TrimSpace(capability)
			flag, isBool := raw.(bool)
			if capability == "" || !isBool || !flag {
				continue
			}
			seen[capability] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for capability := range seen {
		out = append(out, capability)
	}
	sort.Strings(out)
	return out
}

func nativeToolCapabilitySupported(caps llm.NativeToolCapabilities, capability string) bool {
	switch strings.TrimSpace(capability) {
	case "bash":
		return caps.Bash
	case "web_search":
		return caps.WebSearch
	case "file_io":
		return caps.FileIO
	default:
		return false
	}
}
