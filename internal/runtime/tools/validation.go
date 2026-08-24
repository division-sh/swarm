package tools

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func ValidateToolImplementations(source semanticview.Source) ([]error, error) {
	if source == nil {
		return nil, nil
	}
	authoredErrors := append(ValidateRetiredDynamicAgentToolReferences(source), ValidateHITLIdentityLifecycleReferences(source)...)
	if len(authoredErrors) > 0 {
		return nil, errors.Join(authoredErrors...)
	}
	entries := source.ToolEntries()
	names := make([]string, 0, len(entries))
	for name := range entries {
		name = strings.TrimSpace(name)
		if name != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	warnings := make([]error, 0)
	for _, name := range names {
		entry := entries[name]
		if strings.HasPrefix(name, runtimecontracts.PrivateChannelActivityPrefix) {
			return warnings, fmt.Errorf("tool %s uses reserved private channel activity namespace %s*", name, runtimecontracts.PrivateChannelActivityPrefix)
		}
		_, hasManagedCredential := entry.ManagedCredentialExecution()
		_, hasHTTP := entry.HTTPExecution()
		switch entry.Handler() {
		case runtimecontracts.ToolHandlerPlatformBuiltin:
			if hasManagedCredential {
				return warnings, fmt.Errorf("tool %s managed_credential is only supported for handler_type http", name)
			}
			if _, ok := supportedRuntimeToolNames[name]; !ok {
				return warnings, fmt.Errorf("tool %s declares handler_type platform_builtin but is not shipped by the generic runtime", name)
			}
		case runtimecontracts.ToolHandlerHTTP:
			if !hasHTTP {
				return warnings, fmt.Errorf("tool %s resolves as http but has no http block", name)
			}
		case runtimecontracts.ToolHandlerMCP:
			if hasManagedCredential {
				return warnings, fmt.Errorf("tool %s managed_credential is only supported for handler_type http", name)
			}
			if !strings.Contains(name, ".") {
				warnings = append(warnings, fmt.Errorf("tool %s uses handler_type mcp but is not prefixed with a server namespace", name))
			}
		case runtimecontracts.ToolHandlerChannel:
			if !strings.HasPrefix(name, "channel.") || entry.Category() != runtimecontracts.ToolCategoryChannelOperation {
				return warnings, fmt.Errorf("tool %s uses handler_type channel outside the compiled channel runtime surface", name)
			}
			if hasHTTP || hasManagedCredential || len(entry.Credentials()) != 0 {
				return warnings, fmt.Errorf("tool %s channel runtime surface must not expose connector transport or credentials", name)
			}
		case runtimecontracts.ToolHandlerUnspecified:
			warnings = append(warnings, fmt.Errorf("tool %s has no executable implementation in the generic runtime; provide handler_type: http with an http block or expose it via mcp", name))
		default:
			return warnings, fmt.Errorf("tool %s has unsupported admitted handler", name)
		}
	}
	return warnings, nil
}
