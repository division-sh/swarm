package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func ValidateToolImplementations(source semanticview.Source) ([]error, error) {
	if source == nil {
		return nil, nil
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
		rawType := strings.ToLower(strings.TrimSpace(entry.HandlerType))
		normalized := normalizeImplementationClass(name, entry)
		switch rawType {
		case "workflow_registered", "api_call":
			warnings = append(warnings, fmt.Errorf("tool %s uses deprecated handler_type %s; migrate to handler_type: http or mcp", name, rawType))
		}
		switch normalized {
		case implementationPlatformBuiltin:
			if entry.ManagedCredential != nil {
				return warnings, fmt.Errorf("tool %s managed_credential is only supported for handler_type http", name)
			}
			if _, ok := supportedRuntimeToolNames[name]; !ok {
				return warnings, fmt.Errorf("tool %s declares handler_type platform_builtin but is not shipped by the generic runtime", name)
			}
		case implementationHTTP:
			if entry.HTTP == nil {
				return warnings, fmt.Errorf("tool %s resolves as http but has no http block", name)
			}
			if strings.TrimSpace(entry.HTTP.Method) == "" {
				return warnings, fmt.Errorf("tool %s http.method is required", name)
			}
			if strings.TrimSpace(entry.HTTP.URL) == "" {
				return warnings, fmt.Errorf("tool %s http.url is required", name)
			}
			if entry.ManagedCredential != nil && strings.TrimSpace(entry.ManagedCredential.Key) == "" {
				return warnings, fmt.Errorf("tool %s managed_credential.key is required", name)
			}
			if entry.ManagedCredential != nil && strings.TrimSpace(entry.ManagedCredential.Header) == "" && strings.TrimSpace(entry.ManagedCredential.Prefix) != "" {
				return warnings, fmt.Errorf("tool %s managed_credential.header is required when prefix is set", name)
			}
			if entry.ManagedCredential != nil {
				if err := runtimemanagedcredentials.ValidateRequiredGrantType(entry.ManagedCredential.GrantType); err != nil {
					return warnings, fmt.Errorf("tool %s managed_credential.%s", name, err.Error())
				}
				grantType := runtimemanagedcredentials.NormalizeGrantType(entry.ManagedCredential.GrantType)
				installationIDInput := strings.TrimSpace(entry.ManagedCredential.InstallationIDInput)
				if grantType == runtimemanagedcredentials.GrantGitHubAppInstallation && installationIDInput == "" {
					return warnings, fmt.Errorf("tool %s managed_credential.installation_id_input is required for grant_type %s", name, grantType)
				}
				if installationIDInput != "" && grantType != runtimemanagedcredentials.GrantGitHubAppInstallation {
					return warnings, fmt.Errorf("tool %s managed_credential.installation_id_input requires grant_type %s", name, runtimemanagedcredentials.GrantGitHubAppInstallation)
				}
				if err := managedcredentialmodel.ValidateGrantModel(entry.ManagedCredential.GrantModel); err != nil {
					return warnings, fmt.Errorf("tool %s managed_credential.%s", name, err.Error())
				}
				if err := managedcredentialmodel.ValidateTokenRequestProfile(entry.ManagedCredential.TokenRequest); err != nil {
					return warnings, fmt.Errorf("tool %s managed_credential.%s", name, err.Error())
				}
			}
		case implementationMCP:
			if entry.ManagedCredential != nil {
				return warnings, fmt.Errorf("tool %s managed_credential is only supported for handler_type http", name)
			}
			if !strings.Contains(name, ".") {
				warnings = append(warnings, fmt.Errorf("tool %s uses handler_type mcp but is not prefixed with a server namespace", name))
			}
		case implementationChannel:
			if !strings.HasPrefix(name, "channel.") || strings.TrimSpace(entry.Category) != "channel_operation" {
				return warnings, fmt.Errorf("tool %s uses handler_type channel outside the compiled channel runtime surface", name)
			}
			if entry.HTTP != nil || entry.ManagedCredential != nil || len(entry.Credentials) != 0 {
				return warnings, fmt.Errorf("tool %s channel runtime surface must not expose connector transport or credentials", name)
			}
		case "":
			if rawType == "workflow_registered" || rawType == "api_call" {
				warnings = append(warnings, fmt.Errorf("tool %s uses deprecated handler_type %s with no http block; tool is ignored until migrated to handler_type: http or mcp", name, rawType))
				continue
			}
			if rawType == "" {
				warnings = append(warnings, fmt.Errorf("tool %s has no executable implementation in the generic runtime; provide handler_type: http with an http block or expose it via mcp", name))
				continue
			}
			return warnings, fmt.Errorf("tool %s has unsupported handler_type %q", name, strings.TrimSpace(entry.HandlerType))
		}
	}
	return warnings, nil
}
