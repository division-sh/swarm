package tools

import (
	"fmt"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
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
		if strings.HasPrefix(name, runtimecontracts.PrivateChannelActivityPrefix) {
			return warnings, fmt.Errorf("tool %s uses reserved private channel activity namespace %s*", name, runtimecontracts.PrivateChannelActivityPrefix)
		}
		managedCredential, hasManagedCredential := entry.ManagedCredential()
		httpSpec, hasHTTP := entry.HTTP()
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
			if strings.TrimSpace(httpSpec.Method) == "" {
				return warnings, fmt.Errorf("tool %s http.method is required", name)
			}
			if strings.TrimSpace(httpSpec.URL) == "" {
				return warnings, fmt.Errorf("tool %s http.url is required", name)
			}
			if hasManagedCredential && strings.TrimSpace(managedCredential.Key) == "" {
				return warnings, fmt.Errorf("tool %s managed_credential.key is required", name)
			}
			if hasManagedCredential && strings.TrimSpace(managedCredential.Header) == "" && strings.TrimSpace(managedCredential.Prefix) != "" {
				return warnings, fmt.Errorf("tool %s managed_credential.header is required when prefix is set", name)
			}
			if hasManagedCredential {
				if err := runtimemanagedcredentials.ValidateRequiredGrantType(managedCredential.GrantType); err != nil {
					return warnings, fmt.Errorf("tool %s managed_credential.%s", name, err.Error())
				}
				grantType := runtimemanagedcredentials.NormalizeGrantType(managedCredential.GrantType)
				installationIDInput := strings.TrimSpace(managedCredential.InstallationIDInput)
				if grantType == runtimemanagedcredentials.GrantGitHubAppInstallation && installationIDInput == "" {
					return warnings, fmt.Errorf("tool %s managed_credential.installation_id_input is required for grant_type %s", name, grantType)
				}
				if installationIDInput != "" && grantType != runtimemanagedcredentials.GrantGitHubAppInstallation {
					return warnings, fmt.Errorf("tool %s managed_credential.installation_id_input requires grant_type %s", name, runtimemanagedcredentials.GrantGitHubAppInstallation)
				}
				if err := managedcredentialmodel.ValidateGrantModel(managedCredential.GrantModel); err != nil {
					return warnings, fmt.Errorf("tool %s managed_credential.%s", name, err.Error())
				}
				if err := managedcredentialmodel.ValidateTokenRequestProfile(managedCredential.TokenRequest); err != nil {
					return warnings, fmt.Errorf("tool %s managed_credential.%s", name, err.Error())
				}
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
