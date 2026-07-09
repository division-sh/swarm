package providerconnectors

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const Category = "provider_connector"

type RequirementStatus struct {
	Kind                string
	Name                string
	Status              string
	Bound               bool
	GrantType           string
	Scopes              []string
	GrantModel          string
	TokenRequest        managedcredentialmodel.TokenRequestProfile
	InstallationIDInput string
}

type Surface struct {
	ToolID       string
	Provider     string
	Action       string
	EndpointHost string
	EffectClass  string
	Can          []string
	Cannot       []string
	Requires     []RequirementStatus
}

type SurfaceOptions struct {
	StaticCredentials  runtimecredentials.Store
	ManagedCredentials runtimemanagedcredentials.Store
}

func ValidateSource(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	tools := source.ToolEntries()
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, strings.TrimSpace(name))
	}
	sort.Strings(names)
	var errs []error
	for _, name := range names {
		tool := tools[name]
		if !isProviderConnector(tool) {
			continue
		}
		errs = append(errs, validateTool(name, tool)...)
	}
	errs = append(errs, validateProviderConnectorAgentExposure(source)...)
	return errs
}

func Surfaces(ctx context.Context, source semanticview.Source, store runtimecredentials.Store) ([]Surface, error) {
	return SurfacesWithOptions(ctx, source, SurfaceOptions{StaticCredentials: store})
}

func SurfacesWithOptions(ctx context.Context, source semanticview.Source, opts SurfaceOptions) ([]Surface, error) {
	if source == nil {
		return nil, nil
	}
	tools := source.ToolEntries()
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, strings.TrimSpace(name))
	}
	sort.Strings(names)
	out := make([]Surface, 0)
	for _, name := range names {
		tool := tools[name]
		if !isProviderConnector(tool) {
			continue
		}
		surface, err := surfaceForTool(ctx, source, name, tool, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, surface)
	}
	return out, nil
}

func isProviderConnector(tool runtimecontracts.ToolSchemaEntry) bool {
	return strings.EqualFold(strings.TrimSpace(tool.Category), Category)
}

func validateTool(toolID string, tool runtimecontracts.ToolSchemaEntry) []error {
	context := fmt.Sprintf("provider connector tool %q", strings.TrimSpace(toolID))
	var errs []error
	provider, action, ok := splitToolID(toolID)
	if !ok {
		errs = append(errs, fmt.Errorf("%s must use <provider>.<action> id form", context))
	}
	if provider == "" || action == "" {
		errs = append(errs, fmt.Errorf("%s must declare non-empty provider and action identity", context))
	}
	handlerType := strings.TrimSpace(strings.ToLower(tool.HandlerType))
	if handlerType != "" && handlerType != "http" {
		errs = append(errs, fmt.Errorf("%s handler_type %q is not supported; provider connectors use authored HTTP tools", context, tool.HandlerType))
	}
	if tool.HTTP == nil {
		errs = append(errs, fmt.Errorf("%s is missing http block", context))
	} else {
		if strings.TrimSpace(tool.HTTP.Method) == "" {
			errs = append(errs, fmt.Errorf("%s must declare http.method explicitly", context))
		}
		if strings.TrimSpace(tool.HTTP.URL) == "" {
			errs = append(errs, fmt.Errorf("%s must declare http.url", context))
		}
	}
	effectClass := runtimecontracts.NormalizeActivityEffectClass(tool.EffectClass)
	if effectClass != runtimecontracts.ActivityEffectClassNonIdempotentWrite {
		errs = append(errs, fmt.Errorf("%s effect_class must be non_idempotent_write for the Stage 1 connector proof", context))
	}
	hasStaticCredentials := len(tool.Credentials) > 0
	hasManagedCredential := tool.ManagedCredential != nil
	if hasStaticCredentials && hasManagedCredential {
		errs = append(errs, fmt.Errorf("%s must not declare both static credentials and managed_credential; connector auth has one authoritative credential mode", context))
	} else if !hasStaticCredentials && !hasManagedCredential {
		errs = append(errs, fmt.Errorf("%s must declare exactly one credential binding mode: static credentials or managed_credential", context))
	}
	if hasManagedCredential {
		key := strings.TrimSpace(tool.ManagedCredential.Key)
		if key == "" {
			errs = append(errs, fmt.Errorf("%s managed_credential.key is required", context))
		}
		if err := runtimemanagedcredentials.ValidateRequiredGrantType(tool.ManagedCredential.GrantType); err != nil {
			errs = append(errs, fmt.Errorf("%s managed_credential.%s", context, err.Error()))
		}
		grantType := runtimemanagedcredentials.NormalizeGrantType(tool.ManagedCredential.GrantType)
		installationIDInput := strings.TrimSpace(tool.ManagedCredential.InstallationIDInput)
		if grantType == runtimemanagedcredentials.GrantGitHubAppInstallation && installationIDInput == "" {
			errs = append(errs, fmt.Errorf("%s managed_credential.installation_id_input is required for grant_type %s", context, grantType))
		}
		if installationIDInput != "" && grantType != runtimemanagedcredentials.GrantGitHubAppInstallation {
			errs = append(errs, fmt.Errorf("%s managed_credential.installation_id_input requires grant_type %s", context, runtimemanagedcredentials.GrantGitHubAppInstallation))
		}
		if err := managedcredentialmodel.ValidateGrantModel(tool.ManagedCredential.GrantModel); err != nil {
			errs = append(errs, fmt.Errorf("%s managed_credential.%s", context, err.Error()))
		}
		if err := managedcredentialmodel.ValidateTokenRequestProfile(tool.ManagedCredential.TokenRequest); err != nil {
			errs = append(errs, fmt.Errorf("%s managed_credential.%s", context, err.Error()))
		}
	}
	if len(tool.ResponseMapping) > 0 {
		errs = append(errs, fmt.Errorf("%s uses response_mapping; connector activity response mapping is split", context))
	}
	if tool.ResponseSuccess != nil {
		errs = append(errs, validateResponseSuccess(context, *tool.ResponseSuccess)...)
	}
	if required, ok := requiredResponseSuccessForProviderAction(provider, action); ok {
		if tool.ResponseSuccess == nil {
			errs = append(errs, fmt.Errorf("%s must declare response_success %s == %s; this provider action can return HTTP 2xx for provider-level failure", context, required.Path, asConnectorScalar(required.Equals)))
		} else if !responseSuccessMatches(*tool.ResponseSuccess, required) {
			errs = append(errs, fmt.Errorf("%s response_success must be %s == %s for this provider action", context, required.Path, asConnectorScalar(required.Equals)))
		}
	}
	if strings.TrimSpace(tool.RateLimit) != "" || strings.TrimSpace(tool.RateLimitMaxWait) != "" {
		errs = append(errs, fmt.Errorf("%s uses rate_limit; connector activity rate-limit admission is split", context))
	}
	return errs
}

func validateResponseSuccess(context string, check runtimecontracts.HTTPResponseSuccess) []error {
	path := strings.TrimSpace(check.Path)
	var errs []error
	if path == "" {
		errs = append(errs, fmt.Errorf("%s response_success.path is required", context))
	} else if !strings.HasPrefix(path, "response.") {
		errs = append(errs, fmt.Errorf("%s response_success.path must start with response.", context))
	}
	if check.Equals == nil {
		errs = append(errs, fmt.Errorf("%s response_success.equals is required", context))
	} else if !responseSuccessScalar(check.Equals) {
		errs = append(errs, fmt.Errorf("%s response_success.equals must be a scalar value", context))
	}
	return errs
}

func responseSuccessScalar(value any) bool {
	switch value.(type) {
	case string, bool, int, int64, float64, float32, uint, uint64:
		return true
	default:
		return false
	}
}

func requiredResponseSuccessForProviderAction(provider, action string) (runtimecontracts.HTTPResponseSuccess, bool) {
	switch normalizeToken(provider) + "." + normalizeToken(action) {
	case "slack.post_message":
		return runtimecontracts.HTTPResponseSuccess{Path: "response.body.ok", Equals: true}, true
	default:
		return runtimecontracts.HTTPResponseSuccess{}, false
	}
}

func responseSuccessMatches(got, want runtimecontracts.HTTPResponseSuccess) bool {
	return strings.TrimSpace(got.Path) == strings.TrimSpace(want.Path) && responseSuccessValuesEqual(got.Equals, want.Equals)
}

func responseSuccessValuesEqual(got, want any) bool {
	switch wantTyped := want.(type) {
	case bool:
		gotTyped, ok := got.(bool)
		return ok && gotTyped == wantTyped
	case string:
		gotTyped, ok := got.(string)
		return ok && gotTyped == wantTyped
	case int:
		gotFloat, ok := responseSuccessFloat(got)
		return ok && gotFloat == float64(wantTyped)
	case int64:
		gotFloat, ok := responseSuccessFloat(got)
		return ok && gotFloat == float64(wantTyped)
	case float64:
		gotFloat, ok := responseSuccessFloat(got)
		return ok && gotFloat == wantTyped
	case float32:
		gotFloat, ok := responseSuccessFloat(got)
		return ok && gotFloat == float64(wantTyped)
	default:
		return fmt.Sprint(got) == fmt.Sprint(want)
	}
}

func responseSuccessFloat(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	default:
		return 0, false
	}
}

func asConnectorScalar(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(typed)
	}
}

func validateProviderConnectorAgentExposure(source semanticview.Source) []error {
	if source == nil {
		return nil
	}
	agents := source.AgentEntries()
	agentIDs := make([]string, 0, len(agents))
	for agentID := range agents {
		agentIDs = append(agentIDs, strings.TrimSpace(agentID))
	}
	sort.Strings(agentIDs)
	var errs []error
	for _, agentID := range agentIDs {
		entry := agents[agentID]
		for _, toolID := range entry.ConfiguredTools() {
			toolID = strings.TrimSpace(toolID)
			if toolID == "" {
				continue
			}
			tool, ok := source.ToolEntryForAgent(agentID, toolID)
			if !ok || !isProviderConnector(tool) {
				continue
			}
			errs = append(errs, fmt.Errorf("agent %q must not expose provider connector tool %q directly; connectors execute through platform.activity_requested and activity_attempts", agentID, toolID))
		}
	}
	return errs
}

func surfaceForTool(ctx context.Context, source semanticview.Source, toolID string, tool runtimecontracts.ToolSchemaEntry, opts SurfaceOptions) (Surface, error) {
	provider, action, _ := splitToolID(toolID)
	host := ""
	if tool.HTTP != nil {
		parsed, err := url.Parse(strings.TrimSpace(tool.HTTP.URL))
		if err == nil {
			host = strings.TrimSpace(parsed.Host)
		}
	}
	requires := make([]RequirementStatus, 0, len(tool.Credentials)+1)
	flowID, err := connectorToolFlowID(source, toolID, tool)
	if err != nil {
		return Surface{}, err
	}
	for _, key := range tool.Credentials {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		storeKey := key
		if resolved, mapped := semanticview.CredentialStoreKeyForFlow(source, flowID, key); mapped {
			storeKey = strings.TrimSpace(resolved)
		}
		bound := false
		if opts.StaticCredentials != nil && storeKey != "" {
			_, ok, err := opts.StaticCredentials.Get(ctx, storeKey)
			if err != nil {
				return Surface{}, err
			}
			bound = ok
		}
		status := "UNBOUND"
		if bound {
			status = "BOUND"
		}
		requires = append(requires, RequirementStatus{Kind: "secret", Name: key, Status: status, Bound: bound})
	}
	if tool.ManagedCredential != nil {
		key := strings.TrimSpace(tool.ManagedCredential.Key)
		if key != "" {
			requiredGrantType := runtimemanagedcredentials.NormalizeGrantType(tool.ManagedCredential.GrantType)
			requiredGrantModel := managedcredentialmodel.NormalizeGrantModel(tool.ManagedCredential.GrantModel)
			requiredTokenRequest := managedcredentialmodel.NormalizeTokenRequestProfile(tool.ManagedCredential.TokenRequest)
			storeKey := key
			if resolved, mapped := semanticview.CredentialStoreKeyForFlow(source, flowID, key); mapped {
				storeKey = strings.TrimSpace(resolved)
			}
			status := "UNBOUND"
			bound := false
			if opts.ManagedCredentials != nil && strings.TrimSpace(storeKey) != "" {
				record, ok, err := opts.ManagedCredentials.Get(ctx, storeKey)
				if err != nil {
					return Surface{}, err
				}
				if ok {
					desc := record.Descriptor()
					status = strings.ToUpper(strings.TrimSpace(desc.Status))
					bound = desc.Status == runtimemanagedcredentials.StatusConnected
					if bound {
						if err := runtimemanagedcredentials.GrantTypeCovers(desc.GrantType, requiredGrantType); err != nil {
							status = strings.ToUpper(runtimemanagedcredentials.StatusScopeInsufficient)
							bound = false
						}
					}
					if bound {
						if err := managedcredentialmodel.GrantModelCovers(desc.GrantModel, requiredGrantModel); err != nil {
							status = strings.ToUpper(runtimemanagedcredentials.StatusScopeInsufficient)
							bound = false
						}
					}
					if bound {
						if err := managedcredentialmodel.TokenRequestProfileCovers(desc.TokenRequest, requiredTokenRequest); err != nil {
							status = strings.ToUpper(runtimemanagedcredentials.StatusScopeInsufficient)
							bound = false
						}
					}
					if bound && !managedCredentialScopesCover(desc.Scopes, tool.ManagedCredential.Scopes) {
						status = strings.ToUpper(runtimemanagedcredentials.StatusScopeInsufficient)
						bound = false
					}
				}
			}
			if strings.TrimSpace(status) == "" {
				status = "UNBOUND"
			}
			requires = append(requires, RequirementStatus{
				Kind:                "managed_credential",
				Name:                key,
				Status:              status,
				Bound:               bound,
				GrantType:           requiredGrantType,
				Scopes:              append([]string{}, tool.ManagedCredential.Scopes...),
				GrantModel:          requiredGrantModel,
				TokenRequest:        requiredTokenRequest,
				InstallationIDInput: strings.TrimSpace(tool.ManagedCredential.InstallationIDInput),
			})
		}
	}
	sort.Slice(requires, func(i, j int) bool {
		if requires[i].Kind != requires[j].Kind {
			return requires[i].Kind < requires[j].Kind
		}
		return requires[i].Name < requires[j].Name
	})
	actionLabel := connectorActionLabel(provider, action, tool)
	can := []string{
		strings.TrimSpace(actionLabel + endpointSuffix(host)),
		"lower through platform.activity_requested",
		"journal non-idempotent attempts in activity_attempts",
	}
	return Surface{
		ToolID:       strings.TrimSpace(toolID),
		Provider:     provider,
		Action:       action,
		EndpointHost: host,
		EffectClass:  string(runtimecontracts.NormalizeActivityEffectClass(tool.EffectClass)),
		Can:          can,
		Cannot: []string{
			"bypass activity_attempts",
			"retry non_idempotent_write automatically",
			"expose credential values",
		},
		Requires: requires,
	}, nil
}

func managedCredentialScopesCover(actual, required []string) bool {
	if len(required) == 0 {
		return true
	}
	have := map[string]struct{}{}
	for _, scope := range normalizeStringSet(actual) {
		have[scope] = struct{}{}
	}
	for _, scope := range normalizeStringSet(required) {
		if _, ok := have[scope]; !ok {
			return false
		}
	}
	return true
}

func normalizeStringSet(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func connectorToolFlowID(source semanticview.Source, toolID string, tool runtimecontracts.ToolSchemaEntry) (string, error) {
	if source == nil {
		return "", nil
	}
	toolID = strings.TrimSpace(toolID)
	flowIDs := map[string]struct{}{}
	for _, scope := range source.ProjectScopes() {
		if scopedToolMatches(scope.Tools, toolID, tool) {
			if flowID := strings.TrimSpace(scope.OwningFlowID); flowID != "" {
				flowIDs[flowID] = struct{}{}
			}
		}
	}
	for _, scope := range source.FlowScopes() {
		if scopedToolMatches(scope.Tools, toolID, tool) {
			if flowID := strings.TrimSpace(scope.ID); flowID != "" {
				flowIDs[flowID] = struct{}{}
			}
		}
	}
	if len(flowIDs) == 0 {
		return "", nil
	}
	if len(flowIDs) > 1 {
		ids := make([]string, 0, len(flowIDs))
		for id := range flowIDs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return "", fmt.Errorf("provider connector tool %q has ambiguous flow credential scopes: %s", toolID, strings.Join(ids, ", "))
	}
	for id := range flowIDs {
		return id, nil
	}
	return "", nil
}

func scopedToolMatches(tools map[string]runtimecontracts.ToolSchemaEntry, toolID string, tool runtimecontracts.ToolSchemaEntry) bool {
	scoped, ok := tools[strings.TrimSpace(toolID)]
	return ok && reflect.DeepEqual(scoped, tool)
}

func splitToolID(toolID string) (string, string, bool) {
	parts := strings.Split(strings.TrimSpace(toolID), ".")
	if len(parts) != 2 {
		return "", "", false
	}
	provider := normalizeToken(parts[0])
	action := normalizeToken(parts[1])
	return provider, action, provider != "" && action != ""
}

func normalizeToken(raw string) string {
	raw = strings.TrimSpace(strings.ToLower(raw))
	raw = strings.ReplaceAll(raw, "-", "_")
	raw = strings.ReplaceAll(raw, " ", "_")
	return strings.Trim(raw, "_")
}

func connectorActionLabel(provider, action string, tool runtimecontracts.ToolSchemaEntry) string {
	if label := normalizeDisplayLabel(tool.Description); label != "" {
		return label
	}
	actionLabel := strings.ReplaceAll(action, "_", " ")
	if providerLabel := titleToken(provider); providerLabel != "" {
		actionLabel = strings.TrimSpace(actionLabel + " via " + providerLabel)
	}
	return actionLabel
}

func normalizeDisplayLabel(raw string) string {
	label := strings.Join(strings.Fields(strings.TrimSpace(raw)), " ")
	return strings.TrimSpace(strings.TrimRight(label, "."))
}

func titleToken(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	return strings.ToUpper(raw[:1]) + raw[1:]
}

func endpointSuffix(host string) string {
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	return " via " + host
}
