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
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const Category = "provider_connector"

type RequirementStatus struct {
	Name  string
	Bound bool
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
		surface, err := surfaceForTool(ctx, source, name, tool, store)
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
	if len(tool.Credentials) == 0 {
		errs = append(errs, fmt.Errorf("%s must declare at least one static credential binding", context))
	}
	if tool.ManagedCredential != nil {
		errs = append(errs, fmt.Errorf("%s uses managed_credential; managed credential connector execution is split", context))
	}
	if len(tool.ResponseMapping) > 0 {
		errs = append(errs, fmt.Errorf("%s uses response_mapping; connector activity response mapping is split", context))
	}
	if strings.TrimSpace(tool.RateLimit) != "" || strings.TrimSpace(tool.RateLimitMaxWait) != "" {
		errs = append(errs, fmt.Errorf("%s uses rate_limit; connector activity rate-limit admission is split", context))
	}
	return errs
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

func surfaceForTool(ctx context.Context, source semanticview.Source, toolID string, tool runtimecontracts.ToolSchemaEntry, store runtimecredentials.Store) (Surface, error) {
	provider, action, _ := splitToolID(toolID)
	host := ""
	if tool.HTTP != nil {
		parsed, err := url.Parse(strings.TrimSpace(tool.HTTP.URL))
		if err == nil {
			host = strings.TrimSpace(parsed.Host)
		}
	}
	requires := make([]RequirementStatus, 0, len(tool.Credentials))
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
		if store != nil && storeKey != "" {
			_, ok, err := store.Get(ctx, storeKey)
			if err != nil {
				return Surface{}, err
			}
			bound = ok
		}
		requires = append(requires, RequirementStatus{Name: key, Bound: bound})
	}
	sort.Slice(requires, func(i, j int) bool {
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
