package providerconnectors

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/httpresponsesuccess"
	runtimemanagedcredentials "github.com/division-sh/swarm/internal/runtime/managedcredentials"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const Category = "provider_connector"

type GenerationSurface struct {
	GeneratorVersion string
	SourcePath       string
	SourceSHA256     string
	ProfilePath      string
	ProfileSHA256    string
	ManifestSHA256   string
	OperationID      string
	Permissions      []GenerationPermission
	FixtureID        string
	FixtureStatus    string
	ReviewStatus     string
}

type CapabilityOptions struct {
	StaticCredentials  runtimecredentials.Store
	ManagedCredentials runtimemanagedcredentials.Store
	Registry           *PackRegistry
	IncludeInstalled   bool
}

// RequirementsForTool evaluates deployment-owned connector requirements for a
// compiled caller that is not flow-local, such as an outbound channel binding.
func RequirementsForTool(ctx context.Context, toolID string, tool runtimecontracts.ToolSchemaEntry, opts CapabilityOptions) ([]packs.Requirement, error) {
	requires := make([]packs.Requirement, 0, len(tool.Credentials)+1)
	for _, raw := range tool.Credentials {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		descriptor, err := runtimecredentials.Describe(ctx, opts.StaticCredentials, nil, key)
		if err != nil {
			return nil, err
		}
		status := packs.RequirementStatusUnbound
		if descriptor.Present {
			status = packs.RequirementStatusBound
		}
		requires = append(requires, packs.RequirementWithStatus(packs.RequirementSecret, key, "deployment", status, descriptor.Source))
	}
	if tool.ManagedCredential == nil {
		return requires, nil
	}
	key := strings.TrimSpace(tool.ManagedCredential.Key)
	if key == "" {
		return nil, fmt.Errorf("provider connector tool %q managed credential key is required", toolID)
	}
	descriptors, err := runtimemanagedcredentials.ListRequirementDescriptors(ctx, opts.ManagedCredentials, nil)
	if err != nil {
		return nil, err
	}
	descriptor := runtimemanagedcredentials.RequirementDescriptor{Descriptor: runtimemanagedcredentials.Descriptor{Key: key, Status: runtimemanagedcredentials.StatusUnconnected}}
	for _, candidate := range descriptors {
		if strings.TrimSpace(candidate.Key) == key {
			descriptor = candidate
			break
		}
	}
	required := runtimemanagedcredentials.Requirement{
		Kind: "tool", Name: strings.TrimSpace(toolID),
		GrantType:           runtimemanagedcredentials.NormalizeGrantType(tool.ManagedCredential.GrantType),
		Scopes:              append([]string(nil), tool.ManagedCredential.Scopes...),
		GrantModel:          managedcredentialmodel.NormalizeGrantModel(tool.ManagedCredential.GrantModel),
		TokenRequest:        managedcredentialmodel.NormalizeTokenRequestProfile(tool.ManagedCredential.TokenRequest),
		InstallationIDInput: strings.TrimSpace(tool.ManagedCredential.InstallationIDInput),
	}
	evaluation := runtimemanagedcredentials.EvaluateRequirement(descriptor, required)
	status := strings.ToUpper(strings.TrimSpace(evaluation.Descriptor.Status))
	if status == "" {
		status = packs.RequirementStatusUnconnected
	}
	requirement := packs.RequirementWithStatus(packs.RequirementManagedCredential, key, "deployment", status, "managed_credential_store")
	requirement.GrantType = required.GrantType
	requirement.Scopes = normalizeStringSet(required.Scopes)
	requirement.GrantModel = required.GrantModel
	profile := tokenRequestFields(required.TokenRequest)
	requirement.TokenRequest = &profile
	requirement.InstallationIDInput = required.InstallationIDInput
	return append(requires, requirement), nil
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

func HasEffectiveConnectors(source semanticview.Source) bool {
	if source == nil {
		return false
	}
	for _, tool := range source.ToolEntries() {
		if isProviderConnector(tool) {
			return true
		}
	}
	return false
}

func CapabilitySubjects(ctx context.Context, source semanticview.Source, opts CapabilityOptions) ([]packs.Subject, error) {
	if source == nil {
		return nil, nil
	}
	if opts.Registry == nil {
		opts.Registry = DefaultPackRegistry()
	}
	tools := source.ToolEntries()
	names := make([]string, 0, len(tools))
	for name := range tools {
		names = append(names, strings.TrimSpace(name))
	}
	sort.Strings(names)
	out := make([]packs.Subject, 0)
	effective := map[string]struct{}{}
	for _, name := range names {
		tool := tools[name]
		if !isProviderConnector(tool) {
			continue
		}
		surface, err := capabilitySubjectForTool(ctx, source, name, tool, opts)
		if err != nil {
			return nil, err
		}
		effective[name] = struct{}{}
		out = append(out, surface)
	}
	if opts.IncludeInstalled {
		for _, installed := range opts.Registry.Inventory() {
			if _, exists := effective[installed.ToolID]; exists {
				continue
			}
			subject, err := availableCapabilitySubject(installed)
			if err != nil {
				return nil, err
			}
			out = append(out, subject)
		}
	}
	return packs.NormalizeSubjects(out)
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
	if tool.ResponseSuccess == nil {
		errs = append(errs, fmt.Errorf("%s must declare exactly one response_success policy", context))
	} else if err := httpresponsesuccess.Validate(*tool.ResponseSuccess); err != nil {
		errs = append(errs, fmt.Errorf("%s %s", context, err))
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

func capabilitySubjectForTool(ctx context.Context, source semanticview.Source, toolID string, tool runtimecontracts.ToolSchemaEntry, opts CapabilityOptions) (packs.Subject, error) {
	provider, action, _ := splitToolID(toolID)
	host := ""
	if tool.HTTP != nil {
		parsed, err := url.Parse(strings.TrimSpace(tool.HTTP.URL))
		if err == nil {
			host = strings.TrimSpace(parsed.Host)
		}
	}
	requires := make([]packs.Requirement, 0, len(tool.Credentials)+1)
	flowID, err := connectorToolFlowID(source, toolID, tool)
	if err != nil {
		return packs.Subject{}, err
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
		if storeKey == "" {
			return packs.Subject{}, fmt.Errorf("provider connector tool %q credential %q has no deployment binding", toolID, key)
		}
		descriptor, err := runtimecredentials.Describe(ctx, opts.StaticCredentials, source, storeKey)
		if err != nil {
			return packs.Subject{}, err
		}
		status := "UNBOUND"
		if descriptor.Present {
			status = "BOUND"
		}
		requires = append(requires, packs.RequirementWithStatus(packs.RequirementSecret, storeKey, "deployment", status, descriptor.Source))
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
			if storeKey == "" {
				return packs.Subject{}, fmt.Errorf("provider connector tool %q managed credential %q has no deployment binding", toolID, key)
			}
			descriptors, err := runtimemanagedcredentials.ListRequirementDescriptors(ctx, opts.ManagedCredentials, source)
			if err != nil {
				return packs.Subject{}, err
			}
			descriptor := runtimemanagedcredentials.RequirementDescriptor{Descriptor: runtimemanagedcredentials.Descriptor{Key: storeKey, Status: runtimemanagedcredentials.StatusUnconnected}}
			for _, candidate := range descriptors {
				if strings.TrimSpace(candidate.Key) == storeKey {
					descriptor = candidate
					break
				}
			}
			required := runtimemanagedcredentials.Requirement{
				Kind:                "tool",
				Name:                toolID,
				GrantType:           requiredGrantType,
				Scopes:              append([]string{}, tool.ManagedCredential.Scopes...),
				GrantModel:          requiredGrantModel,
				TokenRequest:        requiredTokenRequest,
				InstallationIDInput: strings.TrimSpace(tool.ManagedCredential.InstallationIDInput),
			}
			evaluation := runtimemanagedcredentials.EvaluateRequirement(descriptor, required)
			status := strings.ToUpper(strings.TrimSpace(evaluation.Descriptor.Status))
			if status == "" {
				status = "UNCONNECTED"
			}
			requirement := packs.RequirementWithStatus(packs.RequirementManagedCredential, storeKey, "deployment", status, "managed_credential_store")
			requirement.GrantType = requiredGrantType
			requirement.Scopes = normalizeStringSet(tool.ManagedCredential.Scopes)
			requirement.GrantModel = requiredGrantModel
			tokenRequest := tokenRequestFields(requiredTokenRequest)
			requirement.TokenRequest = &tokenRequest
			requirement.InstallationIDInput = strings.TrimSpace(tool.ManagedCredential.InstallationIDInput)
			requires = append(requires, requirement)
		}
	}
	actionLabel := connectorActionLabel(provider, action, tool)
	var generation *GenerationSurface
	type generationSource interface {
		ConnectorGenerationSurface(string) (GenerationSurface, bool)
	}
	if generated, ok := semanticview.SourceCapability[generationSource](source); ok {
		if evidence, exists := generated.ConnectorGenerationSurface(toolID); exists {
			copy := evidence
			generation = &copy
		}
	}
	sourceKind := "flow_local"
	provenance := ""
	sourcePath := ""
	type importedSource interface {
		ConnectorPackImportSource(string) (string, bool)
	}
	if imported, ok := semanticview.SourceCapability[importedSource](source); ok {
		if importSource, exists := imported.ConnectorPackImportSource(toolID); exists {
			sourceKind = "connector_pack_import"
			sourcePath = strings.TrimSpace(importSource)
			if loaded, found := opts.Registry.Lookup(provider, toolID); found {
				provenance = strings.TrimSpace(loaded.Envelope.Provenance.Source)
			}
		}
	}
	subject := packs.Subject{
		ID:            strings.TrimSpace(toolID),
		Kind:          packs.SubjectProviderConnector,
		Provider:      provider,
		Action:        action,
		Source:        sourceKind,
		Provenance:    provenance,
		SourcePath:    sourcePath,
		Applicability: "effective",
		Capabilities: []packs.Capability{
			{Code: packs.CapabilityCallProviderAction, Target: strings.TrimSpace(actionLabel + endpointSuffix(host))},
			{Code: packs.CapabilityLowerThroughActivity},
			{Code: packs.CapabilityJournalAttempts},
		},
		Requirements: requires,
		Evidence: []packs.Evidence{{Kind: "connector", Fields: map[string]string{
			"effect_class":  string(runtimecontracts.NormalizeActivityEffectClass(tool.EffectClass)),
			"endpoint_host": host,
		}}},
	}
	for _, code := range []string{packs.GuaranteeActivityJournal, packs.GuaranteeNoAutomaticWriteRetry, packs.GuaranteeCredentialRedaction} {
		guarantee, err := packs.NewGuarantee(code)
		if err != nil {
			return packs.Subject{}, err
		}
		subject.Guarantees = append(subject.Guarantees, guarantee)
	}
	if generation != nil {
		subject.Evidence = append(subject.Evidence, generationEvidence(*generation))
	}
	normalized, err := packs.NormalizeSubjects([]packs.Subject{subject})
	if err != nil {
		return packs.Subject{}, err
	}
	return normalized[0], nil
}

func availableCapabilitySubject(installed InstalledTool) (packs.Subject, error) {
	provider, action, _ := splitToolID(installed.ToolID)
	host := ""
	if installed.Tool.HTTP != nil {
		if parsed, err := url.Parse(strings.TrimSpace(installed.Tool.HTTP.URL)); err == nil {
			host = strings.TrimSpace(parsed.Host)
		}
	}
	subject := packs.Subject{
		ID:            strings.TrimSpace(installed.ToolID),
		Kind:          packs.SubjectProviderConnector,
		Provider:      provider,
		Action:        action,
		Source:        "connector_pack",
		Provenance:    strings.TrimSpace(installed.Pack.Envelope.Provenance.Source),
		SourcePath:    strings.TrimSpace(installed.Pack.Directory),
		Applicability: "installed",
		Capabilities: []packs.Capability{
			{Code: packs.CapabilityCallProviderAction, Target: strings.TrimSpace(connectorActionLabel(provider, action, installed.Tool) + endpointSuffix(host))},
			{Code: packs.CapabilityLowerThroughActivity},
			{Code: packs.CapabilityJournalAttempts},
		},
		Requirements: []packs.Requirement{packs.RequirementWithStatus(packs.RequirementImport, installed.ToolID, "package", "NOT_IMPORTED", "connector_pack_registry")},
	}
	for _, code := range []string{packs.GuaranteeActivityJournal, packs.GuaranteeNoAutomaticWriteRetry, packs.GuaranteeCredentialRedaction} {
		guarantee, err := packs.NewGuarantee(code)
		if err != nil {
			return packs.Subject{}, err
		}
		subject.Guarantees = append(subject.Guarantees, guarantee)
	}
	if generation, ok := GenerationSurfaceForPackTool(installed.Pack, installed.ToolID); ok {
		subject.Evidence = append(subject.Evidence, generationEvidence(generation))
	}
	normalized, err := packs.NormalizeSubjects([]packs.Subject{subject})
	if err != nil {
		return packs.Subject{}, err
	}
	return normalized[0], nil
}

func tokenRequestFields(profile managedcredentialmodel.TokenRequestProfile) packs.TokenRequestProfile {
	profile = managedcredentialmodel.NormalizeTokenRequestProfile(profile)
	headers := make(map[string]string, len(profile.StaticHeaders))
	for key, value := range profile.StaticHeaders {
		headers[key] = value
	}
	return packs.TokenRequestProfile{ClientAuth: profile.ClientAuth, Body: profile.Body, StaticHeaders: headers}
}

func generationEvidence(generation GenerationSurface) packs.Evidence {
	permissions := make([]string, 0, len(generation.Permissions))
	for _, permission := range generation.Permissions {
		permissions = append(permissions, strings.TrimSpace(permission.ID)+":"+strings.TrimSpace(permission.Note))
	}
	sort.Strings(permissions)
	return packs.Evidence{Kind: "generation", Fields: map[string]string{
		"generator":     strings.TrimSpace(generation.GeneratorVersion),
		"source":        strings.TrimSpace(generation.SourcePath),
		"source_hash":   strings.TrimSpace(generation.SourceSHA256),
		"profile":       strings.TrimSpace(generation.ProfilePath),
		"profile_hash":  strings.TrimSpace(generation.ProfileSHA256),
		"manifest_hash": strings.TrimSpace(generation.ManifestSHA256),
		"operation":     strings.TrimSpace(generation.OperationID),
		"permissions":   strings.Join(permissions, "; "),
		"fixture":       strings.TrimSpace(generation.FixtureID) + ":" + strings.TrimSpace(generation.FixtureStatus),
		"review":        strings.TrimSpace(generation.ReviewStatus),
	}}
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
