package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/providerconnectors"
	managedcredentialmodel "github.com/division-sh/swarm/internal/runtime/managedcredentials/model"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func appendProviderConnectorToolSurfaceFindings(ctx context.Context, report *localPreflightReport, source semanticview.Source) {
	if report == nil || source == nil {
		return
	}
	var err error
	source, err = providerconnectors.SourceWithConnectorPackImports(source)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_pack_import_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix provider connector pack imports")
		return
	}
	discovered, err := providerconnectors.Surfaces(ctx, source, nil)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_surface_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix provider connector tool declarations")
		return
	}
	if len(discovered) == 0 {
		return
	}
	store, err := buildProviderCredentialStore()
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_credential_store_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the local credential store used by swarm secrets")
		return
	}
	managedStore, err := buildManagedCredentialStore()
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_managed_credential_store_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the local managed credential store used by swarm connections")
		return
	}
	surfaces, err := providerconnectors.SurfacesWithOptions(ctx, source, providerconnectors.SurfaceOptions{
		StaticCredentials:  store,
		ManagedCredentials: managedStore,
	})
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_surface_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix provider connector tool declarations")
		return
	}
	sort.SliceStable(surfaces, func(i, j int) bool {
		return strings.TrimSpace(surfaces[i].ToolID) < strings.TrimSpace(surfaces[j].ToolID)
	})
	for _, surface := range surfaces {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_"+connectorFindingCode(surface.ToolID), localPreflightSeverityInfo, localPreflightStatusOK, providerConnectorSurfaceMessage(surface), "")
	}
}

func providerConnectorSurfaceMessage(surface providerconnectors.Surface) string {
	message := fmt.Sprintf("provider connector tool %s %s %s requires %s",
		strings.TrimSpace(surface.ToolID),
		formatProviderConnectorSurfaceVerbs("CAN", surface.Can),
		formatProviderConnectorSurfaceVerbs("CANNOT", surface.Cannot),
		formatProviderConnectorRequirements(surface.Requires),
	)
	if surface.Generation != nil {
		message += fmt.Sprintf(" generated operation=%s permissions=%s source=%s source_hash=%s profile=%s profile_hash=%s manifest_hash=%s fixture=%s:%s review=%s generator=%s",
			strings.TrimSpace(surface.Generation.OperationID),
			formatProviderConnectorGenerationPermissions(surface.Generation.Permissions),
			strings.TrimSpace(surface.Generation.SourcePath),
			strings.TrimSpace(surface.Generation.SourceSHA256),
			strings.TrimSpace(surface.Generation.ProfilePath),
			strings.TrimSpace(surface.Generation.ProfileSHA256),
			strings.TrimSpace(surface.Generation.ManifestSHA256),
			strings.TrimSpace(surface.Generation.FixtureID),
			strings.TrimSpace(surface.Generation.FixtureStatus),
			strings.TrimSpace(surface.Generation.ReviewStatus),
			strings.TrimSpace(surface.Generation.GeneratorVersion),
		)
	}
	return message
}

func formatProviderConnectorGenerationPermissions(permissions []providerconnectors.GenerationPermission) string {
	if len(permissions) == 0 {
		return "none"
	}
	items := make([]string, 0, len(permissions))
	for _, permission := range permissions {
		items = append(items, strings.TrimSpace(permission.ID)+":"+strings.TrimSpace(permission.Note))
	}
	return "[" + strings.Join(items, "; ") + "]"
}

func formatProviderConnectorSurfaceVerbs(verb string, items []string) string {
	if len(items) == 0 {
		return strings.TrimSpace(verb) + " none"
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		parts = append(parts, strings.TrimSpace(verb)+" "+item)
	}
	if len(parts) == 0 {
		return strings.TrimSpace(verb) + " none"
	}
	return strings.Join(parts, "; ")
}

func formatProviderConnectorRequirements(requirements []providerconnectors.RequirementStatus) string {
	if len(requirements) == 0 {
		return "none"
	}
	parts := make([]string, 0, len(requirements))
	for _, requirement := range requirements {
		status := "UNBOUND"
		if strings.TrimSpace(requirement.Status) != "" {
			status = strings.TrimSpace(requirement.Status)
		} else if requirement.Bound {
			status = "BOUND"
		}
		name := strings.TrimSpace(requirement.Name)
		if strings.TrimSpace(requirement.Kind) == "managed_credential" {
			name = "managed_credential:" + name
		}
		details := make([]string, 0, 3)
		if len(requirement.Scopes) > 0 {
			details = append(details, "scopes="+strings.Join(requirement.Scopes, ","))
		}
		if grantModel := strings.TrimSpace(requirement.GrantModel); grantModel != "" {
			details = append(details, "grant_model="+grantModel)
		}
		if summary := managedcredentialmodel.TokenRequestProfileSummary(requirement.TokenRequest); summary != managedcredentialmodel.TokenRequestProfileSummary(managedcredentialmodel.DefaultTokenRequestProfile()) {
			details = append(details, "token_request="+summary)
		}
		if len(details) > 0 {
			parts = append(parts, name+"="+status+"("+strings.Join(details, ";")+")")
			continue
		}
		parts = append(parts, name+"="+status)
	}
	return strings.Join(parts, "; ")
}

func connectorFindingCode(toolID string) string {
	toolID = strings.TrimSpace(strings.ToLower(toolID))
	toolID = strings.NewReplacer(".", "_", "-", "_", " ", "_").Replace(toolID)
	return strings.Trim(toolID, "_")
}
