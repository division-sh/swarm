package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/providerconnectors"
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
	surfaces, err := providerconnectors.Surfaces(ctx, source, store)
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
	return fmt.Sprintf("provider connector tool %s %s %s requires %s",
		strings.TrimSpace(surface.ToolID),
		formatProviderConnectorSurfaceVerbs("CAN", surface.Can),
		formatProviderConnectorSurfaceVerbs("CANNOT", surface.Cannot),
		formatProviderConnectorRequirements(surface.Requires),
	)
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
		if requirement.Bound {
			status = "BOUND"
		}
		parts = append(parts, strings.TrimSpace(requirement.Name)+"="+status)
	}
	return strings.Join(parts, "; ")
}

func connectorFindingCode(toolID string) string {
	toolID = strings.TrimSpace(strings.ToLower(toolID))
	toolID = strings.NewReplacer(".", "_", "-", "_", " ", "_").Replace(toolID)
	return strings.Trim(toolID, "_")
}
