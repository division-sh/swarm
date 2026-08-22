package cliapp

import (
	"context"

	"github.com/division-sh/swarm/internal/packadmission"
	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func appendProviderConnectorCapabilitySubjects(ctx context.Context, report *LocalPreflightReport, source semanticview.Source) {
	if report == nil || source == nil {
		return
	}
	bundle, ok := semanticview.Bundle(source)
	if !ok || bundle == nil || bundle.PackInventory == nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_surface_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, "bundle-specific effective pack inventory is required", "fix the selected contract source")
		return
	}
	projection, err := packadmission.FromBundle(bundle)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_surface_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the selected platform or project pack inventory")
		return
	}
	opts := providerconnectors.CapabilityOptions{Registry: projection.ProviderConnectors, IncludeInstalled: true}
	if providerconnectors.HasEffectiveConnectors(source) {
		opts.StaticCredentials, err = BuildProviderCredentialStore()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "provider_connector_credential_store_unavailable", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the local credential store used by swarm secrets")
			return
		}
		opts.ManagedCredentials, err = BuildManagedCredentialStore()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "provider_connector_managed_credential_store_unavailable", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix the local managed credential store used by swarm connections")
			return
		}
	}
	subjects, err := providerconnectors.CapabilitySubjects(ctx, source, opts)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_surface_failed", LocalPreflightSeverityBlocker, LocalPreflightStatusFailed, err.Error(), "fix provider connector tool declarations or credential readback")
		return
	}
	report.addCapabilitySubjects(subjects)
}

func connectorFindingCode(toolID string) string {
	return findingCode(toolID)
}
