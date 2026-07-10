package main

import (
	"context"

	"github.com/division-sh/swarm/internal/providerconnectors"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func appendProviderConnectorCapabilitySubjects(ctx context.Context, report *localPreflightReport, source semanticview.Source) {
	if report == nil || source == nil {
		return
	}
	var err error
	source, err = providerconnectors.SourceWithConnectorPackImports(source)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_pack_import_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix provider connector pack imports")
		return
	}
	opts := providerconnectors.CapabilityOptions{Registry: providerconnectors.DefaultPackRegistry(), IncludeInstalled: true}
	if providerconnectors.HasEffectiveConnectors(source) {
		opts.StaticCredentials, err = buildProviderCredentialStore()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "provider_connector_credential_store_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the local credential store used by swarm secrets")
			return
		}
		opts.ManagedCredentials, err = buildManagedCredentialStore()
		if err != nil {
			report.add(localPreflightProviderPackPrerequisite, "provider_connector_managed_credential_store_unavailable", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix the local managed credential store used by swarm connections")
			return
		}
	}
	subjects, err := providerconnectors.CapabilitySubjects(ctx, source, opts)
	if err != nil {
		report.add(localPreflightProviderPackPrerequisite, "provider_connector_surface_failed", localPreflightSeverityBlocker, localPreflightStatusFailed, err.Error(), "fix provider connector tool declarations or credential readback")
		return
	}
	report.addCapabilitySubjects(subjects)
}

func connectorFindingCode(toolID string) string {
	return findingCode(toolID)
}
