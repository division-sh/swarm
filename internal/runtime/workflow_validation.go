package runtime

import (
	"context"
	"fmt"
	"strings"

	runtimeauthority "swarm/internal/runtime/authority"
	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimecredentials "swarm/internal/runtime/credentials"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
)

type WorkflowContractValidationOptions struct {
	Credentials       runtimecredentials.Store
	CheckMCPReachable bool
	StrictEmitSchemas bool
}

type WorkflowContractValidationResult struct {
	BootReport                  runtimebootverify.Report
	ToolImplementationWarnings  []error
	MissingEmitSchemaEventTypes []string
}

func DefaultWorkflowContractValidationOptions(credentials runtimecredentials.Store) WorkflowContractValidationOptions {
	return WorkflowContractValidationOptions{
		Credentials:       credentials,
		CheckMCPReachable: true,
		StrictEmitSchemas: runtimeEnvBool("SWARM_EMIT_SCHEMA_STRICT", true),
	}
}

// ValidateWorkflowContractSurface is the canonical verify/boot contract-validation entrypoint
// for prompt guards, bootverify errors, tool implementation validation, and explicit emit-schema coverage.
func ValidateWorkflowContractSurface(ctx context.Context, source semanticview.Source, opts WorkflowContractValidationOptions) (WorkflowContractValidationResult, error) {
	result := WorkflowContractValidationResult{}
	if source == nil {
		return result, fmt.Errorf("semantic source is required")
	}
	if bundle, ok := semanticview.Bundle(source); ok {
		if err := runtimecontracts.ValidatePromptSchemaGuardsForBundle(bundle); err != nil {
			return result, fmt.Errorf("validate prompt schema guards: %w", err)
		}
	}

	result.BootReport = runtimebootverify.Run(ctx, source, runtimebootverify.Options{
		Credentials:       opts.Credentials,
		CheckMCPReachable: opts.CheckMCPReachable,
	})
	if result.BootReport.HasErrors() {
		return result, fmt.Errorf("boot verification failed:\n%s", formatWorkflowValidationFindings(result.BootReport.Errors()))
	}

	warnings, err := runtimetools.ValidateToolImplementations(source)
	result.ToolImplementationWarnings = warnings
	if err != nil {
		return result, fmt.Errorf("tool implementation validation failed: %w", err)
	}

	emitRegistry := runtimetools.NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	result.MissingEmitSchemaEventTypes = emitRegistry.GeneratedEmitSchemasForAgentRoles()
	if opts.StrictEmitSchemas && len(result.MissingEmitSchemaEventTypes) > 0 {
		sample := result.MissingEmitSchemaEventTypes
		if len(sample) > 10 {
			sample = sample[:10]
		}
		return result, fmt.Errorf("emit schema strict mode enabled: %d agent-emitted schemas are missing explicit EventSchemaRegistry entries (sample: %s)", len(result.MissingEmitSchemaEventTypes), strings.Join(sample, ", "))
	}

	return result, nil
}

func formatWorkflowValidationFindings(findings []runtimebootverify.Finding) string {
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		lines = append(lines, fmt.Sprintf("%s [%s] %s", strings.TrimSpace(finding.CheckID), strings.TrimSpace(finding.Location), strings.TrimSpace(finding.Message)))
	}
	return strings.Join(lines, "\n")
}
