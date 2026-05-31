package runtime

import (
	"context"
	"fmt"
	"strings"

	runtimeauthority "swarm/internal/runtime/authority"
	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimecredentials "swarm/internal/runtime/credentials"
	llmselection "swarm/internal/runtime/llm/selection"
	"swarm/internal/runtime/semanticview"
	runtimetools "swarm/internal/runtime/tools"
)

type WorkflowContractValidationOptions struct {
	Credentials                    runtimecredentials.Store
	CheckMCPReachable              bool
	StrictEmitSchemas              bool
	FatalToolImplementationWarning bool
	FatalBootWarnings              bool
	ExcludedFatalBootWarningChecks []string
	ValidateLLMModelResolution     bool
	LLMProfile                     llmselection.Profile
	ModelAliases                   llmselection.ModelAliases
}

type WorkflowContractValidationResult struct {
	BootReport                       runtimebootverify.Report
	ToolImplementationWarnings       []error
	MissingEmitSchemaEventTypes      []string
	GeneratedEmitSchemaErrors        []error
	GeneratedToolSchemaClosureErrors []error
}

func DefaultWorkflowContractValidationOptions(credentials runtimecredentials.Store) WorkflowContractValidationOptions {
	return WorkflowContractValidationOptions{
		Credentials:                    credentials,
		CheckMCPReachable:              true,
		StrictEmitSchemas:              runtimeEnvBool("SWARM_EMIT_SCHEMA_STRICT", true),
		FatalToolImplementationWarning: bootWarningsFatal(),
		FatalBootWarnings:              bootWarningsFatal(),
		ExcludedFatalBootWarningChecks: []string{"tool_resolution"},
	}
}

// ValidateWorkflowContractSurface is the canonical verify/boot contract-validation entrypoint
// for prompt guards, bootverify errors, tool implementation validation, and explicit emit-schema coverage.
func ValidateWorkflowContractSurface(ctx context.Context, source semanticview.Source, opts WorkflowContractValidationOptions) (WorkflowContractValidationResult, error) {
	result := WorkflowContractValidationResult{}
	if source == nil {
		return result, fmt.Errorf("semantic source is required")
	}

	result.BootReport = runtimebootverify.Run(ctx, source, runtimebootverify.Options{
		Credentials:             opts.Credentials,
		CheckMCPReachable:       opts.CheckMCPReachable,
		ValidateModelResolution: opts.ValidateLLMModelResolution,
		LLMProfile:              opts.LLMProfile,
		ModelAliases:            opts.ModelAliases,
	})
	if result.BootReport.HasErrors() {
		return result, fmt.Errorf("boot verification failed:\n%s", formatWorkflowValidationFindings(result.BootReport.Errors()))
	}
	if opts.FatalBootWarnings {
		warnings := filterWorkflowValidationFindings(result.BootReport.Warnings(), opts.ExcludedFatalBootWarningChecks...)
		if len(warnings) > 0 {
			return result, fmt.Errorf("boot verification warnings:\n%s", formatWorkflowValidationFindings(warnings))
		}
	}

	warnings, err := runtimetools.ValidateToolImplementations(source)
	result.ToolImplementationWarnings = warnings
	if err != nil {
		return result, fmt.Errorf("tool implementation validation failed: %w", err)
	}
	if opts.FatalToolImplementationWarning && len(warnings) > 0 {
		return result, fmt.Errorf("tool implementation warnings:\n%s", formatValidationErrors(warnings))
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
	result.GeneratedEmitSchemaErrors = runtimetools.ValidateGeneratedEmitToolSchemasForSource(source)
	if len(result.GeneratedEmitSchemaErrors) > 0 {
		return result, fmt.Errorf("generated emit tool schema validation failed:\n%s", formatValidationErrors(result.GeneratedEmitSchemaErrors))
	}
	result.GeneratedToolSchemaClosureErrors = runtimetools.ValidateGeneratedToolSchemaClosureForSource(source)
	if len(result.GeneratedToolSchemaClosureErrors) > 0 {
		return result, fmt.Errorf("generated tool schema closure validation failed:\n%s", formatValidationErrors(result.GeneratedToolSchemaClosureErrors))
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

func formatValidationErrors(errs []error) string {
	lines := make([]string, 0, len(errs))
	for _, err := range errs {
		if err == nil {
			continue
		}
		lines = append(lines, strings.TrimSpace(err.Error()))
	}
	return strings.Join(lines, "\n")
}

func filterWorkflowValidationFindings(findings []runtimebootverify.Finding, excludedCheckIDs ...string) []runtimebootverify.Finding {
	if len(findings) == 0 {
		return nil
	}
	excluded := make(map[string]struct{}, len(excludedCheckIDs))
	for _, checkID := range excludedCheckIDs {
		checkID = strings.TrimSpace(checkID)
		if checkID != "" {
			excluded[checkID] = struct{}{}
		}
	}
	out := make([]runtimebootverify.Finding, 0, len(findings))
	for _, finding := range findings {
		if _, skip := excluded[strings.TrimSpace(finding.CheckID)]; skip {
			continue
		}
		out = append(out, finding)
	}
	return out
}
